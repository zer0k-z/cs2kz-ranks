import numpy as np
import common
import mariadb
from urllib.parse import urlparse
import os
import sys
import json
from scipy import stats
from typing import Any, List, Tuple
import time
""" 
Recalculate the distribution points fraction for nub and pro leaderboards of given a filter ID.
Input example: { "filter_id": 74 }
"""
DATABASE_URL = os.getenv("DATABASE_URL", "mysql://schnose:csgo-kz-is-dead-boys@localhost:3306/cs2kz")
conn = None
def get_db_connection():
	try:
		parsed = urlparse(DATABASE_URL)
		conn = mariadb.connect(
			user = parsed.username,
			password = parsed.password,
			host = parsed.hostname,
			port = parsed.port or 3306,
			database = parsed.path.lstrip('/'),
			reconnect = True
		)
		return conn
	except mariadb.Error as e:
		print(f"Error connecting to MariaDB Platform: {e}")
		return None

# See cs2kz-api/crates/cs2kz/migrations/0001_initial.up.sql for database schema.
def process_input(line):
	global conn
	cursor = conn.cursor()
	timings = {}
	
	# Convert line to json
	try:
		data = json.loads(line)
		# Get filter ID
		filter_id = data['filter_id']
		
		# Fetch all best nub records for the filter to recalculate the distribution parameters
		# Database query timing
		start = time.time()
		cursor.execute("SELECT bnr.record_id, bnr.time, bnr.points FROM BestNubRecords bnr WHERE bnr.filter_id = ? ORDER BY bnr.time ASC", (filter_id,))
		nub_records: List[Tuple[Any, float, float]] = cursor.fetchall()
		cursor.execute("SELECT bpr.record_id, bpr.time, bpr.points FROM BestProRecords bpr WHERE bpr.filter_id = ? ORDER BY bpr.time ASC", (filter_id,))
		pro_records: List[Tuple[Any, float, float]] = cursor.fetchall()
		cursor.execute("SELECT cf.nub_tier, cf.pro_tier FROM CourseFilters cf WHERE cf.id = ?", (filter_id,))
		filter_row = cursor.fetchone()
		
		# Fetch previous distribution parameters for warm start (both nub and pro in one query)
		cursor.execute(
		"""
			SELECT is_pro_leaderboard, a, b, loc, scale 
			FROM PointDistributionData 
			WHERE filter_id = ? 
			ORDER BY is_pro_leaderboard
		""", (filter_id,))
		dist_params_rows = cursor.fetchall()
		
		prev_nub_params = None
		prev_pro_params = None
		for row in dist_params_rows:
			if row[0] == 0:  # is_pro_leaderboard = 0 (nub)
				prev_nub_params = (row[1], row[2], row[3], row[4])
			elif row[0] == 1:  # is_pro_leaderboard = 1 (pro)
				prev_pro_params = (row[1], row[2], row[3], row[4])
		
		timings['db_query_ms'] = (time.time() - start) * 1000
		
		if filter_row is None:
			warning = {"warning": f"Filter ID {filter_id} not found in CourseFilters."}
			sys.stderr.write(json.dumps(warning) + '\n')
			return
		
		nub_times = [row[1] for row in nub_records]
		pro_times = [row[1] for row in pro_records]
		nub_tier = filter_row[0]
		pro_tier = filter_row[1]
		
		'''
		There are 3 possible cases:
		1. Less than 50 nub times (and therefore less than 50 pro times as well) -> do not fit distribution, use sigmoid function
		2. 50 or more nub times but less than 50 pro times -> fit nub distribution, use sigmoid for pro
		3. 50 or more nub times and 50 or more pro times -> fit both distributions
		
		Overall/nub portion only depends on its own distribution.
		Pro portion takes the higher of the two distributions ((un)fitted nub or (un)fitted pro) to avoid situations where pro portion is lower than nub portion.
		
		'''
		# Fit nub distribution
		if len(nub_times) >= 50:
			start = time.time()
			nub_dist, nub_params = refit_dist(nub_times, prev_nub_params)
			timings['nub_fit_ms'] = (time.time() - start) * 1000
		elif len(nub_times) > 0:
			nub_dist, nub_params = None, (0,0,0,0,0)
			timings['nub_fit_ms'] = 0
		else:
			error = {"error": f"No overall records found for filter ID {filter_id}."}
			sys.stderr.write(json.dumps(error) + '\n')
			return
	
		# Compute nub fractions
		start = time.time()
		nub_times_array = np.array(nub_times)
		new_fractions = common.get_dist_points_portion(
			nub_times_array,
			nub_times[0],
			nub_dist,
			nub_tier,
			nub_params[4],
			len(nub_times)
		)
		nub_records = [(record_id, time, fraction) for (record_id, time, _), fraction in zip(nub_records, new_fractions)]
		timings['nub_compute_ms'] = (time.time() - start) * 1000

		# Fit pro distribution
		if len(pro_times) >= 50:
			start = time.time()
			pro_dist, pro_params = refit_dist(pro_times, prev_pro_params)
			timings['pro_fit_ms'] = (time.time() - start) * 1000
		elif len(pro_times) > 0:
			pro_dist, pro_params = None, (0,0,0,0,0)
			timings['pro_fit_ms'] = 0
		else:
			# No pro records - just skip
			timings['pro_fit_ms'] = 0

		# Compute pro fractions if there are any pro records
		if len(pro_times) > 0:
			start = time.time()
			pro_times_array = np.array(pro_times)
			new_fractions = np.maximum(common.get_dist_points_portion(pro_times_array, pro_times[0], pro_dist, pro_tier, pro_params[4], len(pro_times)),
								common.get_dist_points_portion(pro_times_array, nub_times[0], nub_dist, nub_tier, nub_params[4], len(nub_times)))
			pro_records = [(record_id, time, fraction) for (record_id, time, _), fraction in zip(pro_records, new_fractions)]
			timings['pro_compute_ms'] = (time.time() - start) * 1000
		else:
			timings['pro_compute_ms'] = 0

		# Database write timing
		start = time.time()
		if len(nub_records) > 0:
			cursor.executemany(
				"UPDATE BestNubRecords SET points = ? WHERE record_id = ?",
				[(points, record_id) for record_id, time, points in nub_records]
			)
		if len(pro_records) > 0:
			cursor.executemany(
				"UPDATE BestProRecords SET points = ? WHERE record_id = ?",
				[(points, record_id) for record_id, time, points in pro_records]
			)
		if len(nub_times) >= 50:
			cursor.execute("""
					INSERT INTO PointDistributionData (filter_id, is_pro_leaderboard, a, b, loc, scale, top_scale) 
					VALUES (?, 0, ?, ?, ?, ?, ?)
					ON DUPLICATE KEY 
					UPDATE a=VALUES(a), b=VALUES(b), loc=VALUES(loc), scale=VALUES(scale), top_scale=VALUES(top_scale)
				""",
				(filter_id, nub_params[0], nub_params[1], nub_params[2], nub_params[3], nub_params[4]))
		if len(pro_times) >= 50:
			cursor.execute("""
					INSERT INTO PointDistributionData (filter_id, is_pro_leaderboard, a, b, loc, scale, top_scale) 
					VALUES (?, 1, ?, ?, ?, ?, ?)
					ON DUPLICATE KEY 
					UPDATE a=VALUES(a), b=VALUES(b), loc=VALUES(loc), scale=VALUES(scale), top_scale=VALUES(top_scale)
				""",
				(filter_id, pro_params[0], pro_params[1], pro_params[2], pro_params[3], pro_params[4]))
		conn.commit()
		timings['db_write_ms'] = (time.time() - start) * 1000
		timings['total_ms'] = sum(timings.values())
		# Print timing summary
		sys.stdout.write(json.dumps({"filter_id": filter_id, "timings": timings}) + '\n')
	except mariadb.Error as e:
		error = {"error": f"Database error: {e}"}
		sys.stderr.write(json.dumps(error) + '\n')
		return
	except json.JSONDecodeError as e:
		error = {"error": f"Invalid JSON input: {e}"}
		sys.stderr.write(json.dumps(error) + '\n')
		return
	except KeyError as e:
		error = {"error": f"Missing key in input data: {e}"}
		sys.stderr.write(json.dumps(error) + '\n')
		return
	except Exception as e:
		error = {"error": f"An unexpected error occurred: {e}"}
		sys.stderr.write(json.dumps(error) + '\n')
		return


def refit_dist(times, prev_params=None):
	if prev_params is not None:
		# Use previous parameters as initial guess for faster convergence
		a_init, b_init, loc_init, scale_init = prev_params
		norminvgauss_params = stats.norminvgauss.fit(
			times,
			a_init, b_init,
			loc=loc_init,
			scale=scale_init
		)
	else:
		# Cold start - no initial parameters
		norminvgauss_params = stats.norminvgauss.fit(times)
	
	norminvgauss_dist = stats.norminvgauss(*norminvgauss_params)
	top_scale = norminvgauss_dist.sf(times[0])
	a, b, loc, scale = norminvgauss_params
	return norminvgauss_dist, (a, b, loc, scale, top_scale)

if __name__ == "__main__":
	conn = get_db_connection()
	if conn is None:
		exit(1)

	# Read from stdin line by line
	for line in sys.stdin:
		process_input(line)

	conn.close()