import json
import sys
import scipy.stats as stats
import common
import sys
"""
Compute nub/pro points fractions based on input data read from stdin.
Input example: { "time": 8.609375, "nub_data": { "tier": 1, "wr": 7.6484375, "leaderboard_size": 224, "dist_params": { "a": 33.53900289787477, "b": 33.52140111667502, "loc": 6.3663207368487065, "scale": 0.4480388195262859, "top_scale": 0.9979285278452101 } }, "pro_data": { "tier": 1, "wr": 7.6484375, "leaderboard_size": 165, "dist_params": { "a": 2.6294814553333743, "b": 2.511121972118702, "loc": 8.713014153227697, "scale": 2.2226724397990805, "top_scale": 0.9952929135343108 }}}
Beautified input example:
{
	"time": 8.609375,
	"nub_data": {
		"tier": 1,
		"wr": 7.6484375,
		"leaderboard_size": 224,
		"dist_params": {
			"a": 33.53900289787477,
			"b": 33.52140111667502,
			"loc": 6.3663207368487065,
			"scale": 0.4480388195262859,
			"top_scale": 0.9979285278452101
		}
	}
	"pro_data": {
		"tier": 1,
		"wr": 7.6484375,
		"leaderboard_size": 165,
		"dist_params": {
			"a": 2.6294814553333743,
			"b": 2.511121972118702,
			"loc": 8.713014153227697,
			"scale": 2.2226724397990805,
			"top_scale": 0.9952929135343108
		}
	}
}
Output format:
{"nub_fraction": 0.9745534941686896, "pro_fraction": 0.9760910013054752}
"""

def process_input(line):
	# Convert line to json
	data = json.loads(line)

	# Process nub/overall points
	try: 
		nub_data = data['nub_data']
		nub_dist = stats.norminvgauss(
			a = nub_data['dist_params']['a'],
			b = nub_data['dist_params']['b'],
			loc = nub_data['dist_params']['loc'],
			scale = nub_data['dist_params']['scale']
		)
		nub_fraction = common.get_dist_points_portion(data['time'],
			nub_data['wr'],
			nub_dist,
			nub_data['tier'],
			nub_data['dist_params']['top_scale'],
			nub_data['leaderboard_size'])
		if 'pro_data' in data:
			# Process pro points
			pro_data = data['pro_data']
			pro_dist = stats.norminvgauss(
				a = pro_data['dist_params']['a'],
				b = pro_data['dist_params']['b'],
				loc = pro_data['dist_params']['loc'],
				scale = pro_data['dist_params']['scale']
			)
			pro_fraction = common.get_dist_points_portion(data['time'],
				pro_data['wr'],
				pro_dist,
				pro_data['tier'],
				pro_data['dist_params']['top_scale'],
				pro_data['leaderboard_size'])
			response = {
				"nub_fraction": nub_fraction,
				# Pro run in the pro leaderboard should never be worth less than the same run in the nub leaderboard.
				"pro_fraction": pro_fraction if pro_fraction > nub_fraction else nub_fraction
			}
			sys.stdout.write(json.dumps(response) + '\n')
			return
		response = {
			"nub_fraction": nub_fraction,
			"pro_fraction": None
		}
		sys.stdout.write(json.dumps(response) + '\n')
	except KeyError as e:
		error = {"error": f"Missing key in input data: {e}"}
		sys.stderr.write(json.dumps(error) + '\n')
		return
	except json.JSONDecodeError as e:
		error = {"error": f"JSON decode error: {e}"}
		sys.stderr.write(json.dumps(error) + '\n')
		return
	except Exception as e:
		error = {"error": f"An unexpected error occurred: {e}"}
		sys.stderr.write(json.dumps(error) + '\n')
		return

def main():
	for line in sys.stdin:
		process_input(line)

if __name__ == "__main__":
	main()