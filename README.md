Scripts to recalculate distribution portions for CS2KZ API.

### calc_fractions.py

Script to calculate distribution portions of a run based on input data read from stdin. Must be provided with a JSON object per line.

Input example:
```
{ "time": 8.609375, "nub_data": { "tier": 1, "wr": 7.6484375, "leaderboard_size": 224, "dist_params": { "a": 33.53900289787477, "b": 33.52140111667502, "loc": 6.3663207368487065, "scale": 0.4480388195262859, "top_scale": 0.9979285278452101 } }, "pro_data": { "tier": 1, "wr": 7.6484375, "leaderboard_size": 165, "dist_params": { "a": 2.6294814553333743, "b": 2.511121972118702, "loc": 8.713014153227697, "scale": 2.2226724397990805, "top_scale": 0.9952929135343108 }}}
```

Output example:
```
{"nub_fraction": 0.9745534941686896, "pro_fraction": 0.9760910013054752}
```

### calc_filter.py

Script to recalculate distribution portions for all personal bests of a certain filter in the database. Connects to the CS2KZ API's MariaDB database specified in the script, update all personal bests with newly calculated distribution portions and update the distribution parameters for each filter based on the personal bests. 

Input example:
```
{ "filter_id": 74 }
```
Output example:
```
{"filter_id": 74, "timings": {"db_query_ms": 7.99870491027832, "nub_fit_ms": 73.8677978515625, "nub_compute_ms": 195.1158046722412, "pro_fit_ms": 19.689559936523438, "pro_compute_ms": 213.7751579284668, "db_write_ms": 138.31448554992676}}
```
(Note that the output doesn't really matter here, it's just for logging purposes.)
