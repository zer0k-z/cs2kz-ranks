
import numpy as np
from scipy import stats

def get_distribution_points_portion_under_50(time, wr_time, tier):
    return ((1+np.exp((2.1 - 0.25 * tier) * -0.5))/(1+np.exp((2.1 - 0.25 * tier) * (time/wr_time-1.5))))

def get_dist_points_portion(time, wr_time, dist: stats.rv_continuous, tier, top_scale, total):
    if total < 50:
        return get_distribution_points_portion_under_50(time, wr_time, tier)
    else:
        return (dist.sf(time)) / top_scale