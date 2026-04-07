import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
import os
from minisom import MiniSom

def make_gp_anomalies(ds, method="monthly"):
    # simple monthly clim
    if method == "monthly":
        climatology = ds.groupby("time.month").mean("time")

    anomalies = ds.groupby("time.month") - climatology

    return anomalies

def min_max_norm(ds):
    ds_max = np.max(ds)
    ds_min = np.min(ds)
    return (ds - ds_min) / (ds_max - ds_min)

def log_transform(ds):
    return np.log(ds + 1.0)

########################
###### INTAKE ######
########################

# gp open and anom calc
gp_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/era5/coarse/"
gp_files = ["gp_500mb_2020.nc", "gp_500mb_2021.nc", "gp_500mb_2022.nc", "gp_500mb_2023.nc"]

print("Opening gp data ...")
ds = xr.open_mfdataset([os.path.join(gp_loc, f) for f in gp_files])

print("Calculating gp anoms ...")
gp_anoms = make_gp_anomalies(ds, method="monthly")
# capture average before min/max transform

# determine precip days
df = pd.read_csv("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_precip_stats2.csv", parse_dates=["time"]).set_index("time")
filtered_df = df[(df["drizzle exceedance"] > 0.05)]
times = filtered_df.index

# select gp_subset
gp_subset = gp_anoms.sel(time=times)["z"].chunk({"time": len(times)})
gp_subset_avg = np.average(gp_subset)

# open pr_subset
pr_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_annual/"
pr_files = os.listdir(pr_loc)

print("Opening pr data ...")
ds = xr.open_mfdataset([os.path.join(pr_loc, f) for f in pr_files])

pr_subset = ds.sel(time=times)["precipitation"]

print("Intake Done.")

########################
###### PREPROCESS ######
########################

gp_scaled = gp_subset.stack(features=("latitude", "longitude"))
pr_scaled = pr_subset.stack(features=("lat", "lon"))

print(gp_scaled)
print(pr_scaled)

print("Doing gp Scaling")
gp_scaled = min_max_norm(gp_scaled).values

print("Doing pr Scaling")
pr_scaled = log_transform(pr_scaled)
pr_scaled = min_max_norm(pr_scaled).values

print("Preprocess Done.")

########################
###### TRAIN ######
########################

# set som lattice dimensions (i.e., number of nodes)
som_grid_rows = 3
som_grid_columns = 3

# number of weights per node
input_length = pr_scaled.shape[1]

# Spread of the neighborhood function, needs to be adequate to the dimensions
# of the map. (rows/columns minus 1 usually)
sigma = 2.0

# initial learning rate (at the iteration t we have learning_rate(t) =
# learning_rate / (1 + t/T) where T is #num_iteration/2)
learning_rate = 0.5

# Function that reduces learning_rate at each iteration (i.e., epochs)
# Possible values: 'inverse_decay_to_zero', 'linear_decay_to_zero', 'asymptotic_decay'
# 'inverse_decay_to_zero' == > C = max_iter / 100.0; where, learning_rate * C / (C + t)
# 'linear_decay_to_zero' == > learning_rate * (1 - t / max_iter)
# 'asymptotic_decay' == > dynamic_parameter / (1 + t / (max_iter / 2))
# ^^Decay function of the learning process and sigma that decays these values asymptotically to 1/3 of their original values.
decay_function='inverse_decay_to_zero'

# Function that reduces sigma at each iteration.
# Possible values: 'inverse_decay_to_one', 'linear_decay_to_one', 'asymptotic_decay'
# 'inverse_decay_to_one' == > C = (sigma - 1) / max_iter; where, sigma / (1 + (t * C))
# 'linear_decay_to_one' == > sigma + (t * (1 - sigma) / max_iter)
sigma_decay_function='inverse_decay_to_one'

# Function that weights the neighborhood of a position in the map.
# Possible values: 'gaussian', 'mexican_hat', 'bubble', 'triangle',
# which takes in sigma.
neighborhood_function = 'gaussian'

# Topology of the map; Possible values: 'rectangular', 'hexagonal'
topology = 'rectangular'

# Distance used to activate the map; Possible values: 'euclidean', 'cosine', 'manhattan', 'chebyshev'
activation_distance = 'euclidean'

# Random seed to use for reproducibility. Using 1.
random_seed = 1