import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import keras
from keras import layers
from minisom import MiniSom

def make_gp_anomalies(ds, method="monthly"):
    # simple monthly clim
    if method == "monthly":
        climatology = ds.groupby("time.month").mean("time")

    anomalies = ds.groupby("time.month") - climatology

    return anomalies

########################
###### PREPROCESS ######
########################

# gp open and anom calc
gp_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/era5/coarse/"
files = ["gp_500mb_2020.nc", "gp_500mb_2021.nc", "gp_500mb_2022.nc", "gp_500mb_2023.nc"]

print("Opening gp data ...")
ds = xr.open_mfdataset([os.path.join(gp_loc, f) for f in files])

print("Calculating gp anoms ...")
gp_anoms = make_gp_anomalies(ds, method="monthly")

# determine precip days
df = pd.read_csv("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_precip_stats2.csv", parse_dates=["time"]).set_index("time")
filtered_df = df[(df["drizzle exceedance"] > 0.05)]

times = filtered_df.index
gp_subset = gp_anoms.sel(time=times)
print(gp_subset)