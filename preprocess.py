import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
import os

def make_gp_anomalies(ds, name, method="monthly"):
    # simple monthly clim
    if method == "monthly":
        climatology = ds.groupby("time.month").mean("time")

    anomalies = ds.groupby("time.month") - climatology

    gp_save = f"/ocean/projects/ees210011p/hdoubler/AOSC650/era5/anoms/gp-500mb_{name}_clim-{method}.nc"
    print(f"Saving {gp_save}")
    anomalies.to_netcdf(gp_save)

gp_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/era5/coarse/"
files = ["gp_500mb_2020.nc", "gp_500mb_2021.nc", "gp_500mb_2022.nc", "gp_500mb_2023.nc"]
name = "2020-2023"

print("Opening data ...")
ds = xr.open_mfdataset([os.path.join(gp_loc, f) for f in files])

print("Calculating anoms ...")
make_gp_anomalies(ds, name, method="monthly")