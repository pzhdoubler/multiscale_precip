import os
import xarray as xr
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime, timedelta
from collections import defaultdict
import netCDF4
import cftime
import time

# ======================
# USER SETTINGS
# ======================
input_dir = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed"
output_dir = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_annual"
var_name = "precipitation"   # change if needed

os.makedirs(output_dir, exist_ok=True)

# ======================
# GET FILE LIST
# ======================
files = sorted(glob(os.path.join(input_dir, "*.nc")))

print(f"Total files found: {len(files)}")

def get_datetime(f):
    return datetime.strptime(f.split("/")[-1] ,"%Y%j.%H.nc")

files_by_year = defaultdict(list)

for f in files:
    try:
        y = get_datetime(f).year
        files_by_year[y].append(f)
    except:
        print(f"Skipping bad file: {f}")

print(f"Years: {files_by_year.keys()}")

# test set
files_by_year[2020] = files[0:20]

# see what original xarray looks like
# test = xr.open_dataset(files_by_year[2020][0])
# print(test)

for yr, flist in files_by_year.items():
    test = netCDF4.Dataset(flist[-1], mode="r")

    lon_var = test.variables["lon"]
    lat_var = test.variables["lat"]
    pr_var = test.variables["precipitation"]
    time_var = test.variables["time"]

    # allocate for times and pr
    times = np.zeros((len(flist)), dtype=cftime.datetime)
    pr = np.zeros((len(flist), lat_var.size, lon_var.size), dtype=np.float32)

    s = time.time()
    init = True

    for f, file in enumerate(flist):
        ds = netCDF4.Dataset(file, mode="r")
        times[f] = get_datetime(file)
        pr[f] = ds.variables["precipitation"][:][0]
        ds.close()
        if init:
            e = time.time()
            init = False
            print(f"One file took {e-s} seconds")
            print(f"Year {yr} will take {(e-s)*len(flist)} seconds")
        if f == len(flist) // 2:
            h = time.time()
            print(f"Halfway, took {h-s} seconds")
    
    xr_ds = xr.Dataset(
        data_vars=dict(
            precipitation=(["time", "lat", "lon"], pr)
        ),
        coords=dict(
            lon=("lon", lon_var[:]),
            lat=("lat", lat_var[:]),
            time=("time", times)
        )
    )
    xr_ds["precipitation"].attrs["units"] = "mm/hr"

    print(f"Saving pr_{yr}.nc")
    print()
    xr_ds.to_netcdf(os.path.join(output_dir, f"pr_{yr}.nc"))
