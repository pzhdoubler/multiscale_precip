import os
import xarray as xr
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime
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

def extract_year(f):
    t = datetime.strptime(f.split("/")[-1] ,"%Y%j.%H.nc")
    return t.year

files_by_year = defaultdict(list)

for f in files:
    try:
        y = extract_year(f)
        files_by_year[y].append(f)
    except:
        print(f"Skipping bad file: {f}")

print(f"Years: {files_by_year.keys()}")

# test set
files_by_year[2020] = files[0:20]

# see what original xarray looks like
test = xr.open_dataset(files_by_year[2020][0])
print(test)

for yr, flist in files_by_year.items():
    test = netCDF4.Dataset(flist[0], mode="r")

    lon_var = test.variables["lon"]
    lat_var = test.variables["lat"]
    pr_var = test.variables["precipitation"]
    time_var = test.variables["time"]

    # pr_attrs = {attr : pr_var.getncattr(attr) for attr in pr_var.ncattrs()}

    # allocate for times and pr
    times = np.zeros((len(flist)), dtype=cftime.datetime)
    pr = np.zeros((len(flist), lat_var.size, lon_var.size), dtype=np.float32)

    s = time.time()
    init = True

    for f, file in enumerate(flist):
        ds = netCDF4.Dataset(file, mode="r")
        times[f] = netCDF4.num2date(ds.variables["time"][:], units=time_var.units, calendar=getattr(time_var, 'calendar', 'standard'))[0]
        pr[f] = ds.variables["precipitation"][:][0]
        np.sum(pr[f])
        ds.close()
        if init:
            e = time.time()
            init = False
            print(f"One file took {e-s} seconds")
            print(f"Year {yr} will take {(e-s)*len(flist)} seconds")
    
    xr_ds = xr.Dataset(
        data_vars=dict(
            precipitation=(["time", "lat", "lon"], pr)
        ),
        coords=dict(
            lon=("lon", lon_var[:]),
            lat=("lat", lat_var[:]),
            time=("time", times.to_datetimeindex(time_unit="ns"))
        )
    )

    print(xr_ds)

    print(f"Saving pr_{yr}.nc")
    xr_ds.to_netcdf(os.path.join(output_dir, f"pr_{yr}.nc"))

    break
