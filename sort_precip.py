import os
import xarray as xr
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime
from collections import defaultdict
import netCDF4

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
        print(f"Processed year {y} ...")
    except:
        print(f"Skipping bad file: {f}")


for yr, flist in files_by_year.items():
    test = netCDF4.Dataset(flist[0], mode="r")

    lon_var = test.variables["lon"]
    lat_var = test.variables["lat"]
    pr_var = test.variables["precipitation"]
    time_var = test.variables["time"]

    # pr_attrs = {attr : pr_var.getncattr(attr) for attr in pr_var.ncattrs()}

    # allocate for times and pr
    times = np.zeros((len(flist)))
    pr = np.zeros((len(flist), lat_var.size, lon_var.size))

    for f, file in enumerate(flist):
        ds = netCDF4.Dataset(file, mode="r")
        times[f] = ds.variables["time"][:][0]
        pr[f] = ds.variables["precipitation"][:][0]
        ds.close()
        break
    
    print(pr)
    break