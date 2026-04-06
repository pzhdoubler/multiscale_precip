import os
import xarray as xr
import pandas as pd
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
    except:
        print(f"Skipping bad file: {f}")

test = netCDF4.Dataset(files[0], mode="r")

print(test)

print(test.variables["precipitation"][:])
print(test.variables["time"][:])
