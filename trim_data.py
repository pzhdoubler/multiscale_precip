import os
import numpy as np
import xarray as xr
from multiprocessing import Pool
from collections import defaultdict

def slice_mswep_with_xu_wrf(ds_coords, file, save):
    ##### GET MSWEP LAT/LON SLICE
    lon_buffer = 0.2
    lat_buffer = 0.2
    pr_lat_min = np.min(ds_coords["lat"].values) - lat_buffer
    pr_lat_max = np.max(ds_coords["lat"].values) + lat_buffer
    pr_lon_min = np.min(ds_coords["lon"].values) - lon_buffer
    pr_lon_max = np.max(ds_coords["lon"].values) + lon_buffer

    ds = xr.open_dataset(file)
    subset = ds.sel(lat=slice(pr_lat_max, pr_lat_min), lon=slice(pr_lon_min, pr_lon_max))
    subset.to_netcdf(save)

##########################
######## MSWEP #############
##########################

# mswep_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/hourly/"
# mswep_save = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/"

# xu_wrf = xr.open_dataset("/ocean/projects/ees210011p/hdoubler/AOSC650/xu_wrf_coordinates.nc")
# mswep = os.listdir(mswep_loc)

# mswep_tasks = [(xu_wrf, os.path.join(mswep_loc, f), os.path.join(mswep_save, f)) for f in mswep]

# with Pool() as pool:
#     done = pool.starmap(slice_mswep_with_xu_wrf, mswep_tasks)
#     print("Done.")

pr_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/"
out_dir = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_yearly/"

os.makedirs(out_dir, exist_ok=True)

# -------------------------
# 1. Group files by year
# -------------------------
files_by_year = defaultdict(list)

for fname in os.listdir(pr_loc):
    if not fname.endswith(".nc"):
        continue

    year = int(fname[:4])  # YYYY from YYYYJJJ.HH.nc
    files_by_year[year].append(os.path.join(pr_loc, fname))

# -------------------------
# 2. Process each year
# -------------------------
for year, files in sorted(files_by_year.items()):
    print(f"\nProcessing year {year} ({len(files)} files)")

    files = sorted(files)  # ensure chronological order

    batch_size = 549

    datasets = []
    for i in range(0, len(files), batch_size):
        batch = files[i:i+batch_size]
        ds_batch = xr.open_mfdataset(
            batch,
            combine="nested",
            concat_dim="time",
            coords="minimal",
            compat="override",
            parallel=True,
            chunks={"time": 24}
        )
        datasets.append(ds_batch)

    ds = xr.concat(datasets, dim="time")

    # -------------------------
    # 3. Write to NetCDF
    # -------------------------
    outfile = os.path.join(out_dir, f"pr_{year}.nc")

    encoding = {
        var: {"zlib": True, "complevel": 4}
        for var in ds.data_vars
    }

    print(f"Writing {outfile}")
    ds.to_netcdf(outfile, encoding=encoding)

    ds.close()

##########################
######## ERA5 #############
##########################

# era5_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/era5/"
# era5_save = "/ocean/projects/ees210011p/hdoubler/AOSC650/era5/coarse/"
# files = ["gp-500mb_2019-2021.grib", "gp-500mb_2-2.grib", "gp-500mb_2-3.grib"]

# for file in files:
#     print(f"Working on {file} ...")

#     # first coarsen the data
#     ds = xr.open_dataset(os.path.join(era5_loc, file), engine="cfgrib")

#     ds = ds.sortby('latitude')

#     weights = np.cos(np.deg2rad(ds['latitude']))

#     weights_2d = weights.broadcast_like(ds)

#     num = (ds * weights_2d).coarsen(latitude=4, longitude=4, boundary='trim').sum()
#     den = weights_2d.coarsen(latitude=4, longitude=4, boundary='trim').sum()

#     ds_coarse = num / den

#     # Group by year
#     for year, ds_year in ds_coarse.groupby('time.year'):
#         outfile = os.path.join(era5_save, f"gp_500mb_{year}.nc")
        
#         print(f"Writing {outfile}")
#         ds_year.to_netcdf(outfile)