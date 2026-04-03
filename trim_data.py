import os
import numpy as np
import xarray as xr
from multiprocessing import Pool

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


mswep_loc = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/hourly/"
mswep_save = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/"

xu_wrf = xr.open_dataset("/ocean/projects/ees210011p/hdoubler/AOSC650/xu_wrf_coordinates.nc")
mswep = os.listdir(mswep_loc)

mswep_tasks = [(xu_wrf, os.path.join(mswep_loc, f), os.path.join(mswep_save, f)) for f in mswep]

with Pool() as pool:
    done = pool.starmap(slice_mswep_with_xu_wrf, mswep_tasks)
    print("Done.")

