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

files = sorted([os.path.join(pr_loc, f) for f in os.listdir(pr_loc)])

ds_all = xr.open_mfdataset(
    files,
    combine="nested",
    concat_dim="time",
    parallel=True,
    coords="minimal",
    compat="override",
    chunks={"time": 100}
)

ds_all.to_zarr("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed.zarr", mode="w")

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