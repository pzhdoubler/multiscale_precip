import os
import numpy as np
import xarray as xr
import time

def slice_mswep_with_xu_wrf(coords_loc, mswep_loc, mswep_save_loc):
    ##### GET MSWEP LAT/LON SLICE
    ds_coords = xr.open_dataset(coords_loc + "xu_wrf_coordinates.nc")

    lon_buffer = 0.2
    lat_buffer = 0.2
    pr_lat_min = np.min(ds_coords["lat"].values) - lat_buffer
    pr_lat_max = np.max(ds_coords["lat"].values) + lat_buffer
    pr_lon_min = np.min(ds_coords["lon"].values) - lon_buffer
    pr_lon_max = np.max(ds_coords["lon"].values) + lon_buffer

    # xu_lat_min = np.min(ds_coords["lat"].values)
    # xu_lat_max = np.max(ds_coords["lat"].values)
    # xu_lon_min = np.min(ds_coords["lon"].values)
    # xu_lon_max = np.max(ds_coords["lon"].values)

    # print("LATS:", xu_lat_min, xu_lat_max)
    # print("LONS:", xu_lon_min, xu_lon_max)

    # print("BUFF LATS:", pr_lat_min, pr_lat_max)
    # print("BUFF LONS:", pr_lon_min, pr_lon_max)

    # list mswep files
    mswep = os.listdir(mswep_loc)
    init = True

    ds = xr.open_mfdataset(
        os.path.join(mswep_loc, "*.nc"),
        combine="by_coords",
        parallel=True,
        chunks={"lat": 200, "lon": 200}
    )

    subset = ds.sel(
        lat=slice(pr_lat_max, pr_lat_min),
        lon=slice(pr_lon_min, pr_lon_max)
    )

    subset.to_netcdf("combined_subset.nc")

    # for file in mswep:
    #     s = time.time()
    #     ds = xr.open_dataset(file)
    #     subset = ds.sel(lat=slice(pr_lat_max, pr_lat_min), lon=slice(pr_lon_min, pr_lon_max))
    #     subset.to_netcdf(mswep_save_loc + file)
    #     if init:
    #         init = False
    #         e = time.time()
    #         print(f"One file took {e - s} seconds")
    #         print(f"All files estimated to take {len(mswep) * (e - s)} seconds")



coords_loc = "../"
mswep_loc = "../mswep/hourly/"
mswep_save = "../mswep/trimmed/"

slice_mswep_with_xu_wrf(coords_loc, mswep_loc, mswep_save)