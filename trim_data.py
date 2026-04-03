import os
import numpy as np
import xarray as xr

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

    print(mswep[:5])

    # for file in mswep:
    #     ds = xr.open_dataset(file)
    #     subset = ds.sel(lat=slice(pr_lat_max, pr_lat_min), lon=slice(pr_lon_min, pr_lon_max))
    #     subset.to_netcdf(mswep_save_loc + file)


coords_loc = "../"
mswep_loc = "../mswep/hourly/"
mswep_save = "../mswep/trimmed/"

slice_mswep_with_xu_wrf(coords_loc, mswep_loc, mswep_save)