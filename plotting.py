import os
import xarray as xr
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature


ds = xr.open_dataset("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/2020001.12.nc")

lon = ds["lon"]
lat = ds["lat"]
time = ds["time"]

lon2d, lat2d = np.meshgrid(lon, lat)

fig = plt.figure(figsize=(10, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

mesh = ax.contourf(
    lon2d, lat2d, ds["precipitation"],
    transform=ccrs.PlateCarree()
)

# Add features
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.add_feature(cfeature.STATES, edgecolor='gray', linewidth=0.8)

ax.set_extent([-108.0, -103.0, 38.0, 41.0], ccrs.PlateCarree())

plt.show()
plt.close()

ds = xr.open_dataarray("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_annual/pr_2020.nc")

lon = ds["lon"]
lat = ds["lat"]

lon2d, lat2d = np.meshgrid(lon, lat)

fig = plt.figure(figsize=(10, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

mesh = ax.contourf(
    lon2d, lat2d, ds["precipitation"].sel(time=time),
    transform=ccrs.PlateCarree()
)

# Add features
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.add_feature(cfeature.STATES, edgecolor='gray', linewidth=0.8)

ax.set_extent([-108.0, -103.0, 38.0, 41.0], ccrs.PlateCarree())

plt.show()
plt.close()