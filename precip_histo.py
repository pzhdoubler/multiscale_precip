import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import glob
import time
from dask.diagnostics import ProgressBar

with ProgressBar():
    ds = xr.open_mfdataset(
        "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/202100*.nc",
        combine="by_coords",
        chunks={'time': 1, 'lat': 500, 'lon': 500}
    )

    var = ds['precipitation']

    means = var.mean(dim=['lat','lon']).compute()
    maxs = var.max(dim=['lat','lon']).compute()
    mins = var.min(dim=['lat','lon']).compute()

plt.figure()
plt.hist(means, bins=50)
plt.title("Mean Precipitation")
plt.xlabel("Value")
plt.ylabel("Frequency")
plt.show()

plt.figure(figsize=(8,5))
plt.hist(means, bins=100, range=(0, np.percentile(means, 50)))  # zoom on lower half
plt.title("Zoomed-in Histogram of Precipitation (lower end)")
plt.xlabel("Precipitation value")
plt.ylabel("Frequency")
plt.show()

plt.figure(figsize=(8,5))
plt.hist(means, bins=100, range=(0, np.percentile(means, 90)), log=True)
plt.title("Histogram of Precipitation (log scale)")
plt.xlabel("Precipitation value")
plt.ylabel("Log frequency")
plt.show()

plt.figure()
plt.hist(maxs, bins=50)
plt.title("Max Precipitation")
plt.show()
