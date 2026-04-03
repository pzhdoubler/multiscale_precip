import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import glob
import time
from dask.diagnostics import ProgressBar

with ProgressBar():
    ds = xr.open_mfdataset(
        "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/*.nc",
        combine="by_coords",
        chunks={'time': 1, 'lat': 500, 'lon': 500}
    )

    var = ds['precip']

    means = var.mean(dim=['lat','lon']).compute()
    maxs = var.max(dim=['lat','lon']).compute()
    mins = var.min(dim=['lat','lon']).compute()

plt.figure()
plt.hist(means, bins=50)
plt.title("Mean Precipitation")
plt.xlabel("Value")
plt.ylabel("Frequency")
plt.show()

plt.figure()
plt.hist(maxs, bins=50)
plt.title("Max Precipitation")
plt.show()

plt.figure()
plt.hist(mins, bins=50)
plt.title("Min Precipitation")
plt.show()
