import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import glob
import time

files = sorted(glob.glob("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/*.nc"))

means = []
maxs = []
mins = []

init = True

for f in files:
    s = time.time()
    ds = xr.open_dataset(f)

    # replace 'precip' with your actual variable name
    var = ds['precipitation']

    means.append(var.mean().item())
    maxs.append(var.max().item())
    mins.append(var.min().item())

    ds.close()
    if init:
        e = time.time()
        print(f"One file: {e-s} s")
        print(f"All files: {len(files)*(e-s)} s")
        init = False

means = np.array(means)
maxs = np.array(maxs)
mins = np.array(mins)

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
