import xarray as xr
import numpy as np
import pandas as pd
import glob
from datetime import datetime
import os
from multiprocessing import Pool

def get_precip_stats(path, file):
    ds = xr.open_dataset(os.path.join(path, file))
    var = ds['precipitation']
    dt_obj = datetime.strptime(file, "%Y%j.%H.nc")

    t = dt_obj.strftime("%Y-%m-%d %H:%M")
    avg = var.mean().item()
    max = var.max().item()
    min = var.min().item()
    sum = var.sum().item()
    return [file, t, avg, max, min, sum]

# Path to your NetCDF files
mswep_path = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/"

files = sorted(os.listdir(mswep_path))
files = files[:10]

mswep_files = [(mswep_path, f) for f in files]

print(files)

with Pool() as pool:
    # file, time, avg, max, min, sum
    stats = np.asarray(pool.starmap(get_precip_stats, mswep_files))
    print(stats.shape)
    print("Stats done. Saving...")
    df = pd.DataFrame({
        'file': stats[:,0],
        'time': stats[:,1],
        'mean': stats[:,2],
        'max': stats[:,3],
        'min': stats[:,4],
        'sum': stats[:,5]
    })
    # Save to CSV
    df.to_csv("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_precip_stats.csv", index=False)
    print("Saved statistics to trimmed_precip_stats.csv")


# plt.figure()
# plt.hist(means, bins=50)
# plt.title("Mean Precipitation")
# plt.xlabel("Value")
# plt.ylabel("Frequency")
# plt.show()

# plt.figure(figsize=(8,5))
# plt.hist(means, bins=100, range=(0, np.percentile(means, 50)))  # zoom on lower half
# plt.title("Zoomed-in Histogram of Precipitation (lower end)")
# plt.xlabel("Precipitation value")
# plt.ylabel("Frequency")
# plt.show()

# plt.figure(figsize=(8,5))
# plt.hist(means, bins=100, range=(0, np.percentile(means, 90)), log=True)
# plt.title("Histogram of Precipitation (log scale)")
# plt.xlabel("Precipitation value")
# plt.ylabel("Log frequency")
# plt.show()

# plt.figure()
# plt.hist(maxs, bins=50)
# plt.title("Max Precipitation")
# plt.show()