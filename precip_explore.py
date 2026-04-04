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
    median = np.median(var.values)
    nf_per = np.percentile(var.values, 95)
    max = var.max().item()
    min = var.min().item()
    drizzle_threshold = 0.1
    exceed = (var > drizzle_threshold).sum().item() / var.size
    return [t, avg, median, nf_per, max, min, exceed]

# Path to your NetCDF files
mswep_path = "/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed/"

files = sorted(os.listdir(mswep_path))
files = files[:50]

mswep_files = [(mswep_path, f) for f in files]

with Pool() as pool:
    # [time, avg, max, min, sum]
    stats = np.asarray(pool.starmap(get_precip_stats, mswep_files))
    print("Stats done. Saving...")
    df = pd.DataFrame({
        'time': stats[:,0],
        'mean': stats[:,1],
        'median': stats[:,2],
        '95th': stats[:,3],
        'max': stats[:,4],
        'min': stats[:,5],
        'drizzle exceedance': stats[:,6]
    })
    # Save to CSV
    df.to_csv("/ocean/projects/ees210011p/hdoubler/AOSC650/mswep/trimmed_precip_stats2.csv", index=False)
    print("Saved statistics to trimmed_precip_stats2.csv")
