import xarray as xr
import numpy as np
ds=xr.open_dataset("/mnt/disks/wrf-mod/WRFV4_C/test/em_real/wrfout_d01_2019-03-21_18:00:00")
print("min lat",np.min(ds.XLAT),
      "max lat",np.max(ds.XLAT),
      "min lon",np.min(ds.XLONG),
      "max lon",np.max(ds.XLONG))
