#=================================
#JF Vuillaume 2018
# Plot stations C
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime as dt  # Python standard library datetime  module
import numpy as np
from netCDF4 import Dataset  # http://code.google.com/p/netcdf4-python/
import netCDF4
import pandas as pd
from datetime import date, timedelta
import datetime as dt
import datetime
import xarray as xr
date=str(date.today() - timedelta(1)).split()[0]

#link d01
#ln -fs /mnt/disks/wrf-mod/NCLoutput_E/d01__E.nc ./d01__E.nc
#Extract lat #6.3 7.4 
#        lon #79.6 81
nc_f = "./d01__E.nc"
#Save RAINNC and RAINC
nc_fid = Dataset(nc_f, 'r')
lats = nc_fid.variables['XLAT'][0,:,:]  # extract/copy the data
lons = nc_fid.variables['XLONG'][0,:,:]
#print(lats[38:44,0]) #89, 79
#print(lons[0,34:41])
ds = xr.open_dataset(nc_f, engine="netcdf4")
ds.RAINNC.to_netcdf(path="d01_all-Kelani_RAINNC_"+str(date)+"_E.nc",engine="scipy")
ds.RAINC.to_netcdf(path="d01_all-Kelani_RAINC_"+str(date)+"_E.nc",engine="scipy")
