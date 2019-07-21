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
print(lats[38:44,0]) #89, 79
print(lons[0,34:41])
ds = xr.open_dataset(nc_f, engine="netcdf4")
ds.RAINNC.to_netcdf(path="d01_RAINNC_"+str(date)+"_E.nc",engine="scipy")
ds.RAINC.to_netcdf(path="d01_RAINC_"+str(date)+"_E.nc",engine="scipy")
ds.RAINNC[:,38:44,34:41].to_netcdf(path="d01_Kelani_RAINNC_"+str(date)+"_E.nc",engine="scipy")
ds.RAINC[:,38:44,34:41].to_netcdf(path="d01_Kelani_RAINC_"+str(date)+"_E.nc",engine="scipy")
ds.U10[:,38:44,34:41].to_netcdf(path="d01_Kelani_U10_"+str(date)+"_E.nc",engine="scipy")
ds.V10[:,38:44,34:41].to_netcdf(path="d01_Kelani_V10_"+str(date)+"_E.nc",engine="scipy")
ds.U[:,0,38:44,34:41].to_netcdf(path="d03_Kelani_U1_"+str(date)+"_E.nc",engine="scipy")
ds.V[:,0,38:44,34:41].to_netcdf(path="d03_Kelani_V1_"+str(date)+"_E.nc",engine="scipy")
#[6.2375183 6.4810715 6.7245026 6.967819  7.211014  7.454071 ]
#[79.54869  79.79375  80.03881  80.283875 80.52894  80.774    81.019066]
nc_f = "./d03__E.nc"
#Save RAINNC and RAINC
ds = xr.open_dataset(nc_f, engine="netcdf4")
ds.RAINNC.to_netcdf(path="d03_RAINNC_"+str(date)+"_E.nc",engine="scipy")
ds.U10.to_netcdf(path="d03_U10_"+str(date)+"_E.nc",engine="scipy")
ds.V10.to_netcdf(path="d03_V10_"+str(date)+"_E.nc",engine="scipy")
ds.U[:,0,:,:].to_netcdf(path="d03_U1_"+str(date)+"_E.nc",engine="scipy")
ds.V[:,0,:,:].to_netcdf(path="d03_V1_"+str(date)+"_E.nc",engine="scipy")
#ds.RAINC.to_netcdf(path="RAINC_"+str(date)+"_E.nc",engine="scipy")
#ln -fs "/mnt/disks/wrf-mod/OUTPUT_E_18/wrfout_d03"+str(date)+"18:00:00" ./d03_E.nc
nc_fid = Dataset(nc_f, 'r')  # Dataset is the class behavior to open the file
                             # and create an instance of the ncCDF4 class
    # Extract data from NetCDF file
lats = nc_fid.variables['XLAT'][0,:,:]  # extract/copy the data
lons = nc_fid.variables['XLONG'][0,:,:]
#time = nc_fid.variables['TIME'][:]
rainc = nc_fid.variables['RAINC'][:]  # shape is time, lat, lon as shown above
rainnc = nc_fid.variables['RAINNC'][:]
rain = rainc + rainnc
tf,nc,nr=rain.shape
#================================================================
NumberOfSamples = 72
dates = pd.date_range((datetime.date.today()- timedelta(1)).strftime('%m-%d-%Y 23:30'),periods=NumberOfSamples,freq='H')
np.savetxt("dates.txt",dates, delimiter=" ", fmt="%s")
#================================================================
np.savetxt("Glencourse_E_"+date+".txt",rain[0:tf,46,24],fmt='%10.5f')
np.savetxt("Ruwanwella_E_"+date+".txt",rain[0:tf,50,27],fmt='%10.5f')
np.savetxt("Holombuwa_E_"+date+".txt",rain[0:tf,55,27],fmt='%10.5f')
np.savetxt("Kitulgala_E_"+date+".txt",rain[0:tf,47,34],fmt='%10.5f')
np.savetxt("Deraniyagala_E_"+date+".txt",rain[0:tf,44,30],fmt='%10.5f')
np.savetxt("Norton_reservoir_E_"+date+".txt",rain[0:tf,44,37],fmt='%10.5f')
np.savetxt("Kotmale_E_"+date+".txt",rain[0:tf,49,40],fmt='%10.5f')
np.savetxt("Norwood_E_"+date+".txt",rain[0:tf,41,40],fmt='%10.5f')
np.savetxt("Jaffna_E_"+date+".txt",rain[0:tf,146,19],fmt='%10.5f')
np.savetxt("Mahapallegama_E_"+date+".txt",rain[0:tf,53,27],fmt='%10.5f')
np.savetxt("Hingurana_E_"+date+".txt",rain[0:tf,43,19],fmt='%10.5f')
np.savetxt("Kottawa_E_"+date+".txt",rain[0:tf,42,16],fmt='%10.5f')
np.savetxt("Orugodawatta_E_"+date+".txt",rain[0:tf,45,13],fmt='%10.5f')
np.savetxt("Uduwawala_E_"+date+".txt",rain[0:tf,59,40],fmt='%10.5f')
np.savetxt("Ibattara2_E_"+date+".txt",rain[0:tf,44,15],fmt='%10.5f')
np.savetxt("Waga_E_"+date+".txt",rain[0:tf,43,22],fmt='%10.5f')
np.savetxt("Ambewela_E_"+date+".txt",rain[0:tf,43,47],fmt='%10.5f')
np.savetxt("Mulleriyawa_E_"+date+".txt",rain[0:tf,44,15],fmt='%10.5f')
np.savetxt("Dickoya_E_"+date+".txt",rain[0:tf,41,40],fmt='%10.5f')
np.savetxt("Malabe_E_"+date+".txt",rain[0:tf,44,16],fmt='%10.5f')
np.savetxt("Mutwal_E_"+date+".txt",rain[0:tf,46,12],fmt='%10.5f')
np.savetxt("Urumewella_E_"+date+".txt",rain[0:tf,50,30],fmt='%10.5f')
#--------------------------------------------------------------------
np.savetxt("Kotikawatta_E_"+date+".txt",rain[0:tf,45,14],fmt='%10.5f')
np.savetxt("Naula_E_"+date+".txt",rain[0:tf,75,43],fmt='%10.5f')
#================================================================
plt.plot(np.arange(0,tf,1),rain[0:tf,46,24], label="Gencourse")
plt.plot(np.arange(0,tf,1),rain[0:tf,50,27], label="Ruwanwella")
plt.plot(np.arange(0,tf,1),rain[0:tf,55,27], label="Holombuwa")
plt.plot(np.arange(0,tf,1),rain[0:tf,47,34], label="Kitulgala")
plt.plot(np.arange(0,tf,1),rain[0:tf,44,30], label="Deraniyagala")
plt.plot(np.arange(0,tf,1),rain[0:tf,44,37], label="Norton_reservoir")
plt.plot(np.arange(0,tf,1),rain[0:tf,49,40], label="Kotmale")
plt.plot(np.arange(0,tf,1),rain[0:tf,41,40], label="Norwood")
#=================================================================
plt.plot(np.arange(0,tf,1),rain[0:tf,146,19], label="Jaffna")
plt.plot(np.arange(0,tf,1),rain[0:tf,53,27], label="Mahapallegama")
plt.plot(np.arange(0,tf,1),rain[0:tf,43,19], label="Hingurana")
plt.plot(np.arange(0,tf,1),rain[0:tf,42,16], label="Kottawa")
plt.plot(np.arange(0,tf,1),rain[0:tf,45,13], label="Orugodawatta")
plt.plot(np.arange(0,tf,1),rain[0:tf,59,40], label="Uduwawala")
plt.plot(np.arange(0,tf,1),rain[0:tf,44,15], label="Ibattara2")
plt.plot(np.arange(0,tf,1),rain[0:tf,43,22], label="Waga")
plt.plot(np.arange(0,tf,1),rain[0:tf,43,47], label="Ambewela")
plt.plot(np.arange(0,tf,1),rain[0:tf,44,15], label="Mulleriyawa")
plt.plot(np.arange(0,tf,1),rain[0:tf,41,40], label="Dickoya")
plt.plot(np.arange(0,tf,1),rain[0:tf,44,16], label="Malabe")
plt.plot(np.arange(0,tf,1),rain[0:tf,46,12], label="Mutwal")
plt.plot(np.arange(0,tf,1),rain[0:tf,50,30], label="Urumewella")
#------------------------------------------------------------------
plt.plot(np.arange(0,tf,1),rain[0:tf,45,14], label="Kotikawatta")
plt.plot(np.arange(0,tf,1),rain[0:tf,75,43], label="Naula")
#===============================================================
plt.legend(loc=6)
plt.xlabel(str(date)+"_23:30")
plt.ylabel("mm")
plt.xticks( rotation=90 )
plt.savefig('./St_E.png')