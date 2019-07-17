#=================================
#JF Vuillaume 2018
# Plot stations C
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime as dt  # Python standard library datetime  module
import numpy as np
from netCDF4 import Dataset  # http://code.google.com/p/netcdf4-python/
from mpl_toolkits.basemap import Basemap, addcyclic, shiftgrid
import netCDF4
from datetime import date, timedelta
date=str(date.today()).split()[0]
#tf=29
nc_f = "./d03_06_C.nc"
#ln -fs "/mnt/disks/wrf-mod/OUTPUT_12_C_18/wrfout_d03"+str(date)+"18:00:00" ./d03_C.nc
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
np.savetxt("Ruwanwell_06_C_"+date+".txt",rain[0:tf,50,27],fmt='%10.5f')
np.savetxt("Holombuwa_06_C_"+date+".txt",rain[0:tf,55,27],fmt='%10.5f')
np.savetxt("Kitulgala_06_C_"+date+".txt",rain[0:tf,47,34],fmt='%10.5f')
np.savetxt("Deraniyagala_06_C_"+date+".txt",rain[0:tf,44,30],fmt='%10.5f')
np.savetxt("Norton_reservoir_06_C_"+date+".txt",rain[0:tf,44,37],fmt='%10.5f')
np.savetxt("Kotmale_06_C_"+date+".txt",rain[0:tf,49,40],fmt='%10.5f')
np.savetxt("Norwood_06_C_"+date+".txt",rain[0:tf,41,40],fmt='%10.5f')
np.savetxt("Jaffna_06_C_"+date+".txt",rain[0:tf,146,19],fmt='%10.5f')
np.savetxt("Mahapallegama_06_C_"+date+".txt",rain[0:tf,53,27],fmt='%10.5f')
np.savetxt("Hingurana_06_C_"+date+".txt",rain[0:tf,43,19],fmt='%10.5f')
np.savetxt("Kottawa_06_C_"+date+".txt",rain[0:tf,42,16],fmt='%10.5f')
np.savetxt("Orugodawatta_06_C_"+date+".txt",rain[0:tf,45,13],fmt='%10.5f')
np.savetxt("Uduwawala_06_C_"+date+".txt",rain[0:tf,59,40],fmt='%10.5f')
np.savetxt("Ibattara2_06_C_"+date+".txt",rain[0:tf,44,15],fmt='%10.5f')
np.savetxt("Waga_06_C_"+date+".txt",rain[0:tf,43,22],fmt='%10.5f')
np.savetxt("Ambewela_06_C_"+date+".txt",rain[0:tf,43,47],fmt='%10.5f')
np.savetxt("Mulleriyawa_06_C_"+date+".txt",rain[0:tf,44,15],fmt='%10.5f')
np.savetxt("Dickoya_06_C_"+date+".txt",rain[0:tf,41,40],fmt='%10.5f')
np.savetxt("Malabe_06_C_"+date+".txt",rain[0:tf,44,16],fmt='%10.5f')
np.savetxt("Mutwal_06_C_"+date+".txt",rain[0:tf,46,12],fmt='%10.5f')
np.savetxt("Urumewella_06_C_"+date+".txt",rain[0:tf,50,30],fmt='%10.5f')
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
plt.legend(loc=6)
plt.xlabel(str(date)+"_12:00")
plt.ylabel("mm")
plt.xticks( rotation=90 )
plt.savefig('./St_06_C.png')
