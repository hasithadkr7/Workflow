import numpy as np
import gzip
import glob
import matplotlib.pyplot as plt
import datetime
from matplotlib.colors import LogNorm
from netCDF4 import Dataset
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.cm as cm
import sys
from mpl_toolkits.basemap import Basemap
from scipy import interpolate
from datetime import datetime, date, time
import csv
import pandas as pd
import datetime
filelist = glob.glob('./*.nc')
Total_rain=[]
Total_rain_sw=[]
th=10
for fl in filelist:
#    ncfile = netcdf_file(fname,'r')
#    #the rest of your reading code
#    fig = plt.figure()
#filename='gsmap_now.20170804.0000_0059.nc'
    filename=fl
    fh = Dataset(filename, mode='r')
    lons = fh.variables['lon']
    lats = fh.variables['lat']
    precip_raw = fh.variables['rain']
    lons_sl = lons[785:830]
    lats_sl = lats[620:720]
    rain_sl_raw = precip_raw[620:720,785:830].T #30 x 35
    Total_rain.append(rain_sl_raw)
    rain_sl_sw_raw = precip_raw[649:691,780:806]
    Total_rain_sw.append(rain_sl_sw_raw)
rain_sl=sum(Total_rain)
rain_sl_sw=sum(Total_rain_sw)
#SW SL
lons_sl_sw=lons[780:806] #77.9E to 80.4E
lats_sl_sw=lats[649:691]#5N to 9N
cmap=cm.rainbow
#cmap.set_under("w",alpha=0)
lon_0 = lons_sl.mean()
lat_0 = lats_sl.mean()
levels=[0, 1, 2, 4, 8, 16, 32, 64, 128, 256]
m = Basemap(llcrnrlon=77, llcrnrlat=5, urcrnrlon=82,urcrnrlat=10, projection='lcc',lat_0=lat_0,lon_0=lon_0, resolution='f')
#lats_sl=lats_sl[::-1]
#rain_sl=rain_sl[::-1]
#precip_T=np.flipud(precip_T)
lon, lat = np.meshgrid(lons_sl, lats_sl)
xi, yi = m(lon, lat)
fig = plt.figure(figsize=(15.,15.))
precip_T=rain_sl
# Plot Data
cs = m.contourf(xi,yi,precip_T,cmap=plt.cm.rainbow, levels=levels)
# Add Grid Lines
m.drawparallels(np.arange(-80., 81., 1.), labels=[1,0,0,0], fontsize=1)
m.drawmeridians(np.arange(-180., 181., 1.), labels=[0,0,0,1], fontsize=1)
# Add Coastlines, States, and Country Boundaries
m.drawcoastlines()
m.drawstates()
m.drawcountries()
m.drawrivers()
# Add Colorbar
im = plt.imshow(precip_T,cmap = plt.cm.rainbow, vmin=0, vmax=100)
plt.colorbar(im)
plt.title('Precipitation')
fig.savefig("24h_sl_rain"+str(int(date.today().strftime('%Y%m%d')))+".png", dpi=200)
#plt.imshow()
plt.close()
#PLOT SW corner
precip_T_sw=rain_sl_sw
cmap=cm.rainbow
lon_0 = lons_sl_sw.mean()
lat_0 = lats_sl_sw.mean()
m = Basemap(llcrnrlon=77, llcrnrlat=5, urcrnrlon=80.5,urcrnrlat=9, projection='lcc',lat_0=lat_0,lon_0=lon_0, resolution='f')
lon, lat = np.meshgrid(lons_sl_sw, lats_sl_sw)
xi, yi = m(lon, lat)
fig = plt.figure(figsize=(15.,15.))
precip_T=rain_sl_sw
cs = m.contourf(xi,yi,precip_T_sw,cmap=plt.cm.rainbow, levels=levels)
m.drawparallels(np.arange(-80., 81., 1.), labels=[1,0,0,0], fontsize=1)
m.drawmeridians(np.arange(-180., 181., 1.), labels=[0,0,0,1], fontsize=1)
m.drawcoastlines()
m.drawstates()
m.drawcountries()
m.drawrivers()
im = plt.imshow(precip_T,cmap = plt.cm.rainbow, vmin=0, vmax=100)
plt.colorbar(im)
plt.title('Precipitation')
fig.savefig("24h_sl_sw_rain"+str(int(date.today().strftime('%Y%m%d')))+".png", dpi=200)
plt.close()
#Total_rain_24.shape 
#Total_rain_sum = Total_rain.sum(axis=0)
#cmap=cm.jet_r
#cmap.set_under("w",alpha=0)
#lon_0 = lons_sl.mean()
#lat_0 = lats_sl.mean()
#m = Basemap(width=600000,height=750000,resolution='l',projection='stere',lat_ts=40,lat_0=lat_0,lon_0=lon_0)
#lats_sl=lats_sl[::-1]
#rain_sl=rain_sl[::-1]
#precip_T=np.flipud(precip_T)
#lon, lat = np.meshgrid(lons_sl, lats_sl)
#xi, yi = m(lon, lat)
#fig = plt.figure(figsize=(20.,20.))
#precip_T=rain_sl.T
#precip_T=np.flipud(precip_T)
#precip_T=np.fliplr(precip_T)
# Plot Data
#cs = m.pcolor(xi,yi,np.squeeze(precip_T)) 
# Add Grid Lines
#m.drawparallels(np.arange(-80., 81., 10.), labels=[1,0,0,0], fontsize=1)
#m.drawmeridians(np.arange(-180., 181., 10.), labels=[0,0,0,1], fontsize=1)
# Add Coastlines, States, and Country Boundaries
#m.drawcoastlines()
#m.drawstates()
#m.drawcountries()
# Add Colorbar
#cbar = m.colorbar(cs, location='bottom', pad="20%")
# cbar.set_label(precip_units)
#cbar.ax.tick_params(labelsize=10) 
# Add Title
#plt.title('Precipitation')
#fig.savefig("24h_rain"+str(int(date.today().strftime('%Y%m%d')))+".png", dpi=200)
#np.savetxt("24h_rain"+str(int(date.today().strftime('%Y%m%d')))+".csv", precip_T, delimiter=",")
#np.savetxt("lon_sl.csv", xi, delimiter=",")
#np.savetxt("lat_sl.csv", yi, delimiter=",")
#plt.close()
#Compute gravecenter
img=rain_sl.T
(X, Y) = np.meshgrid(lons_sl, lats_sl)
x_grav_gsmap = (X*img).sum() / img.sum().astype("float")
y_grav_gsmap = (Y*img).sum() / img.sum().astype("float")
precip_T_sw=rain_sl_sw
x_grav_gsmap_sw = np.float64((X*img).sum()) / img.sum().astype("float")
y_grav_gsmap_sw = np.float64((Y*img).sum()) / img.sum().astype("float")

#Compute rain area
sum_raingrid_gsmap_sw=0
for j in range(0,len(lons_sl_sw)):
    for i in range(0,len(lats_sl_sw)):
        if (rain_sl_sw[i,j] > th): sum_raingrid_gsmap_sw=sum_raingrid_gsmap_sw+1

#Sum of rainfall volume
sum_rain_gsmap_sw=0
sum_rain_gsmap_sw= rain_sl_sw[rain_sl_sw>th].sum()

#Export file
value=[x_grav_gsmap_sw,y_grav_gsmap_sw,sum_raingrid_gsmap_sw,sum_rain_gsmap_sw]
np.savetxt("gsmap.csv",value,delimiter=",")


#sys.stdout = open('gsmap_gravcenter.txt', 'w')
#print("xy grav center",x_grav_gsmap,y_grav_gsmap)
#Compute rain area
#th = 0
#sum_raingrid_gsmap=0
#for i in range(0,len(lons_sl)):
#    for j in range(0,len(lats_sl)):
#        if (rain_sl[i,j] > th): sum_raingrid_gsmap=sum_raingrid_gsmap+1
#Sum of rainfall volume
#sum_rain_gsmap=0
#sum_rain_gsmap = rain_sl[rain_sl>th].sum()
#Score=[x_grav_gsmap,y_grav_gsmap,sum_raingrid_gsmap,sum_rain_gsmap]
#np.savetxt("gsmap.csv",Score,delimiter=",")
