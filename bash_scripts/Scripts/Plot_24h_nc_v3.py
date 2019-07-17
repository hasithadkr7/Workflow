import numpy as np
import gzip
import glob
import matplotlib
matplotlib.use("Agg")
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
th=10
filelist = glob.glob('./*.nc')
Total_rain=[]
Total_sw_rain=[]
for fl in filelist:
    filename=fl
    fh = Dataset(filename, mode='r')
    lons = fh.variables['lon']
    lats = fh.variables['lat']
    precip_raw = fh.variables['rain']
    lons_sl = lons[780:830] #78E to 82.85E
    lats_sl = lats[620:720] #2N to 11.95N
    rain_sl_raw = precip_raw[620:720,780:830] #30 x 35
    rain_sl_sw_raw = precip_raw[649:691,780:806]
    Total_rain.append(rain_sl_raw)
    Total_sw_rain.append(rain_sl_sw_raw)
rain_sl=sum(Total_rain)
rain_sl_sw=sum(Total_sw_rain)
rain_sl[rain_sl<0]=0
#rain_sl=rain_sl/2
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
#Compute criteria on SW Sri-Lanka
img=precip_T
(X, Y) = np.meshgrid(lons_sl_sw, lats_sl_sw)
x_grav_gsmap = np.float64((X*img).sum()) / img.sum().astype("float")
y_grav_gsmap = np.float64((Y*img).sum()) / img.sum().astype("float")
#sys.stdout = open('gsmap_gravcenter.txt', 'w')
#print("xy grav center",x_grav_gsmap,y_grav_gsmap)
#Compute rain area
sum_raingrid_gsmap=0
for j in range(0,len(lons_sl_sw)):
    for i in range(0,len(lats_sl_sw)):
        if (rain_sl_sw[i,j] > th): sum_raingrid_gsmap=sum_raingrid_gsmap+1

#Sum of rainfall volume
sum_rain_gsmap=0
sum_rain_gsmap = rain_sl_sw[rain_sl_sw>th].sum()
name=["x_grav_gsmap","y_grav_gsmap","sum_rain_gsmap","Total_rain_gsmap"]
value=[x_grav_gsmap,y_grav_gsmap,sum_raingrid_gsmap,sum_rain_gsmap]
np.savetxt("gsmap.csv",value,delimiter=",")

