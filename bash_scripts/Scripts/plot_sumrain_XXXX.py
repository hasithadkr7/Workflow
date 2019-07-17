from netCDF4 import Dataset
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.cm import get_cmap
import cartopy.crs as crs
import sys
from cartopy.feature import NaturalEarthFeature
from wrf import (getvar, interplevel, to_np, latlon_coords, get_cartopy,
                 cartopy_xlim, cartopy_ylim)
#ncks -d Time,72 d03__C.nc d72.nc
# Open the NetCDF file
#ncfile = Dataset("wrfout_d01_2016-10-07_00_00_00")
ncfile = Dataset("d03__C.nc")
#print(ncfile)
idx=str(sys.argv[1])
print(idx)
tot_rainc=0
tot_rainnc=0
for i in np.arange(1,int(idx)):
    print(i)
    rainc=getvar(ncfile,"RAINC",timeidx=int(idx))
    rainnc=getvar(ncfile,"RAINNC",timeidx=int(idx))
    tot_rainc=to_np(rainc) + tot_rainc
    tot_rainnc=to_np(rainnc) + tot_rainnc
    print(tot_rainc)
rainc=getvar(ncfile,"RAINC",timeidx=int(idx))
rainnc=getvar(ncfile, "RAINNC",timeidx=int(idx))
date=getvar(ncfile, "Times",timeidx=int(idx))
#print(np.max(rain),rain.shape,np.max(rainnc))
#p = getvar(ncfile, "pressure")
#z = getvar(ncfile, "z", units="dm")
ua = getvar(ncfile, "ua", units="kt")
va = getvar(ncfile, "va", units="kt")
wspd = getvar(ncfile, "wspd_wdir", units="kts")[0,:]

# Get the lat/lon coordinates
lats, lons = latlon_coords(rainc)

# Get the map projection information
cart_proj = get_cartopy(rainc)

# Create the figure
fig = plt.figure(figsize=(12,9))
ax = plt.axes(projection=cart_proj)

# Download and add the states and coastlines
states = NaturalEarthFeature(category="cultural", scale="50m",
                             facecolor="none",
                             name="admin_1_states_provinces_shp")
ax.add_feature(states, linewidth=0.5, edgecolor="black")
ax.coastlines('50m', linewidth=0.8)

# Add the 500 hPa geopotential height contours
#levels = np.arange(980., 1080., 10.)
#contours = plt.contour(to_np(lons), to_np(lats), to_np(rain),
#                       levels=levels, colors="black",
#                       transform=crs.PlateCarree())
#plt.clabel(contours, inline=1, fontsize=10, fmt="%i")
#plt.colorbar(contours, ax=ax, orientation="horizontal", pad=.05)
# Add the wind speed contours
#levels = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
#cmap=get_cmap("jet")
cmap=get_cmap("rainbow")
vmax=200
levels=np.arange(0,vmax,10)
tot_rain=tot_rainc + tot_rainnc
tot_rain[tot_rain>int(vmax)]=int(vmax)
tot_rain[tot_rain<1]="nan"
tot_rain[1,1]=2

if np.nanmin(tot_rain)> 0:
    rain_contours = plt.contourf(to_np(lons), to_np(lats), tot_rain ,
                             levels=levels,
                             cmap=get_cmap("rainbow"),
                             transform=crs.PlateCarree())
#plt.colorbar(rain_contours, ax=ax, orientation="horizontal", pad=.01)
    plt.colorbar(rain_contours, ax=ax, orientation="horizontal",fraction=0.03, pad=0.05)
#cmap=get_cmap("jet")
# Add the 500 hPa wind barbs, only plotting every 125th data point.
#plt.barbs(to_np(lons[::10,::10]), to_np(lats[::10,::10]),
#          to_np(ua[::10, ::10]), to_np(va[::10, ::10]),
#          transform=crs.PlateCarree(), length=6)

# Set the map bounds
ax.set_xlim(cartopy_xlim(rainc))
ax.set_ylim(cartopy_ylim(rainc))
ax.gridlines()
plt.title(date.values)
plt.savefig("raincum_"+str(idx)+".pdf")

