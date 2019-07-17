from netCDF4 import Dataset
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.cm import get_cmap
import cartopy.crs as crs
import sys
from cartopy.feature import NaturalEarthFeature
from wrf import (getvar, interplevel, to_np, latlon_coords, get_cartopy,
                 cartopy_xlim, cartopy_ylim)

ncfile = Dataset("d03__A.nc")
idx=str(sys.argv[1])
print(idx)
rainc=getvar(ncfile,"RAINC",timeidx=int(idx))
rainnc=getvar(ncfile, "RAINNC",timeidx=int(idx))
date=getvar(ncfile, "Times",timeidx=int(idx))
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

cmap_r = matplotlib.cm.get_cmap('rainbow_r')
#cmap_r=get_cmap("rainbow")
#cmap = reverse_colourmap(cmap_r)
levels=np.arange(0.001,11,1)
rain=(to_np(rainc) + to_np(rainnc))
#rain[rain<0.1]="nan"
rain[1,1]=0.1

rain_contours = plt.contourf(to_np(lons), to_np(lats), rain,
                             levels=levels,
                             cmap=cmap_r,
                             transform=crs.PlateCarree())
plt.colorbar(rain_contours, ax=ax, orientation="horizontal",fraction=0.03, pad=0.05)

# Set the map bounds
ax.set_xlim(cartopy_xlim(rainnc))
ax.set_ylim(cartopy_ylim(rainnc))
ax.gridlines()
plt.title(date.values)
plt.savefig("rain_"+str(idx)+".pdf")
#plt.show()

