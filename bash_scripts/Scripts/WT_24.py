#!/usr/bin/env python
import os
import subprocess
from netCDF4 import Dataset
import datetime
import numpy as np
import sys
file_nc='gfs.t18z.pgrb2.0p50.f024.nc'
fh = Dataset(file_nc, mode='r')
#lons = fh.variables['longitude'][:]
#lats = fh.variables['latitude'][:]
msl = fh.variables['PRMSL_meansealevel']
#lons_grid=lons[624:655]
#lats_grid=lats[230:271]
g=msl[0,230:271,624:655]
P1 = g[0,10]
P2 = g[0,20]
P3 = g[10,0]
P4 = g[10,10]
P5 = g[10,20]
P6 = g[10,30]
P7 = g[20,0]
P8 = g[20,10]
P9 = g[20,20]
P10 = g[20,30]
P11 = g[30,0]
P12 = g[30,10]
P13 = g[30,20]
P14 = g[30,30]
P15 = g[40,10]
P16 = g[40,20]
WF = (0.5*(P12 + P13) - 0.5*(P4 + P5))
SF = 1.35*(0.25*(P5 + 2*P9 + P13) - 0.25 * (P4 + 2*P8 + P12))
F = (SF**2 + WF**2)**0.5
WSV = 1.12 * (0.5 * (P15 + P16) - 0.5*(P8 + P9)) - 0.91 * (0.5 * (P8 + P9) - 0.5*(P1 +P2))
SSV = 0.85*(0.25*(P6 + 2*P10 + P14) - 0.25*(P5 + 2*P9 + P13) - 0.25*(P4 + 2*P8 + P12) + 0.25*(P3 + 2*P7 +P11))
Z = WSV + SSV
D=0
if abs(Z < F) :
    F = 1
    C=0
if abs(Z) > (2*F) and Z > 0:
    C = 1
    A=0
if abs(Z) > (2*F) and Z < 0:
    A = 1
FT="H"
if F==1 and WF > 0: D =  np.arctan(WF/SF) + 180
if F==1 and WF < 0: D = np.arctan(WF/SF)
if F==1 and D > -67.5 and D < -22.5: FT='NW'
if F==1 and D > -22.5 and D < 22.5: FT='N'
if F==1 and D > 22.5 and D < 67.5: FT='NE'
if F==1 and D > 67.5 and D < 112.5: FT='E'
if F==1 and D > 112.5 and D < 157.5: FT='SE'
if F==1 and D > 157.5 and D < 202.5: FT='S'
if F==1 and D > 202.5 and D < 247.5: FT='SW'
if F==1 and D < -67.5 or D  > 247.5: FT='W'
if F==0 and A > 0: FT = 'A'
if F==0 and C > 0: FT = 'C'
#sys.exit(FT)
with open('WT_24_'+datetime.date.today().isoformat()+'.txt', 'wb') as fh:
    fh.write(str(FT)+'\n')
