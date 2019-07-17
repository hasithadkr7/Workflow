#=============================================================
# WRF_Score.py
# Compute the score of performance of the scheme
# 
# J.F. Vuillaume and S. Herath 2018
#=============================================================
import numpy as np
import gzip
import glob
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.colors import LogNorm
from netCDF4 import Dataset
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.cm as cm
import sys
from mpl_toolkits.basemap import Basemap
from scipy import interpolate
import csv
from datetime import datetime

#Load today GSMAP
gsmap_f="/mnt/disks/wrf-mod/GSMAP_"+str(datetime.now().strftime('%Y%m%d'))+"/gsmap.csv"
print(gsmap_f)
gsmap_file=np.loadtxt(gsmap_f,delimiter=",")
gsmap_file=np.loadtxt("/mnt/disks/wrf-mod/GSMAP_"+str(datetime.now().strftime('%Y%m%d'))+"/gsmap.csv",delimiter=",")
gsmap=np.array(gsmap_file)
print(gsmap_file)
#Load yesterday WRF run
file_name=[]
data=[]
WTs=["A","C","E","SE"]
for i in range(4):
    file="SL_d03_"+str(WTs[i])+".nc_24.csv"
    print(file)
    file_name.append(file)
    a = np.loadtxt(file,delimiter=",")
    data.append(a)
data=np.array(data)
#Area Ratio
Ar = np.float64(data[:,2]*2.222**2)/(gsmap[2]*10**2)

#GRavity distance
dr = np.sqrt((data[:,0]-gsmap[0])**2+(data[:,1]-gsmap[1])**2)

#Volume rain ratio
#Rr = Rain(WRF) / Rain(GSMAP) 
Rr = np.float64(data[:,3]*2.222**2)/(gsmap[3]*10**2)

f1=np.zeros(data.shape[0])
f2=np.zeros(data.shape[0])
f3=np.zeros(data.shape[0])

#Area score
for i in range(data.shape[0]):
    if (Ar[i] > 0.7 and Ar[i] < 1.2): f1[i]=1.0 
for i in range(data.shape[0]):
    if (Ar[i] < 0.7): f1[i] = 0.3
for i in range(data.shape[0]):
    if (Ar[i] > 1.2): f1[i]=0.4

for i in range(data.shape[0]):
    if (dr[i] < 0.06): 
        f2[i]=1
    else:
        f2[i] = 0.06 / dr[i]

for i in range(data.shape[0]):
    if (Rr[i] > 1): 
        f3[i] = (0.9 / Rr[i])
    else :
        f3[i] = Rr[i] * 1.1

w1 = np.zeros(data.shape[0])
w2 = np.zeros(data.shape[0])
w3 = np.zeros(data.shape[0])
for i in range(data.shape[0]):
    if (f1[i] == 1):
        w1[i] = 1; w2[i]=1; w3[i]=1
    if (f1[i] == 0.3):
        w1[i] = 1 ; w2[i]=0.3 ; w3[i]=0.3        
    if (f1[i] == 0.4):
        w1[i] = 1 ; w2[i]=0.5 ; w3[i]=0.7

Score = np.zeros(data.shape[0])
for i in range(data.shape[0]):
    Score[i] = f1[i] * w1[i] + f2[i] * w2[i] + f3[i] * w3[i] 
np.savetxt("/mnt/disks/wrf-mod/Score/score_"+datetime.now().strftime('%Y%m%d')+".csv",Score,delimiter=",")

Crit_1 = np.zeros(data.shape[0])
for i in range(data.shape[0]):
    Crit_1[i] = f1[i] * w1[i] 
Crit_2 = np.zeros(data.shape[0])
for i in range(data.shape[0]):
    Crit_2[i] = f2[i] * w2[i]
Crit_3 = np.zeros(data.shape[0])
for i in range(data.shape[0]):
    Crit_3[i] = f3[i] * w3[i]
np.savetxt("/mnt/disks/wrf-mod/Crit_1/Crit_1_"+datetime.now().strftime('%Y%m%d')+".csv",Crit_1,delimiter=",")
np.savetxt("/mnt/disks/wrf-mod/Crit_2/Crit_2_"+datetime.now().strftime('%Y%m%d')+".csv",Crit_2,delimiter=",")
np.savetxt("/mnt/disks/wrf-mod/Crit_3/Crit_3_"+datetime.now().strftime('%Y%m%d')+".csv",Crit_3,delimiter=",")







