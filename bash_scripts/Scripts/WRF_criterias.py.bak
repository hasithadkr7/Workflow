#!/usr/bin/python
def WRF_criterias(nc,hours,th,WT):
    import csv
    import pandas as pd
    from netCDF4 import Dataset
    import numpy as np
    fh = Dataset(nc, mode='r')
    lons_WRF = fh.variables['XLONG'][0]
    lats_WRF = fh.variables['XLAT'][0]
    rainc = fh.variables['RAINC'][hours]
    rainnc = fh.variables['RAINNC'][hours]
    precip_WRF=rainc + rainnc
    lons_WRF_SW=lons_WRF[14:81,18:60]
    lats_WRF_SW=lats_WRF[14:81,18:60]
    precip_WRF_SW=precip_WRF[14:81,18:60]
    img=precip_WRF_SW
    X=lons_WRF_SW
    Y=lats_WRF_SW
    x_grav_WRF = (X*img).sum() / img.sum().astype("float")
    y_grav_WRF = (Y*img).sum() / img.sum().astype("float")
    #Rain area
    sum_rain_surf=0
    for i in range(0,precip_WRF_SW.shape[0]):
        for j in range(0,precip_WRF_SW.shape[1]):
            if (precip_WRF_SW[i,j] > th): sum_rain_surf=sum_rain_surf+1
    #Rain volume
    Total_rain_vol = precip_WRF_SW.sum()
    #name=["x_grav_WRF","y_grav_WRF","sum_rain_surf","Total_rain_vol"]
    value=[x_grav_WRF,y_grav_WRF,sum_rain_surf,Total_rain_vol]
    #a=[name,value]
    #a=np.array(value)
    #df = pd.DataFrame(a)	
    #df.to_csv(str(nc)+"_hours_"+str(hours)+"_th_"+str(th)+"_"+str(WT)+".csv")
    np.savetxt("SL_"+str(nc)+"_"+str(hours)+".csv", value, delimiter=",")
