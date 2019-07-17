#===================================================================
# Loop to create Figures
# Vuillaume J.F
# Septembre 2017
#===================================================================
import matplotlib
matplotlib.use('Agg')
#from Plot_WRF import Plot_WRF
from WRF_criterias import WRF_criterias
import subprocess
import time
from datetime import datetime, timedelta
date = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
#date = datetime.utcnow().strftime("%Y-%m-%d")

#Parameters
vmax=100
hours=24
th=10
th_area=20
th_vol=1
wrfs=[0, 1, 2, 3]
WTs=["A", "C", "E", "SE"]

#Create daily name file
for i in range(4):
    print i
    nc="d03_"+str(WTs[i])+".nc"
    print nc
    #Plot_WRF(str(nc),hours,str(WTs[i]),vmax)
    WRF_criterias(str(nc),hours,th,str(WTs[i]))
