#!/bin/bash
rundate=`date '+%Y-%m-%d' --date="1 days ago"`
echo $rundate
datadir="/mnt/disks/wrf-mod/Plots/"
echo $datadir
cd $datadir
scp -i ~/.ssh/uwcc-admin SLD03${rundate}_A.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/A.gif
scp  -i ~/.ssh/uwcc-admin SLD03${rundate}_C.gif   uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/C.gif
scp  -i ~/.ssh/uwcc-admin SLD03${rundate}_E.gif   uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/E.gif
scp  -i ~/.ssh/uwcc-admin SLD03${rundate}_SE.gif  uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/SE.gif
scp  -i ~/.ssh/uwcc-admin SLD03${rundate}hourly_C.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/hourly_C.gif
scp  -i ~/.ssh/uwcc-admin SLD02${rundate}_Stream_C.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/Stream_C.gif
scp  -i ~/.ssh/uwcc-admin SLD03${rundate}hourly_A.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/hourly_A.gif
scp  -i ~/.ssh/uwcc-admin SLD02${rundate}_Stream_A.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/Stream_A.gif
scp  -i ~/.ssh/uwcc-admin SLD03${rundate}hourly_E.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/hourly_E.gif
scp  -i ~/.ssh/uwcc-admin SLD02${rundate}_Stream_E.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/Stream_E.gif
scp  -i ~/.ssh/uwcc-admin SLD03${rundate}hourly_SE.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/hourly_SE.gif
scp  -i ~/.ssh/uwcc-admin SLD02${rundate}_Stream_SE.gif uwcc-admin@10.138.0.6:/var/www/html/wrf/v3/Stream_SE.gif
