#=============================================================================
#!/bin/bash
#
# J.F. Vuillaume
# August 2017
#
#=============================================================================
#cd /mnt/disks/wrf-mod/Scratch_Jean/Run_daily/
#date -u +"%Y%m%d"
today_folder=$(date -u +%Y%m%d)
#Remove today folder
env_keep="DISPLAY"
rm -Rf "${today_folder}"
#Remove 2 days ago folder
yesterday_folder=$(date -ud "-1 days" +"%Y%m%d")
rm -Rf "${yesterday_folder}"
echo "Running gsmap download and post-treatment"
sh gsmap_convert.sh
cd "GSMAP_${today_folder}"
echo "Plot 24h cumulative"
#python ../Plot_1h_nc_v2.py
#python ../Plot_24h_nc_global.py
python ../Plot_24h_nc_v2.py
#python ../Crit_gsmap.py
