#=============================================================================
#!/bin/bash
# wrf.sh
# Copy WRFV3 +24h A,C,E and SE
# Compute the score of eh weather type
# J.F. Vuillaume 2018
# 
#=============================================================================
lib_path="/opt/lib"
export NETCDF="$lib_path"/netcdf
export LD_LIBRARY_PATH="$lib_path"/mpich/lib:"$lib_path"/grib2/lib:$LD_LIBRARY_PATH
export LD_INCLUDE_PATH="$lib_path"/mpich/include:/usr/include:"$lib_path"/grib2/include:$LD_INCLUDE_PATH
export PATH=$PATH:"$lib_path"/mpich/bin/
#=================================================================
cd /mnt/disks/wrf-mod/
folder=$(date -ud "-1 days" +"%Y%m%d")
#folder=$(date -u +"%Y%m%d")
rundate1=`date '+%Y-%m-%d' --date="1 days ago"`
#rundate1=`date '+%Y-%m-%d'  --date="1 days ago"`
#rundate1=`date '+%Y-%m-%d'`
rm -Rf "WRF_${folder}"
mkdir -p "WRF_${folder}"
cd "WRF_${folder}"
cp /mnt/disks/wrf-mod/Scripts/wrfs.py .
cp /mnt/disks/wrf-mod/Scripts/WRF_criterias.py .
cp /mnt/disks/wrf-mod/Scripts/WRF_score.py .
cp /mnt/disks/wrf-mod/OUTPUT_A_18/wrfout_d03_${rundate1}_18:00:00 ./d03_A.nc
cp /mnt/disks/wrf-mod/OUTPUT_C_18/wrfout_d03_${rundate1}_18:00:00 ./d03_C.nc
cp /mnt/disks/wrf-mod/OUTPUT_E_18/wrfout_d03_${rundate1}_18:00:00 ./d03_E.nc
cp /mnt/disks/wrf-mod/OUTPUT_SE_18/wrfout_d03_${rundate1}_18:00:00 ./d03_SE.nc
#ln -fs /mnt/disks/wrf-mod/NCLoutput_C/d03_C.nc .
#ln -fs /mnt/disks/wrf-mod/NCLoutput_C/d03_A.nc .
#ln -fs /mnt/disks/wrf-mod/NCLoutput_C/d03_E.nc .
#ln -fs /mnt/disks/wrf-mod/NCLoutput_C/d03_SE.nc .

python wrfs.py
python WRF_score.py
rm *.nc
#rm -f /mnt/disks/wrf-mod/OUTPUT_C_18/wrfout*
#rm -f /mnt/disks/wrf-mod/OUTPUT_A_18/wrfout*
#rm -f /mnt/disks/wrf-mod/OUTPUT_E_18/wrfout*
#rm -f /mnt/disks/wrf-mod/OUTPUT_SE_18/wrfout*
