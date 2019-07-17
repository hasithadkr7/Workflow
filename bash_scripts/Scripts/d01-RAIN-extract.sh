#!/bin/bash
export NCARG_ROOT=/usr/local/ncarg
export PATH=$PATH:/usr/bin/
lib_path="/opt/lib"
export NETCDF="$lib_path"/netcdf
export LD_LIBRARY_PATH="$lib_path"/mpich/lib:"$lib_path"/grib2/lib:$LD_LIBRARY_PATH
export LD_INCLUDE_PATH="$lib_path"/mpich/include:/usr/include:"$lib_path"/grib2/include:$LD_INCLUDE_PATH
export PATH=$PATH:"$lib_path"/mpich/bin/
rundate1=`date '+%Y-%m-%d' --date="1 days ago"`
cd /mnt/disks/wrf-mod/
mkdir -p STATIONS_$rundate1
cd STATIONS_$rundate1

cp /mnt/disks/wrf-mod/Scripts/extract_rain_E_d01.py .
cp /mnt/disks/wrf-mod/Scripts/extract_rain_SE_d01.py .

ln -fs /mnt/disks/wrf-mod/NCLoutput_E/d01__E.nc .
ln -fs /mnt/disks/wrf-mod/NCLoutput_SE/d01__SE.nc .


python3 extract_rain_E_d01.py
python3 extract_rain_SE_d01.py
#rm *.bash
exit;
