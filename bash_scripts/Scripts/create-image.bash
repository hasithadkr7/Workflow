#!/bin/bash

lib_path="/opt/lib"
wrf_home="/mnt/disks/wrf-mod"

geog_home="$wrf_home/DATA/geog/"
gfs_home="$wrf_home/DATA/GFS/"

src_home="$wrf_home/wrf-scripts/src"
ncl_home="$wrf_home/wrf-scripts/ncl"
log_home="$wrf_home/logs"
log_file="create.images."$(date +"%Y-%m-%d_%H%M")".log"

wrf_output="$wrf_home/OUTPUT"
ncl_output="$wrf_output/NCL"
pdf_output="$wrf_output/PDF"

echo "Redirecting logs to $log_home/$log_file"
mkdir -p $log_home
exec > "$log_home/$log_file"

export NCARG_ROOT=/usr/share/ncarg

rm -f *.pdf
rm -f *.gif

rundate=$(date '+%Y-%m-%d' --date="1 days ago")

cd $wrf_home || exit

#Domain 01
file_name="${wrf_output}/wrfout_d01_${rundate}_00:00:00.nc"
ncl file_name=\"${file_name}\" $ncl_home/precip-d01.ncl

pdfseparate plt_Precip1.pdf d01%02d.pdf
convert -delay 30 d01*.pdf d01.gif

#Domain 02
file_name="${wrf_output}/wrfout_d02_${rundate}_00:00:00.nc"
ncl file_name=\"${file_name}\" $ncl_home/precip-d02.ncl
pdfseparate plt_Precip2.pdf d02%02d.pdf
convert -delay 30 d02*.pdf d02.gif

#Domain 03
file_name="${wrf_output}/wrfout_d03_${rundate}_00:00:00.nc"
ncl file_name=\"${file_name}\" $ncl_home/precip-d03.ncl
pdfseparate plt_Precip3.pdf d03%02d.pdf
convert -delay 30 d03*.pdf d03.gif

mkdir -p $ncl_output

cp d01.gif $ncl_output/SLD01$rundate.gif 
cp d02.gif $ncl_output/SLD02$rundate.gif
cp d03.gif $ncl_output/SLD03$rundate.gif

mkdir -p $pdf_output

cp plt_Precip1.pdf $pdf_output/SLD01$rundate.pdf
cp plt_Precip2.pdf $pdf_output/SLD02$rundate.pdf
cp plt_Precip3.pdf $pdf_output/SLD03$rundate.pdf


rm -f *.pdf
rm -f *.gif

exit
