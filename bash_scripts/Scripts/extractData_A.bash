#!/bin/bash
# This coordinates are from 22-05-2015(Domain changed) 
export NETCDF=/opt/lib/netcdf
export NCARG_ROOT=/usr/local/ncarg
export PATH=$PATH:/usr/bin/
lib_path="/opt/lib"
export NETCDF="$lib_path"/netcdf
export LD_LIBRARY_PATH="$lib_path"/mpich/lib:"$lib_path"/grib2/lib:$LD_LIBRARY_PATH
export LD_INCLUDE_PATH="$lib_path"/mpich/include:/usr/include:"$lib_path"/grib2/include:$LD_INCLUDE_PATH
export PATH=$PATH:"$lib_path"/mpich/bin/
#rundate=`date '+%Y%m%d'`
rundate1=`date '+%Y-%m-%d' --date="1 days ago"`
#rundate1=2016-09-09
rundate2=`date '+%Y-%m-%d' --date="0 days ago"`
rundate3=`date '+%Y-%m-%d' --date="2 days"`
#cd /mnt/disks/wrf-mod/NCLoutput_A
#year1=`date '+%Y'`
#month1=`date '+%m'`
#date1=`date '+%d'`
#year2=`date '+%Y' --date " 3 days"`
#month2=`date '+%m' --date " 3 days"`
#date2=`date '+%d' --date " 3 days"`
#h="wrfout_d03_"
#t="_18:00:00"
#file1=$h$rundate1$t
#echo $file1
#ln -fs /mnt/disks/wrf-mod/OUTPUT_A.18/$file1 .
#ln -fs /mnt/disks/wrf-mod/NCLoutput_A/d03_A.nc ./d03_A.nc
ncap   -s   "PRCP=RAINC+RAINNC+SNOWNC+GRAUPELNC" d03__A.nc  precip.nc
ncks   -v Times   -A    d03__A.nc   precip.nc
ncks -h -d Time,0,71,1 precip.nc first.nc
ncks -h -d Time,1,72,1 precip.nc second.nc
ncdiff second.nc first.nc difference.nc
#============================================================================
#Glencourse
ncks -d west_east,24 -d south_north,46 difference.nc Glencourse.nc
ncdump -v PRCP Glencourse.nc > Glencourse.txt
ncdump -v Times Glencourse.nc > timedata.txt
sed '/netcdf Glencourse {/,/ PRCP =/d' Glencourse.txt > Glencourse2.txt
sed '/netcdf Glencourse {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Glencourse2.txt > Glencourse_A.$rundate1.txt
rm Glencourse.nc
rm Glencourse.txt Glencourse2.txt
#============================================================================
#Ruwanwella
ncks -d west_east,27 -d south_north,50 difference.nc Ruwanwella.nc
ncdump -v PRCP Ruwanwella.nc > Ruwanwella.txt
ncdump -v Times Ruwanwella.nc > timedata.txt
sed '/netcdf Ruwanwella {/,/ PRCP =/d' Ruwanwella.txt > Ruwanwella2.txt
sed '/netcdf Ruwanwella {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Ruwanwella2.txt > Ruwanwella_A.$rundate1.txt
rm Ruwanwella.nc
rm Ruwanwella.txt Ruwanwella2.txt
#================================================================================
#Holombuwa
ncks -d west_east,27 -d south_north,55 difference.nc Holombuwa.nc
ncdump -v PRCP Holombuwa.nc > Holombuwa.txt
ncdump -v Times Holombuwa.nc > timedata.txt
sed '/netcdf Holombuwa {/,/ PRCP =/d' Holombuwa.txt > Holombuwa2.txt
sed '/netcdf Holombuwa {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Holombuwa2.txt > Holombuwa_A.$rundate1.txt
rm Holombuwa.nc
rm Holombuwa.txt Holombuwa2.txt
#================================================================================
#Kitulgala
ncks -d west_east,34 -d south_north,47 difference.nc Kitulgala.nc
ncdump -v PRCP Kitulgala.nc > Kitulgala.txt
ncdump -v Times Kitulgala.nc > timedata.txt
sed '/netcdf Kitulgala {/,/ PRCP =/d' Kitulgala.txt > Kitulgala2.txt
sed '/netcdf Kitulgala {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Kitulgala2.txt > Kitulgala_A.$rundate1.txt
rm Kitulgala.nc
rm Kitulgala.txt Kitulgala2.txt
#============================================================================================
#Daraniyagala
ncks -d west_east,30 -d south_north,44 difference.nc Daraniyagala.nc
ncdump -v PRCP Daraniyagala.nc > Daraniyagala.txt
ncdump -v Times Daraniyagala.nc > timedata.txt
sed '/netcdf Daraniyagala {/,/ PRCP =/d' Daraniyagala.txt > Daraniyagala2.txt
sed '/netcdf Daraniyagala {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Daraniyagala2.txt > Daraniyagala_A.$rundate1.txt
rm Daraniyagala.nc
rm Daraniyagala.txt Daraniyagala2.txt
#=============================================================================================
#Norton_reservoir
ncks -d west_east,37 -d south_north,44 difference.nc Norton_reservoir.nc
ncdump -v PRCP Norton_reservoir.nc > Norton_reservoir.txt
ncdump -v Times Norton_reservoir.nc > timedata.txt
sed '/netcdf Norton_reservoir {/,/ PRCP =/d' Norton_reservoir.txt > Norton_reservoir2.txt
sed '/netcdf Norton_reservoir {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Norton_reservoir2.txt > Norton_reservoir_A.$rundate1.txt
rm Norton_reservoir.nc
rm Norton_reservoir.txt Norton_reservoir2.txt
#=============================================================================
#Kotmale_reservoir
ncks -d west_east,40 -d south_north,49 difference.nc Kotmale_reservoir.nc
ncdump -v PRCP Kotmale_reservoir.nc > Kotmale_reservoir.txt
ncdump -v Times Kotmale_reservoir.nc > timedata.txt
sed '/netcdf Kotmale_reservoir {/,/ PRCP =/d' Kotmale_reservoir.txt > Kotmale_reservoir2.txt
sed '/netcdf Kotmale_reservoir {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Kotmale_reservoir2.txt > Kotmale_reservoir_A.$rundate1.txt
rm Kotmale_reservoir.nc
rm Kotmale_reservoir.txt Kotmale_reservoir2.txt
#=============================================================================
#Norwood
ncks -d west_east,40 -d south_north,41 difference.nc Norwood.nc
ncdump -v PRCP Norwood.nc > Norwood.txt
ncdump -v Times Norwood.nc > timedata.txt
sed '/netcdf Norwood {/,/ PRCP =/d' Norwood.txt > Norwood2.txt
sed '/netcdf Norwood {/,/ Times =/d' timedata.txt > timedata2.txt
paste timedata2.txt Norwood2.txt > Norwood_A.$rundate1.txt
rm Norwood.nc
rm Norwood.txt Norwood2.txt
#=============================================================================
#Jaffna
ncks -d west_east,19 -d south_north,146 difference.nc Jaffna.nc
ncdump -v PRCP Jaffna.nc > Jaffna.txt
sed '/netcdf Jaffna {/,/ PRCP =/d' Jaffna.txt > Jaffna2.txt
paste timedata2.txt Jaffna2.txt > Jaffna_A.$rundate1.txt
rm Jaffna.nc
rm Jaffna.txt Jaffna2.txt
#=============================================================================
#Mahapallegama
ncks -d west_east,27 -d south_north,53 difference.nc Mahapallegama.nc
ncdump -v PRCP Mahapallegama.nc > Mahapallegama.txt
sed '/netcdf Mahapallegama {/,/ PRCP =/d' Mahapallegama.txt > Mahapallegama2.txt
paste timedata2.txt Mahapallegama2.txt > Mahapallegama_A.$rundate1.txt
rm Mahapallegama.nc
rm Mahapallegama.txt Mahapallegama2.txt
#=============================================================================
#Hingurana
ncks -d west_east,19 -d south_north,43 difference.nc Hingurana.nc
ncdump -v PRCP Hingurana.nc > Hingurana.txt
sed '/netcdf Hingurana {/,/ PRCP =/d' Hingurana.txt > Hingurana2.txt
paste timedata2.txt Hingurana2.txt > Hingurana_A.$rundate1.txt
rm Hingurana.nc 
rm Hingurana.txt Hingurana2.txt
#=============================================================================
#Kottawa
ncks -d west_east,16 -d south_north,42 difference.nc Kottawa.nc
ncdump -v PRCP Kottawa.nc > Kottawa.txt
sed '/netcdf Kottawa {/,/ PRCP =/d' Kottawa.txt > Kottawa2.txt
paste timedata2.txt Kottawa2.txt > Kottawa_A.$rundate1.txt
rm Kottawa.nc 
rm Kottawa.txt Kottawa2.txt
#=============================================================================
#Orugodawatta
ncks -d west_east,13 -d south_north,45 difference.nc Orugodawatta.nc
ncdump -v PRCP Orugodawatta.nc > Orugodawatta.txt
sed '/netcdf Orugodawatta {/,/ PRCP =/d' Orugodawatta.txt > Orugodawatta2.txt
paste timedata2.txt Orugodawatta2.txt > Orugodawatta_A.$rundate1.txt
rm Orugodawatta.nc
rm Orugodawatta.txt Orugodawatta2.txt
#=============================================================================
#Uduwawala
ncks -d west_east,40 -d south_north,59 difference.nc Uduwawala.nc
ncdump -v PRCP Uduwawala.nc > Uduwawala.txt
sed '/netcdf Uduwawala {/,/ PRCP =/d' Uduwawala.txt > Uduwawala2.txt
paste timedata2.txt Uduwawala2.txt > Uduwawala_A.$rundate1.txt
rm Uduwawala.nc
rm Uduwawala.txt Uduwawala2.txt
#=============================================================================
#Ibattara2
ncks -d west_east,15 -d south_north,44 difference.nc Ibattara2.nc
ncdump -v PRCP Ibattara2.nc > Ibattara2.txt
sed '/netcdf Ibattara2 {/,/ PRCP =/d' Ibattara2.txt > Ibattara22.txt
paste timedata2.txt Ibattara22.txt > Ibattara2_A.$rundate1.txt
rm Ibattara2.nc
rm Ibattara2.txt Ibattara22.txt
#=============================================================================
#Waga
ncks -d west_east,22 -d south_north,43 difference.nc Waga.nc
ncdump -v PRCP Waga.nc > Waga.txt
sed '/netcdf Waga {/,/ PRCP =/d' Waga.txt > Waga2.txt
paste timedata2.txt Waga2.txt > Waga_A.$rundate1.txt
rm Waga.nc
rm Waga.txt Waga2.txt 
#=============================================================================
#Ambewela
ncks -d west_east,47 -d south_north,43 difference.nc Ambewela.nc
ncdump -v PRCP Ambewela.nc > Ambewela.txt
sed '/netcdf Ambewela {/,/ PRCP =/d' Ambewela.txt > Ambewela2.txt
paste timedata2.txt Ambewela2.txt > Ambewela_A.$rundate1.txt
rm Ambewela.nc
rm Ambewela.txt Ambewela2.txt
#=============================================================================
#Mulleriyawa
ncks -d west_east,15 -d south_north,44 difference.nc Mulleriyawa.nc
ncdump -v PRCP Mulleriyawa.nc > Mulleriyawa.txt
sed '/netcdf Mulleriyawa {/,/ PRCP =/d' Mulleriyawa.txt > Mulleriyawa2.txt
paste timedata2.txt Mulleriyawa2.txt > Mulleriyawa_A.$rundate1.txt
rm Mulleriyawa.nc
rm Mulleriyawa.txt Mulleriyawa2.txt
#=============================================================================
#Dickoya
ncks -d west_east,40 -d south_north,41 difference.nc Dickoya.nc
ncdump -v PRCP Dickoya.nc > Dickoya.txt
sed '/netcdf Dickoya {/,/ PRCP =/d' Dickoya.txt > Dickoya2.txt
paste timedata2.txt Dickoya2.txt > Dickoya_A.$rundate1.txt
rm Dickoya.nc
rm Dickoya.txt Dickoya2.txt
#=============================================================================
#Malabe
ncks -d west_east,16 -d south_north,44 difference.nc Malabe.nc
ncdump -v PRCP Malabe.nc > Malabe.txt
sed '/netcdf Malabe {/,/ PRCP =/d' Malabe.txt > Malabe2.txt
paste timedata2.txt Malabe2.txt > Malabe_A.$rundate1.txt
rm Malabe.nc
rm Malabe.txt Malabe2.txt
#=============================================================================
#Mutwal
ncks -d west_east,12 -d south_north,46 difference.nc Mutwal.nc
ncdump -v PRCP Mutwal.nc > Mutwal.txt
sed '/netcdf Mutwal {/,/ PRCP =/d' Mutwal.txt > Mutwal2.txt
paste timedata2.txt Mutwal2.txt > Mutwal_A.$rundate1.txt
rm Mutwal.nc
rm Mutwal.txt Mutwal2.txt
#=============================================================================
#Urumewella
ncks -d west_east,30 -d south_north,50 difference.nc Urumewella.nc
ncdump -v PRCP Urumewella.nc > Urumewella.txt
sed '/netcdf Urumewella {/,/ PRCP =/d' Urumewella.txt > Urumewella2.txt
paste timedata2.txt Urumewella2.txt > Urumewella_A.$rundate1.txt
rm Urumewella.nc
rm Urumewella.txt Urumewella2.txt
#rm *.tmp
rm precip.nc
rm first.nc
rm second.nc
rm difference.nc timedata.txt timedata2.txt
#cd ../NCLoutput_A
#mv $file1 d03_A.nc
#python plot_rain_A.py
#mv *.txt /mnt/disks/wrf-mod/STATIONS_$rundate1
#exit;
