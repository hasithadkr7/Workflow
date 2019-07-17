#=============================================================================
#!/bin/bash
# gsmap.sh
# Download, compute and plot the rainfall from GSMAP
#
# J.F. Vuillaume 2018
#
#=============================================================================
cd /mnt/disks/wrf-mod/
echo "Running gsmap download and post-treatment"
date -u +"%Y%m%d"
time_stamp=$(date -u +%Y%m%d)
mkdir -p "GSMAP_${time_stamp}"
cd "GSMAP_${time_stamp}"

#Load a complete day
wget --user rainmap --password Niskur+1404 ftp://hokusai.eorc.jaxa.jp/realtime/latest/gsmap_nrt.*

for i in *.gz; do
    gunzip "$i"
done

#Create a ctl for each file
for i in *.dat; do
cat << EOF > ${i%.*}.ctl
DSET ^$i
TITLE GSMaP_MVK 0.1deg hourly (V4.8.4)
UNDEF -999.9
OPTIONS YREV LITTLE_ENDIAN TEMPLATE
XDEF 3600 LINEAR    0.05 0.1
YDEF 1200 LINEAR  -59.95 0.1
zdef 1 levels 1000
tdef 6000 linear 00:00z1jul2005 1hr
VARS 1
precip           0 99 hourly precip(mm/hr)
ENDVARS
EOF
done

#Use grads convert to netcdf
for i in *.ctl; do
cat << EOF > ${i%.*}.gs
'reinit'
'open ${i%.*}.ctl'
'rain=precip'
'set sdfwrite -flt ${i%.*}.nc'
'sdfwrite rain'
'clear sdfwrite'
EOF
done

for i in *.gs; do
    grads -b -lcx $i
done
echo "Plot 1h and 24h cumulative"
cp /mnt/disks/wrf-mod/Scripts/Plot_24h_nc_v3.py .
python Plot_24h_nc_v3.py
mv *.png /mnt/disks/wrf-mod/Plots/
