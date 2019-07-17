#=============================================================================
#!/bin/bash
# wt.sh
# Download gfs data at 00, 24, 48 and 72h GFS forecast
# Compute weather type
# J.F. Vuillaume 2018
#
#=============================================================================
cd /mnt/disks/wrf-mod/
time_stamp=$(date -u +%Y%m%d)
mkdir -p "WT_${time_stamp}"
cd "WT_${time_stamp}"

cp /mnt/disks/wrf-mod/Data/GFS_18/*000 ./gfs.t18z.pgrb2.0p50.f000.grb2
cp /mnt/disks/wrf-mod/Data/GFS_18/*024 ./gfs.t18z.pgrb2.0p50.f024.grb2
cp /mnt/disks/wrf-mod/Data/GFS_18/*048 ./gfs.t18z.pgrb2.0p50.f048.grb2
cp /mnt/disks/wrf-mod/Data/GFS_18/*072 ./gfs.t18z.pgrb2.0p50.f072.grb2

/mnt/disks/wrf-mod/Scratch_Jean/Run_daily/grib2/wgrib2/./wgrib2 gfs.t18z.pgrb2.0p50.f000.grb2 -netcdf gfs.t18z.pgrb2.0p50.f000.nc
/mnt/disks/wrf-mod/Scratch_Jean/Run_daily/grib2/wgrib2/./wgrib2 gfs.t18z.pgrb2.0p50.f024.grb2 -netcdf gfs.t18z.pgrb2.0p50.f024.nc
/mnt/disks/wrf-mod/Scratch_Jean/Run_daily/grib2/wgrib2/./wgrib2 gfs.t18z.pgrb2.0p50.f048.grb2 -netcdf gfs.t18z.pgrb2.0p50.f048.nc
/mnt/disks/wrf-mod/Scratch_Jean/Run_daily/grib2/wgrib2/./wgrib2 gfs.t18z.pgrb2.0p50.f072.grb2 -netcdf gfs.t18z.pgrb2.0p50.f072.nc

cp ../Scripts/WT_* .
python WT_00.py
python WT_24.py
python WT_48.py
python WT_72.py
rm *.nc *.grb2

FT=$(cat WT_24.txt)
pbl=6
cu=1
mp=3
ra=1
echo $FT
if [ ${FT} = "C" ] ; then pbl=6;cu=14;mp=3,ra=1 ; fi
if [ ${FT} = "H" ] ; then pbl=2;cu=1;mp=4,ra=4 ; fi  #mp=14 was better but too slow 
if [ ${FT} = "NW" ] ; then pbl=6; cu=1; mp=4; ra=1 ; fi
if [ ${FT} = "SW" ] ; then pbl=1; cu=2; mp=4; ra=1; fi
if [ ${FT} = "W" ] ; then pbl=1; cu=1; mp=4; ra=4; fi
cat << EOF > namelist.input
&time_control
 run_days                            = 3,
 run_hours                           = 0,
 run_minutes                         = 0,
 run_seconds                         = 0,
 start_year                          = YYYY1, YYYY1,  YYYY1,
 start_month                         = MM1, MM1,  MM1,
 start_day                           = DD1, DD1,  DD1,
 start_hour                          = 00,   00,   00,
 start_minute                        = 00,   00,   00,
 start_second                        = 00,   00,   00,
 end_year                            = YYYY2, YYYY2,  YYYY2,
 end_month                           = MM2, MM2,  MM2,
 end_day                             = DD2, DD2,  DD2,
 end_hour                            = 00,   00,   00,
 end_minute                          = 00,   00,   00,
 end_second                          = 00,   00,   00,
 interval_seconds                    = 10800
 input_from_file                     = .true.,.true.,.true.,
 history_interval                    = 180,  60,   60,
 frames_per_outfile                  = 1000, 1000, 1000,
 restart                             = .false.,
 restart_interval                    = 5000,
 io_form_history                     = 2
 io_form_restart                     = 2
 io_form_input                       = 2
 io_form_boundary                    = 2
 debug_level                         = 0
 /

 &domains
 time_step                           = 180,
 time_step_fract_num                 = 0,
 time_step_fract_den                 = 1,
 max_dom                             = 3,
 e_we                                = 80,    103,   100,
 e_sn                                = 90,    121,    163,
 e_vert                              = 30,    30,    30,
 p_top_requested                     = 5000,
 num_metgrid_levels                  = 32,
 num_metgrid_soil_levels             = 4,
 dx                                  = 27000, 9000,  3000,
 dy                                  = 27000, 9000,  3000,
 grid_id                             = 1,     2,     3,
 parent_id                           = 1,     1,     2,
 i_parent_start                      = 1,     24,    35,
 j_parent_start                      = 1,     26,    35,
 parent_grid_ratio                   = 1,     3,     3,
 parent_time_step_ratio              = 1,     3,     3,
 feedback                            = 1,
 smooth_option                       = 0
 /

 &physics
 mp_physics                          = ${mp},     ${mp},     ${mp},
 ra_lw_physics                       = ${ra},     ${ra},     ${ra},
 ra_sw_physics                       = ${ra},     ${ra},     ${ra},
 radt                                = 30,    10,    10,
 sf_sfclay_physics                   = 6,     6,     6,
 sf_surface_physics                  = 2,     2,     2,
 bl_pbl_physics                      = ${pbl},     ${pbl},     ${pbl},
 bldt                                = 0,     0,     0,
 cu_physics                          = ${cu},     ${cu},     ${cu},
 cudt                                = 5,     5,     5,
 isfflx                              = 1,
 ifsnow                              = 0,
 icloud                              = 1,
 surface_input_source                = 1,
 num_soil_layers                     = 4,
 sf_urban_physics                    = 0,     0,     0,
 /

 &fdda
 /

 &dynamics
 w_damping                           = 0,
 diff_opt                            = 1,
 km_opt                              = 4,
 diff_6th_opt                        = 0,      0,      0,
 diff_6th_factor                     = 0.12,   0.12,   0.12,
 base_temp                           = 290.
 damp_opt                            = 0,
 zdamp                               = 5000.,  5000.,  5000.,
 dampcoef                            = 0.2,    0.2,    0.2
 khdif                               = 0,      0,      0,
 kvdif                               = 0,      0,      0,
 non_hydrostatic                     = .true., .true., .true.,
 moist_adv_opt                       = 1,      1,      1,
 scalar_adv_opt                      = 1,      1,      1,
 /

 &bdy_control
 spec_bdy_width                      = 5,
 spec_zone                           = 1,
 relax_zone                          = 4,
 specified                           = .true., .false.,.false.,
 nested                              = .false., .true., .true.,
 /

 &grib2
 /

EOF
