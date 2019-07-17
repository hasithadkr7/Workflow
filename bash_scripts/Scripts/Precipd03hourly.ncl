 ; Example script to produce plots for a WRF real-data run,
; with the ARW coordinate dynamics option.
; In this example we first get the entire field over time, which will
; make it easier to calculate tendencies

load "/usr/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "/usr/lib/ncarg/nclscripts/wrf/WRFUserARW.ncl"

begin
;
; The WRF ARW input file.
; This needs to have a ".nc" appended, so just do it.
;  diri = getenv("DIRI")
;  domain = getenv("DOMAIN")

;   Files = systemfunc("ls "+diri+"/wrfout_d0"+domain+"*00:00")
   Files = "./d03.nc"
   print(Files)

; We generate plots, but what kind do we prefer?
; type = "x11"
; type = "pdf"
  type = "ps"
; type = "ncgm"

; Set some basic resources
  res = True
  res@MainTitle = "REAL-TIME WRF"

  pltres = True
  mpres = True
  mpres@mpGeophysicalLineColor = "Black"
  mpres@mpNationalLineColor = "Black"
  mpres@mpUSStateLineColor = "Black"
  mpres@mpGridLineColor = "Black"
  mpres@mpLimbLineColor = "Black"
  mpres@mpPerimLineColor = "Black"
  mpres@mpFillOn = True
  mpres@mpFillDrawOrder = "PreDraw"
  mpres@mpLandFillColor = "DarkOliveGreen3"
  mpres@mpOceanFillColor = -1
  mpres@mpInlandWaterFillColor = -1

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; What times and how many time steps are in the data set?
  ntimes = dimsizes(Files) ; number of times in the file

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  times = new(ntimes,string)
  a = addfile(Files(0)+".nc","r")
  times(0) = wrf_user_list_times(a) ; get times in the file

  mpres@mpDataBaseVersion = "Ncarg4_1" ; higher res data base
  if (a@GRID_ID .ge. 3) then
    mpres@mpDataBaseVersion = "HighRes"
  end if

  do it = 1,ntimes-1,1

    a = addfile(Files(it)+".nc","r")
    a_last = addfile(Files(it-1)+".nc","r")
    ff = floattointeger(a->XTIME/60.)

    PlotName = "pp_d0"+domain+"_"+sprinti("%0.2i",ff)
    print("PlotName: "+PlotName)
    wks = gsn_open_wks(type,PlotName)
    colors = (/"white","black","white","yellow","orange","DarkOrange",\
             "OrangeRed","Red1","Red2","Red4","DarkOrchid1","purple",\
             "MediumPurple3","Blue2","Blue4","DarkOliveGreen3"/)

    gsn_define_colormap(wks, colors)

    times(it) = wrf_user_list_times(a) ; get times in the file
    print("Working on time: " + times(0) )
    res@TimeLabel = times(it) ; Set Valid time to use on plots
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; First get the variables we will need

    slp = wrf_user_getvar(a,"slp",0) ; slp
    wrf_smooth_2d( slp, 3 ) ; smooth slp

  ; Get non-convective, convective
  ; Calculate total precipitation
    rain_exp = a->RAINNC(0,:,:)
    rain_con = a->RAINC(0,:,:)

    rain_exp = rain_exp - a_last->RAINNC(0,:,:)
    rain_con = rain_con - a_last->RAINC(0,:,:)
    rain_tot = rain_exp + rain_con
    rain_tot@description = "Total Precipitation"

; Plotting options for Sea Level Pressure
    opts_psl = res
    opts_psl@ContourParameters = (/ 900., 1100., 2. /)
    opts_psl@cnLineColor = "Blue"
    opts_psl@cnInfoLabelOn = False
    opts_psl@cnLineLabelFontHeightF = 0.01
    opts_psl@cnLineLabelPerimOn = False
    opts_psl@gsnContourLineThicknessesScale = 1.5
    contour_psl = wrf_contour(a,wks,slp,opts_psl)
    delete(opts_psl)

   ; Plotting options for Precipitation
    opts_r = res
    opts_r@UnitLabel = "mm"
    opts_r@cnLevelSelectionMode = "ExplicitLevels"
    opts_r@cnLevels = (/ .1, .5, 1., 2., 4., 8. /)
    opts_r@cnFillColors = (/"transparent","Yellow","orange",\
                                   "DarkOrange",\
                                   "OrangeRed","Red1","Violet"/)
    opts_r@cnInfoLabelOn = False
    opts_r@cnConstFLabelOn = False
    opts_r@cnFillOn = True
 

   ; Total Precipitation (color fill)
;; contour_tot = wrf_contour(a,wks, rain_tot, opts_r)
 
   ; Precipitation Tendencies
    opts_r@SubFieldTitle = "from " + times(it-1) + " to " + times(it)
 
    contour_tend = wrf_contour(a,wks, rain_tot,opts_r) ; total (color)
    delete(opts_r)

   ; MAKE PLOTS

    plot = wrf_map_overlays(a,wks,(/contour_tend,contour_psl/),pltres,mpres)

    delete(a)
    delete(a_last)

  end do ; END OF TIME LOOP

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

end 
