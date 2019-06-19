# Download Rain cell files, download Mean-ref files.
# Check Rain fall files are available in the bucket.
import csv
import fnmatch
import sys
import os
import json
import getopt
from datetime import datetime, timedelta
from google.cloud import storage
from curwmysqladapter import MySQLAdapter
import numpy as np
import pkg_resources
from netCDF4 import Dataset
from shapely.geometry import Point, shape
import shapefile
import pandas as pd
from rainfallcsv.get_obs_mean import get_observed_kub_mean, get_observed_klb_mean
# from .get_obs_mean import get_observed_kub_mean, get_observed_klb_mean
# from get_obs_mean import get_observed_kub_mean, get_observed_klb_mean


def get_resource_path(resource):
    res = pkg_resources.resource_filename(__name__, resource)
    if os.path.exists(res):
        return res
    else:
        raise UnableFindResource(resource)


class UnableFindResource(Exception):
    def __init__(self, res):
        Exception.__init__(self, 'Unable to find %s' % res)


class CurwObservationException(Exception):
    pass


def usage():
    usage_text = """
Usage: ./CSVTODAT.py [-d YYYY-MM-DD] [-t HH:MM:SS] [-h]

-h  --help          Show usage
-d  --date          Date in YYYY-MM-DD. 
-t  --time          Time in HH:00:00.
-f  --forward       Future day count
-b  --backward      Past day count
-T  --tag           Tag to differential simultaneous Forecast Runs E.g. wrf1, wrf2 ...
    --wrf-rf        Path of WRF Rf(Rainfall) Directory. Otherwise using the `RF_DIR_PATH` from CONFIG.json
    --wrf-kub       Path of WRF kelani-upper-basin(KUB) Directory. Otherwise using the `KUB_DIR_PATH` from CONFIG.json
"""
    print(usage_text)


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def extract_variables(nc_f, var_list, lat_min, lat_max, lon_min, lon_max, lat_var='XLAT', lon_var='XLONG',
                      time_var='Times'):
    """
    extract variables from a netcdf file
    :param nc_f:
    :param var_list: comma separated string for variables / list of strings
    :param lat_min:
    :param lat_max:
    :param lon_min:
    :param lon_max:
    :param lat_var:
    :param lon_var:
    :param time_var:
    :return:
    variables dict {var_key --> var[time, lat, lon], xlat --> [lat], xlong --> [lon], times --> [time]}
    """
    if not os.path.exists(nc_f):
        raise IOError('File %s not found' % nc_f)

    nc_fid = Dataset(nc_f, 'r')

    times = np.array([''.join([y.decode() for y in x]) for x in nc_fid.variables[time_var][:]])
    lats = nc_fid.variables[lat_var][0, :, 0]
    lons = nc_fid.variables[lon_var][0, 0, :]

    lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
    lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

    vars_dict = {}
    if isinstance(var_list, str):
        var_list = var_list.replace(',', ' ').split()
    # var_list = var_list.replace(',', ' ').split() if isinstance(var_list, str) else var_list
    for var in var_list:
        vars_dict[var] = nc_fid.variables[var][:, lat_inds[0], lon_inds[0]]

    nc_fid.close()

    vars_dict[time_var] = times
    vars_dict[lat_var] = lats[lat_inds[0]]
    vars_dict[lon_var] = lons[lon_inds[0]]

    # todo: implement this archiving procedure
    # if output is not None:
    #     logging.info('%s will be archied to %s' % (nc_f, output))
    #     ncks_extract_variables(nc_f, var_str, output)

    return vars_dict


def extract_time_data(nc_f):
    nc_fid = Dataset(nc_f, 'r')
    times_len = len(nc_fid.dimensions['Time'])
    try:
        times = [''.join(x) for x in nc_fid.variables['Times'][0:times_len]]
    except TypeError:
        times = np.array([''.join([y.decode() for y in x]) for x in nc_fid.variables['Times'][:]])
    nc_fid.close()
    return times_len, times


def get_two_element_average(prcp, return_diff=True):
    avg_prcp = (prcp[1:] + prcp[:-1]) * 0.5
    if return_diff:
        return avg_prcp - np.insert(avg_prcp[:-1], 0, [0], axis=0)
    else:
        return avg_prcp


def extract_area_rf_series(nc_f, lat_min, lat_max, lon_min, lon_max):
    if not os.path.exists(nc_f):
        raise IOError('File %s not found' % nc_f)

    nc_fid = Dataset(nc_f, 'r')

    times_len, times = extract_time_data(nc_f)
    lats = nc_fid.variables['XLAT'][0, :, 0]
    lons = nc_fid.variables['XLONG'][0, 0, :]

    lon_min_idx = np.argmax(lons >= lon_min) - 1
    lat_min_idx = np.argmax(lats >= lat_min) - 1
    lon_max_idx = np.argmax(lons >= lon_max)
    lat_max_idx = np.argmax(lats >= lat_max)

    prcp = nc_fid.variables['RAINC'][:, lat_min_idx:lat_max_idx, lon_min_idx:lon_max_idx] + nc_fid.variables['RAINNC'][
                                                                                            :, lat_min_idx:lat_max_idx,
                                                                                            lon_min_idx:lon_max_idx]

    diff = get_two_element_average(prcp)

    nc_fid.close()

    return diff, lats[lat_min_idx:lat_max_idx], lons[lon_min_idx:lon_max_idx], np.array(times[0:times_len - 1])


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def is_inside_polygon(polygons, lat, lon):
    point = Point(lon, lat)
    for i, poly in enumerate(polygons.shapeRecords()):
        polygon = shape(poly.shape.__geo_interface__)
        if point.within(polygon):
            return 1
    return 0


def get_kub_forecasted_data(nc_f, basin_shp_file, kub_basin_extent, output_name, rain_dict, start_time='', end_time=''):
    lon_min, lat_min, lon_max, lat_max = kub_basin_extent

    nc_vars = extract_variables(nc_f, ['RAINC', 'RAINNC'], lat_min, lat_max, lon_min, lon_max)
    lats = nc_vars['XLAT']
    lons = nc_vars['XLONG']
    prcp = nc_vars['RAINC'] + nc_vars['RAINNC']
    times = nc_vars['Times']

    diff = get_two_element_average(prcp)

    polys = shapefile.Reader(basin_shp_file)
    rain_dict[output_name] = []
    for t in range(0, len(times) - 1):
        cnt = 0
        rf_sum = 0.0
        for y in range(0, len(lats)):
            for x in range(0, len(lons)):
                if is_inside_polygon(polys, lats[y], lons[x]):
                    cnt = cnt + 1
                    rf_sum = rf_sum + diff[t, y, x]
        mean_rf = rf_sum / cnt

        t_str = (
            datetime_utc_to_lk(datetime.strptime(times[t], '%Y-%m-%d_%H:%M:%S'),
                                     shift_mins=30)).strftime('%Y-%m-%d %H:%M:%S')
        rain_dict[output_name].append([t_str, mean_rf])


def get_klb_forecasted_data(nc_f, basin_shp_file, klb_basin_extent, output_name,rain_dict, start_time='', end_time=''):
    lon_min, lat_min, lon_max, lat_max = klb_basin_extent

    nc_vars = extract_variables(nc_f, ['RAINC', 'RAINNC'], lat_min, lat_max, lon_min, lon_max)
    lats = nc_vars['XLAT']
    lons = nc_vars['XLONG']
    prcp = nc_vars['RAINC'] + nc_vars['RAINNC']
    times = nc_vars['Times']

    diff = get_two_element_average(prcp)

    polys = shapefile.Reader(basin_shp_file)
    rain_dict[output_name] = []
    for t in range(0, len(times) - 1):
        cnt = 0
        rf_sum = 0.0
        for y in range(0, len(lats)):
            for x in range(0, len(lons)):
                if is_inside_polygon(polys, lats[y], lons[x]):
                    cnt = cnt + 1
                    rf_sum = rf_sum + diff[t, y, x]
        mean_rf = rf_sum / cnt

        t_str = (
            datetime_utc_to_lk(datetime.strptime(times[t], '%Y-%m-%d_%H:%M:%S'),
                                     shift_mins=30)).strftime('%Y-%m-%d %H:%M:%S')
        rain_dict[output_name].append([t_str, mean_rf])


def download_netcdf(download_location, net_cdf_file_name, key_file, bucket_name):
    try:
        client = storage.Client.from_service_account_json(key_file)
        bucket = client.get_bucket(bucket_name)
        prefix = initial_path_prefix + '_'
        blobs = bucket.list_blobs(prefix=prefix)
        print("prefix : ", prefix)
        print("net_cdf_file_name : ", net_cdf_file_name)
        for blob in blobs:
            if fnmatch.fnmatch(blob.name, "*" + net_cdf_file_name):
                print(blob.name)
                directory = download_location + "/" + net_cdf_file_name
                if not os.path.exists(download_location):
                    os.makedirs(download_location)
                    print('download_netcdf|download_location: ', download_location)
                blob.download_to_filename(directory)
    except Exception as e:
        print('Wrf net cdf file download failed.')
        print(str(e))


try:
    run_date = datetime.now().strftime("%Y-%m-%d")
    run_time = datetime.now().strftime("%H:00:00")
    tag = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:t:T:f:b:", [
            "help", "date=", "time=", "forward=", "backward=", "wrf-rf=", "wrf-kub=", "tag="
        ])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-d", "--date"):
            run_date = arg # 2018-05-24
        elif opt in ("-t", "--time"):
            run_time = arg # 16:00:00
        elif opt in ("-f","--forward"):
            forward = arg
        elif opt in ("-b","--backward"):
            backward = arg
        elif opt in ("--wrf-rf"):
            RF_DIR_PATH = arg
        elif opt in ("--wrf-kub"):
            KUB_DIR_PATH = arg
        elif opt in ("-T", "--tag"):
            tag = arg
    #run_date = '2019-04-29'
    print("WrfTrigger run_date : ", run_date)
    print("WrfTrigger run_time : ", run_time)
    backward = 2
    forward = 3
    start_ts_lk = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    start_ts_lk = start_ts_lk.strftime('%Y-%m-%d_%H:00')  # '2018-05-24_08:00'
    print("WrfTrigger start_ts_lk : ", start_ts_lk)
    duration_days = (int(backward), int(forward))
    print("WrfTrigger duration_days : ", duration_days)
    with open('config.json') as json_file:
        config_data = json.load(json_file)
        key_file = config_data["KEY_FILE_PATH"]
        bucket_name = config_data["BUCKET_NAME"]
        initial_path_prefix = config_data["INITIAL_PATH_PREFIX"]
        net_cdf_file_format = config_data["NET_CDF_FILE"]
        wrf_data_dir = config_data["WRF_DATA_DIR"]
        net_cdf_date = datetime.strptime(run_date, '%Y-%m-%d') - timedelta(hours=24)
        net_cdf_date = net_cdf_date.strftime("%Y-%m-%d")
        download_location = wrf_data_dir + run_date
        print("download_location : ", download_location)
        print("net_cdf_date : ", net_cdf_date)

        MYSQL_HOST = config_data['db_host']
        MYSQL_USER = config_data['db_user']
        MYSQL_DB = config_data['db_name']
        MYSQL_PASSWORD = config_data['db_password']

        klb_observed_stations = config_data['klb_obs_stations']
        kub_observed_stations = config_data['kub_obs_stations']

        kub_basin_extent = config_data['KELANI_UPPER_BASIN_EXTENT']
        klb_basin_extent = config_data['KELANI_LOWER_BASIN_EXTENT']

        klb_points = get_resource_path('extraction/local/kelani_basin_points_250m.txt')
        kelani_lower_basin_shp = get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
        kelani_upper_basin_shp = get_resource_path('extraction/shp/kub-wgs84/kub-wgs84.shp')

        adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
        name_list = net_cdf_file_format.split("-")
        net_cdf_file_name = name_list[0] + "_" + net_cdf_date + "_" + name_list[1]
        try:
            net_cdf_file_path = download_location + "/" + net_cdf_file_name
            print("net_cdf_file_path : ", net_cdf_file_path)
            if not os.path.isfile(net_cdf_file_path):
                download_netcdf(download_location, net_cdf_file_name, key_file, bucket_name)
            if os.path.isfile(net_cdf_file_path):
                hourly_csv_file_dir = os.path.join(wrf_data_dir, run_date, run_time)
                create_dir_if_not_exists(hourly_csv_file_dir)
                raincsv_file_path = os.path.join(hourly_csv_file_dir, 'DailyRainObs.csv')
                if not os.path.isfile(raincsv_file_path):
                    rain_dict = {}
                    get_kub_forecasted_data(net_cdf_file_path, kelani_upper_basin_shp, kub_basin_extent, "kub_mean", rain_dict)
                    get_klb_forecasted_data(net_cdf_file_path, kelani_lower_basin_shp, klb_basin_extent, "klb_mean", rain_dict)
                    fcst_kub_mean_df = pd.DataFrame(data=rain_dict['kub_mean'], columns=['time', 'value']).set_index(keys='time')
                    fcst_klb_mean_df = pd.DataFrame(data=rain_dict['klb_mean'], columns=['time', 'value']).set_index(keys='time')
                    # print('fcst_kub_mean_df : ', fcst_kub_mean_df)
                    # print('fcst_klb_mean_df : ', fcst_klb_mean_df)
                    obs_ts_lk = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
                    obs_ts_lk = obs_ts_lk.strftime('%Y-%m-%d_00:00')
                    obs_start = datetime.strptime(obs_ts_lk, '%Y-%m-%d_%H:%M') - timedelta(days=duration_days[0])
                    obs_end = datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M')
                    print('[obs_start, obs_end] : ', [obs_start, obs_end])
                    obs_kub_mean_df = get_observed_kub_mean(adapter, kub_observed_stations, obs_start, obs_end)
                    obs_klb_mean_df = get_observed_klb_mean(adapter, klb_observed_stations, obs_start, obs_end)
                    # print('obs_kub_mean_df : ', obs_kub_mean_df)
                    # print('obs_klb_mean_df : ', obs_klb_mean_df)
                    kub_mean_df = pd.concat([obs_kub_mean_df, fcst_kub_mean_df])
                    klb_mean_df = pd.concat([obs_klb_mean_df, fcst_klb_mean_df])
                    #print('kub_mean_df : ', kub_mean_df)
                    #print('klb_mean_df : ', klb_mean_df)
                    mean_df = pd.merge(kub_mean_df, klb_mean_df, on='time')
                    print('mean_df : ', mean_df)
                    fh = open(raincsv_file_path, 'w')
                    csvWriter = csv.writer(fh, delimiter=',', quotechar='|')
                    # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
                    csvWriter.writerow(['Location Names', 'Awissawella', 'Colombo'])
                    csvWriter.writerow(['Location Ids', 'Awissawella', 'Colombo'])
                    csvWriter.writerow(['Time', 'Rainfall', 'Rainfall'])
                    fh.close()
                    with open(raincsv_file_path, 'a') as f:
                        mean_df.to_csv(f, header=False)
            adapter.close()
        except Exception as ex:
            adapter.close()
            print("Download required files|Exception: ", str(ex))
except Exception as e:
    print("Exception occurred: ", str(e))
