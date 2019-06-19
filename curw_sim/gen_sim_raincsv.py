# Download Rain cell files, download Mean-ref files.
# Check Rain fall files are available in the bucket.
import csv
import sys
import os
import json
import getopt
from datetime import datetime, timedelta
from curwmysqladapter import MySQLAdapter
import pkg_resources
import pandas as pd
import logging
from curw_sim.get_obs_mean import get_observed_kub_mean, get_observed_klb_mean

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(filename='/home/uwcc-admin/hechms_hourly/Workflow/curw_sim/sim_rainfall.log',
                        level=logging.DEBUG,
                        format=LOG_FORMAT)
log = logging.getLogger()


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


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


try:
    run_date = datetime.now().strftime("%Y-%m-%d")
    run_time = datetime.now().strftime("%H:00:00")
    backward = 3
    forward = 3
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
    print("rainfall gen run_date : ", run_date)
    print("rainfall run_time : ", run_time)
    backward = int(backward)
    forward = int(forward)
    run_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    run_datetime_for_ts = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
    ts_start_datetime = run_datetime_for_ts - timedelta(days=backward)
    ts_end_datetime = run_datetime_for_ts + timedelta(days=forward)
    with open('config_rainfall.json') as json_file:
        config = json.load(json_file)
        wrf_data_dir = config["WRF_DATA_DIR"]
        if 'curw_db_config' in config:
            curw_db_config = config['curw_db_config']
        else:
            log.error('curw_db_config data not found')
        if 'fcst_db_configs' in config:
            fcst_db_configs = config['fcst_db_configs']
        else:
            log.error('fcst_db_configs data not found')
        klb_observed_stations = config['klb_obs_stations']
        kub_observed_stations = config['kub_obs_stations']

        kub_basin_extent = config['KELANI_UPPER_BASIN_EXTENT']
        klb_basin_extent = config['KELANI_LOWER_BASIN_EXTENT']

        kelani_lower_basin_shp = get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
        kelani_upper_basin_shp = get_resource_path('extraction/shp/kub-wgs84/kub-wgs84.shp')
        hourly_csv_file_dir = os.path.join(wrf_data_dir, run_date, run_time)
        create_dir_if_not_exists(hourly_csv_file_dir)
        raincsv_file_path = os.path.join(hourly_csv_file_dir, 'DailyRain.csv')
        if not os.path.isfile(raincsv_file_path):
            curw_adapter = MySQLAdapter(host=curw_db_config['host'], user=curw_db_config['user'],
                                        password=curw_db_config['password'], db=curw_db_config['db'])

            # fcst_adapter = MySQLAdapter(host=fcst_db_configs['host'], user=fcst_db_configs['user'],
            #                             password=fcst_db_configs['password'], db=fcst_db_configs['db'])

            obs_start = ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S')
            obs_end = run_datetime.strftime('%Y-%m-%d %H:%M:%S')
            print('[obs_start, obs_end] : ', [obs_start, obs_end])

            fcst_start = obs_end
            fcst_end = ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
            print('[fcst_start, fcst_end] : ', [fcst_start, fcst_end])
            forecast_duration = int((datetime.strptime(fcst_end, '%Y-%m-%d %H:%M:%S') - datetime.strptime(
                fcst_start, '%Y-%m-%d %H:%M:%S')).total_seconds() / (60 * 60))

            obs_kub_mean_df = get_observed_kub_mean(curw_adapter, kub_observed_stations, obs_start, obs_end)
            obs_klb_mean_df = get_observed_klb_mean(curw_adapter, klb_observed_stations, obs_start, obs_end)

            curw_adapter.close()
            # fcst_adapter.close()

            mean_df = pd.merge(obs_kub_mean_df, obs_klb_mean_df, on='time')
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
            fh = open(raincsv_file_path, 'a')
            csvWriter = csv.writer(fh, delimiter=',', quotechar='|')
            for t in range(forecast_duration-1):
                time_step = datetime.strptime(fcst_start, '%Y-%m-%d %H:%M:%S')+timedelta(hours=t+1)
                csvWriter.writerow([time_step.strftime('%Y-%m-%d %H:%M:%S'), '0.0', '0.0'])

except Exception as e:
    log.error('rainfall csv file generation error: {}'.format(str(e)))
