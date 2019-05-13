# Download Rain cell files, download Mean-ref files.
# Check Rain fall files are available in the bucket.
import sys
import os
import json
import getopt
from datetime import datetime, timedelta
from curwmysqladapter import MySQLAdapter
import pandas as pd
import numpy as np


def get_forecast_timeseries(my_adapter, my_event_id, my_opts):
    existing_timeseries = my_adapter.retrieve_timeseries([my_event_id], my_opts)
    new_timeseries = []
    if len(existing_timeseries) > 0 and len(existing_timeseries[0]['timeseries']) > 0:
        existing_timeseries = existing_timeseries[0]['timeseries']
        for ex_step in existing_timeseries:
            if ex_step[0] - ex_step[0].replace(minute=0, second=0, microsecond=0) > timedelta(minutes=30):
                new_timeseries.append(
                    [ex_step[0].replace(minute=0, second=0, microsecond=0) + timedelta(hours=1), ex_step[1]])
            else:
                new_timeseries.append(
                    [ex_step[0].replace(minute=0, second=0, microsecond=0), ex_step[1]])

    return new_timeseries


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_tidal_data(db_adapter,  my_opts):
    event_id = 'ebcc2df39aea35de15cca81bc5f15baffd94bcebf3f169add1fd43ee1611d367'
    ts = get_forecast_timeseries(db_adapter, event_id, my_opts)
    if len(ts) > 0:
        ts_df = pd.DataFrame(data=ts, columns=['time', 'value']).set_index(keys='time')
        print(ts_df)
        ts_df.to_csv('May_tidal_data.csv')


def create_outflow(dir_path, run_date, run_time, forward = 3, backward = 2):
    try:
        # run_date = datetime.now().strftime("%Y-%m-%d")
        # run_time = datetime.now().strftime("%H:00:00")
        # tag = ''
        # try:
        #     opts, args = getopt.getopt(sys.argv[1:], "hd:t:T:f:b:", [
        #         "help", "date=", "time=", "forward=", "backward=", "wrf-rf=", "wrf-kub=", "tag="
        #     ])
        # except getopt.GetoptError:
        #     sys.exit(2)
        # for opt, arg in opts:
        #     if opt in ("-h", "--help"):
        #         sys.exit()
        #     elif opt in ("-d", "--date"):
        #         run_date = arg # 2018-05-24
        #     elif opt in ("-t", "--time"):
        #         run_time = arg # 16:00:00
        #     elif opt in ("-f","--forward"):
        #         forward = arg
        #     elif opt in ("-b","--backward"):
        #         backward = arg
        #     elif opt in ("--wrf-rf"):
        #         RF_DIR_PATH = arg
        #     elif opt in ("--wrf-kub"):
        #         KUB_DIR_PATH = arg
        #     elif opt in ("-T", "--tag"):
        #         tag = arg
        #run_date = '2019-04-29'
        print("WrfTrigger run_date : ", run_date)
        print("WrfTrigger run_time : ", run_time)
        # backward = 2
        # forward = 3
        startDateTime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
        print("startDateTime : ", startDateTime)
        config_path = os.path.join(os.getcwd(), 'outflowdat', 'config.json')
        print('config_path : ', config_path)
        with open(config_path) as json_file:
            config_data = json.load(json_file)
            output_dir = dir_path
            inittidal_conf_path = os.path.join(os.getcwd(), 'outflowdat', 'INITTIDAL.CONF')

            CONTROL_INTERVAL = config_data["CONTROL_INTERVAL"]
            DAT_WIDTH = config_data["DAT_WIDTH"]
            TIDAL_FORECAST_ID = config_data["TIDAL_FORECAST_ID"]

            MYSQL_HOST = config_data['db_host']
            MYSQL_USER = config_data['db_user']
            MYSQL_DB = config_data['db_name']
            MYSQL_PASSWORD = config_data['db_password']

            adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
            try:
                hourly_outflow_file = os.path.join(output_dir, 'OUTFLOW.DAT')
                print("hourly_outflow_file : ", hourly_outflow_file)
                if not os.path.isfile(hourly_outflow_file):
                    opts = {
                        'from': (startDateTime - timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M:%S"),
                        'to': (startDateTime + timedelta(minutes=CONTROL_INTERVAL)).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    tidal_timeseries = get_forecast_timeseries(adapter, TIDAL_FORECAST_ID, opts)
                    if len(tidal_timeseries) > 0:
                        print('tidal_timeseries::', len(tidal_timeseries), tidal_timeseries[0], tidal_timeseries[-1])
                        f = open(hourly_outflow_file, 'w')
                        lines = []
                        print('Reading INIT TIDAL CONF...')
                        with open(inittidal_conf_path) as initTidalConfFile:
                            initTidalLevels = initTidalConfFile.readlines()
                            for initTidalLevel in initTidalLevels:
                                if len(initTidalLevel.split()):  # Check if not empty line
                                    lines.append(initTidalLevel)
                                    if initTidalLevel[0] == 'N':
                                        lines.append('{0} {1:{w}} {2:{w}}\n'.format('S', 0, 0, w=DAT_WIDTH))
                                        base_date_time = startDateTime.replace(minute=0, second=0, microsecond=0)
                                        for step in tidal_timeseries:
                                            hours_so_far = (step[0] - base_date_time)
                                            hours_so_far = 24 * hours_so_far.days + hours_so_far.seconds / (60 * 60)
                                            lines.append('{0} {1:{w}} {2:{w}{b}}\n'
                                                         .format('S', int(hours_so_far), float(step[1]), b='.2f',
                                                                 w=DAT_WIDTH))
                        f.writelines(lines)
                        f.close()
                        print('Finished writing OUTFLOW.DAT')
                    else:
                        print('No data found for tidal timeseries: ', tidal_timeseries)
                        sys.exit(1)
                adapter.close()
            except Exception as ex:
                adapter.close()
                print("Download required files|Exception: ", str(ex))
    except Exception as e:
        print("Exception occurred: ", str(e))
