# Download Rain cell files, download Mean-ref files.
# Check Rain fall files are available in the bucket.
import sys
import os
import json
import getopt
import copy
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from curwmysqladapter import MySQLAdapter

RUN_NAME = 'hourly_run'


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


def format_timeseries(adapter, event_meta, opts):
    event_id = adapter.get_event_id(copy.deepcopy(event_meta))
    ts = np.array(adapter.retrieve_timeseries([event_id], opts)[0]['timeseries'])
    ts_df = pd.DataFrame(data=ts, columns=['time', 'value']).set_index(keys='time')
    return ts_df


def get_discharge_data(my_adapter, startDateTime):
    observed_end_time = startDateTime.strftime('%Y-%m-%d %H:%M:%S')
    observed_start_time = (datetime.strptime(startDateTime.strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S') - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
    print('[observed_start_time, observed_end_time]', [observed_start_time, observed_end_time])
    observed_meta = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': 'Forecast-0-d',
        'source': 'HEC-HMS',
        'name': RUN_NAME,
    }
    obs_opts = {
        'from': observed_start_time,
        'to': observed_end_time,
    }
    obs_timeseries = format_timeseries(my_adapter, observed_meta, obs_opts)

    fcst_d0_start = startDateTime.strftime('%Y-%m-%d %H:%M:%S')
    fcst_d0_end = (datetime.strptime(startDateTime.strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S') + timedelta(
            days=1)).strftime('%Y-%m-%d %H:%M:%S')
    fcst_d0_meta = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': 'Forecast-0-d',
        'source': 'HEC-HMS',
        'name': RUN_NAME,
    }
    fcst_d0_opts = {
        'from': fcst_d0_start,
        'to': fcst_d0_end,
    }
    print('fcst_d0_opts', fcst_d0_opts)
    fcst_d0_timeseries = format_timeseries(my_adapter, fcst_d0_meta, fcst_d0_opts)

    fcst_d1_start = fcst_d0_end
    fcst_d1_end = (datetime.strptime(fcst_d0_end, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    fcst_d1_meta = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': 'Forecast-1-d-after',
        'source': 'HEC-HMS',
        'name': RUN_NAME,
    }
    fcst_d1_opts = {
        'from': fcst_d1_start,
        'to': fcst_d1_end,
    }
    print('fcst_d1_opts', fcst_d1_opts)
    fcst_d1_timeseries = format_timeseries(my_adapter, fcst_d1_meta, fcst_d1_opts)

    fcst_d2_start = fcst_d1_end
    fcst_d2_end = (datetime.strptime(fcst_d1_end, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    fcst_d2_meta = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': 'Forecast-2-d-after',
        'source': 'HEC-HMS',
        'name': RUN_NAME,
    }
    fcst_d2_opts = {
        'from': fcst_d2_start,
        'to': fcst_d2_end,
    }
    print('fcst_d2_opts', fcst_d2_opts)
    fcst_d2_timeseries = format_timeseries(my_adapter, fcst_d2_meta, fcst_d2_opts)

    fcst_d3_start = fcst_d2_end
    fcst_d3_end = (datetime.strptime(fcst_d2_end, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    fcst_d3_meta = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': 'Forecast-3-d-after',
        'source': 'HEC-HMS',
        'name': RUN_NAME,
    }
    fcst_d3_opts = {
        'from': fcst_d3_start,
        'to': fcst_d3_end,
    }
    print('fcst_d3_opts', fcst_d3_opts)
    fcst_d3_timeseries = format_timeseries(my_adapter, fcst_d3_meta, fcst_d3_opts)

    fcst_d4_start = fcst_d3_end
    fcst_d4_end = (datetime.strptime(fcst_d3_end, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    fcst_d4_meta = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': 'Forecast-4-d-after',
        'source': 'HEC-HMS',
        'name': RUN_NAME,
    }
    fcst_d4_opts = {
        'from': fcst_d4_start,
        'to': fcst_d4_end,
    }
    print('fcst_d4_opts', fcst_d4_opts)
    fcst_d4_timeseries = format_timeseries(my_adapter, fcst_d4_meta, fcst_d4_opts)

    fcst_d5_start = fcst_d4_end
    fcst_d5_end = (datetime.strptime(fcst_d4_end, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    fcst_d5_meta = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': 'Forecast-5-d-after',
        'source': 'HEC-HMS',
        'name': RUN_NAME,
    }
    fcst_d5_opts = {
        'from': fcst_d5_start,
        'to': fcst_d5_end,
    }
    print('fcst_d5_opts', fcst_d5_opts)
    fcst_d5_timeseries = format_timeseries(my_adapter, fcst_d5_meta, fcst_d5_opts)
    complete_df = pd.concat([obs_timeseries, fcst_d0_timeseries, fcst_d1_timeseries, fcst_d2_timeseries, fcst_d3_timeseries, fcst_d4_timeseries, fcst_d5_timeseries])
    return complete_df


def update_initial_water_levels(OBSERVED_WL_IDS,my_adapter, startDateTime):
    initial_water_levels = []
    with my_adapter.connection.cursor() as cursor:
        for water_level_json in OBSERVED_WL_IDS:
            if water_level_json is not None:
                cell_id = water_level_json["cell_id"]
                custom_sql = "select value from curw.data where id='" + water_level_json[
                    "water_level_id"] + "' and time <= '" + startDateTime + "' ORDER BY time DESC limit 1"
                cursor.execute(custom_sql)
                water_level = cursor.fetchone()[0]
                print('water_level: ', type(water_level))
                row = 'R         ' + cell_id + '          ' + str(water_level) + '\n'
                initial_water_levels.append(row)
                print('update_initial_water_levels|row : ', row)
    constant_row = 'R         3559          6.6\n'
    initial_water_levels.append(constant_row)
    return initial_water_levels


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def create_inflow(dir_path, run_date, run_time):
    try:
        # run_date = datetime.now().strftime("%Y-%m-%d")
        # run_time = datetime.now().strftime("%H:00:00")
        # FLO-2D parameters
        IHOURDAILY = 0  # 0-hourly interval, 1-daily interval
        IDEPLT = 0  # Set to 0 on running with Text mode. Otherwise cell number e.g. 8672
        IFC = 'C'  # foodplain 'F' or a channel 'C'
        INOUTFC = 0  # 0-inflow, 1-outflow
        KHIN = 8655  # inflow nodes
        HYDCHAR = 'H'  # Denote line of inflow hydrograph time and discharge pairs
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
        config_path = os.path.join(os.getcwd(), 'inflowdat', 'config.json')
        print('config_path : ', config_path)
        with open(config_path) as json_file:
            config_data = json.load(json_file)
            output_dir = dir_path
            inflow_file = config_data["inflow_file"]

            # CONTROL_INTERVAL = config_data["CONTROL_INTERVAL"]
            # CSV_NUM_METADATA_LINES = config_data["CSV_NUM_METADATA_LINES"]
            DAT_WIDTH = config_data["DAT_WIDTH"]
            OBSERVED_WL_IDS = config_data["OBSERVED_WL_IDS"]

            MYSQL_HOST = config_data['db_host']
            MYSQL_USER = config_data['db_user']
            MYSQL_DB = config_data['db_name']
            MYSQL_PASSWORD = config_data['db_password']

            adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
            try:
                #hourly_inflow_file_dir = os.path.join(output_dir, run_date, run_time)
                hourly_inflow_file = os.path.join(output_dir, inflow_file)
                #create_dir_if_not_exists(hourly_inflow_file_dir)
                print("hourly_outflow_file : ", hourly_inflow_file)
                if not os.path.isfile(hourly_inflow_file):
                    discharge_df = get_discharge_data(adapter, startDateTime)
                    print('discharge_df', discharge_df)
                    initial_water_levels = update_initial_water_levels(OBSERVED_WL_IDS, adapter, startDateTime.strftime("%Y-%m-%d %H:%M:%S"))
                    f = open(hourly_inflow_file, 'w')
                    line1 = '{0} {1:{w}{b}}\n'.format(IHOURDAILY, IDEPLT, b='d', w=DAT_WIDTH)
                    line2 = '{0} {1:{w}{b}} {2:{w}{b}}\n'.format(IFC, INOUTFC, KHIN, b='d', w=DAT_WIDTH)
                    line3 = '{0} {1:{w}{b}} {2:{w}{b}}\n'.format(HYDCHAR, 0.0, 0.0, b='.1f', w=DAT_WIDTH)
                    f.writelines([line1, line2, line3])
                    lines = [];
                    i = 1.0
                    for time, row in discharge_df.iterrows():
                        lines.append(
                            '{0} {1:{w}{b}} {2:{w}{b}}\n'.format(HYDCHAR, i, float(row["value"]), b='.1f', w=DAT_WIDTH))
                        i += 1.0
                    lines.extend(initial_water_levels)
                    f.writelines(lines)
                    f.close()
                adapter.close()
            except Exception as ex:
                adapter.close()
                print("Download required files|Exception: ", str(ex))
    except Exception as e:
        print("Exception occurred: ", str(e))
