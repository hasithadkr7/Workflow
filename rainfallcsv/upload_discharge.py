import csv
import getopt
from datetime import datetime
import json
import sys
import os

import copy
from curwmysqladapter import MySQLAdapter

RUN_NAME = 'hourly_run'
CSV_NUM_METADATA_LINES = 2


def extractForecastTimeseries(timeseries, extract_date, extract_time, by_day=False):
    """
    Extracted timeseries upward from given date and time
    E.g. Consider timeseries 2017-09-01 to 2017-09-03
    date: 2017-09-01 and time: 14:00:00 will extract a timeseries which contains
    values that timestamp onwards
    """
    print('LibForecastTimeseries:: extractForecastTimeseries')
    if by_day:
        extract_date_time = datetime.strptime(extract_date, '%Y-%m-%d')
    else:
        extract_date_time = datetime.strptime('%s %s' % (extract_date, extract_time), '%Y-%m-%d %H:%M:%S')

    is_date_time = isinstance(timeseries[0][0], datetime)
    new_timeseries = []
    for i, tt in enumerate(timeseries):
        tt_date_time = tt[0] if is_date_time else datetime.strptime(tt[0], '%Y-%m-%d %H:%M:%S')
        if tt_date_time >= extract_date_time:
            new_timeseries = timeseries[i:]
            break
    return new_timeseries


def extractForecastTimeseriesInDays(timeseries):
    """
    Devide into multiple timeseries for each day
    E.g. Consider timeseries 2017-09-01 14:00:00 to 2017-09-03 23:00:00
    will devide into 3 timeseries with
    [
        [2017-09-01 14:00:00-2017-09-01 23:00:00],
        [2017-09-02 14:00:00-2017-09-02 23:00:00],
        [2017-09-03 14:00:00-2017-09-03 23:00:00]
    ]
    """
    new_timeseries = []
    if len(timeseries) > 0:
        group_timeseries = []
        is_date_time_obs = isinstance(timeseries[0][0], datetime)
        prev_date = timeseries[0][0] if is_date_time_obs else datetime.strptime(timeseries[0][0], '%Y-%m-%d %H:%M:%S')
        prev_date = prev_date.replace(hour=0, minute=0, second=0, microsecond=0)
        for tt in timeseries:
            # Match Daily
            tt_date_time = tt[0] if is_date_time_obs else datetime.strptime(tt[0], '%Y-%m-%d %H:%M:%S')
            if prev_date == tt_date_time.replace(hour=0, minute=0, second=0, microsecond=0) :
                group_timeseries.append(tt)
            else :
                new_timeseries.append(group_timeseries[:])
                group_timeseries = []
                prev_date = tt_date_time.replace(hour=0, minute=0, second=0, microsecond=0)
                group_timeseries.append(tt)
        if len(group_timeseries) > 0:
            new_timeseries.append(group_timeseries[:])
    return new_timeseries


def save_forecast_timeseries(my_adapter, timeseries, my_model_date, my_model_time, my_opts):
    print('CSVTODAT:: save_forecast_timeseries:: len', len(timeseries), my_model_date, my_model_time)
    forecast_timeseries = extractForecastTimeseries(timeseries, my_model_date, my_model_time, by_day=True)
    # print(forecastTimeseries[:10])
    extracted_timeseries = extractForecastTimeseriesInDays(forecast_timeseries)
    print("-----------------extracted_timeseries: ", extracted_timeseries[-1])
    print('Extracted forecast types # :', len(extracted_timeseries))
    # for ll in extractedTimeseries :
    #     print(ll)

    force_insert = my_opts.get('forceInsert', False)
    my_model_date_time = datetime.datetime.strptime('%s %s' % (my_model_date, my_model_time), '%Y-%m-%d %H:%M:%S')

    # TODO: Check whether station exist in Database
    run_name = my_opts.get('runName', RUN_NAME)
    less_char_index = run_name.find('<')
    greater_char_index = run_name.find('>')
    # if less_char_index > -1 and greater_char_index > -1 and less_char_index < greater_char_index :
    if -1 < less_char_index < greater_char_index > -1:
        start_str = run_name[:less_char_index]
        date_format_str = run_name[less_char_index + 1:greater_char_index]
        end_str = run_name[greater_char_index + 1:]
        try:
            date_str = my_model_date_time.strftime(date_format_str)
            run_name = start_str + date_str + end_str
        except ValueError:
            raise ValueError("Incorrect data format " + date_format_str)
    types = [
        'Forecast-0-d',
        'Forecast-1-d-after',
        'Forecast-2-d-after',
        'Forecast-3-d-after',
        'Forecast-4-d-after',
        'Forecast-5-d-after',
        'Forecast-6-d-after',
        'Forecast-7-d-after',
        'Forecast-8-d-after',
        'Forecast-9-d-after'
    ]
    meta_data = {
        'station': 'Hanwella',
        'variable': 'Discharge',
        'unit': 'm3/s',
        'type': types[0],
        'source': 'HEC-HMS',
        'name': run_name,
    }
    for index in range(0, min(len(types), len(extracted_timeseries))):
        meta_data_copy = copy.deepcopy(meta_data)
        meta_data_copy['type'] = types[index]
        event_id = my_adapter.get_event_id(meta_data_copy)
        if event_id is None:
            event_id = my_adapter.create_event_id(meta_data_copy)
            print('HASH SHA256 created: ', event_id)
        else:
            print('HASH SHA256 exists: ', event_id)
            if not force_insert:
                print('Timeseries already exists. User --force to update the existing.\n')
                continue
        row_count = my_adapter.insert_timeseries(event_id, extracted_timeseries[index], force_insert)
        print('%s rows inserted.\n' % row_count)


try:
    run_date = datetime.now().strftime("%Y-%m-%d")
    run_time = datetime.now().strftime("%H:00:00")
    forceInsert = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:t:", ["help", "date=", "time="])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            sys.exit()
        elif opt in ("-d", "--date"):
            run_date = arg # 2018-05-24
        elif opt in ("-t", "--time"):
            run_time = arg # 16:00:00
    print("run_date : ", run_date)
    print("run_time : ", run_time)
    with open('config.json') as json_file:
        config_data = json.load(json_file)
        data_dir = config_data['WRF_DATA_DIR']
        MYSQL_HOST = config_data['db_host']
        MYSQL_USER = config_data['db_user']
        MYSQL_DB = config_data['db_name']
        MYSQL_PASSWORD = config_data['db_password']
        adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
        try:
            hourly_csv_file_dir = os.path.join(data_dir, run_date, run_time)
            dischargecsv_file_path = os.path.join(hourly_csv_file_dir, 'DailyDischarge.csv')
            if os.path.isfile(dischargecsv_file_path):
                print('Open Discharge CSV ::', dischargecsv_file_path)
                csvReader = csv.reader(open(dischargecsv_file_path, 'r'), delimiter=',', quotechar='|')
                csvList = list(csvReader)
                opts = {
                    'forceInsert': forceInsert,
                    'runName': RUN_NAME
                }
                save_forecast_timeseries(adapter, csvList[CSV_NUM_METADATA_LINES:], run_date, run_time, opts)
            adapter.close()
        except Exception as ex:
            adapter.close()
            print("Download required files|Exception: ", str(ex))
except Exception as e:
    print("Exception occurred: ", str(e))

