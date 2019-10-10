import json
import os
import re

import copy
from curwmysqladapter import MySQLAdapter
from datetime import datetime, timedelta

COMMON_DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
UTC_OFFSET = '+00:00:00'


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def get_water_level_of_channels(lines, channels=None):
    """
     Get Water Levels of given set of channels
    :param lines:
    :param channels:
    :return:
    """
    if channels is None:
        channels = []
    water_levels = {}
    for line in lines[1:]:
        if line == '\n':
            break
        v = line.split()
        if v[0] in channels:
            # Get flood level (Elevation)
            water_levels[v[0]] = v[5]
            # Get flood depth (Depth)
            # water_levels[int(v[0])] = v[2]
    return water_levels


def extractForecastTimeseries(timeseries, extract_date, extract_time, by_day=False):
    """
    Extracted timeseries upward from given date and time
    E.g. Consider timeseries 2017-09-01 to 2017-09-03
    date: 2017-09-01 and time: 14:00:00 will extract a timeseries which contains
    values that timestamp onwards
    """
    ##print('LibForecastTimeseries:: extractForecastTimeseries')
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
            if prev_date == tt_date_time.replace(hour=0, minute=0, second=0, microsecond=0):
                group_timeseries.append(tt)
            else:
                new_timeseries.append(group_timeseries[:])
                group_timeseries = []
                prev_date = tt_date_time.replace(hour=0, minute=0, second=0, microsecond=0)
                group_timeseries.append(tt)
        if len(group_timeseries) > 0:
            new_timeseries.append(group_timeseries[:])
    return new_timeseries


def save_forecast_timeseries(my_adapter, my_timeseries, my_model_date, my_model_time, my_opts):
    print('EXTRACTFLO2DWATERLEVEL:: save_forecast_timeseries >>', my_opts)

    # Convert date time with offset
    date_time = datetime.strptime('%s %s' % (my_model_date, my_model_time), COMMON_DATE_TIME_FORMAT)
    if 'utcOffset' in my_opts:
        date_time = date_time + my_opts['utcOffset']
        my_model_date = date_time.strftime('%Y-%m-%d')
        my_model_time = date_time.strftime('%H:%M:%S')

    # If there is an offset, shift by offset before proceed
    forecast_timeseries = []
    if 'utcOffset' in my_opts:
        # print('Shit by utcOffset:', my_opts['utcOffset'].resolution)
        for item in my_timeseries:
            forecast_timeseries.append(
                [datetime.strptime(item[0], COMMON_DATE_TIME_FORMAT) + my_opts['utcOffset'], item[1]])

        forecast_timeseries = extractForecastTimeseries(forecast_timeseries, my_model_date, my_model_time, by_day=True)
    else:
        forecast_timeseries = extractForecastTimeseries(my_timeseries, my_model_date, my_model_time, by_day=True)

    extracted_timeseries = extractForecastTimeseriesInDays(forecast_timeseries)
    # print('save_forecast_timeseries|extracted_timeseries start: ', extracted_timeseries[0])

    # Check whether existing station
    force_insert = my_opts.get('forceInsert', True)
    station = my_opts.get('station', '')
    source = my_opts.get('source', 'FLO2D')
    is_station_exists = my_adapter.get_station({'name': station})

    if is_station_exists is None:
        # print('WARNING: Station %s does not exists. Continue with others.' % station)
        return
    # TODO: Create if station does not exists.

    run_name = my_opts.get('run_name', 'Cloud-1')
    less_char_index = run_name.find('<')
    greater_char_index = run_name.find('>')
    if -1 < less_char_index > -1 < greater_char_index:
        start_str = run_name[:less_char_index]
        date_format_str = run_name[less_char_index + 1:greater_char_index]
        end_str = run_name[greater_char_index + 1:]
        try:
            date_str = date_time.strftime(date_format_str)
            run_name = start_str + date_str + end_str
        except ValueError:
            raise ValueError("Incorrect data format " + date_format_str)

    types = [
        'Forecast-0-d',
        'Forecast-1-d-after'
    ]
    meta_data = {
        'station': station,
        'variable': 'WaterLevel',
        'unit': 'm',
        'type': types[0],
        'source': source,
        'name': run_name
    }

    for i in range(0, min(len(types), len(extracted_timeseries))):
        meta_data_copy = copy.deepcopy(meta_data)
        meta_data_copy['type'] = types[i]
        event_id = my_adapter.get_event_id(meta_data_copy)
        if event_id is None:
            event_id = my_adapter.create_event_id(meta_data_copy)
            # print('HASH SHA256 created: ', event_id)
        else:
            # print('HASH SHA256 exists: ', event_id)
            if not force_insert:
                # print('Timeseries already exists. User --force to update the existing.\n')
                continue

        row_count = my_adapter.insert_timeseries(event_id, extracted_timeseries[i], force_insert)
        print('extracted_timeseries[' + str(i) + '] : ', extracted_timeseries[i])
        print('%s rows inserted.\n' % row_count)


def getUTCOffset(utcOffset, default=False):
    """
    Get timedelta instance of given UTC offset string.
    E.g. Given UTC offset string '+05:30' will return
    timedelta(hours=5, minutes=30))

    :param string utcOffset: UTC offset in format of [+/1][HH]:[MM]
    :param boolean default: If True then return 00:00 time offset on invalid format.
    Otherwise return False on invalid format.
    """
    offset_pattern = re.compile("[+-]\d\d:\d\d")
    match = offset_pattern.match(utcOffset)
    if match:
        utcOffset = match.group()
    else:
        if default:
            # print("UTC_OFFSET :", utcOffset, " not in correct format. Using +00:00")
            return timedelta()
        else:
            return False

    if utcOffset[0] == "-":  # If timestamp in negtive zone, add it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=int(offset_str[0]), minutes=int(offset_str[1]))
    if utcOffset[0] == "+":  # If timestamp in positive zone, deduct it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=-1 * int(offset_str[0]), minutes=-1 * int(offset_str[1]))


def upload_waterlevels_curw(dir_path, ts_start_date, ts_start_time, run_date, run_time):
    print('upload_waterlevels_curw|[ts_start_date, ts_start_time, run_date, run_time] : ', [ts_start_date,
                                                                                            ts_start_time, run_date,
                                                                                            run_time])
    SERIES_LENGTH = 0
    MISSING_VALUE = -999

    try:
        config_path = os.path.join(os.getcwd(), 'extract', 'config.json')
        # print('config_path : ', config_path)
        utc_offset = ''
        with open(config_path) as json_file:
            config_data = json.load(json_file)
            output_dir = dir_path
            HYCHAN_OUT_FILE = config_data['hychan_out_file']
            TIMDEP_FILE = config_data['timdep_out_file']
            FLO2D_MODEL = config_data['flo2d_model']
            RUN_NAME = config_data['run_name']
            hychan_out_file_path = os.path.join(dir_path, HYCHAN_OUT_FILE)
            timdep_file_path = os.path.join(dir_path, TIMDEP_FILE)
            # print('hychan_out_file_path : ', hychan_out_file_path)
            # print('timdep_file_path : ', timdep_file_path)
            forceInsert = True
            MYSQL_HOST = config_data['db_host']
            MYSQL_USER = config_data['db_user']
            MYSQL_DB = config_data['db_name']
            MYSQL_PASSWORD = config_data['db_password']
            # if 'UTC_OFFSET' in config_data and len(
            #         config_data['UTC_OFFSET']):  # Use FLO2D Config file data, if available
            #     UTC_OFFSET = config_data['UTC_OFFSET']
            # if utc_offset:
            #     UTC_OFFSET = config_data
            utcOffset = getUTCOffset('', default=True)
            adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)

            flo2d_source = adapter.get_source(name=FLO2D_MODEL)
            try:
                flo2d_source = json.loads(flo2d_source.get('parameters', "{}"))
                CHANNEL_CELL_MAP = {}
                if 'CHANNEL_CELL_MAP' in flo2d_source:
                    CHANNEL_CELL_MAP = flo2d_source['CHANNEL_CELL_MAP']
                FLOOD_PLAIN_CELL_MAP = {}
                if 'FLOOD_PLAIN_CELL_MAP' in flo2d_source:
                    FLOOD_PLAIN_CELL_MAP = flo2d_source['FLOOD_PLAIN_CELL_MAP']
                ELEMENT_NUMBERS = CHANNEL_CELL_MAP.keys()
                FLOOD_ELEMENT_NUMBERS = FLOOD_PLAIN_CELL_MAP.keys()
                # Calculate the size of time series
                bufsize = 65536
                with open(hychan_out_file_path) as infile:
                    isWaterLevelLines = False
                    isCounting = False
                    countSeriesSize = 0  # HACK: When it comes to the end of file, unable to detect end of time series
                    while True:
                        lines = infile.readlines(bufsize)
                        if not lines or SERIES_LENGTH:
                            break
                        for line in lines:
                            if line.startswith('CHANNEL HYDROGRAPH FOR ELEMENT NO:', 5):
                                isWaterLevelLines = True
                            elif isWaterLevelLines:
                                cols = line.split()
                                if len(cols) > 0 and cols[0].replace('.', '', 1).isdigit():
                                    countSeriesSize += 1
                                    isCounting = True
                                elif isWaterLevelLines and isCounting:
                                    SERIES_LENGTH = countSeriesSize
                                    break

                # print('Series Length is :', SERIES_LENGTH)
                bufsize = 65536
                #################################################################
                # Extract Channel Water Level elevations from HYCHAN.OUT file   #
                #################################################################
                with open(hychan_out_file_path) as infile:
                    isWaterLevelLines = False
                    isSeriesComplete = False
                    waterLevelLines = []
                    seriesSize = 0  # HACK: When it comes to the end of file, unable to detect end of time series
                    while True:
                        lines = infile.readlines(bufsize)
                        if not lines:
                            break
                        for line in lines:
                            if line.startswith('CHANNEL HYDROGRAPH FOR ELEMENT NO:', 5):
                                seriesSize = 0
                                elementNo = line.split()[5]

                                if elementNo in ELEMENT_NUMBERS:
                                    isWaterLevelLines = True
                                    waterLevelLines.append(line)
                                else:
                                    isWaterLevelLines = False

                            elif isWaterLevelLines:
                                cols = line.split()
                                if len(cols) > 0 and isfloat(cols[0]):
                                    seriesSize += 1
                                    waterLevelLines.append(line)

                                    if seriesSize == SERIES_LENGTH:
                                        isSeriesComplete = True

                            if isSeriesComplete:
                                baseTime = datetime.strptime('%s %s' % (ts_start_date, ts_start_time),
                                                             '%Y-%m-%d %H:%M:%S')
                                timeseries = []
                                elementNo = waterLevelLines[0].split()[5]
                                # print('Extracted Cell No', elementNo, CHANNEL_CELL_MAP[elementNo])
                                for ts in waterLevelLines[1:]:
                                    v = ts.split()
                                    if len(v) < 1:
                                        continue
                                    # Get flood level (Elevation)
                                    value = v[1]
                                    # Get flood depth (Depth)
                                    # value = v[2]
                                    if not isfloat(value):
                                        value = MISSING_VALUE
                                        continue  # If value is not present, skip
                                    if value == 'NaN':
                                        continue  # If value is NaN, skip
                                    timeStep = float(v[0])
                                    currentStepTime = baseTime + timedelta(hours=timeStep)
                                    dateAndTime = currentStepTime.strftime("%Y-%m-%d %H:%M:%S")
                                    timeseries.append([dateAndTime, value])
                                # Save Forecast values into Database
                                opts = {
                                    'forceInsert': forceInsert,
                                    'station': CHANNEL_CELL_MAP[elementNo],
                                    'run_name': RUN_NAME
                                }
                                # print('>>>>>', opts)
                                if utcOffset != timedelta():
                                    opts['utcOffset'] = utcOffset

                                # folder_path_ts = os.path.join(dir_path, 'time_series')
                                # if not os.path.exists(folder_path_ts):
                                #     try:
                                #         os.makedirs(folder_path_ts)
                                #     except OSError as e:
                                #         print(str(e))
                                # file_path = os.path.join(folder_path_ts, 'time_series_' + elementNo + '.txt')
                                # with open(file_path, 'w') as f:
                                #     for item in timeseries:
                                #         f.write("%s\n" % item)
                                save_forecast_timeseries(adapter, timeseries, run_date, run_time, opts)

                                isWaterLevelLines = False
                                isSeriesComplete = False
                                waterLevelLines = []
                        # -- END for loop
                    # -- END while loop

                #################################################################
                # Extract Flood Plain water elevations from BASE.OUT file       #
                #################################################################
                with open(timdep_file_path) as infile:
                    waterLevelLines = []
                    waterLevelSeriesDict = dict.fromkeys(FLOOD_ELEMENT_NUMBERS, [])
                    while True:
                        lines = infile.readlines(bufsize)
                        if not lines:
                            break
                        for line in lines:
                            if len(line.split()) == 1:
                                if len(waterLevelLines) > 0:
                                    waterLevels = get_water_level_of_channels(waterLevelLines, FLOOD_ELEMENT_NUMBERS)
                                    # Get Time stamp Ref:http://stackoverflow.com/a/13685221/1461060
                                    # print(waterLevelLines[0].split())
                                    ModelTime = float(waterLevelLines[0].split()[0])
                                    baseTime = datetime.strptime('%s %s' % (ts_start_date, ts_start_time),
                                                                 '%Y-%m-%d %H:%M:%S')
                                    currentStepTime = baseTime + timedelta(hours=ModelTime)
                                    dateAndTime = currentStepTime.strftime("%Y-%m-%d %H:%M:%S")

                                    for elementNo in FLOOD_ELEMENT_NUMBERS:
                                        tmpTS = waterLevelSeriesDict[elementNo][:]
                                        if elementNo in waterLevels:
                                            tmpTS.append([dateAndTime, waterLevels[elementNo]])
                                        else:
                                            tmpTS.append([dateAndTime, MISSING_VALUE])
                                        waterLevelSeriesDict[elementNo] = tmpTS

                                    isWaterLevelLines = False
                                    # for l in waterLevelLines :
                                    # #print(l)
                                    waterLevelLines = []
                            waterLevelLines.append(line)
                    for elementNo in FLOOD_ELEMENT_NUMBERS:
                        opts = {
                            'forceInsert': forceInsert,
                            'station': FLOOD_PLAIN_CELL_MAP[elementNo],
                            'run_name': RUN_NAME,
                            'source': FLO2D_MODEL
                        }
                        if utcOffset != timedelta():
                            opts['utcOffset'] = utcOffset
                        # folder_path_ts = os.path.join(dir_path, 'time_series')
                        # if not os.path.exists(folder_path_ts):
                        #     try:
                        #         os.makedirs(folder_path_ts)
                        #     except OSError as e:
                        #         print(str(e))
                        # file_path = os.path.join(folder_path_ts, 'time_series_'+elementNo+'.txt')
                        # with open(file_path, 'w') as f:
                        #     for item in waterLevelSeriesDict[elementNo]:
                        #         f.write("%s\n" % item)
                        save_forecast_timeseries(adapter, waterLevelSeriesDict[elementNo], run_date, run_time, opts)
                        # print('Extracted Cell No', elementNo, FLOOD_PLAIN_CELL_MAP[elementNo])
            except Exception as ex:
                print('source data loading exception:', str(ex))
    except Exception as e:
        print('config reading exception:', str(e))


