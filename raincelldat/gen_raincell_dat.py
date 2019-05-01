import json
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import csv
from netCDF4 import Dataset
# from curwmysqladapter import MySQLAdapter
from db_util.get_db_data import MySqlAdapter


def get_forecast_stations_from_net_cdf(wrf_model, net_cdf_file, min_lat, min_lon, max_lat, max_lon):
    nc_fid = Dataset(net_cdf_file, 'r')
    init_lats = nc_fid.variables['XLAT'][:][0]
    lats = []
    for lat_row in init_lats:
        lats.append(lat_row[0])
    lons = nc_fid.variables['XLONG'][:][0][0]

    lon_min_idx = np.argmax(lons >= min_lon) -1
    lat_min_idx = np.argmax(lats >= min_lat) -1
    lon_max_idx = np.argmax(lons >= max_lon)
    lat_max_idx = np.argmax(lats >= max_lat)

    lats = lats[lat_min_idx:lat_max_idx]
    lons = lons[lon_min_idx:lon_max_idx]

    print('get_forecast_stations_from_net_cdf : ', [lats[0], lats[-1], lons[0], lons[-1]])

    width = len(lons)
    height = len(lats)

    stations = []
    station_points = {}
    for y in range(height):
        for x in range(width):
            lat = lats[y]
            lon = lons[x]
            station_name = '%s_%.6f_%.6f' % (wrf_model, lon, lat)
            stations.append(station_name)
            station_points[station_name] = [lon, lat, 'WRF', station_name]
    return stations, station_points


def get_forecast_precipitation(wrf_model, run_name, forecast_stations, adapter, run_datetime, forward_days=3):
    '''
    :param forecast_stations: []
    :param adapter: curwmysqladapter
    :param run_datetime: '2019-04-29 13:00:00'
    :param forward_days: 3
    :param forecast_source: 'wrf0'
    :param type: 'Forecast-0-d','Forecast-1-d-after','Forecast-2-d-after'
    :return:
    '''
    forecast = {}
    if forward_days == 3:
        fcst_d0_start = run_datetime
        fcst_d0_end = (datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        fcst_opts_d0 = {
            'from': fcst_d0_start,
            'to': fcst_d0_end,
        }
        fcst_d1_start = fcst_d0_end
        fcst_d1_end = (datetime.strptime(fcst_d1_start, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        fcst_opts_d1 = {
            'from': fcst_d1_start,
            'to': fcst_d1_end,
        }
        fcst_d2_start = fcst_d1_end
        fcst_d2_end = (datetime.strptime(fcst_d2_start, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime(
            '%Y-%m-%d 00:00:00')
        fcst_opts_d2 = {
            'from': fcst_d2_start,
            'to': fcst_d2_end,
        }
        print('fcst_opts_d0 : ', fcst_opts_d0)
        print('fcst_opts_d1 : ', fcst_opts_d1)
        print('fcst_opts_d2 : ', fcst_opts_d2)
        count = 1
        for s in forecast_stations:
            print('station : ', s)
            station_d0 = {'station': s,
                       'variable': 'Precipitation',
                       'unit': 'mm',
                       'type': 'Forecast-0-d',
                       'name' : run_name,
                       'source': wrf_model
                       }
            event_id0 = adapter.get_event_id(station_d0)
            if event_id0 is not None:
                row_ts_d0 = adapter.get_time_series_values(event_id0, fcst_d0_start, fcst_d0_end)
            print('event_id0 : ', event_id0)
            print('row_ts_d0 : ', row_ts_d0)
            #row_ts_d0 = adapter.retrieve_timeseries(station_d0, fcst_opts_d0)
            # station_d1 = {'station': s,
            #               'variable': 'Precipitation',
            #               'unit': 'mm',
            #               'type': 'Forecast-1-d-after',
            #               'name': run_name,
            #               'source': wrf_model
            #               }
            # event_id1 = adapter.get_event_id(station_d1)
            # row_ts_d1 = adapter.retrieve_timeseries(station_d1, fcst_opts_d1)
            # station_d2 = {'station': s,
            #               'variable': 'Precipitation',
            #               'unit': 'mm',
            #               'type': 'Forecast-2-d-after',
            #               'name': run_name,
            #               'source': wrf_model
            #               }
            # event_id2 = adapter.get_event_id(station_d2)
            # row_ts_d2 = adapter.retrieve_timeseries(station_d2, fcst_opts_d2)
            # print('row_ts_d0 : ', row_ts_d0)
            # print('row_ts_d1 : ', row_ts_d1)
            # print('row_ts_d2 : ', row_ts_d2)
            if count == 5:
                exit(0)
            # row_ts = adapter.retrieve_timeseries(station, opts)
            # if len(row_ts) == 0:
            #     print('No data for {} station from {} to {} .'.format(s, start_dt, end_dt))
            # else:
            #     ts = np.array(row_ts[0]['timeseries'])
            #     if len(ts) != 0 :
            #         ts_df = pd.DataFrame(data=ts, columns=['ts', 'precip'], index=ts[0:])
            #         ts_sum = ts_df.groupby(by=[ts_df.ts.map(lambda x: x.strftime('%Y-%m-%d %H:00'))]).sum()
            #         forecast[s] = ts_sum
            count = count+1
        print('get_forecast_precip|success')
        return forecast
    else:
        print('TODO')


try:
    run_datetime = '2019-04-29 10:00:00'
    back_days = 2
    forward_days = 3
    print('run_datetime : ', run_datetime)
    print('back_days : ', back_days)
    print('forward_days : ', forward_days)
    wrf_model = 'wrf0'
    run_name = 'Cloud-1'
    # wrf_model = 'wrf_v3_A'
    #run_name = 'WRFv3_A'
    # run_name = 'evening_18hrs'
    config = json.loads(open('/home/hasitha/PycharmProjects/Workflow/rainfallcsv/config.json').read())
    reference_net_cdf = '/home/hasitha/PycharmProjects/Workflow/raincelldat/wrf_wrfout_d03_2019-03-31_18_00_00_rf'
    kelani_basin_points_file = '/home/hasitha/PycharmProjects/Workflow/raincelldat/kelani_basin_points_250m.txt'
    if 'rain_fall_file' in config:
        rain_fall_file = config['rain_fall_file']
    if 'db_host' in config:
        db_host = config['db_host']
    if 'db_port' in config:
        db_port = config['db_port']
    if 'db_user' in config:
        db_user = config['db_user']
    if 'db_password' in config:
        db_password = config['db_password']
    if 'db_name' in config:
        db_name = config['db_name']
    db_adapter = MySqlAdapter(db_user,db_password,db_host,db_name, 3306)

    points = np.genfromtxt(kelani_basin_points_file, delimiter=',')

    kel_lon_min = np.min(points, 0)[1]
    kel_lat_min = np.min(points, 0)[2]
    kel_lon_max = np.max(points, 0)[1]
    kel_lat_max = np.max(points, 0)[2]
    forecast_stations, station_points = get_forecast_stations_from_net_cdf(wrf_model, reference_net_cdf,
                                                                           kel_lat_min,
                                                                           kel_lon_min,
                                                                           kel_lat_max,
                                                                           kel_lon_max)
    print('forecast_stations length : ', len(forecast_stations))
    try:
        get_forecast_precipitation(wrf_model, run_name, forecast_stations, db_adapter, run_datetime)
        db_adapter.close_connection()
    except Exception as e:
        print('prepare_input_files|Exception: ', e)
        logging.debug("prepare_input_files|Exception|{}".format(e))
        db_adapter.close_connection()
except Exception as e:
    print(e.with_traceback())

