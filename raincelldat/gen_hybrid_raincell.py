import json
import traceback
import numpy as np
import os
import copy
import pkg_resources
from shapely.geometry import Polygon, Point
from scipy.spatial import Voronoi
from netCDF4 import Dataset
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from curwmysqladapter import MySQLAdapter
import csv


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


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_max_min_lat_lon(basin_points_file):
    points = np.genfromtxt(basin_points_file, delimiter=',')
    # points = [[id, longitude, latitude],[],[]]
    kel_lon_min = np.min(points, 0)[1]
    kel_lat_min = np.min(points, 0)[2]
    kel_lon_max = np.max(points, 0)[1]
    kel_lat_max = np.max(points, 0)[2]
    print('[kel_lon_min, kel_lat_min, kel_lon_max, kel_lat_max] : ', [kel_lon_min, kel_lat_min, kel_lon_max, kel_lat_max])
    print(points[0][1])
    print(points[0][2])


def generate_missing_ts(observed_end, observed_start, ts_df):
    first_available_ts = datetime.strptime(ts_df.index[0], '%Y-%m-%d %H:%M')
    last_available_ts = datetime.strptime(ts_df.index[-1], '%Y-%m-%d %H:%M')
    observed_start_ts = datetime.strptime(observed_start, '%Y-%m-%d %H:%M:%S')
    observed_end_ts = datetime.strptime(observed_end, '%Y-%m-%d %H:%M:%S')
    print('first_available_ts : ', first_available_ts)
    print('last_available_ts : ', last_available_ts)
    print('observed_start_ts : ', observed_start_ts)
    print('observed_end_ts : ', observed_end_ts)
    df1 = pd.DataFrame()
    if int((observed_end_ts - last_available_ts).total_seconds() / 3600) > 0:
        ts_list = []
        for i in range(int((observed_end_ts - last_available_ts).total_seconds() / 3600)):
            ts_list.append([(last_available_ts+timedelta(hours=i)).strftime('%Y-%m-%d %H:%M'), 0.00])
            print(i)
        print(ts_list)
        df1 = pd.DataFrame(ts_list, columns=['ts', 'precip'])
        df1.set_index('ts', inplace=True)
        print(df1)
    df2 = pd.DataFrame()
    if int((first_available_ts - observed_start_ts).total_seconds() / 3600) > 0:
        ts_list = []
        for i in range(int((first_available_ts - observed_start_ts).total_seconds() / 3600)):
            ts_list.append([(observed_start_ts + timedelta(hours=i)).strftime('%Y-%m-%d %H:%M'), 0.00])
            print(i)
        print(ts_list)
        df2 = pd.DataFrame(ts_list, columns=['ts', 'precip'])
        df2.set_index('ts', inplace=True)
        print(df2)
    return pd.concat([df1, ts_df, df2])


def get_observed_precip(stations, start_dt, end_dt, observed_duration, adapter, forecast_source='wrf0'):
    obs = {}
    opts = {
        'from': start_dt,
        'to': end_dt,
    }

    for s in stations.keys():
        print('obs_stations[s][2]: ', stations[s][2])
        station = {'station': s,
                   'variable': 'Precipitation',
                   'unit': 'mm',
                   'type': 'Observed',
                   'source': 'WeatherStation',
                   'name': stations[s][2]
                   }
        row_ts = adapter.retrieve_timeseries(station, opts)
        if len(row_ts) == 0:
            print('No data for {} station from {} to {} .'.format(s, start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')))
        else:
            ts = np.array(row_ts[0]['timeseries'])
            if len(ts) != 0:
                ts_df = pd.DataFrame(data=ts, columns=['ts', 'precip'], index=ts[0:])
                ts_sum = ts_df.groupby(by=[ts_df.ts.map(lambda x: x.strftime('%Y-%m-%d %H:00'))]).sum()
                (row_count, column_count) = ts_sum.shape
                print('row_count : ', row_count)
                print('column_count : ', column_count)
                if row_count >= observed_duration:
                    obs[s] = ts_sum
                #elif ((observed_duration - row_count)*100)/observed_duration < 10.00:  #Percentage of data missing should be less than 10%.
                elif (observed_duration - row_count) < 3:  #Percentage of data missing should be less than 10%.
                    #last_available_date = ts_sum.index.values.tolist()[-1]
                    #obs[s] = pd.concat([ts_sum, extended_ts])
                    obs[s] = generate_missing_ts(end_dt, start_dt, ts_sum)
                else:
                    print('Invalid station : ', s)
                    print('row_count:', row_count)
                print('station : {}| first ts:{}| last ts: {}'.format(s, obs[s].index[0], obs[s].index[-1]))
    print('get_observed_precip|success')
    return obs


def get_observed_precip_previous(stations, start_dt, end_dt, duration_days, adapter, forecast_source='wrf0'):
    def _validate_ts(_s, _ts_sum, _opts):
        print('len(_ts_sum):', len(_ts_sum))
        print('duration_days[0] * 24 + 1:', duration_days[0] * 24 + 1)
        if len(_ts_sum) == duration_days[0] * 24 + 1:
            return

        f_station = {'station': stations[_s][3],
                     'variable': 'Precipitation',
                     'unit': 'mm',
                     'type': 'Forecast-0-d',
                     'source': forecast_source,
                     }
        f_ts = np.array(adapter.retrieve_timeseries(f_station, _opts)[0]['timeseries'])

        if len(f_ts) != duration_days[0] * 24 + 1:
            raise CurwObservationException('%s Forecast time-series validation failed' % _s)

        for j in range(duration_days[0] * 24 + 1):
            d = start_dt + timedelta(hours=j)
            d_str = d.strftime('%Y-%m-%d %H:00')
            if j < len(_ts_sum.index.values):
                if _ts_sum.index[j] != d_str:
                    _ts_sum.loc[d_str] = f_ts[j, 1]
                    _ts_sum.sort_index(inplace=True)
            else:
                _ts_sum.loc[d_str] = f_ts[j, 1]

        if len(_ts_sum) == duration_days[0] * 24 + 1:
            return
        else:
            raise CurwObservationException('time series validation failed')

    obs = {}
    opts = {
        'from': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'to': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
    }

    for s in stations.keys():
        print('obs_stations[s][2]: ', stations[s][2])
        station = {'station': s,
                   'variable': 'Precipitation',
                   'unit': 'mm',
                   'type': 'Observed',
                   'source': 'WeatherStation',
                   'name': stations[s][2]
                   }
        # print('station : ', s)
        row_ts = adapter.retrieve_timeseries(station, opts)
        if len(row_ts) == 0:
            print('No data for {} station from {} to {} .'.format(s, start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')))
        else:
            ts = np.array(row_ts[0]['timeseries'])
            #print('ts length:', len(ts))
            if len(ts) != 0 :
                ts_df = pd.DataFrame(data=ts, columns=['ts', 'precip'], index=ts[0:])
                ts_sum = ts_df.groupby(by=[ts_df.ts.map(lambda x: x.strftime('%Y-%m-%d %H:00'))]).sum()
                try:
                    _validate_ts(s, ts_sum, opts)
                except Exception as e:
                    print('_validate_ts|Exception : ', str(e))

        obs[s] = ts_sum
    print('get_observed_precip|success')
    return obs


def get_forecast_precipitation(wrf_model, run_name, forecast_stations, adapter, run_datetime, forward_days=3):
    print('get_forecast_precipitation| run_datetime: ', run_datetime)
    forecast = {}
    if forward_days == 3:
        fcst_d0_start = run_datetime
        fcst_d0_end = (datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime(
                '%Y-%m-%d 00:00:00')
        fcst_opts_d0 = {
            'from': fcst_d0_start,
            'to': fcst_d0_end,
        }
        fcst_d1_start = fcst_d0_end
        fcst_d1_end = (datetime.strptime(fcst_d1_start, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)).strftime(
                '%Y-%m-%d 00:00:00')
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
        for s in forecast_stations:
            station_d0 = {'station': s,
                              'variable': 'Precipitation',
                              'unit': 'mm',
                              'type': 'Forecast-0-d',
                              'name': run_name,
                              'source': wrf_model
                              }
            row_ts_d0 = adapter.retrieve_timeseries(station_d0, fcst_opts_d0)

            station_d1 = {'station': s,
                              'variable': 'Precipitation',
                              'unit': 'mm',
                              'type': 'Forecast-1-d-after',
                              'name': run_name,
                              'source': wrf_model
                            }
            row_ts_d1 = adapter.retrieve_timeseries(station_d1, fcst_opts_d1)

            station_d2 = {'station': s,
                          'variable': 'Precipitation',
                          'unit': 'mm',
                          'type': 'Forecast-2-d-after',
                          'name': run_name,
                          'source': wrf_model
                          }
            row_ts_d2 = adapter.retrieve_timeseries(station_d2, fcst_opts_d2)

            ts_d0_df = pd.DataFrame(columns=['time', 'value'])
            if len(row_ts_d0) > 0:
                ts_d0 = np.array(row_ts_d0[0]['timeseries'])
                if len(ts_d0) > 0:
                    ts_d0_df = pd.DataFrame(data=ts_d0, columns=['time', 'value']).set_index(keys='time')
            ts_d1_df = pd.DataFrame(columns=['time', 'value'])
            if len(row_ts_d1):
                ts_d1 = np.array(row_ts_d1[0]['timeseries'])
                if len(ts_d1) != 0:
                    ts_d1_df = pd.DataFrame(data=ts_d1, columns=['time', 'value']).set_index(keys='time')
            ts_d2_df = pd.DataFrame(columns=['time', 'value'])
            if len(row_ts_d2):
                ts_d2 = np.array(row_ts_d2[0]['timeseries'])
                if len(ts_d2) != 0:
                    ts_d2_df = pd.DataFrame(data=ts_d2, columns=['time', 'value']).set_index(keys='time')

            station_df = pd.concat([ts_d0_df, ts_d1_df, ts_d2_df])

            if len(station_df.index) > 0:
                forecast[s] = station_df
        return forecast
    else:
        print('TODO')
        return forecast


def get_forecast_precipitation_from_curw(forecast_stations, start_dt, end_dt, adapter, forecast_source):
    forecast = {}
    opts = {
        'from': start_dt,
        'to': end_dt,
    }

    for s in forecast_stations:
        station = {'station': s,
                   'variable': 'Precipitation',
                   'unit': 'mm',
                   'type': 'Forecast-0-d',
                   'source': forecast_source
                   }
        #print('station : ', s)
        row_ts = adapter.retrieve_timeseries(station, opts)
        if len(row_ts) == 0:
            print('No data for {} station from {} to {} .'.format(s, start_dt, end_dt))
        else:
            ts = np.array(row_ts[0]['timeseries'])
            if len(ts) != 0 :
                ts_df = pd.DataFrame(data=ts, columns=['ts', 'precip'], index=ts[0:])
                ts_sum = ts_df.groupby(by=[ts_df.ts.map(lambda x: x.strftime('%Y-%m-%d %H:00'))]).sum()
                forecast[s] = ts_sum
    print('get_forecast_precip|success')
    return forecast


def get_forecast_stations_from_point_file(basin_points_file):
    forecast_stations_list = []
    points = np.genfromtxt(basin_points_file, delimiter=',')
    for point in points:
        forecast_stations_list.append('wrf0_{}_{}'.format(point[1], point[2]))
    return forecast_stations_list


def get_forecast_stations_from_net_cdf(model_prefix, net_cdf_file, min_lat, min_lon, max_lat, max_lon):
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
            station_name = '%s_%.6f_%.6f' % (model_prefix, lon, lat)
            stations.append(station_name)
            station_points[station_name] = [lon, lat, 'WRF', station_name]
    return stations, station_points


def get_forecast_stations_from_net_cdf_back(model_prefix, net_cdf_file, min_lat, min_lon, max_lat, max_lon):
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
    csv_file_name = '/home/hasitha/PycharmProjects/Workflow/raincelldat/wrf0_points.csv'
    line1 = ['wrf_point', 'latitude', 'longitude']
    count = 1
    with open(csv_file_name, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(line1)
        for y in range(height):
            for x in range(width):
                lat = lats[y]
                lon = lons[x]
                station_name = '%s_%.6f_%.6f' % (model_prefix, lon, lat)
                stations.append(station_name)
                station_points[station_name] = [lon, lat, 'WRF', station_name]
                index = 'wrf0_point%s' % (count)
                lat_val = '%.6f' % (lat)
                lon_val = '%.6f' % (lon)
                writer.writerow([index, lat_val, lon_val])
                count = count+1
    csvFile.close()
    return stations, station_points


def get_two_element_average(prcp, return_diff=True):
    avg_prcp = (prcp[1:] + prcp[:-1]) * 0.5
    if return_diff:
        return avg_prcp - np.insert(avg_prcp[:-1], 0, [0], axis=0)
    else:
        return avg_prcp


def is_inside_geo_df(geo_df, lon, lat, polygon_attr='geometry', return_attr='id'):
    point = Point(lon, lat)
    for i, poly in enumerate(geo_df[polygon_attr]):
        if point.within(poly):
            return geo_df[return_attr][i]
    return None


def _voronoi_finite_polygons_2d(vor, radius=None):
    """
    Reconstruct infinite voronoi regions in a 2D diagram to finite
    regions.

    Parameters
    ----------
    vor : Voronoi
        Input diagram
    radius : float, optional
        Distance to 'points at infinity'.

    Returns
    -------
    regions : list of tuples
        Indices of vertices in each revised Voronoi regions.
    vertices : list of tuples
        Coordinates for revised Voronoi vertices. Same as coordinates
        of inputs vertices, with 'points at infinity' appended to the
        end.

    from: https://stackoverflow.com/questions/20515554/colorize-voronoi-diagram

    """

    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D inputs")

    new_regions = []
    new_vertices = vor.vertices.tolist()

    center = vor.points.mean(axis=0)
    if radius is None:
        radius = vor.points.ptp().max()

    # Construct a map containing all ridges for a given point
    all_ridges = {}
    for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
        all_ridges.setdefault(p1, []).append((p2, v1, v2))
        all_ridges.setdefault(p2, []).append((p1, v1, v2))

    # Reconstruct infinite regions
    for p1, region in enumerate(vor.point_region):
        vertices = vor.regions[region]

        if all(v >= 0 for v in vertices):
            # finite region
            new_regions.append(vertices)
            continue

        # reconstruct a non-finite region
        ridges = all_ridges[p1]
        new_region = [v for v in vertices if v >= 0]

        for p2, v1, v2 in ridges:
            if v2 < 0:
                v1, v2 = v2, v1
            if v1 >= 0:
                # finite ridge: already in the region
                continue

            # Compute the missing endpoint of an infinite ridge

            t = vor.points[p2] - vor.points[p1]  # tangent
            t /= np.linalg.norm(t)
            n = np.array([-t[1], t[0]])  # normal

            midpoint = vor.points[[p1, p2]].mean(axis=0)
            direction = np.sign(np.dot(midpoint - center, n)) * n
            far_point = vor.vertices[v2] + direction * radius

            new_region.append(len(new_vertices))
            new_vertices.append(far_point.tolist())

        # sort region counterclockwise
        vs = np.asarray([new_vertices[v] for v in new_region])
        c = vs.mean(axis=0)
        angles = np.arctan2(vs[:, 1] - c[1], vs[:, 0] - c[0])
        new_region = np.array(new_region)[np.argsort(angles)]

        # finish
        new_regions.append(new_region.tolist())

    return new_regions, np.asarray(new_vertices)


def get_voronoi_polygons(points_dict, shape_file, shape_attribute=None, output_shape_file=None, add_total_area=True):
    """
    :param points_dict: dict of points {'id' --> [lon, lat]}
    :param shape_file: shape file path of the area
    :param shape_attribute: attribute list of the interested region [key, value]
    :param output_shape_file: if not none, a shape file will be created with the output
    :param add_total_area: if true, total area shape will also be added to output
    :return:
    geo_dataframe with voronoi polygons with columns ['id', 'lon', 'lat','area', 'geometry'] with last row being the area of the
    shape file
    """
    if shape_attribute is None:
        shape_attribute = ['OBJECTID', 1]

    shape_df = gpd.GeoDataFrame.from_file(shape_file)
    shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
    shape_polygon = shape_df['geometry'][shape_polygon_idx]

    ids = [p if type(p) == str else np.asscalar(p) for p in points_dict.keys()]
    points = np.array(list(points_dict.values()))[:, :2]
    #print('get_voronoi_polygons|points : ', points)
    vor = Voronoi(points)
    regions, vertices = _voronoi_finite_polygons_2d(vor)

    data = []
    for i, region in enumerate(regions):
        polygon = Polygon([tuple(x) for x in vertices[region]])
        if polygon.intersects(shape_polygon):
            intersection = polygon.intersection(shape_polygon)
            data.append({'id': ids[i], 'lon': vor.points[i][0], 'lat': vor.points[i][1], 'area': intersection.area,
                         'geometry': intersection
                         })
    if add_total_area:
        data.append({'id': '__total_area__', 'lon': shape_polygon.centroid.x, 'lat': shape_polygon.centroid.y,
                     'area': shape_polygon.area, 'geometry': shape_polygon})

    df = gpd.GeoDataFrame(data, columns=['id', 'lon', 'lat', 'area', 'geometry'], crs=shape_df.crs)

    if output_shape_file is not None:
        df.to_file(output_shape_file)

    return df


def create_hybrid_raincell(dir_path, run_date, run_time, forward, backward, res_mins = '60', model_prefix ='wrf', forecast_source ='wrf0', run_name ='hourly_run'):
    try:
        forecast_adapter = None
        observed_adapter = None
        kelani_basin_points_file = get_resource_path('extraction/local/kelani_basin_points_250m.txt')
        kelani_lower_basin_shp_file = get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
        reference_net_cdf = get_resource_path('extraction/netcdf/wrf_wrfout_d03_2019-03-31_18_00_00_rf')
        # config_path = os.path.join(os.getcwd(), 'raincelldat', 'config.json')
        config_path = os.path.join(os.getcwd(), 'config.json')
        with open(config_path) as json_file:
            config = json.load(json_file)
            if 'forecast_db_config' in config:
                forecast_db_config = config['forecast_db_config']
            if 'observed_db_config' in config:
                observed_db_config = config['observed_db_config']
            if 'klb_obs_stations' in config:
                obs_stations = copy.deepcopy(config['klb_obs_stations'])
            print('[run_date, run_time] : ', [run_date, run_time])
            start_ts_lk = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
            start_ts_lk = start_ts_lk.strftime('%Y-%m-%d_%H:00')  # '2018-05-24_08:00'
            duration_days = (int(backward), int(forward))
            obs_start = datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') - timedelta(days=duration_days[0])
            obs_end = datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M')
            forecast_end = datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') + timedelta(days=duration_days[1])
            print([obs_start, obs_end, forecast_end])

            fcst_duration_start = obs_end.strftime('%Y-%m-%d %H:%M:%S')
            fcst_duration_end = (datetime.strptime(fcst_duration_start, '%Y-%m-%d %H:%M:%S') + timedelta(days=3)).strftime('%Y-%m-%d 00:00:00')
            obs_duration_start = (datetime.strptime(fcst_duration_start, '%Y-%m-%d %H:%M:%S') - timedelta(days=2)).strftime('%Y-%m-%d 00:00:00')

            print('obs_duration_start : ', obs_duration_start)
            print('fcst_duration_start : ', fcst_duration_start)
            print('fcst_duration_end : ', fcst_duration_end)

            observed_duration = int((datetime.strptime(fcst_duration_start, '%Y-%m-%d %H:%M:%S') - datetime.strptime(obs_duration_start, '%Y-%m-%d %H:%M:%S')).total_seconds() / (60 * res_mins))
            forecast_duration = int((datetime.strptime(fcst_duration_end, '%Y-%m-%d %H:%M:%S') - datetime.strptime(fcst_duration_start, '%Y-%m-%d %H:%M:%S')).total_seconds() / (60 * res_mins))
            total_duration = int((datetime.strptime(fcst_duration_end, '%Y-%m-%d %H:%M:%S') - datetime.strptime(obs_duration_start, '%Y-%m-%d %H:%M:%S')).total_seconds() / (60 * res_mins))

            print('observed_duration : ', observed_duration)
            print('forecast_duration : ', forecast_duration)
            print('total_duration : ', total_duration)

            raincell_file_path = os.path.join(dir_path, 'RAINCELL.DAT')
            if not os.path.isfile(raincell_file_path):
                points = np.genfromtxt(kelani_basin_points_file, delimiter=',')

                kel_lon_min = np.min(points, 0)[1]
                kel_lat_min = np.min(points, 0)[2]
                kel_lon_max = np.max(points, 0)[1]
                kel_lat_max = np.max(points, 0)[2]

                print('[kel_lon_min, kel_lat_min, kel_lon_max, kel_lat_max] : ', [kel_lon_min, kel_lat_min, kel_lon_max, kel_lat_max])

                forecast_adapter = MySQLAdapter(host=forecast_db_config['host'],
                                                user=forecast_db_config['user'],
                                                password=forecast_db_config['password'],
                                                db=forecast_db_config['db'])
                # #min_lat, min_lon, max_lat, max_lon
                forecast_stations, station_points = get_forecast_stations_from_net_cdf(model_prefix, reference_net_cdf,
                                                                                       kel_lat_min,
                                                                                       kel_lon_min,
                                                                                       kel_lat_max,
                                                                                       kel_lon_max)
                print('forecast_stations length : ', len(forecast_stations))

                observed_adapter = MySQLAdapter(host=observed_db_config['host'],
                                                user=observed_db_config['user'],
                                                password=observed_db_config['password'],
                                                db=observed_db_config['db'])

                # print('obs_stations : ', obs_stations)
                observed_precipitations = get_observed_precip(obs_stations,
                                                              obs_duration_start,
                                                              fcst_duration_start,
                                                              observed_duration,
                                                              observed_adapter, forecast_source='wrf0')
                #print('observed_precipitations : ', observed_precipitations)
                observed_adapter.close()
                observed_adapter = None
                validated_obs_station = {}
                print('observed_precipitations.keys() : ', observed_precipitations.keys())
                #"""
                for station_name in obs_stations.keys():
                    if station_name in observed_precipitations.keys():
                        validated_obs_station[station_name] = obs_stations[station_name]
                    else:
                        print('station_name : ', station_name)
                print('validated_obs_station : ', validated_obs_station)
                
                if len(validated_obs_station) >= 3 and bool(observed_precipitations):
                    thess_poly = get_voronoi_polygons(validated_obs_station, kelani_lower_basin_shp_file,
                                                      add_total_area=False)
                    print('thess_poly : ', thess_poly)
                    fcst_thess_poly = get_voronoi_polygons(station_points, kelani_lower_basin_shp_file,
                                                           add_total_area=False)
                    forecast_precipitations = get_forecast_precipitation(forecast_source, run_name, forecast_stations,
                                                                         forecast_adapter,
                                                                         obs_end.strftime('%Y-%m-%d %H:%M:%S'),
                                                                         forward_days=3)
                    print('forecast_precipitations : ', forecast_precipitations)
                    forecast_adapter.close()
                    forecast_adapter = None
                    if bool(forecast_precipitations):
                        fcst_point_thess_idx = []
                        for point in points:
                            fcst_point_thess_idx.append(is_inside_geo_df(fcst_thess_poly, lon=point[1], lat=point[2]))
                            pass
                        point_thess_idx = []
                        for point in points:
                            point_thess_idx.append(is_inside_geo_df(thess_poly, lon=point[1], lat=point[2]))
                            pass

                        print('len(points)', len(points))
                        print('len(point_thess_idx)', len(point_thess_idx))
                        print('point_thess_idx', point_thess_idx)

                        with open(raincell_file_path, 'w') as output_file:
                            output_file.write(
                                "%d %d %s %s\n" % (res_mins, total_duration, obs_duration_start, fcst_duration_end))

                            print('range 1 : ', int(24 * 60 * duration_days[0] / res_mins) + 1)
                            print('range 2 : ', int(24 * 60 * duration_days[1] / res_mins) - 1)

                            for t in range(observed_duration):
                                for i, point in enumerate(points):
                                    #print('point_thess_idx[{}]:{}'.format(i, point_thess_idx[i]))
                                    rf = float(observed_precipitations[point_thess_idx[i]].values[t]) if point_thess_idx[i] is not None else 0
                                    output_file.write('%d %.1f\n' % (point[0], rf))

                            for t in range(forecast_duration):
                                for j, point in enumerate(points):
                                    rf = float(forecast_precipitations[fcst_point_thess_idx[j]].values[t]) if fcst_point_thess_idx[j] is not None else 0
                                    output_file.write('%d %.1f\n' % (point[0], rf))
                    else:
                        print('----------------------------------------------')
                        print('No forecast data.')
                        print('----------------------------------------------')
                else:
                    print('----------------------------------------------')
                    print('No observed data.')
                    print('----------------------------------------------')
                #"""
    except Exception as e:
        print('Raincell generation error.')
        traceback.print_exc()
        try:
            if forecast_adapter is not None:
                forecast_adapter.close()
            if observed_adapter is not None:
                observed_adapter.close()
        except Exception as ex:
            print(str(ex))


# if __name__ == "__main__":
#     run_date = '2019-06-07'
#     run_time = '17:00:00'
#     dir_path = os.path.join('/home/hasitha/PycharmProjects/Workflow/output', run_date, run_time)
#     create_dir_if_not_exists(dir_path)
#     forward = 3
#     backward = 2
#     res_mins = 60
#     model_prefix = 'wrf'
#     forecast_source = 'wrf0'
#     run_name = 'Cloud-1'
#     create_hybrid_raincell(dir_path, run_date, run_time, forward, backward, res_mins, model_prefix, forecast_source, run_name)
    # last_available = '2019-06-07 10:00'
    # observed_end = '2019-06-07 17:00:00'
    # generate_missing_ts(last_available, observed_end)

