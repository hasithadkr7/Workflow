# Download Rain cell files, download Mean-ref files.
# Check Rain fall files are available in the bucket.

import fnmatch
import sys
import os
import json
import getopt
from datetime import datetime, timedelta
from google.cloud import storage
from curwmysqladapter import MySQLAdapter
import numpy as np
import pandas as pd
import raincelldat.manager as res_mgr
from netCDF4 import Dataset
import logging
import geopandas as gpd
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, Point


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


def get_observed_precip(obs_stations, start_dt, end_dt, duration_days, adapter, forecast_source='wrf0', ):
    def _validate_ts(_s, _ts_sum, _opts):
        if len(_ts_sum) == duration_days[0] * 24 + 1:
            return

        logging.warning('%s Validation count fails. Trying to fill forecast for missing values' % _s)
        f_station = {'station': obs_stations[_s][3],
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

    for s in obs_stations.keys():
        try:
            station = {'station': s,
                       'variable': 'Precipitation',
                       'unit': 'mm',
                       'type': 'Observed',
                       'source': 'WeatherStation',
                       'name': obs_stations[s][2]
                       }

            ts = np.array(adapter.retrieve_timeseries(station, opts)[0]['timeseries'])
            if len(ts) > 0:
                ts_df = pd.DataFrame(data=ts, columns=['ts', 'precip'], index=ts[0:])
                ts_sum = ts_df.groupby(by=[ts_df.ts.map(lambda x: x.strftime('%Y-%m-%d %H:00'))]).sum()
                print('station : ', station)
                try:
                    _validate_ts(s, ts_sum, opts)
                except Exception as e:
                    print('_validate_ts|Exception: ', str(e))
                obs[s] = ts_sum
        except Exception as e:
            print('get_observed_precip|Exception: ', str(e))
            print('get_observed_precip|Exception in station : ', s)
    return obs


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
        of input vertices, with 'points at infinity' appended to the
        end.

    from: https://stackoverflow.com/questions/20515554/colorize-voronoi-diagram

    """

    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D input")

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


def is_inside_geo_df(geo_df, lon, lat, polygon_attr='geometry', return_attr='id'):
    point = Point(lon, lat)
    for i, poly in enumerate(geo_df[polygon_attr]):
        if point.within(poly):
            return geo_df[return_attr][i]
    return None


def datetime_lk_to_utc(timestamp_lk,  shift_mins=0):
    return timestamp_lk - timedelta(hours=5, minutes=30 + shift_mins)


def extract_kelani_basin_rainfall_flo2d_with_obs(run_time, nc_f, adapter, obs_stations, output_dir, start_ts_lk,
                                                 duration_days=None, output_prefix='RAINCELL',
                                                 kelani_lower_basin_points=None, kelani_lower_basin_shp=None):
    """
    check test_extract_kelani_basin_rainfall_flo2d_obs test case
    :param nc_f: file path of the wrf output
    :param adapter:
    :param obs_stations: dict of stations. {station_name: [lon, lat, name variable, nearest wrf point station name]}
    :param output_dir:
    :param start_ts_lk: start time of the forecast/ end time of the observations
    :param duration_days: (optional) a tuple (observation days, forecast days) default (2,3)
    :param output_prefix: (optional) output file name of the RAINCELL file. ex: output_prefix=RAINCELL-150m --> RAINCELL-150m.DAT
    :param kelani_lower_basin_points: (optional)
    :param kelani_lower_basin_shp: (optional)
    :return:
    """

    print('obs_stations : ', obs_stations)

    if duration_days is None:
        duration_days = (2, 3)

    if kelani_lower_basin_points is None:
        kelani_lower_basin_points = res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt')

    if kelani_lower_basin_shp is None:
        kelani_lower_basin_shp = res_mgr.get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')

    points = np.genfromtxt(kelani_lower_basin_points, delimiter=',')

    kel_lon_min = np.min(points, 0)[1]
    kel_lat_min = np.min(points, 0)[2]
    kel_lon_max = np.max(points, 0)[1]
    kel_lat_max = np.max(points, 0)[2]

    diff, kel_lats, kel_lons, times = extract_area_rf_series(nc_f, kel_lat_min, kel_lat_max, kel_lon_min,
                                                                       kel_lon_max)

    def get_bins(arr):
        sz = len(arr)
        return (arr[1:sz - 1] + arr[0:sz - 2]) / 2

    lat_bins = get_bins(kel_lats)
    lon_bins = get_bins(kel_lons)

    t0 = datetime.strptime(times[0], '%Y-%m-%d_%H:%M:%S')
    t1 = datetime.strptime(times[1], '%Y-%m-%d_%H:%M:%S')
    output_dir = os.path.join(output_dir, run_time)
    create_dir_if_not_exists(output_dir)

    obs_start = datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') - timedelta(days=duration_days[0])
    obs_end = datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M')
    forecast_end = datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') + timedelta(days=duration_days[1])
    obs = get_observed_precip(obs_stations, obs_start, obs_end, duration_days, adapter)
    thess_poly = get_voronoi_polygons(obs_stations, kelani_lower_basin_shp, add_total_area=False)

    output_file_path = os.path.join(output_dir, output_prefix + '.DAT')

    # update points array with the thessian polygon idx
    point_thess_idx = []
    for point in points:
        point_thess_idx.append(is_inside_geo_df(thess_poly, lon=point[1], lat=point[2]))
        pass

    with open(output_file_path, 'w') as output_file:
        res_mins = int((t1 - t0).total_seconds() / 60)
        data_hours = int(sum(duration_days) * 24 * 60 / res_mins)
        start_ts_lk = obs_start.strftime('%Y-%m-%d %H:%M:%S')
        end_ts = forecast_end.strftime('%Y-%m-%d %H:%M:%S')

        output_file.write("%d %d %s %s\n" % (res_mins, data_hours, start_ts_lk, end_ts))

        for t in range(int(24 * 60 * duration_days[0] / res_mins) + 1):
            for i, point in enumerate(points):
                rf = float(obs[point_thess_idx[i]].values[t]) if point_thess_idx[i] is not None else 0
                output_file.write('%d %.1f\n' % (point[0], rf))

        forecast_start_idx = int(
            np.where(times == datetime_lk_to_utc(obs_end, shift_mins=30).strftime('%Y-%m-%d_%H:%M:%S'))[0])
        for t in range(int(24 * 60 * duration_days[1] / res_mins) - 1):
            for point in points:
                rf_x = np.digitize(point[1], lon_bins)
                rf_y = np.digitize(point[2], lat_bins)
                if t + forecast_start_idx + 1 < len(times):
                    output_file.write('%d %.1f\n' % (point[0], diff[t + forecast_start_idx + 1, rf_y, rf_x]))
                else:
                    output_file.write('%d %.1f\n' % (point[0], 0))


def create_raincell_file(run_time, adapter, net_cdf_file_name, start_ts_lk,
                         wrf_data_dir, klb_obs_stations, klb_points, duration_days):
    try:
        extract_kelani_basin_rainfall_flo2d_with_obs(run_time, net_cdf_file_name, adapter,
                                                     klb_obs_stations,
                                                     wrf_data_dir,
                                                     start_ts_lk,
                                                     kelani_lower_basin_points=klb_points,
                                                     duration_days=duration_days)
    except Exception as ex:
        print("create_raincell_file|Exception: ", str(ex))


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
        klb_points = res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt')

        adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
        name_list = net_cdf_file_format.split("-")
        net_cdf_file_name = name_list[0] + "_" + net_cdf_date + "_" + name_list[1]
        try:
            net_cdf_file_path = download_location + "/" + net_cdf_file_name
            print("net_cdf_file_path : ", net_cdf_file_path)
            if not os.path.isfile(net_cdf_file_path):
                download_netcdf(download_location, net_cdf_file_name, key_file, bucket_name)
            if os.path.isfile(net_cdf_file_path):
                raincell_file_path = os.path.join(wrf_data_dir, run_date, run_time, 'RAINCELL.DAT')
                if not os.path.isfile(raincell_file_path):
                    create_raincell_file(run_time, adapter, net_cdf_file_path, start_ts_lk,
                                         os.path.join(wrf_data_dir, run_date), klb_observed_stations, klb_points, duration_days)
            adapter.close()
        except Exception as ex:
            adapter.close()
            print("Download required files|Exception: ", str(ex))
except Exception as e:
    print("Exception occurred: ", str(e))
