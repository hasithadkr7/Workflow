# Download Rain cell files, download Mean-ref files.
# Check Rain fall files are available in the bucket.
import csv
import sys
import os
import json
import getopt
from datetime import datetime, timedelta
from db_layer import CurwSimAdapter
import copy
import pkg_resources
import numpy as np
from scipy.spatial import Voronoi
import shapefile
from shapely.geometry import Polygon, Point
from shapely.geometry import shape
import geopandas as gpd
import pandas as pd


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


def get_available_stations_in_sub_basin(db_adapter, sub_basin_shape_file, date_time):
    available_stations = db_adapter.get_available_stations_info(date_time)
    if len(available_stations):
        for station, info in available_stations.items():
            point = (info['latitude'], info['longitude'])  # an x,y tuple
            shp = shapefile.Reader(sub_basin_shape_file)  # open the shapefile
            all_shapes = shp.shapes()  # get all the polygons
            all_records = shp.records()
            for i in len(all_shapes):
                boundary = all_shapes[i]  # get a boundary polygon
                if Point(point).within(shape(boundary)):  # make a point and see if it's in the polygon
                    name = all_records[i][2]  # get the second field of the corresponding record
                    print("The point is in", name)
                else:
                    available_stations.pop(station)
        return available_stations
    else:
        print('Not available stations..')
        return {}


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


class KUBObservationMean:
    def __init__(self):
        self.shape_file = get_resource_path('extraction/shp/kub-wgs84/kub-wgs84.shp')
        self.percentage_factor = 100

    def calc_station_fraction(self, stations, precision_decimal_points=3):
        """
        Given station lat lon points must reside inside the KUB shape, otherwise could give incorrect results.
        :param stations: dict of station_name: [lon, lat] pairs
        :param precision_decimal_points: int
        :return: dict of station_id: area percentage
        """

        if stations is None:
            raise ValueError("'stations' cannot be null.")

        station_list = stations.keys()
        if len(station_list) <= 0:
            raise ValueError("'stations' cannot be empty.")

        station_fractions = {}
        if len(station_list) < 3:
            for station in station_list:
                station_fractions[station] = np.round(self.percentage_factor / len(station_list),
                                                      precision_decimal_points)
            return station_fractions

        # station_fractions = {}
        total_area = 0

        # calculate the voronoi/thesian polygons w.r.t given station points.
        voronoi_polygons = get_voronoi_polygons(points_dict=stations, shape_file=self.shape_file, add_total_area=True)

        for row in voronoi_polygons[['id', 'area']].itertuples(index=False, name=None):
            id = row[0]
            print('voronoi_polygons id: ', id)
            area = np.round(row[1], precision_decimal_points)
            station_fractions[id] = area
            # get_voronoi_polygons calculated total might not equal to sum of the rest, thus calculating total.
            if id != '__total_area__':
                total_area += area
        total_area = np.round(total_area, precision_decimal_points)

        for station in station_list:
            if station in station_fractions:
                station_fractions[station] = np.round(
                    (station_fractions[station] * self.percentage_factor) / total_area, precision_decimal_points)
            else:
                station_fractions[station] = np.round(0.0, precision_decimal_points)

        return station_fractions

    def calc_kub_mean(self, timerseries_dict, normalizing_factor='H', filler=0.0, precision_decimal_points=3):
        """
        :param timeseries: dict of (station_name: dict_inside) pairs. dict_inside should have
            ('lon_lat': [lon, lat]) and ('timeseries': pandas df with time(index), value columns)
        :param normalizing_factor: resampling factor, should be one of pandas resampling type
            (ref_link: http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases)
        :param filler value for missing values occur when normalizing
        :return: kub mean timeseries, [[time, value]...]
        """

        stations = {}
        timerseries_list = []
        print('1')
        for key in timerseries_dict.keys():
            stations[key] = timerseries_dict[key]['lon_lat']
            # Resample given set of timeseries.
            # tms = timerseries_dict[key]['timeseries'].astype('float').resample(normalizing_factor).sum()
            tms = timerseries_dict[key]['timeseries'].astype('float')
            # Rename coulmn_name 'value' to its own staion_name.
            tms = tms.rename(axis='columns', mapper={'value': key})
            timerseries_list.append(tms)
        print('2')
        if len(timerseries_list) <= 0:
            raise ValueError('Empty timeseries_dict given.')
        elif len(timerseries_list) == 1:
            matrix = timerseries_list[0]
        else:
            matrix = timerseries_list[0].join(other=timerseries_list[1:len(timerseries_list)], how='outer')
        print('3')

        # Note:
        # After joining resampling+sum does not work properly. Gives NaN and sum that is not correct.
        # Therefore resamplig+sum is done for each timeseries. If this issue could be solved,
        # then resampling+sum could be carried out after joining.

        # Fill in missing values after joining into one timeseries matrix.
        matrix.fillna(value=np.round(filler, precision_decimal_points), inplace=True, axis='columns')
        print('4')
        station_fractions = self.calc_station_fraction(stations)
        print('--------------------------------station_fractions : ', station_fractions)
        print('5')
        # Make sure only the required station weights remain in the station_fractions, else raise ValueError.
        matrix_station_list = list(matrix.columns.values)
        weights_station_list = list(station_fractions.keys())
        print('6')
        invalid_stations = [key for key in weights_station_list if key not in matrix_station_list]
        print('7')
        for key in invalid_stations:
            station_fractions.pop(key, None)
        if not len(matrix_station_list) == len(station_fractions.keys()):
            raise ValueError('Problem in calculated station weights.', stations, station_fractions)
        print('8')
        # Prepare weights to calc the kub_mean.
        weights = pd.DataFrame.from_dict(data=station_fractions, orient='index', dtype='float')
        print('9')
        weights = weights.divide(self.percentage_factor, axis='columns')
        # print('weights.shape : ', weights.shape)
        # print('matrix.shape : ', matrix.shape)
        # print('weights : ', weights)
        # print('matrix : ', matrix)
        # print('type(matrix) : ', type(matrix))
        # print('weights[0] : ', weights[0])
        # print('type(weights[0]) : ', type(weights[0]))
        print('10')
        # kub_mean = (matrix * weights[0]).sum(axis='columns')
        # kub_mean = matrix.mul(weights[0], axis=0).sum(axis='columns')
        kub_mean = matrix.dot(weights).sum(axis='columns')
        print('11')
        kub_mean_timeseries = kub_mean.to_frame(name='value')
        print('12')
        return kub_mean_timeseries


class KLBObservationMean:
    def __init__(self):
        self.shape_file = get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
        self.percentage_factor = 100

    def calc_station_fraction(self, stations, precision_decimal_points=3):
        """
        Given station lat lon points must reside inside the KUB shape, otherwise could give incorrect results.
        :param stations: dict of station_name: [lon, lat] pairs
        :param precision_decimal_points: int
        :return: dict of station_id: area percentage
        """

        if stations is None:
            raise ValueError("'stations' cannot be null.")

        station_list = stations.keys()
        if len(station_list) <= 0:
            raise ValueError("'stations' cannot be empty.")

        station_fractions = {}
        if len(station_list) < 3:
            for station in station_list:
                station_fractions[station] = np.round(self.percentage_factor / len(station_list),
                                                      precision_decimal_points)
            return station_fractions

        station_fractions = {}
        total_area = 0

        # calculate the voronoi/thesian polygons w.r.t given station points.
        voronoi_polygons = get_voronoi_polygons(points_dict=stations, shape_file=self.shape_file, add_total_area=True)

        for row in voronoi_polygons[['id', 'area']].itertuples(index=False, name=None):
            id = row[0]
            area = np.round(row[1], precision_decimal_points)
            station_fractions[id] = area
            # get_voronoi_polygons calculated total might not equal to sum of the rest, thus calculating total.
            if id != '__total_area__':
                total_area += area
        total_area = np.round(total_area, precision_decimal_points)

        for station in station_list:
            if station in station_fractions:
                station_fractions[station] = np.round(
                    (station_fractions[station] * self.percentage_factor) / total_area, precision_decimal_points)
            else:
                station_fractions[station] = np.round(0.0, precision_decimal_points)

        return station_fractions

    def calc_klb_mean(self, timerseries_dict, normalizing_factor='H', filler=0.0, precision_decimal_points=3):
        """
        :param timeseries: dict of (station_name: dict_inside) pairs. dict_inside should have
            ('lon_lat': [lon, lat]) and ('timeseries': pandas df with time(index), value columns)
        :param normalizing_factor: resampling factor, should be one of pandas resampling type
            (ref_link: http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases)
        :param filler value for missing values occur when normalizing
        :return: kub mean timeseries, [[time, value]...]
        """

        stations = {}
        timerseries_list = []
        for key in timerseries_dict.keys():
            stations[key] = timerseries_dict[key]['lon_lat']
            # Resample given set of timeseries.
            # tms = timerseries_dict[key]['timeseries'].astype('float').resample(normalizing_factor).sum()
            tms = timerseries_dict[key]['timeseries'].astype('float')
            # Rename coulmn_name 'value' to its own staion_name.
            tms = tms.rename(axis='columns', mapper={'value': key})
            timerseries_list.append(tms)

        if len(timerseries_list) <= 0:
            raise ValueError('Empty timeseries_dict given.')
        elif len(timerseries_list) == 1:
            print("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
            matrix = timerseries_list[0]
        else:
            print('yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy')
            # print('len(timerseries_list): ', len(timerseries_list))
            # print('timerseries_list[0]: ', timerseries_list[0])
            # print('timerseries_list[1]: ', timerseries_list[1])
            matrix = timerseries_list[0].join(other=timerseries_list[1:len(timerseries_list)], how='outer')

        # Note:
        # After joining resampling+sum does not work properly. Gives NaN and sum that is not correct.
        # Therefore resamplig+sum is done for each timeseries. If this issue could be solved,
        # then resampling+sum could be carried out after joining.

        # Fill in missing values after joining into one timeseries matrix.
        matrix.fillna(value=np.round(filler, precision_decimal_points), inplace=True, axis='columns')

        station_fractions = self.calc_station_fraction(stations)
        print('----------------------------------station_fractions : ', station_fractions)
        # Make sure only the required station weights remain in the station_fractions, else raise ValueError.
        matrix_station_list = list(matrix.columns.values)
        weights_station_list = list(station_fractions.keys())
        invalid_stations = [key for key in weights_station_list if key not in matrix_station_list]
        for key in invalid_stations:
            station_fractions.pop(key, None)
        if not len(matrix_station_list) == len(station_fractions.keys()):
            raise ValueError('Problem in calculated station weights.', stations, station_fractions)

        # Prepare weights to calc the kub_mean.
        weights = pd.DataFrame.from_dict(data=station_fractions, orient='index', dtype='float')
        weights = weights.divide(self.percentage_factor, axis='columns')

        # klb_mean = (matrix * weights[0]).sum(axis='columns')
        klb_mean = matrix.dot(weights).sum(axis='columns')
        klb_mean_timeseries = klb_mean.to_frame(name='value')
        return klb_mean_timeseries


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def get_stations_timeseries(hourly_csv_file_dir, adapter, stations, start_time, end_time):
    print('[start_time, end_time] : ', [start_time, end_time])
    timeseries_data = copy.deepcopy(stations)
    for key, value in stations.items():
        ts_df = adapter.get_station_timeseries(start_time, end_time, key, value['run_name'])
        if ts_df is not None:
            if ts_df.empty:
                timeseries_data.pop(key, None)
            else:
                timeseries_data[key]['timeseries'] = ts_df
                file_name = '{}_rain.csv'.format(key)
                full_path = os.path.join(hourly_csv_file_dir, file_name)
                ts_df.to_csv(full_path, header=False)
        else:
            timeseries_data.pop(key, None)
    return timeseries_data


def get_kub_mean(hourly_csv_file_dir, db_adapter, stations, ts_start, ts_end):
    try:
        timeseries_data = get_stations_timeseries(hourly_csv_file_dir, db_adapter, stations, ts_start, ts_end)
        kub_mean = KUBObservationMean()
        kub_mean_timeseries = kub_mean.calc_kub_mean(timeseries_data)
        return kub_mean_timeseries
    except Exception as e:
        print('get_kub_mean|Exception : ', str(e))
        return pd.DataFrame(columns=['time', 'value'])


def get_klb_mean(hourly_csv_file_dir, db_adapter, stations, ts_start, ts_end):
    try:
        timeseries_data = get_stations_timeseries(hourly_csv_file_dir, db_adapter, stations, ts_start, ts_end)
        klb_mean = KLBObservationMean()
        klb_mean_timeseries = klb_mean.calc_klb_mean(timeseries_data)
        return klb_mean_timeseries
    except Exception as e:
        print('get_klb_mean|Exception : ', str(e))
        return pd.DataFrame(columns=['time', 'value'])


try:
    run_date = datetime.now().strftime("%Y-%m-%d")
    # run_date ='2019-06-17'
    run_time = datetime.now().strftime("%H:00:00")
    # run_time = '08:00:00'
    backward = 2
    forward = 3
    time_step = 60  # in minutes
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
            run_date = arg  # 2018-05-24
        elif opt in ("-t", "--time"):
            run_time = arg  # 16:00:00
        elif opt in ("-f", "--forward"):
            forward = arg
        elif opt in ("-b", "--backward"):
            backward = arg
        elif opt in ("--wrf-rf"):
            RF_DIR_PATH = arg
        elif opt in ("--wrf-kub"):
            KUB_DIR_PATH = arg
        elif opt in ("-T", "--tag"):
            tag = arg
    print("rainfall gen run_date : ", run_date)
    print("rainfall gen run_time : ", run_time)
    backward = int(backward)
    forward = int(forward)
    run_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    run_datetime_for_ts = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
    ts_start_datetime = run_datetime_for_ts - timedelta(days=backward)
    ts_end_datetime = run_datetime_for_ts + timedelta(days=forward)
    with open('/home/uwcc-admin/hechms_hourly/Workflow/curw_sim/config_rainfall.json') as json_file:
        config = json.load(json_file)
        wrf_data_dir = config["WRF_DATA_DIR"]
        if 'sim_db_config' in config:
            sim_db_config = config['sim_db_config']
        else:
            print('sim_db_config data not found')
        klb_stations = config['klb_obs_stations']
        kub_stations = config['kub_obs_stations']

        kub_basin_extent = config['KELANI_UPPER_BASIN_EXTENT']
        klb_basin_extent = config['KELANI_LOWER_BASIN_EXTENT']

        kelani_lower_basin_shp = get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
        kelani_upper_basin_shp = get_resource_path('extraction/shp/kub-wgs84/kub-wgs84.shp')
        hourly_csv_file_dir = os.path.join(wrf_data_dir, run_date, run_time)
        create_dir_if_not_exists(hourly_csv_file_dir)
        raincsv_file_path = os.path.join(hourly_csv_file_dir, 'DailyRain.csv')
        if not os.path.isfile(raincsv_file_path):
            # mysql_user, mysql_password, mysql_host, mysql_db
            print('sim_db_config : ', sim_db_config)
            sim_adapter = CurwSimAdapter(sim_db_config['user'], sim_db_config['password'], sim_db_config['host'],
                                         sim_db_config['db'])
            forecast_duration = int((ts_end_datetime - ts_start_datetime).total_seconds() / (60 * time_step))
            # sim_adapter.get_station_timeseries('2019-06-16 00:00:00', '2019-06-19 23:30:00', 'Kotikawatta', 'Leecom')
            klb_ts = get_klb_mean(hourly_csv_file_dir, sim_adapter, klb_stations,
                                  ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                  ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
            kub_ts = get_kub_mean(hourly_csv_file_dir, sim_adapter, kub_stations,
                                  ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                  ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
            print('klb_ts: ', klb_ts)
            print('kub_ts: ', kub_ts)
            sim_adapter.close_connection()
            mean_df = pd.merge(kub_ts, klb_ts, on='time')
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
except Exception as e:
    print('rainfall csv file generation error: {}'.format(str(e)))
