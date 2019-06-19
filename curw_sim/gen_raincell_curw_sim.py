import json
import logging
import os
import traceback

import pkg_resources
import pymysql

from curw_sim.db_layer import CurwSimAdapter
from datetime import datetime, timedelta

"""
Create hybrid(observed+forecast) raincell file using data from curw_sim database.
"""
LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(filename=r'D:\flo2d_hourly\logs\workflow.log',
                        level=logging.DEBUG,
                        format=LOG_FORMAT)
log = logging.getLogger()

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_resource_path(resource):
    res = pkg_resources.resource_filename(__name__, resource)
    if os.path.exists(res):
        return res
    else:
        raise UnableFindResource(resource)


class UnableFindResource(Exception):
    def __init__(self, res):
        Exception.__init__(self, 'Unable to find %s' % res)


def create_dir_if_not_exists(path):
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_ts_start_end(run_date, run_time, forward=3, backward=2):
    result = []
    """
    method for geting timeseries start and end using input params.
    :param run_date:run_date: string yyyy-mm-ddd
    :param run_time:run_time: string hh:mm:ss
    :param forward:int
    :param backward:int
    :return: tuple (string, string)
    """
    run_datetime = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
    ts_start_datetime = run_datetime - timedelta(days=backward)
    ts_end_datetime = run_datetime + timedelta(days=forward)
    result.append(ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    result.append(ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print(result)
    return result


def get_cell_timeseries(db_adapter, hash_id, observed_end, timseries_start, timeseries_end, res_mins):
    """
    Get timse series dataframe after filling missing values in 5min intervals, 15 min intervals,
     append missing complete day data with 0 values, return df with 5min intervals.
    :param db_adapter: Object of data layer
    :param hash_id: string
    :param observed_end: string
    :param timseries_start: string
    :param timeseries_end: string
    :param res_mins: int
    :return: dataframe
    """
    tms_df = db_adapter.get_cell_timeseries(timseries_start, timeseries_end, hash_id, res_mins)
    if tms_df is not None:
        #print('tms_df : ', tms_df)
        # for row in tms_df.itertuples():
        #     print('')
        return tms_df
    else:
        return None


def create_sim_hybrid_raincells(dir_path, run_date, run_time, forward, backward,
                               res_mins = 60, flo2d_model ='flo2d_250',
                               calc_method = 'MME'):
    """
    Generate raincell file for flo2d using both observed and forecast data using
    :param log: get log module.
    :param dir_path: string path name
    :param run_date: string yyyy-mm-ddd
    :param run_time: string hh:mm:ss
    :param forward: int
    :param backward: int
    :param res_mins: int
    :param flo2d_modele: string
    :return:
    """
    log.info('start creating raincell {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    log.info('create_hybrid_raincell|{},{},{},{},{}'.format(dir_path, run_date, run_time, forward, backward))
    db_adapter = None
    db_config = None
    flo2d_model_config = {}
    backward = int(backward)
    forward = int(forward)
    try:
        raincell_file_path = os.path.join(dir_path, 'RAINCELL.DAT')
        if not os.path.isfile(raincell_file_path):
            print(os.getcwd())
            config_path = os.path.join(os.getcwd(), 'curw_sim', 'config.json')
            with open(config_path) as json_file:
                config = json.load(json_file)
                if 'db_config' in config:
                    db_config = config['db_config']
                else:
                    log.error('db config data not found')
                if db_config is not None:
                    print(db_config)
                    db_adapter = CurwSimAdapter(db_config['user'], db_config['password'], db_config['host'], db_config['db'])
                if '{}_model_config'.format(flo2d_model) in config:
                    flo2d_model_config = config['{}_model_config'.format(flo2d_model)]
                else:
                    log.error('model config data not found')
                if db_adapter is not None or flo2d_model_config:
                    id_date_list = db_adapter.get_flo2d_tms_ids(flo2d_model, calc_method)
                    flo2d_grid_prefix = flo2d_model_config['grid_prefix']
                    flo2d_cell_count = flo2d_model_config['cell_count']
                    [timeseries_start, timeseries_end] = get_ts_start_end(run_date, run_time)
                    print('timeseries_start:', timeseries_start)
                    print('timeseries_end:', timeseries_end)
                    flo2d_cell_data = {}
                    for item in id_date_list:
                        tms_df = get_cell_timeseries(db_adapter, item['hash_id'], item['obs_end'].strftime('%Y-%m-%d %H:%M:%S'), timeseries_start, timeseries_end, res_mins)
                        if tms_df is not None:
                            flo2d_cell_data[item['grid_id']] = tms_df
                    if len(flo2d_cell_data) == flo2d_cell_count:
                        with open(raincell_file_path, 'w') as output_file:
                            timestep_count = int(((forward + backward) * 24 * 60) / res_mins)
                            print('timestep_count : ', timestep_count)
                            output_file.write(
                                "%d %d %s %s\n" % (res_mins, timestep_count, timeseries_start, timeseries_end))
                            tms_start_time = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S')
                            for step in range(timestep_count):
                                tms_step = (tms_start_time + timedelta(minutes=step * res_mins)).strftime(
                                    '%Y-%m-%d %H:%M:%S')
                                for cell_index in range(flo2d_cell_count):
                                    flo2d_grid_id = flo2d_grid_prefix.format(cell_index + 1)
                                    if flo2d_grid_id in flo2d_cell_data:
                                        tms_df = flo2d_cell_data[flo2d_grid_id]
                                        #print('tms_step : ', tms_step)
                                        rf_array = tms_df[tms_df.time == tms_step]['value']
                                        if len(rf_array) > 0:
                                            rf_value = rf_array.values[0]
                                        else:
                                            rf_value = 0
                                        output_file.write('%d %.1f\n' % (cell_index + 1, rf_value))
                                    else:
                                        log.warning('flo2d {} grid point data not found'.format(flo2d_grid_id))
                    else:
                        log.error('Cell data missing')
                else:
                    log.error('Required config data not found')
                    if db_adapter is not None:
                        db_adapter.close_connection()
        else:
            log.info('{} file exists'.format(raincell_file_path))
    except FileNotFoundError as ex:
        log.error('config.json:{}|FileNotFoundError|{}'.format(config_path, str(ex)))
        raise
    finally:
        log.info('Process completed creating raincell {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        if db_adapter is not None:
            db_adapter.close_connection()


def write_to_file(file_name, data):
    with open(file_name, 'w') as f:
        for _list in data:
            for i in range(len(_list) - 1):
                # f.seek(0)
                f.write(str(_list[i]) + ' ')
            f.write(str(_list[len(_list) - 1]))
            f.write('\n')
        f.close()


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        for _list in data:
            for i in range(len(_list) - 1):
                # f.seek(0)
                f.write(str(_list[i]) + ' ')
            f.write(str(_list[len(_list) - 1]))
            f.write('\n')
        f.close()


def prepare_flo2d_250_MME_raincell_5_min_step(start_time, end_time, output_file):
    # Connect to the database
    connection = pymysql.connect(host='35.230.102.148',
            user='root',
            password='cfcwm07',
            db='curw_sim',
            cursorclass=pymysql.cursors.DictCursor)

    print("Connected to database")

    end_time = datetime.strptime(end_time, DATE_TIME_FORMAT)
    start_time = datetime.strptime(start_time, DATE_TIME_FORMAT)

    length = int(((end_time-start_time).total_seconds()/60)/5)
    START = True
    try:
        # Extract raincells
        timestamp = start_time
        while timestamp < end_time:
            timestamp = timestamp + timedelta(minutes=5)
            raincell = []

            # Extract raincell from db
            with connection.cursor() as cursor1:
                cursor1.callproc('flo2d_250_MME_5_min_raincell', (timestamp,))
                results = cursor1.fetchall()
                for result in results:
                    raincell.append([result.get('cell_id'), '%.1f' % (result.get('value'))])
            if START:
                raincell.insert(0, ["{} {} {} {}".format(5, length, start_time, end_time)])
                write_to_file(output_file, raincell)
                START=False
            else:
                append_to_file(output_file, raincell)
            print(timestamp)
    except Exception as ex:
        traceback.print_exc()
    finally:
        connection.close()
        print("{} raincell generation process completed".format(datetime.now()))


def create_sim_hybrid_raincell(dir_path, run_date, run_time, forward, backward,
                               res_mins = 60, flo2d_model ='flo2d_250',
                               calc_method = 'MME'):
    [timeseries_start, timeseries_end] = get_ts_start_end(run_date, run_time)
    raincell_file_path = os.path.join(dir_path, 'RAINCELL.DAT')
    if not os.path.isfile(raincell_file_path):
        prepare_flo2d_250_MME_raincell_5_min_step(timeseries_start, timeseries_end, raincell_file_path)




# if __name__ == "__main__":
#     run_date = '2019-06-13'
#     run_time = '17:00:00'
#     dir_path = os.path.join('/home/hasitha/PycharmProjects/Workflow/output', run_date, run_time)
#     create_dir_if_not_exists(dir_path)
#     forward = 3
#     backward = 2
#     res_minutes = 5
#     required_flo2d_model = 'flo2d_250'
#     required_calc_method = 'MME'
#     create_hybrid_raincell(dir_path, run_date, run_time, forward, backward, res_mins=res_minutes, flo2d_model=required_flo2d_model, calc_method=required_calc_method)
#
