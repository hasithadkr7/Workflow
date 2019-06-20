import json
import logging
import os
import traceback
import numpy as np
import pkg_resources
import pymysql
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


def write_to_file(file_name, data):
    fh = open(file_name, 'w')
    fh.write(data)
    fh.close()


def append_to_file(file_name, data):
    np.savetxt(open(file_name, 'a'), data, fmt="%s")


def prepare_flo2d_250_MME_raincell_5_min_step(start_time, end_time, raincell_file_path):
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
    write_to_file(raincell_file_path,
            '{} {} {} {}\n'.format(5, length, start_time.strftime(DATE_TIME_FORMAT), end_time.strftime(DATE_TIME_FORMAT)))
    try:
        timestamp = start_time
        while timestamp < end_time:
            raincell = []
            timestamp = timestamp + timedelta(minutes=5)
            # Extract raincell from db
            with connection.cursor() as cursor:
                cursor.callproc('flo2d_250_MME_5_min_raincell', (timestamp,))
                cursor.nextset()
                results = cursor.fetchall()
                for result in results:
                    raincell.append('{} {}'.format(result.get('cell_id'), '%.1f' % result.get('value')))
            append_to_file(raincell_file_path, raincell)
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
