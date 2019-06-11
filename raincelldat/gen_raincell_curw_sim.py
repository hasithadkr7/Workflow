import json
import os
import pkg_resources
from curw_sim.db_layer import CurwSimAdapter
from logs.workflow_logger import get_logger

"""
Create hybrid(observed+forecast) raincell file using data from curw_sim database.
"""


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
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def create_hybrid_raincell(logger, dir_path, run_date, run_time, forward, backward, res_mins = 60,
                           model_prefix ='wrf', forecast_source ='wrf0', run_name ='Cloud-1'):
    """
    Generate raincell file for flo2d using both observed and forecast data using
    :param logger: get log module.
    :param dir_path: string path name
    :param run_date: string yyyy-mm-ddd
    :param run_time: string hh:mm:ss
    :param forward: int
    :param backward: int
    :param res_mins: int
    :param model_prefix: string
    :param forecast_source: string
    :param run_name: string
    :return:
    """
    logger.info('create_hybrid_raincell|{},{},{},{},{}'.format(dir_path, run_date, run_time, forward, backward))
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
    except FileNotFoundError as ex:
        logger.error('config.json:{}|FileNotFoundError|{}'.format(config_path, str(ex)))
        raise


if __name__ == "__main__":
    run_date = '2019-06-07'
    run_time = '17:00:00'
    dir_path = os.path.join('/home/hasitha/PycharmProjects/Workflow/output', run_date, run_time)
    create_dir_if_not_exists(dir_path)
    forward = 3
    backward = 2
    res_mins = 60
    model_prefix = 'wrf'
    forecast_source = 'wrf0'
    run_name = 'Cloud-1'
    logger = get_logger()
    create_hybrid_raincell(logger, dir_path, run_date, run_time, forward, backward,
                           res_mins, model_prefix, forecast_source, run_name)

