import getopt
import sys
from raincelldat.gen_raincell import create_hybrid_raincell
import os
from datetime import datetime, timedelta


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:t:T:f:b:", [
            "help", "date=", "time=", "forward=", "backward=" ])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            sys.exit()
        elif opt in ("-d", "--date"):
            run_date = arg # 2018-05-24
        elif opt in ("-t", "--time"):
            run_time = arg # 16:00:00
        elif opt in ("-f","--forward"):
            forward = arg
        elif opt in ("-b","--backward"):
            backward = arg
    dir_path = '/home/uwcc-admin/udp/wrf_data'
    # run_date = '2019-05-24'
    # run_time = '00:00:00'
    # forward = '3'
    # backward = '2'
    output_path = os.path.join(dir_path, run_date+'_'+run_time)
    file_path = os.path.join(output_path, 'RAINCELL.DAT')
    if not os.path.exists(file_path):
        create_dir_if_not_exists(output_path)
        # because observe data end date should be day before run_date
        run_date = (datetime.strptime(run_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        create_hybrid_raincell(output_path, run_date, run_time, forward, backward)

