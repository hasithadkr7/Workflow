import getopt
from datetime import datetime, timedelta
import traceback
import os
import sys

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def append_to_file(file_name, data):
    with open(file_name, 'a+') as f:
        f.write('\n'.join(data))


def check_time_format(time, model):
    try:
        time = datetime.strptime(time, DATE_TIME_FORMAT)
        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if model == "flo2d_250" and time.strftime('%M') not in (
        '05', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '00'):
            print("Minutes should be multiple of 5 fro flo2d_250")
            exit(1)
        if model == "flo2d_150" and time.strftime('%M') not in ('15', '30', '45', '00'):
            print("Minutes should be multiple of 15 for flo2d_150")
            exit(1)
        return True
    except Exception:
        traceback.print_exc()
        print("Time {} is not in proper format".format(time))
        exit(1)


def prepare_raincell(raincell_file_path, start_time, end_time, rain_fall_value, timestep=15, target_model="flo2d_250"):
    """
    :param raincell_file_path: str
    :param start_time: str '2019-10-23 15:40:00'
    :param end_time: str '2019-10-29 15:40:00'
    :param rain_fall_value: float
    :param timestep: int
    :param target_model: str "flo2d_250"/"flo2d_150"
    :return:
    """
    print('prepare_raincell|raincell_file_path : ', raincell_file_path)
    print('prepare_raincell|start_time : ', start_time)
    print('prepare_raincell|end_time : ', end_time)
    print('prepare_raincell|timestep : ', timestep)
    print('prepare_raincell|target_model : ', target_model)
    print('prepare_raincell|rain_fall_value : ', rain_fall_value)

    end_time = datetime.strptime(end_time, DATE_TIME_FORMAT)
    start_time = datetime.strptime(start_time, DATE_TIME_FORMAT)

    if end_time < start_time:
        print("start_time should be less than end_time")
        exit(1)
    if target_model is "flo2d_250":
        grid_points = 5
    elif target_model is "flo2d_150":
        grid_points = 15

    length = int(((end_time - start_time).total_seconds() / 60) / timestep)

    write_to_file(raincell_file_path,
                  ['{} {} {} {}\n'.format(timestep, length, start_time.strftime(DATE_TIME_FORMAT),
                                          end_time.strftime(DATE_TIME_FORMAT))])
    try:
        timestamp = start_time
        while timestamp < end_time:
            raincell = []
            for i in range(grid_points):
                raincell.append('{} {}'.format(i+1, '%.1f' % rain_fall_value))
            raincell.append('')
            print(timestamp)
            timestamp = timestamp + timedelta(minutes=timestep)
        append_to_file(raincell_file_path, raincell)
    except Exception as ex:
        print('prepare_raincell|Exception : ', str(ex))
    finally:
        print("{} raincell generation process completed".format(datetime.now()))


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def usage():
    usageText = """
    Usage: .\gen_raincell.py [-m flo2d_XXX][-s "YYYY-MM-DD HH:MM:SS"] [-e "YYYY-MM-DD HH:MM:SS"]

    -h  --help          Show usage
    -m  --model         FLO2D model (e.g. flo2d_250, flo2d_150). Default is flo2d_250.
    -s  --start_time    Raincell start time (e.g: "2019-06-05 00:00:00"). Default is 23:30:00, 3 days before today.
    -e  --end_time      Raincell end time (e.g: "2019-06-05 23:30:00"). Default is 23:30:00, tomorrow.
    """
    print(usageText)


if __name__ == "__main__":
    try:
        start_time = None
        end_time = None
        flo2d_model = None
        timestep = None
        rain_fall_value = 0
        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:e:t:r:",
                                       ["help", "flo2d_model=", "start_time=", "end_time=", "timestep=", "rain_fall_value="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-m", "--flo2d_model"):
                flo2d_model = arg.strip()
            elif opt in ("-s", "--start_time"):
                start_time = arg.strip()
            elif opt in ("-e", "--end_time"):
                end_time = arg.strip()
            elif opt in ("-t", "--timestep"):
                timestep = arg.strip()
            elif opt in ("-r", "--rain_fall_value"):
                rain_fall_value = arg.strip()

        print('User inputs|flo2d_model : ', flo2d_model)
        print('User inputs|start_time : ', start_time)
        print('User inputs|end_time : ', end_time)
        print('User inputs|timestep : ', timestep)
        print('User inputs|rain_fall_value : ', rain_fall_value)

        os.chdir(r"D:\raincells")
        os.system(r"venv\Scripts\activate")

        if flo2d_model is None:
            flo2d_model = "flo2d_250"
        if timestep is None:
            timestep = 60
        elif flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        if start_time is None:
            start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d 23:30:00')
        else:
            check_time_format(time=start_time, model=flo2d_model)

        if end_time is None:
            end_time = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 23:30:00')
        else:
            check_time_format(time=end_time, model=flo2d_model)

        raincell_file_path = os.path.join(r"D:\raincells",
                                          'RAINCELL_DESIGN_{}_{}_{}.DAT'.format(flo2d_model, start_time, end_time).replace(' ',
                                                                                                                    '_').replace(
                                              ':', '-'))

        if not os.path.isfile(raincell_file_path):
            print("{} start preparing raincell".format(datetime.now()))
            prepare_raincell(raincell_file_path, start_time, end_time, rain_fall_value,
                             timestep=timestep, target_model=flo2d_model, start_time=start_time,
                             end_time=end_time)
            print("{} completed preparing raincell".format(datetime.now()))
        else:
            print('Raincell file already in path : ', raincell_file_path)
    except Exception:
        traceback.print_exc()