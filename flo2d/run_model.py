# Copy template and exe directories to daily hour folder and execute exe.
import os
from distutils.dir_util import copy_tree


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def execute_flo2d_250m(dir_path, run_date, run_time):
    print("Flo2d run_date : ", run_date)
    print("Flo2d run_time : ", run_time)
    output_dir = dir_path
    template_dir = os.path.join(os.getcwd(), 'template')
    executable_dir = os.path.join(os.getcwd(), 'RunForProjectFolder')
    try:
        copy_tree(template_dir, output_dir)
        copy_tree(executable_dir, output_dir)
        try:
            os.chdir(output_dir)
            try:
                os.system(os.path.join(output_dir, 'FLOPRO.exe'))
            except Exception as exx:
                print('Execute FLOPRO.exe|Exception : ', str(exx))
        except Exception as ex:
            print('Change the current working directory|Exception : ', str(ex))
    except Exception as e:
        print('template/executable copy error|Exception : ', str(e))

