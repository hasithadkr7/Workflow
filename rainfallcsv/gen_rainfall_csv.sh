#!/usr/bin/env bash

# Print execution date time
echo `date`

force_run=false

while getopts d:t:f:b: option
do
    case "${option}"
    in
        d) run_date=${OPTARG};;
        t) run_time=${OPTARG};;
        f) forward=${OPTARG};;
        b) backward=${OPTARG};;
    esac
done

# Change directory into where netcdf_data_uploader is located.
echo "Changing into ~/hechms_hourly/Workflow"
cd /home/uwcc-admin/hechms_hourly/Workflow
echo "Inside `pwd`"

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

python3 rainfallcsv/gen_raincsv.py -d ${run_date} -t ${run_time} -f ${forward} -b ${backward} >> gen_rainfall.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate