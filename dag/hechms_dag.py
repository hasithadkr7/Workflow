from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'hec-hms-dag'
queue = 'default'
schedule_interval = '10 * * * *'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0, hour=6),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': queue,
    'catchup': False,
}

# initiate the DAG
dag = DAG(
    prod_dag_name,
    default_args=default_args,
    description='Run HecHms DAG',
    schedule_interval=schedule_interval)


create_rainfall_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/gen_rainfall_csv.sh " \
                    "-d {{ macros.ds_add(ds, -1) }} -t {{ execution_date.strftime(\"%H:00:00\") }} \" \'"

run_hechms_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/hec_hms_runner.sh " \
                    "-d {{ macros.ds_add(ds, -1) }} -t {{ execution_date.strftime(\"%H:00:00\") }} \" \'"

upload_discharge_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/upload_discharge_data.sh " \
                    "-d {{ macros.ds_add(ds, -1) }} -t {{ execution_date.strftime(\"%H:00:00\") }} \" \'"

create_rainfall = BashOperator(
    task_id='create_rainfall',
    bash_command=create_rainfall_cmd,
    dag=dag,
    pool=dag_pool,
)

run_hechms = BashOperator(
    task_id='run_hechms',
    bash_command=run_hechms_cmd,
    dag=dag,
    pool=dag_pool,
)

upload_discharge = BashOperator(
    task_id='upload_discharge',
    bash_command=upload_discharge_cmd,
    dag=dag,
    pool=dag_pool,
)

create_rainfall >> run_hechms >> upload_discharge

