from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator

prod_dag_name = 'curw_hourly_workflow'
queue = 'default'
schedule_interval = '15 * * * *'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': datetime.strptime('2019-05-15 07:00:00','%Y-%m-%d %H:%M:%S'),
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
    description='Curw hourly run DAG',
    schedule_interval=schedule_interval)


create_rainfall_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/gen_rainfall_csv.sh " \
                    "-d {{ execution_date.strftime(\"%Y-%m-%d\") }} -t {{ execution_date.strftime(\"%H:00:00\") }} \" \'"

run_hechms_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/hec_hms_runner.sh " \
                    "-d {{ execution_date.strftime(\"%Y-%m-%d\") }} -t {{ execution_date.strftime(\"%H:00:00\") }} \" \'"

upload_discharge_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/upload_discharge_data.sh " \
                    "-d {{ execution_date.strftime(\"%Y-%m-%d\") }} -t {{ execution_date.strftime(\"%H:00:00\") }} \" \'"

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

create_raincell = SimpleHttpOperator(
    task_id='create_raincell',
    method='GET',
    endpoint='10.138.0.4:8088/create-outflow',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:00:00\") }}", "forward": 3, "backward": 2},
    headers={},
    dag=dag,
)

create_inflow = SimpleHttpOperator(
    task_id='create_inflow',
    method='GET',
    endpoint='10.138.0.4:8088/create-inflow',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:00:00\") }}"},
    headers={},
    dag=dag,
)

create_outflow = SimpleHttpOperator(
    task_id='create_outflow',
    method='GET',
    endpoint='10.138.0.4:8088/create-raincell',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:00:00\") }}", "forward": 3, "backward": 2},
    headers={},
    dag=dag,
)

run_flo2d_250m = SimpleHttpOperator(
    task_id='run_flo2d_250m',
    method='GET',
    endpoint='10.138.0.4:8088/run-flo2d',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:00:00\") }}"},
    headers={},
    dag=dag,
)

create_rainfall >> run_hechms >> upload_discharge >> create_raincell >> create_inflow >> create_outflow >> run_flo2d_250m

