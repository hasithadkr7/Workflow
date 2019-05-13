from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'curw_hourly_workflow'
queue = 'default'
schedule_interval = '10 * * * *'
dag_pool = 'curw_prod_runs'


default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
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

#-----------HecHms Tasks-------------
create_rainfall_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/gen_rainfall_csv.sh " \
                    "-d {{ execution_date.strftime(\"%Y-%m-%d\") }} -t {{ execution_date.strftime(\"%H:%M:%S\") }} \" \'"

run_hechms_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/hec_hms_runner.sh " \
                    "-d {{ execution_date.strftime(\"%Y-%m-%d\") }} -t {{ execution_date.strftime(\"%H:%M:%S\") }} \" \'"

upload_discharge_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/hechms_hourly/upload_discharge_data.sh " \
                    "-d {{ execution_date.strftime(\"%Y-%m-%d\") }} -t {{ execution_date.strftime(\"%H:%M:%S\") }} \" \'"

#-----------Flo2d Tasks---------------

create_raincell_cmd = "echo \"create_raincell_cmd\""

create_inflow_cmd = "echo \"create_inflow_cmd\""

create_outflow_cmd = "echo \"create_outflow_cmd\""

run_flo2d_250m_cmd = "echo \"run_flo2d_250m_cmd\""


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

create_raincell = BashOperator(
    task_id='create_raincell',
    bash_command=create_raincell_cmd,
    dag=dag,
    pool=dag_pool,
)

create_inflow = BashOperator(
    task_id='create_inflow',
    bash_command=create_inflow_cmd,
    dag=dag,
    pool=dag_pool,
)

create_outflow = BashOperator(
    task_id='create_outflow',
    bash_command=create_outflow_cmd,
    dag=dag,
    pool=dag_pool,
)

run_flo2d_250m = BashOperator(
    task_id='run_flo2d_250m',
    bash_command=run_flo2d_250m_cmd,
    dag=dag,
    pool=dag_pool,
)

create_rainfall >> run_hechms >> create_raincell >> create_inflow >> create_outflow >> run_flo2d_250m

