from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

prod_dag_name = 'flo2d-250m-dag-3'
queue = 'default'
schedule_interval = '20 * * * *'
dag_pool = 'curw_prod_runs'

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': datetime.strptime('2019-11-06 10:00:00', '%Y-%m-%d %H:%M:%S'),
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
    prod_dag_name, catchup=False,
    dagrun_timeout=3300,
    default_args=default_args,
    description='Run Flo2d 250m DAG using curw_sim db',
    schedule_interval=schedule_interval)

create_raincell_cmd = 'curl -X GET "http://10.138.0.4:8088/create-sim-raincell?' \
                      'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                      '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                      '&forward=3&backward=2"'

create_inflow_cmd = 'curl -X GET "http://10.138.0.4:8088/create-inflow?' \
                    'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                    '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'

create_outflow_cmd = 'curl -X GET "http://10.138.0.4:8088/create-outflow?' \
                     'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                     '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                     '&forward=3&backward=2"'

run_flo2d_250m_cmd = 'curl -X GET "http://10.138.0.4:8088/run-flo2d?' \
                     'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                     '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'

extract_water_level_cmd = 'curl -X GET "http://10.138.0.4:8088/extract-data?' \
                          'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                          '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'

extract_water_level_curw_cmd = 'curl -X GET "http://10.138.0.4:8088/extract-curw?' \
                               'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                               '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'

create_raincell = BashOperator(
    task_id='create_raincell',
    bash_command=create_raincell_cmd,
    execution_timeout=600,
    dag=dag,
    pool=dag_pool,
)

create_inflow = BashOperator(
    task_id='create_inflow',
    bash_command=create_inflow_cmd,
    execution_timeout=300,
    dag=dag,
    pool=dag_pool,
)

create_outflow = BashOperator(
    task_id='create_outflow',
    bash_command=create_outflow_cmd,
    execution_timeout=300,
    dag=dag,
    pool=dag_pool,
)

run_flo2d_250m = BashOperator(
    task_id='run_flo2d_250m',
    bash_command=run_flo2d_250m_cmd,
    execution_timeout=2700,
    dag=dag,
    pool=dag_pool,
)


extract_water_level = BashOperator(
    task_id='extract_water_level',
    bash_command=extract_water_level_cmd,
    execution_timeout=300,
    dag=dag,
    pool=dag_pool,
)

extract_water_level_curw = BashOperator(
    task_id='extract_water_level_curw',
    bash_command=extract_water_level_curw_cmd,
    execution_timeout=300,
    dag=dag,
    pool=dag_pool,
)

create_raincell >> create_inflow >> create_outflow >> run_flo2d_250m >> extract_water_level >> extract_water_level_curw
