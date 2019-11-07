from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'flo2d-150m-dag-1'
queue = 'default'
schedule_interval = '0 */6 * * *'
dag_pool = 'flo2d_150_pool'


default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': datetime.strptime('2019-10-09 18:20:00', '%Y-%m-%d %H:%M:%S'),
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
    dagrun_timeout=timedelta(hours=6),
    default_args=default_args,
    description='Run Flo2d 150m DAG',
    schedule_interval=schedule_interval)

create_raincell_cmd = 'curl -X GET "http://10.138.0.7:8089/create-sim-raincell?' \
                      'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                      '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                      '&forward=3&backward=5"'

create_inflow_cmd = 'curl -X GET "http://10.138.0.7:8089/create-inflow?' \
                    'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                    '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                    '&forward=3&backward=5"'

create_outflow_cmd = 'curl -X GET "http://10.138.0.7:8089/create-outflow?' \
                     'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                     '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                     '&forward=3&backward=5"'

run_flo2d_150m_cmd = 'curl -X GET "http://10.138.0.7:8089/run-flo2d?' \
                     'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                     '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}"'

extract_water_level_curw_fcst_cmd = 'curl -X GET "http://10.138.0.7:8089/extract-curw-fcst?' \
                               'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                               '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                               '&forward=3&backward=5"'

extract_water_level_curw_cmd = 'curl -X GET "http://10.138.0.7:8089/extract-curw?' \
                     'run_date={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%Y-%m-%d\") }}' \
                     '&run_time={{ (execution_date - macros.timedelta(days=1) + macros.timedelta(hours=5,minutes=30)).strftime(\"%H:00:00\") }}' \
                     '&forward=3&backward=5"'


create_raincell = BashOperator(
    task_id='create_raincell',
    bash_command=create_raincell_cmd,
    execution_timeout=timedelta(hours=1),
    dag=dag,
    pool=dag_pool,
)

create_inflow = BashOperator(
    task_id='create_inflow',
    bash_command=create_inflow_cmd,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
    pool=dag_pool,
)

create_outflow = BashOperator(
    task_id='create_outflow',
    bash_command=create_outflow_cmd,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
    pool=dag_pool,
)

run_flo2d_150m = BashOperator(
    task_id='run_flo2d_150m',
    bash_command=run_flo2d_150m_cmd,
    execution_timeout=timedelta(hours=4, minutes=30),
    dag=dag,
    pool=dag_pool,
)

extract_water_level_curw_fcst = BashOperator(
    task_id='extract_water_level_curw_fcst',
    bash_command=extract_water_level_curw_fcst_cmd,
    execution_timeout=timedelta(minutes=20),
    dag=dag,
    pool=dag_pool,
)

extract_water_level_curw = BashOperator(
    task_id='extract_water_level_curw',
    bash_command=extract_water_level_curw_cmd,
    execution_timeout=timedelta(minutes=20),
    dag=dag,
    pool=dag_pool,
)

create_raincell >> create_inflow >> create_outflow >> \
run_flo2d_150m >> extract_water_level_curw_fcst >> extract_water_level_curw

