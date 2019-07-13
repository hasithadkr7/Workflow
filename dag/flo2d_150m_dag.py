from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


prod_dag_name = 'flo2d-150m-dag'
queue = 'default'
schedule_interval = '20 */4 * * *'
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
    description='Run Flo2d 150m DAG',
    schedule_interval=schedule_interval)


create_raincell_cmd = 'echo "create_raincell_cmd"'

create_inflow_cmd = 'echo "create_inflow_cmd"'

create_outflow_cmd = 'echo "create_outflow_cmd"'

run_flo2d_150m_cmd = 'echo "run_flo2d_150m_cmd"'

extract_water_level_cmd = 'echo "extract_water_level_cmd"'


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

run_flo2d_150m = BashOperator(
    task_id='run_flo2d_150m',
    bash_command=run_flo2d_150m_cmd,
    dag=dag,
    pool=dag_pool,
)

extract_water_level = BashOperator(
    task_id='extract_water_level',
    bash_command=extract_water_level_cmd,
    dag=dag,
    pool=dag_pool,
)


create_raincell >> create_inflow >> create_outflow >> run_flo2d_150m >> extract_water_level

