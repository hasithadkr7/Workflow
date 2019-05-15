from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

prod_dag_name = 'flo2d_250m_workflow'
queue = 'default'
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
    description='Curw flo2d 250m run DAG')

#-----------Flo2d Tasks---------------
#curl -X GET "http://10.138.0.4:8088/create-outflow?run_date=2019-05-15&run_time=09:00:00&forward=3&backward=2"

create_raincell = SimpleHttpOperator(
    task_id='create_raincell',
    method='GET',
    endpoint='10.138.0.4:8088/create-outflow',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:%M:%S\") }}", "forward": 3, "backward": 2},
    headers={},
    dag=dag,
)

create_inflow = SimpleHttpOperator(
    task_id='create_inflow',
    method='GET',
    endpoint='10.138.0.4:8088/create-inflow',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:%M:%S\") }}"},
    headers={},
    dag=dag,
)

create_outflow = SimpleHttpOperator(
    task_id='create_outflow',
    method='GET',
    endpoint='10.138.0.4:8088/create-raincell',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:%M:%S\") }}", "forward": 3, "backward": 2},
    headers={},
    dag=dag,
)

run_flo2d_250m = SimpleHttpOperator(
    task_id='run_flo2d_250m',
    method='GET',
    endpoint='10.138.0.4:8088/run-flo2d',
    data={"run_date":"{{ execution_date.strftime(\"%Y-%m-%d\") }}", "run_time":"{{ execution_date.strftime(\"%H:%M:%S\") }}"},
    headers={},
    dag=dag,
)

create_raincell >> create_inflow >> create_outflow >> run_flo2d_250m

