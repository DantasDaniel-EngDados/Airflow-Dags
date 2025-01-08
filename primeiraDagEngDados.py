from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

default_arg = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

@dag(
    dag_id="dag_structure",
    start_date=datetime(2025,1,5),
    max_active_runs=1,
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args=default_arg,
    tags=['first', 'dag']
)

def init():

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> end


dag = init()