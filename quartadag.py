from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'Quarta_dag',
    description = 'Nossa Quarta DAG',
    schedule_interval = None,
    start_date = datetime(2024,10,20),
    catchup = False
    ) as dag:

    task1 = BashOperator(
        task_id="tsk1",
        bash_command="sleep 5"
    )

    task2 = BashOperator(
        task_id="tsk2",
        bash_command="sleep 5"
    )

    task3 = BashOperator(
        task_id="tsk3",
        bash_command="sleep 5"
    )

    task4 = BashOperator(
        task_id="tsk4",
        bash_command="sleep 5"
    )

    task1.set_upstream(task2)
    task2.set_upstream(task4)
    task3.set_upstream(task4)