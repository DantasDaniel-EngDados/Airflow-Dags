from datetime import datetime
from airflow.decorators import dag, task
import requests  # Importando a biblioteca requests
import logging   # Importando a biblioteca logging
from airflow.utils.task_group import TaskGroup

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

@dag(
    dag_id="tf-bitcoin",
    schedule="@daily",
    start_date=datetime(2025,1,6),
    catchup=False
)

def main():
    #todo TaskGroup
    with TaskGroup("transformer") as trasnformes:
        #Task 1
        @task(task_id="extract",retries=2)
        def extract_bitcoin():
            return requests.get(API).json()["bitcoin"]
        #Task 2
        @task(task_id="transform")
        def process_bitcoin(response):
            return {"usd": response["usd"], "change": response["usd_24h_change"]}
        #Todo Dependencies
        processed_data = process_bitcoin(extract_bitcoin())

    with TaskGroup("store") as stores:

        @task(task_id="estore")
        def store_bitcoin(data):
            logging.info(f"Bitcoin price: {data['usd']},change:{data['change']}")

        store_bitcoin(processed_data)
        
main()