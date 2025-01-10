import requests
import logging

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

cryptocurrencies = ["ethereum","dogecoin","bitcoin"]
api_call_template = "https://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd"

@dag(
    dag_id="dtm-cryptocurrency-prices",
    schedule_interval="@daily",
    start_date=datetime(2025,1,9),
    catchup=False
)

def crypto_prices():
    @task(map_index_template="{{ crypto }}")
    def fetch_price(crypto: str):
        context = get_current_context()
        context["crypto"] = crypto
        
        api_url = api_call_template.format(crypto=crypto)
        response = requests.get(api_url).json()
        price = response[crypto]['usd']
        logging.info(f"the price of {crypto} is ${price}")

        return price
    
    prices = fetch_price.partial().expand(crypto = cryptocurrencies)

    prices

crypto_prices()