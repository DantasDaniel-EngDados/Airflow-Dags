import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from pendulum import DateTime, now

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

# Função para verificar se é Black Friday
def is_black_friday(current_date: DateTime) -> bool:
    if current_date.month == 11 and current_date.weekday() == 4:  # Novembro e sexta-feira
        last_day_of_november = current_date.end_of("month")
        return current_date.day > (last_day_of_november.day - 7)  # Verifica se é a última sexta-feira
    return False

# Função para controle do agendamento de execução
def dynamic_execution():
    current_date = now()
    if is_black_friday(current_date):
        logging.info("Black Friday detected: Running every hour")
        return True  # Marca para rodar a cada hora
    else:
        logging.info("Not Black Friday: Running daily at 9 AM")
        return False  # Marca para rodar apenas uma vez por dia

# Definindo o DAG
@dag(
    dag_id="schedule-timetables-black-friday-dynamic",  # Novo ID do DAG
    start_date=datetime(2025, 1, 8),      # Data de início
    catchup=False,                        # Não fazer o catchup de execuções passadas
    schedule_interval="0 9 * * *",         # Agendamento fixo diário às 9h
)
def main():
    # Definindo os grupos de tarefas
    with TaskGroup("transform_group") as transform_group:
        @task(task_id="extract")
        def extract_bitcoin():
            # Simula a extração dos dados de uma API
            response = requests.get(API).json()
            return response["bitcoin"]
        
        @task(task_id="transform")
        def process_bitcoin(response):
            # Processa os dados de Bitcoin extraídos
            return {"usd": response["usd"], "change": response["usd_24h_change"]}
    
    with TaskGroup("store_group") as store_group:
        @task(task_id="store")
        def store_bitcoin(data):
            # Armazena os dados ou loga as informações
            logging.info(f"Bitcoin price: {data['usd']}, change: {data['change']}")

    # Verifica se é Black Friday ou não e define o comportamento da execução
    if dynamic_execution():
        # Define que deve rodar uma vez por hora
        @task
        def run_hourly():
            logging.info("Running hourly task")
        
        run_hourly()

    else:
        # Se não for Black Friday, executa a DAG normalmente
        bitcoin_data = extract_bitcoin()
        transformed_data = process_bitcoin(bitcoin_data)
        store_bitcoin(transformed_data)

# Executa a DAG
main()
