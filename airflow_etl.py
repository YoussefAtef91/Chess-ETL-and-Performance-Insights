from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from extract import extract
from transform import transform
from load import load

# Define default arguments for the DAG
default_args = {
    'owner': 'Youssef Atef',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG with its attributes
@dag(default_args=default_args,
     dag_id='chess_etl_dag',
     description='a dag to automate the chess etl process',
     start_date=datetime(2024, 7, 27),
     schedule_interval='@daily',
     catchup=False) # Prevent backfilling if DAG start date is in the past
def ETL():
    # Extract data from the API
    pgn_data = extract()
    # Transform the extracted data
    df_dict = transform(pgn_data)
    # Load the transformed data into the database
    load(df_dict)

# Instantiate the DAG
ETL()