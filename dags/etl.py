import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from tasks.lichess.lichess_extract import lichess_extract
from tasks.lichess.lichess_transform import lichess_transform
from tasks.chesscom.chesscom_extract import chesscom_extract
from tasks.chesscom.chesscom_transform import chesscom_transform
from tasks.validation.quality_check import validate
from tasks.load.load import load



# Define default arguments for the DAG
default_args = {
    'owner': 'Youssef Atef',
    'retries': 5,
    'retry_delay': timedelta(seconds=60)
}

# Define the DAG with its attributes
@dag(
    default_args=default_args,
    dag_id='chess_etl_dag',
    description='a dag to automate the chess etl process',
    start_date=datetime(2020, 5, 1),
    schedule_interval='@daily',
    catchup=False
    )
def etl():
    """
    Defines an ETL process for chess data extraction, transformation, validation, and loading.
    """
    # Extract the data
    extracted_chesscom_games  = chesscom_extract()
    extracted_lichess_games = lichess_extract()

    # Transform the extracted data
    transformed_chesscom_games = chesscom_transform(extracted_chesscom_games)
    transformed_lichess_games = lichess_transform(extracted_lichess_games)

    # Validate the transformed data
    validated_chesscom_data = validate(transformed_chesscom_games)
    validated_lichess_data = validate(transformed_lichess_games)

    # Load the validated data
    load(validated_chesscom_data, validated_lichess_data)

# Run the ETL
etl()