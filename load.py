import os
from airflow.models import Variable
import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.decorators import task

@task()
def load(df_dict):
        # Convert the dictionary format data into a DataFrame
        df = pd.DataFrame(df_dict)

        # Crate a SQL insert query to insert the dataframe into the games table
        insert_query = """
        INSERT INTO games (GameUrl, Opponent, Result, Variant, TimeControl, ECO, Opening, PiecesColor, OpponentTitle,
        MyRatingDiff, OpponentRatingDiff, GameType, TimeClass, CairoDatetime, MyElo, OpponentElo) VALUES
        """ + ', '.join([f"('{row['GameUrl']}', '{row['Opponent']}', '{row['Result']}', '{row['Variant']}', '{row['TimeControl']}', '{row['ECO']}', "
                        f"'{row['Opening']}', '{row['PiecesColor']}', '{row['OpponentTitle']}', {row['MyRatingDiff']}, "
                        f"{row['OpponentRatingDiff']}, '{row['GameType']}', '{row['TimeClass']}', '{row['CairoDatetime']}', "
                        f"'{row['MyElo']}', '{row['OpponentElo']}')" for index, row in df.iterrows()]) + """
        ON CONFLICT (GameUrl) DO NOTHING;
        """

        # Create a PostgresHook instance to interact with the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")

        # Execute the SQL query to insert the data into the database
        pg_hook.run(insert_query)
