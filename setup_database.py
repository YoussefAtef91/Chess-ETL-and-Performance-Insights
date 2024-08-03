import psycopg2
from psycopg2.sql import SQL
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Existing database connection parameters
existing_db_params = {
    'dbname': 'postgres',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

new_db_name = os.getenv('DB_NAME')

# Connect to the existing database
conn = psycopg2.connect(**existing_db_params)
conn.autocommit = True # Allow auto-commit for database creation
cur = conn.cursor()

# Check if the new database already exists
cur.execute(SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [new_db_name])
exists = cur.fetchone()

# Create the new database if it doesn't exist
if not exists:
    cur.execute(SQL("CREATE DATABASE {}").format(
            sql.Identifier(new_db_name)
        ))
    print(f'Dababase {new_db_name} created successfully')
else:
    print(f'Database {new_db_name} alreay exists')

cur.close()
conn.close()

# Update connection parameters to the new database
new_db_params = existing_db_params.copy()
new_db_params['dbname'] = new_db_name

conn = psycopg2.connect(**new_db_params)
cur = conn.cursor()

# SQL query to create the 'games' table if it does not already exist
create_table_query = create_table_query = """ 
        CREATE TABLE IF NOT EXISTS games (
        GameUrl VARCHAR PRIMARY KEY,
        Opponent VARCHAR,
        Result VARCHAR,
        Variant VARCHAR,
        TimeControl VARCHAR,
        ECO VARCHAR(3),
        Opening VARCHAR,
        PiecesColor VARCHAR,
        OpponentTitle VARCHAR,
        MyRatingDiff INT,
        OpponentRatingDiff INT,
        GameType VARCHAR,
        TimeClass VARCHAR,
        CairoDatetime TIMESTAMP,
        MyElo VARCHAR,
        OpponentElo VARCHAR
);"""

cur.execute(SQL(create_table_query))
conn.commit()

cur.close()
conn.close()