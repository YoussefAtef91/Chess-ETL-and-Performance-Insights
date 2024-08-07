import pandas as pd
import numpy as np
import requests
from datetime import datetime
import pytz
import psycopg2
from psycopg2.sql import SQL
from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()

# Existing database connection parameters
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

#Extract All games
try:
    url = "https://lichess.org/api/games/user/YoussefAtef91?tags=true&clocks=true&evals=true&opening=true"
    response = requests.get(url)
except Exception as e:
    print("Error: {e}")

data = response.text

# Function to determine game speed based on time control
def TimeClass(TimeControl):
    if TimeControl == '-':
        return 'Unlimited'
    Seconds = int(TimeControl.split("+")[0])
    if Seconds <= 15:
        return "HyperBullet"
    elif Seconds < 180:
        return "Bullet"
    elif Seconds < 600:
        return "Blitz"
    elif Seconds < 1800:
        return "Rapid"
    else:
        return "Classical"

# Function to localize UTC datetime to Cairo timezone
def localize_UTCDatetime(UTCDate, UTCTime):
    utc_time = datetime.strptime(f'{UTCDate} {UTCTime}', '%Y.%m.%d %H:%M:%S')
    utc_timezone = pytz.utc
    utc_time = utc_timezone.localize(utc_time)
    local_timezone = pytz.timezone("Africa/Cairo")
    local_time = utc_time.astimezone(local_timezone)
    return local_time

# Split the PGN data into individual games
games = data.split("\n\n\n")
games_list = []

# Convert the data into a DataFrame
for game in games:
    game_dict = {}
    for i in game.split("\n")[:-1]:
        if len(i) != 0:
            key = i.split(' "')[0][1:]
            value = i.split(' "')[1][:-2]
            game_dict[key] = value
    games_list.append(game_dict)

df = pd.DataFrame(games_list[:-1])

# Add a column for pieces color
df['PiecesColor'] = df.apply(lambda x: 'White' if x.White.lower() == 'YoussefAtef91'.lower() else 'Black', axis=1)

# Add a column for Opponent username and title
df['Opponent'] = df.apply(lambda x: x.White if x.PiecesColor == 'Black' else x.Black, axis=1)
if 'WhiteTitle' not in df.columns:
    df['WhiteTitle'] = np.nan
if 'BlackTitle' not in df.columns:
    df['BlackTitle'] = np.nan
df[['WhiteTitle','BlackTitle']] = df[['WhiteTitle','BlackTitle']].fillna('Untitled')
df['OpponentTitle'] = df.apply(lambda x: x.WhiteTitle if x.PiecesColor=='Black' else x.BlackTitle, axis=1)

# Add a column for game result
df['Winner'] = df.apply(lambda x: 'White' if x.Result == '1-0' else 'Black' if x.Result == '0-1' else 'Draw', axis=1)
df['Result'] = df.apply(lambda x: 'Win' if x.Winner == x.PiecesColor else 'Draw' if x.Winner == 'Draw' else 'Loss', axis=1)

# et my elo and opponent elo out of WhiteElo and BlackElo columns
df['MyElo'] = df.apply(lambda x: x.WhiteElo if x.PiecesColor == 'White' else x.BlackElo, axis=1)
df['OpponentElo'] = df.apply(lambda x: x.WhiteElo if x.PiecesColor == 'Black' else x.BlackElo, axis=1)

# Get my rating diff and opponent rating diff out of WhiteRatingDiff and BlackRatingDiff columns
if 'BlackRatingDiff' not in df.columns and 'WhiteRatingDiff' not in df.columns:
    df['BlackRatingDiff'] = np.zeros(len(df))
    df['WhiteRatingDiff'] = np.zeros(len(df))
df[['WhiteRatingDiff','BlackRatingDiff']] = df[['WhiteRatingDiff','BlackRatingDiff']].fillna(0)
df['MyRatingDiff'] = df.apply(lambda x: x.WhiteRatingDiff if x.PiecesColor=='White' else x.BlackRatingDiff, axis=1).astype(int)
df['OpponentRatingDiff'] = df.apply(lambda x: x.WhiteRatingDiff if x.PiecesColor=='Black' else x.BlackRatingDiff, axis=1).astype(int)
df['MyRatingDiff'] = df['MyRatingDiff'].apply(lambda x: str(x).replace("+" ,"")).astype(int)
df['OpponentRatingDiff'] = df['OpponentRatingDiff'].apply(lambda x: str(x).replace("+" ,"")).astype(int)

# Add columns for Game Type ans Time Class
df['GameType'] = df.apply(lambda x: 'Casual' if 'Casual' in x.Event else 'Rated', axis=1)
df['TimeClass'] = df['TimeControl'].map(TimeClass)

# Localize the Datetime to Cairo timezone
df['CairoDatetime'] = df.apply(lambda x: localize_UTCDatetime(x.UTCDate, x.UTCTime), axis=1)

# Replace the apostrophe with comman so it can be loaded to the Database properly
df['Opening'] = df['Opening'].apply(lambda x: x.replace("'", ","))

# Drop unnecessary columns
columns_to_drop = ['White','Black','Winner','WhiteTitle','BlackTitle', 'WhiteRatingDiff','BlackRatingDiff',
                    'Event','Date','UTCDate','UTCTime','WhiteElo','BlackElo']
df.drop(columns_to_drop,axis=1,inplace=True)

if 'FEN' in df.columns:
    df.drop('FEN', axis=1, inplace=True)

if 'SetUp' in df.columns:
    df.drop('SetUp', axis=1, inplace=True)

# Rename the site column to GameUrl
df.rename({"Site":"GameUrl"},axis=1, inplace=True)

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

# Create a DB connection
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Execute the query
cur.execute(SQL(insert_query))
conn.commit()

print("Data Inserted Successfully")

cur.close()
conn.close()
