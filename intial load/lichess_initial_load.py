import pandas as pd
import numpy as np
import requests
from datetime import datetime
import pytz
import psycopg2
from psycopg2.sql import SQL
from dotenv import load_dotenv
import os
import chess

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

# Set account username
USERNAME = 'YoussefAtef91'

# Helper function to determine game speed based on time control
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

# Helper function to localize UTC datetime to Cairo timezone
def localize_UTCDatetime(UTCDate, UTCTime):
    utc_time = datetime.strptime(f'{UTCDate} {UTCTime}', '%Y.%m.%d %H:%M:%S')
    utc_timezone = pytz.utc
    utc_time = utc_timezone.localize(utc_time)
    local_timezone = pytz.timezone("Africa/Cairo")
    local_time = utc_time.astimezone(local_timezone)
    return local_time

#Extract All games
try:
    games_url = f"https://lichess.org/api/games/user/{USERNAME}?tags=true&clocks=true&evals=true&opening=true"
    games_response = requests.get(games_url)
except Exception as e:
    print(f"Error: {e}")

games_data = games_response.text

#Extract Ratings
try:
    ratings_url = f"https://lichess.org/api/user/{USERNAME}"
    ratings_response = requests.get(ratings_url)
except Exception as e:
    print(f"Error: {e}")

ratings_data = ratings_response.json()


# Split the PGN data into individual games
games = games_data.split("\n\n\n")
games_list = []

# Convert the data into a DataFrame
for game in games:
    game_dict = {}
    if len(game) != 0:
        game_dict['Moves'] = game.split("\n\n")[1]
    for i in game.split("\n")[:-1]:
        if len(i) != 0:
            key = i.split(' "')[0][1:]
            value = i.split(' "')[1][:-2]
            game_dict[key] = value
    games_list.append(game_dict)

df = pd.DataFrame(games_list[:-1])

# Add a column for pieces color
df['PiecesColor'] = df.apply(lambda x: 'White' if x.White.lower() == USERNAME.lower() else 'Black', axis=1)

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

# Get my elo and opponent elo out of WhiteElo and BlackElo columns
df['MyElo'] = df.apply(lambda x: x.WhiteElo if x.PiecesColor == 'White' else x.BlackElo, axis=1)
df['OpponentElo'] = df.apply(lambda x: x.WhiteElo if x.PiecesColor == 'Black' else x.BlackElo, axis=1)

# Remove the '?' out of the MyElo and OpponentElo columns
df['MyElo'] = df['MyElo'].str.replace('?', '', regex=False)
df['OpponentElo'] = df['OpponentElo'].str.replace('?', '', regex=False)

df['MyElo'] = df['MyElo'].replace('', '0')
df['OpponentElo'] = df['OpponentElo'].replace('', '0')

df['MyElo'] = pd.to_numeric(df['MyElo'])
df['OpponentElo'] = pd.to_numeric(df['OpponentElo'])

# Replce '?' in ECO with A00 which refers to unkown openings
df['ECO'] = df['ECO'].str.replace('?', 'A00')

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
df['CairoDatetime'] = pd.to_datetime(df['CairoDatetime']).astype(str)

df['Account_id'] = 1

# Drop unnecessary columns
columns_to_drop = ['White','Black','Winner','WhiteTitle','BlackTitle', 'WhiteRatingDiff','BlackRatingDiff',
                    'Event','Date','UTCDate','UTCTime','WhiteElo','BlackElo', 'Opening']
df.drop(columns_to_drop,axis=1,inplace=True)

if 'FEN' in df.columns:
    df.drop('FEN', axis=1, inplace=True)

if 'SetUp' in df.columns:
    df.drop('SetUp', axis=1, inplace=True)

# Rename the site column to GameUrl
df.rename({"Site":"GameUrl"},axis=1, inplace=True)


moves_list = []

# Iterate over the dataframe
for i in range(len(df)):
    # Extract moves and game_url from the dataframe
    moves = df[['GameUrl', 'Moves']].values[i][1]
    game_url = df[['GameUrl', 'Moves']].values[i][0]
    
    # Initialize the chess board
    board = chess.Board()  # Start from the initial position
    
    while True:
        start = 0
        end = moves.find("}") + 1
        
        # Break the loop if no more moves are found
        if end == 0:
            break
        
        # Extract the current move
        move = moves[start:end]
        
        # Remove the extracted move from the remaining string
        moves = moves[end+1:]
        
        # Break if there are no more moves left
        if len(moves) < 5:
            break
        
        # Extract move number (could be multiple digits, so slice properly)
        move_number = move.split()[0]
        move_number = int(move_number[:move_number.index(".")])
        
        # Determine the turn (Black or White)
        turn = "Black" if move[1:3] == '..' else 'White'
        
        # Extract the actual move (depends on how moves are formatted)
        move_ = move[move.find(" ") + 1: move.find(" ", move.find(" ") + 1)]
        
        # Extract the time (if applicable)
        time = move[move.index("k") + 2: move.index("]")]
        
        # Get the FEN before making the move
        fen_before = board.fen()
        
        # Apply the move to the chess board
        try:
            board.push_san(move_)
        except ValueError:
            # Handle invalid move, break the loop
            break
        
        # Get the FEN after making the move
        fen_after = board.fen()
        
        # Store the move and FEN data in the list
        moves_list.append([game_url, move_number, turn, move_, time, fen_before, fen_after])

moves_df = pd.DataFrame(data=moves_list,columns=['GameUrl','MoveNumber','Turn','Move','CLK','FEN_Before','FEN_After'])


lichess_ratings = {'Account_id':1,'Datetime':str(datetime.now())}
variants = ['ultraBullet', 'bullet', 'blitz', 'rapid', 'classical', 'chess960',
            'kingOfTheHill', 'threeCheck', 'antichess', 'atomic', 'racingKings', 'crazyhouse']
for variant in variants:
    if variant in ratings_data['perfs'].keys():
        lichess_ratings[variant.capitalize()] = ratings_data['perfs'][variant]['rating']
    else:
        lichess_ratings[variant.capitalize()] = None


df.drop('Moves',axis=1,inplace=True)



# Crate a SQL insert query to insert the dataframes into the database
insert_query = """
INSERT INTO Games ( GameUrl, Result, Variant, TimeControl, ECO, Termination, PiecesColor,
                    Opponent, OpponentTitle, MyElo, OpponentElo, MyRatingDiff, OpponentRatingDiff,
                    GameType, TimeClass, CairoDatetime, Account_id ) VALUES
""" + ', '.join([f"('{row['GameUrl']}', '{row['Result']}', '{row['Variant']}', '{row['TimeControl']}', '{row['ECO']}', '{row['Termination']}', "
                f"'{row['PiecesColor']}', '{row['Opponent']}', '{row['OpponentTitle']}', {row['MyElo']}, "
                f"{row['OpponentElo']}, '{row['MyRatingDiff']}', '{row['OpponentRatingDiff']}', '{row['GameType']}', "
                f"'{row['TimeClass']}', '{row['CairoDatetime']}', {row['Account_id']})" for index, row in df.iterrows()]) + """
ON CONFLICT (GameUrl) DO NOTHING;

INSERT INTO Moves ( GameUrl, MoveNumber, PieceColor, Move, CLK, FEN_Before, FEN_After ) VALUES 
""" + ', '.join([f"('{row['GameUrl']}', '{row['MoveNumber']}', '{row['Turn']}', '{row['Move']}', '{row['CLK']}', "
                f"'{row['FEN_Before']}', '{row['FEN_After']}')" for index, row in moves_df.iterrows()]) + f"""
ON CONFLICT (GameUrl, MoveNumber, PieceColor) DO NOTHING;

INSERT INTO Ratings ( Account_id, Datetime, Ultrabullet, Bullet, Blitz, Rapid, Classical,
                     Chess960, Kingofthehill, Threecheck, Antichess, Atomic, Racingkings, Crazyhouse )
VALUES ( '{lichess_ratings['Account_id']}', '{lichess_ratings['Datetime']}', '{lichess_ratings['Ultrabullet']}',
              '{lichess_ratings['Bullet']}', '{lichess_ratings['Blitz']}', '{lichess_ratings['Rapid']}',
              '{lichess_ratings['Classical']}', '{lichess_ratings['Chess960']}', '{lichess_ratings['Kingofthehill']}',
              '{lichess_ratings['Threecheck']}', '{lichess_ratings['Antichess']}', '{lichess_ratings['Atomic']}',
              '{lichess_ratings['Racingkings']}', '{lichess_ratings['Crazyhouse']}')
ON CONFLICT (Account_id, Datetime) DO NOTHING;
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
