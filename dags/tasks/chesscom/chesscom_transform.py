import pandas as pd
import numpy as np
import json
import re
import time
import pytz
import chess
from typing import List, Any

from datetime import datetime
from airflow.decorators import task


# Function to localize UTC datetime to Cairo timezone
def localize_UTCDatetime(UTCDatetime):
    """
    Localizes UTC datetime to Cairo timezone.
    
    Args:
        utc_datetime (str): UTC datetime string in the format '%Y-%m-%d %H:%M:%S'.
    
    Returns:
        datetime: Localized Cairo time.
    """
    utc_time = datetime.strptime(f'{UTCDatetime}', '%Y-%m-%d %H:%M:%S')
    utc_timezone = pytz.utc
    utc_time = utc_timezone.localize(utc_time)
    local_timezone = pytz.timezone("Africa/Cairo")
    local_time = utc_time.astimezone(local_timezone)
    return local_time

@task()
def chesscom_transform(extracted_data: tuple) -> tuple:
    """
    Transforms extracted Chess.com data into a usable format.
    
    Args:
        extracted_data (List[Any]): A list containing games data and ratings data.
    
    Returns:
        list: A list containing three elements:
              - games_dict (dict): A dictionary containing the user's chess.com games.
              - moves_dict (dict): A dictionary containing the games' moves.
              - ratings_dict (dict): A dictionary containing the user's rating statistics.
    """

    games_data, ratings_data = extracted_data
    
    # Convert games data into DataFrame
    df = pd.DataFrame(games_data)
    
    # Parse PGN to extract moves and ECO codes
    moves_list = []
    eco_list = []
    
    for game in games_data:
        pgn = game['pgn'].splitlines()
        moves = []
        eco = None
        
        for line in pgn:
            if line.startswith("[ECO "):
                eco = line.split('"')[1].strip()
            if not line.startswith('[') and len(line.strip()) > 0:
                moves.append(line)
        
        moves_list.append(' '.join(moves))
        eco_list.append(eco)

    df['Moves'] = moves_list
    df['ECO'] = eco_list

    # Rename columns
    df.rename(columns={
        'url':'GameUrl', 
        'time_control':'TimeControl', 
        'rated':'GameType', 
        'time_class':'TimeClass', 
        'rules':'Variant'
    }, inplace=True)

    # Map GameType to Rated/Casual
    df['GameType'] = df['GameType'].map({True:'Rated',False:'Casual'})

    # Extract player info
    df['white_username'] = df['white'].apply(lambda x: x['username'])
    df['white_rating'] = df['white'].apply(lambda x: x['rating'])
    df['white_result'] = df['white'].apply(lambda x: x['result'])

    df['black_username'] = df['black'].apply(lambda x: x['username'])
    df['black_rating'] = df['black'].apply(lambda x: x['rating'])
    df['black_result'] = df['white'].apply(lambda x: x['result'])

    # Assign opponent and Elo ratings
    df['Opponent'] = df.apply(lambda x: x.white_username if x.black_username == 'Youssefatef91' else x.black_username, axis=1)
    df['MyElo'] = df.apply(lambda x: x.white_rating if x.white_username == 'Youssefatef91' else x.black_rating, axis=1)
    df['OpponentElo'] = df.apply(lambda x: x.white_rating if x.black_username == 'Youssefatef91' else x.black_rating, axis=1)

    # Map results
    result_code = {'win':'Win','checkmated':'Loss','stalemate':'Draw','resigned':'Loss',
                   'timeout':'Loss', 'abandoned':'Loss','repetition':'Draw', 'insufficient':'Draw',
                   'timevsinsufficient':'Draw','agreed':'Draw','lose':'Loss','50move':'Draw'}

    df['Termination'] = df.apply(lambda x: x.white_result if x.white_username == 'Youssefatef91' else x.black_result, axis=1)
    
    df['white_result'] = df['white_result'].map(result_code)
    df['Result'] = df.apply(lambda x: 'Draw' if x.white_result == 'Draw' 
                            else 'Win' if (x.white_result == 'Win' and x.white_username == 'Youssefatef91') 
                            or (x.white_result == 'Loss' and x.black_username == 'Youssefatef91') 
                            else 'Loss', axis=1)

    # Standardize Variant and TimeClass
    df['Variant'] = df['Variant'].str.replace('chess','Standard').str.capitalize()
    df['TimeClass'] = df['TimeClass'].str.capitalize()
    
    # Determine piece color
    df['PiecesColor'] = df.apply(lambda x: 'White' if x.white_username == 'Youssefatef91' else 'Black', axis=1)
    
    # Convert time to Cairo timezone
    df['GMTDatetime'] = pd.to_datetime(df['end_time'], unit='s')
    df['CairoDatetime'] = df['GMTDatetime'].apply(lambda x: localize_UTCDatetime(x))
    
    # Handle rating differences
    updated_df = df.copy()
    rated_games = df[df['GameType'] == 'Rated']

    for variant in rated_games['Variant'].unique():
        variant_games = rated_games[rated_games['Variant'] == variant]
        for time_class in variant_games['TimeClass'].unique():
            sorted_games = variant_games[variant_games['TimeClass'] == time_class].sort_values('CairoDatetime')
            sorted_games['LastElo'] = sorted_games['MyElo'].shift(1).fillna(800).astype(int)
            sorted_games['MyRatingDiff'] = sorted_games['MyElo'].astype(int) - sorted_games['LastElo']
            updated_df.loc[sorted_games.index, 'LastElo'] = sorted_games['LastElo']
            updated_df.loc[sorted_games.index, 'MyRatingDiff'] = sorted_games['MyRatingDiff']

    df['LastElo'] = updated_df['LastElo']
    df['MyRatingDiff'] = updated_df['MyRatingDiff'].fillna(0).astype(int)
    df['OpponentTitle'] = 'Unknown'
    df['OpponentRatingDiff'] = 0
    
    # Set Account id
    df['Account_id'] = 2

    # Drop unnecessary columns
    columns_to_drop = ['LastElo', 'pgn', 'tcn', 'uuid', 'initial_setup', 'fen',
    'accuracies', 'white', 'black', 'white_username', 'black_username', 'white_rating', 
    'black_rating', 'white_result', 'black_result', 'end_time', 'GMTDatetime', 'eco']
    
    df.drop(columns_to_drop, axis=1, inplace=True)

    # Moves processing
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
    df.drop('Moves', axis=1, inplace=True)

    # Ratings processing
    chesscom_ratings = {'Account_id': 2, 'Datetime':str(datetime.now())}
    variants = ['chess_daily', 'chess960_daily', 'chess_rapid', 'chess_blitz', 'chess_bullet']
    
    for variant in variants:
        key = variant.capitalize().replace('chess_', '')
        value = ratings_data.get(variant, {}).get('last', {}).get('rating', None)
        chesscom_ratings[key] = value
    
    games_dict = df.to_dict(orient='records')
    moves_dict = moves_df.to_dict(orient='records')
    ratings_dict = chesscom_ratings

    return games_dict, moves_dict, ratings_dict