import os
from airflow.models import Variable
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

@task()
def load(transformed_chesscom_games: tuple, transformed_lichess_games: tuple) -> None:
    """
    Loads the extracted Lichess and Chess.com data into the PostgreSQL database. 
    Args:
        transformed_chesscom_games (tuple): A tuple containing dictionaries for Chess.com games, moves, and ratings.
        transformed_lichess_games (tuple): A tuple containing dictionaries for Lichess games, moves, and ratings.
    """

    # Unpack transformed data from Chess.com
    chesscom_games_dict, chesscom_moves_dict, chesscom_ratings_dict = transformed_chesscom_games
    
    # Unpack transformed data from Lichess
    lichess_games_dict, lichess_moves_dict, lichess_ratings_dict = transformed_lichess_games
    
    # Create a PostgreSQL hook for database interaction
    pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")

    # SQL query for inserting games from Chess.com
    insert_chesscom_games_query = """
    INSERT INTO games (
            GameUrl, Result, Variant, TimeControl, ECO, Termination, PiecesColor,
            Opponent, OpponentTitle, MyElo, OpponentElo, MyRatingDiff, OpponentRatingDiff,
            GameType, TimeClass, CairoDatetime, Account_id
    ) VALUES (
            %(GameUrl)s, %(Result)s, %(Variant)s, %(TimeControl)s, %(ECO)s, %(Termination)s,
            %(PiecesColor)s, %(Opponent)s, %(OpponentTitle)s, %(MyElo)s, %(OpponentElo)s, 
            %(MyRatingDiff)s, %(OpponentRatingDiff)s, %(GameType)s, %(TimeClass)s, %(CairoDatetime)s, %(Account_id)s
    )
    ON CONFLICT (GameUrl) DO NOTHING
    """

    # Execute the query for each Chess.com game
    for game in chesscom_games_dict:
        pg_hook.run(insert_chesscom_games_query, parameters=game)

    # SQL query for inserting moves from Chess.com
    insert_chesscom_moves_query = """
    INSERT INTO moves (
            GameUrl, MoveNumber, PieceColor, Move, CLK, FEN_Before, FEN_After
    ) VALUES (
            %(GameUrl)s, %(MoveNumber)s, %(Turn)s, %(Move)s, %(CLK)s, %(FEN_Before)s, %(FEN_After)s
    )
    ON CONFLICT (GameUrl, MoveNumber, PieceColor) DO NOTHING
    """

    # Execute the query for each Chess.com move
    for move in chesscom_moves_dict:
        pg_hook.run(insert_chesscom_moves_query, parameters=move)

    # SQL query for inserting ratings from Chess.com
    insert_chesscom_ratings_query = """
    INSERT INTO ratings (
            Account_id, Datetime, Daily, Chess960, Rapid, Blitz, Bullet
    ) VALUES (
            %(Account_id)s, %(Datetime)s, %(Chess_daily)s, %(Chess960_daily)s, %(Chess_rapid)s, 
            %(Chess_blitz)s, %(Chess_bullet)s
    )
    ON CONFLICT (Account_id, Datetime) DO NOTHING
    """

    # Execute the query for Chess.com ratings
    pg_hook.run(insert_chesscom_ratings_query, parameters=chesscom_ratings_dict)

    # SQL query for inserting games from Lichess
    insert_lichess_games_query = """
    INSERT INTO games (
            GameUrl, Result, Variant, TimeControl, ECO, Termination, PiecesColor,
            Opponent, OpponentTitle, MyElo, OpponentElo, MyRatingDiff, OpponentRatingDiff,
            GameType, TimeClass, CairoDatetime, Account_id
    ) VALUES (
            %(GameUrl)s, %(Result)s, %(Variant)s, %(TimeControl)s, %(ECO)s, %(Termination)s,
            %(PiecesColor)s, %(Opponent)s, %(OpponentTitle)s, %(MyElo)s, %(OpponentElo)s, 
            %(MyRatingDiff)s, %(OpponentRatingDiff)s, %(GameType)s, %(TimeClass)s, %(CairoDatetime)s, %(Account_id)s
    )
    ON CONFLICT (GameUrl) DO NOTHING
    """

    # Execute the query for each Lichess game
    for game in lichess_games_dict:
        pg_hook.run(insert_lichess_games_query, parameters=game)

    # SQL query for inserting moves from Lichess
    insert_lichess_moves_query = """
    INSERT INTO moves (
            GameUrl, MoveNumber, PieceColor, Move, CLK, FEN_Before, FEN_After
    ) VALUES (
            %(GameUrl)s, %(MoveNumber)s, %(Turn)s, %(Move)s, %(CLK)s, %(FEN_Before)s, %(FEN_After)s
    )
    ON CONFLICT (GameUrl, MoveNumber, PieceColor) DO NOTHING
    """

    # Execute the query for each Lichess move
    for move in lichess_moves_dict:
        pg_hook.run(insert_lichess_moves_query, parameters=move)

    # SQL query for inserting ratings from Lichess
    insert_lichess_ratings_query = """
    INSERT INTO ratings (
            Account_id, Datetime, Ultrabullet, Bullet, Blitz, Rapid, Classical,
            Chess960, Kingofthehill, Threecheck, Antichess, Atomic, Racingkings, Crazyhouse
    ) VALUES (
            %(Account_id)s, %(Datetime)s, %(Ultrabullet)s, %(Bullet)s, %(Blitz)s, 
            %(Rapid)s, %(Classical)s, %(Chess960)s, %(Kingofthehill)s, %(Threecheck)s,
            %(Antichess)s, %(Atomic)s, %(Racingkings)s, %(Crazyhouse)s
    )
    ON CONFLICT (Account_id, Datetime) DO NOTHING
    """

    # Execute the query for Lichess ratings
    pg_hook.run(insert_lichess_ratings_query, parameters=lichess_ratings_dict)
