import pandas as pd
from airflow.decorators import task
from tasks.validation.validations import values_check, nulls_check, datatypes_check, game_url_check, possible_values_check, columns_check

@task
def validate(transformed_data: tuple) -> tuple:
    """
    Validate the transformed data (games, moves, and ratings) to ensure its consistency.
    
    Args:
        transformed_data (tuple): Tuple containing games, moves, and ratings dictionaries.
    
    Returns:
        tuple: The validated transformed data.
    """
    games_dict, moves_dict, ratings_dict = transformed_data

    # Games check
    games_df = pd.DataFrame(games_dict)
    games_df['CairoDatetime'] = pd.to_datetime(games_df['CairoDatetime']).dt.tz_convert('Africa/Cairo')

    games_col_dict = {  
        'GameUrl': 'O', 'TimeControl': 'O', 'GameType': 'O', 'TimeClass': 'O', 'Variant': 'O', 'ECO': 'O',
        'Account_id': 'int64', 'Opponent': 'O', 'MyElo': 'int64', 'OpponentElo': 'int64', 'Result': 'O',
        'PiecesColor': 'O', 'Termination': 'O', 'CairoDatetime': 'datetime64[ns, Africa/Cairo]',
        'MyRatingDiff': 'int64', 'OpponentRatingDiff': 'int64', 'OpponentTitle': 'O'
    }

    nulls_check(games_df)
    datatypes_check(games_df, games_col_dict)
    columns_check(games_df.columns.tolist(), list(games_col_dict.keys()), strict=True)

    for col in ['MyElo', 'OpponentElo']:
        values_check(games_df, col, minval=0, maxval=35000)

    game_url_check(games_df)
    possible_values_check(games_df, 'GameType', ['Rated', 'Casual'])
    possible_values_check(games_df, 'TimeClass', ['Daily', 'Rapid', 'Blitz', 'Bullet'])
    possible_values_check(games_df, 'Result', ['Win', 'Loss', 'Draw'])
    possible_values_check(games_df, 'PiecesColor', ['White', 'Black'])

    # Moves Check
    moves_df = pd.DataFrame(moves_dict)

    moves_col_dict = {
        'GameUrl': 'O', 'MoveNumber': 'int64', 'Turn': 'O', 'Move': 'O', 'CLK': 'O',
        'FEN_Before': 'O', 'FEN_After': 'O'
    }

    nulls_check(moves_df)
    datatypes_check(moves_df, moves_col_dict)
    columns_check(moves_df.columns.tolist(), list(moves_col_dict.keys()), strict=True)

    values_check(moves_df, 'MoveNumber', minval=0)
    game_url_check(moves_df)

    # Ratings Check
    ratings_keys = (
        'Account_id', 'Datetime', 'Daily', 'Rapid', 'Ultrabullet', 'Bullet', 'Blitz', 'Classical', 
        'Chess960', 'Kingofthehill', 'Threecheck', 'Antichess', 'Atomic', 'Racingkings', 'Crazyhouse',
        'Chess_daily', 'Chess960_daily', 'Chess_rapid', 'Chess_blitz', 'Chess_bullet'
    )

    columns_check(list(ratings_dict.keys()), list(ratings_keys), strict=False)

    return transformed_data