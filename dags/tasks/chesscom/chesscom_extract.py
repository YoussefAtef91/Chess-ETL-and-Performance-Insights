from datetime import datetime
import requests
from airflow.decorators import task

USERNAME = 'Youssefatef91'
HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0'
    }

@task()
def chesscom_extract() -> tuple:
    """
    Extracts the Chess.com games and ratings.

    Returns:
        list: A list containing two elements:
              - games_data (list): A list of the user's games in the current month.
              - ratings_data (dict): A dictionary containing the user's rating statistics.
    """

    # Define the current year and month
    year = str(datetime.now().year)
    month = str(datetime.now().month).zfill(2)
    YearMonth = year + '/'+ month
    
    # Extracting the Chess.com games
    try:
        games_url = f"https://api.chess.com/pub/player/{USERNAME}/games/{YearMonth}"
        games_response = requests.get(games_url, headers=HEADERS)
        games_data = games_response.json()['games']
    except Exception as e:
        print(f"Error: {e}")

    # Extracting the chess.com ratings
    try:
        ratings_url = f"https://api.chess.com/pub/player/{USERNAME}/stats"
        ratings_response = requests.get(ratings_url, headers=HEADERS)
        ratings_data = ratings_response.json()

    except Exception as e:
        print(f"Error: {e}")

    return games_data, ratings_data