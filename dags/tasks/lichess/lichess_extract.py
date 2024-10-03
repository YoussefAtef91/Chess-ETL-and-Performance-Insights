import requests
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from airflow.decorators import task

USERNAME = 'YoussefAtef91'

@task()
def lichess_extract() -> tuple:
    """
    Extracts the Lichess games and ratings.

    Returns:
        list: A list containing two elements:
              - games_data (list): A list of the user's games in the current month.
              - ratings_data (dict): A dictionary containing the user's rating statistics.
    """
    
    # Get The Unix timestamp in UTC for 24 hours ago
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=1)

    # Convert to Unix timestamp in milliseconds
    start= int(start_utc.timestamp()) * 1000

    # Extract the Lichess games
    try:
        games_url = f"https://lichess.org/api/games/user/{USERNAME}?tags=true&clocks=true&evals=true&opening=true&since={start}"
        games_response = requests.get(games_url)
        games_data = games_response.text
    except Exception as e:
        print(f"Error: {e}")

    # Extract the Lichess ratings
    try:
        ratings_url = f"https://lichess.org/api/user/{USERNAME}"
        ratings_response = requests.get(ratings_url)
        ratings_data = ratings_response.json()
    except Exception as e:
        print(f"Error: {e}")

    return games_data, ratings_data