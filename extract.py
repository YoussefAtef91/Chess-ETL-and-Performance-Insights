import requests
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from airflow.decorators import task

@task()
def extract():
    # Check if this is the first time to run the ETL
    first_run = Variable.get('first_run', default_var="True")

    if first_run == "True":
        url = "https://lichess.org/api/games/user/YoussefAtef91?tags=true&clocks=true&evals=true&opening=true"
    else:
        # If it's not the first time, run the Extract for the last 24 hours

        # Get The Unix timestamp in UTC for 24 hours ago
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(days=1)

        # Convert to Unix timestamp in milliseconds
        start= int(start_utc.timestamp()) * 1000

        url = f"https://lichess.org/api/games/user/YoussefAtef91?tags=true&clocks=true&evals=true&opening=true&since={start}"
        
        #Set the first_run variable to False
        Variable.set("first_run", "False")

    # Make a request to the Lichess API
    response = requests.get(url)
    
    data = response.text

    return data