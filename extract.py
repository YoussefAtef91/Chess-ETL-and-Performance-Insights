import requests
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.decorators import task

@task()
def extract():
    # Get the 'start' and 'end' variables from Airflow, defaulting to None if they don't exist
    start = Variable.get('start', default_var=None)
    end = Variable.get('end', default_var=None)

    # Check if 'start' and 'end' variables are None (first run)
    if start == None and end == None:
        url = "https://lichess.org/api/games/user/YoussefAtef91?tags=true&clocks=true&evals=true&opening=true"
    else:
        url = f"https://lichess.org/api/games/user/YoussefAtef91?tags=true&clocks=true&evals=true&opening=true&since={start}&until={end}"
    
    # Update 'start' and 'end' variables to cover the last 24 hours
    start = int((datetime.now() - timedelta(days=1)).timestamp()) * 1000  # Start time: 24 hours ago
    end = int(datetime.now().timestamp()) * 1000 # End time: current time

    # Set the updated 'start' and 'end' variables in Airflow
    Variable.set('start', start)
    Variable.set('end', end)

    # Make a request to the Lichess API
    response = requests.get(url)
    data = response.text

    return data