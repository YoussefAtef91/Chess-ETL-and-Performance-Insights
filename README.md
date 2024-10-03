# Chess ETL and Performance Insights

## Overview

In this project, I extracted my online chess games data using Lichess API and Chess.com API, then cleaned and transformed the data using Python's Pandas. Then ran the transformed data through validation checks. Then loaded the validated data into a PostgreSQL database. The entire workflow is automated daily using Apache Airflow. Finally, I used Power BI to create an interactive dashboard for analyzing and visualizing my chess performance, providing valuable insights into game statistics and trends.

## Project Structure
```
Chess-ETL  
├───dags  
│   │   .env # Contains the environment variables  
│   │   etl.py # Contains the dag of the workflow  
│   │  
│   └───tasks # Contains the tasks of the workflow  
│       ├───chesscom  
│       │       chesscom_extract.py # Contains the task to extract the Chess.com data  
│       │       chesscom_transform.py # Contains the task to transform the extracted Chess.com data  
│       │  
│       ├───lichess  
│       │       lichess_extract.py # Contains the task to extract the Lichess data  
│       │       lichess_transform.py # Contains the task to transform the extracted Lichess data  
│       │  
│       ├───validation  
│       │       quality_check.py # Contains the task to validate the transformed data  
|       |       validations.py # Contains the validation check functions used in the quality_check task  
│       │  
│       └───load
│               load.py # Containes the task to load the validated data into the PostgreSQL  
|  
├───dashboard  
│       Chess-Insights.pbix # A Power BI dashboard connected to PostgreSQL Database  
│  
├───data  
│       Openings.csv # A csv of the openings names and their eco code for the database creation  
│  
├───database  
│       ChessSchema.sql # An SQL script to create the database schema  
│  
└───intial load  
        chesscom_initial_load.py # A script to initially load all the Chess.com games before starting the workflow  
        lichess_initial_load.py # A script to initially load all the Lichess games before starting the workflow  
```
## Prerequisites
```
- numpy==2.0.1
- pandas==2.2.2
- apache-airflow==2.6.1
- psycopg2-binary==2.9.9
- python-dateutil==2.9.0.post0
- python-dotenv==1.0.1
- pytz==2024.1
- requests==2.32.3
- SQLAlchemy==2.0.31
- typing_extensions==4.12.2
```
## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights
   cd Chess-ETL-and-Performance-Insights
   ```

2. **Create and Activate a Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup PostgreSQL**:
   - Run the ```./database/ChessSchema.sql``` in PostgreSQL.
   - Run the two scripts in the ```initial_load``` folder, and remember to change the usernames in both files with yours.

5. **Configure Airflow**:
   - Set up Airflow with the appropriate configurations and environment variables.
   - Add the DAG `./dags/etl.py` to your Airflow instance.
   - Build the docker-compose.yaml using ```docker-compose build``` command.
   - Create an admin user and create a postgresql connection, check the files in ```./airflow_config```.
   - Run the airflow using ```docker-compose up -d``` command.
   - Navigate to ```http://localhost:8080/``` to track the workflow.

6. **Power BI Dashboard**:
   - Connect Power BI to your PostgreSQL database using the PostgreSQL connector.
   - Open the ```./dashboard/Chess-Insights.pbix``` file, load your data, and make the necessary adjustments to tailor it to your dataset.
  
## Database Schema
<img src="https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights/blob/main/Screenshots/Database%20Schema.png">

## Apache Airflow
<img src="https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights/blob/main/Screenshots/Airflow.png">

## Power BI Dashboard
<table>
    <tr>
        <td><img src="https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights/blob/main/Screenshots/PowerBI1.png" alt="Image 1" style="width:100%; height:auto;"></td>
        <td><img src="https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights/blob/main/Screenshots/PowerBI2.png" alt="Image 2" style="width:100%; height:auto;"></td>
    </tr>
    <tr>
        <td><img src="https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights/blob/main/Screenshots/PowerBI3.png" alt="Image 3" style="width:100%; height:auto;"></td>
        <td><img src="https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights/blob/main/Screenshots/PowerBI4.png" alt="Image 4" style="width:100%; height:auto;"></td>
    </tr>
    <tr>
       <td><img src="https://github.com/YoussefAtef91/Chess-ETL-and-Performance-Insights/blob/main/Screenshots/PowerBI5.png" alt="Image 4" style="width:100%; height:auto;"></td>
    </tr>
</table>

## Notes

- The Airflow DAG is scheduled to run daily, fetching new game data and updating the database.
- This Project uses my Lichess and Chess.com usernames, so remember to change them to yours before running the project.
- The Power BI dashboard provides interactive visualizations to analyze game statistics and trends based on my data, so before using it make the necessary adjustments to tailor it to your dataset.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
