# Chess ETL Pipeline

## Overview

In this project I extracted my chess games data using the Lichess API, then cleaned and transformed the data using Python's Pandas library. Then loaded the processed data into a PostgreSQL database. The entire workflow is automated daily using Apache Airflow. Finally, I used Power BI to create an interactive dashboard for analyzing and visualizing my chess performance, providing valuable insights into game statistics and trends.

## Project Structure

- **`airflow_dag.py`**: Contains the Airflow DAG for orchestrating the ETL process. The DAG consists of tasks for extracting data from Lichess, transforming it into a suitable format, and loading it into a PostgreSQL database.

- **`extract.py`**: Defines the `extract` task that retrieves chess game data from the Lichess API. It supports fetching data based on date ranges specified by Airflow variables.

- **`transform.py`**: Defines the `transform` task that processes the raw PGN data, converts it into a DataFrame, and performs necessary data transformations, including calculating game attributes and formatting dates.

- **`load.py`**: Defines the `load` task that loads the transformed data into a PostgreSQL database. It uses SQL queries to insert the data into the `games` table and handles conflicts by ignoring duplicate entries.

- **`setup_database.py`**: Contains scripts for setting up the PostgreSQL database and creating the necessary tables. It ensures that the database and table are created if they don't already exist.

- **`Chess_Dashboard`**: A Power BI file that contains interactive visualizations and dashboards for exploring and analyzing the chess game data. This dashboard connects to the PostgreSQL database and provides insights into various aspects of the games.

## Prerequisites

- Python 3.x
- PostgreSQL
- Apache Airflow
- Power BI
- Python libraries: `requests`, `pandas`, `numpy`, `psycopg2`, `dotenv`, `pytz`

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
   - Configure the `setup_database.py` with your PostgreSQL credentials.
   - Run `setup_database.py` to create the database and table.

5. **Configure Airflow**:
   - Set up Airflow with the appropriate configurations and environment variables.
   - Add the DAG from `airflow_dag.py` to your Airflow instance.

6. **Power BI Dashboard**:
   - Connect Power BI to your PostgreSQL database using the PostgreSQL connector.
   - Create visualizations based on the data in the `games` table.

## Usage

1. **Run the Airflow DAG**:
   - Trigger the DAG from the Airflow UI to start the ETL process.

2. **Visualize Data**:
   - Open Power BI and use the dashboard to explore and analyze the chess game data.

## Notes

- The Airflow DAG is scheduled to run daily, fetching new game data and updating the database.
- The Power BI dashboard provides interactive visualizations to analyze game statistics and trends.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
