from airflow.sdk import dag, task, asset
import  pendulum 
import os

@asset(
    schedule="@daily",
    uri="/opt/airflow/logs/data/data_extract.txt",
    name="fetch_data"
)

def fetch_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)


    with open(self.uri, 'w')as f:
        f.write(f"Data fetched on {pendulum.now('America/Toronto'):%Y-%m-%d %H:%M:%S %Z}")