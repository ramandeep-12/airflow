from airflow.sdk import dag, task, asset, Asset
import  pendulum 
import os
from assests import fetch_data
@asset(
    schedule=Asset.ref(name="fetch_data"),
    uri="/opt/airflow/logs/data/data_processed.txt",
    name="process_data"
)

def process_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)


    with open(self.uri, 'w')as f:
        f.write(f"data processed successfully")