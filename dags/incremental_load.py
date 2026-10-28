from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
        schedule=CronDataIntervalTimetable("@daily", timezone="America/Toronto"),
        start_date=datetime(year=2026, month=10, day=26, tz="America/Toronto"),
        end_date=datetime(year=2026, month=10, day=29, tz="America/Toronto"),
        catchup=True
)
def incremental_load():
    @task.python
    def incremetnal_data_fetch(**kwargs):
        date_interval_start=kwargs['data_interval_start']
        date_interval_end=kwargs['data_interval_end']
        print(f"fetching data from {date_interval_start} to {date_interval_end}")


    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"


    fetch_task=incremetnal_data_fetch()
    process_task=incremental_data_process()

    fetch_task>>process_task

incremental_load()