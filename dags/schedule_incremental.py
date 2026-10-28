from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates=EventsTimetable(
    event_dates=[
        datetime(2026,10,1),
        datetime(2026,10,15),
        datetime(2026,10,26),
        datetime(2026,10,30)
    ]
)

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=10, day=1, tz="America/Toronto"),
    end_date=datetime(year=2026, month=10, day=31, tz="America/Toronto"),
    catchup=True
)

def special_dates_dag():
    @task.python
    def special_event_task(**kwargs):
        execution_date=kwargs['logical_date']
        print(f"Running task for special event on {execution_date}")

    special_event=special_event_task()
    special_event

special_dates_dag()

