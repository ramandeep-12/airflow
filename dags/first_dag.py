from airflow.sdk import dag, task

@dag
def first_dag():
    @task.python
    def first_task():
        print("first dag")

    @task.python
    def second_task():
        print("second dag")

    @task.python
    def third_task():
        print("third dag")

    #defining variables
    first=first_task()
    second=second_task()
    third=third_task()

    # dependencies
    first>>second>>third

#initialize dag
first_dag()
