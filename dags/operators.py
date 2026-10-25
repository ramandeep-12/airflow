from airflow.sdk import dag, task

@dag
def first_dag():

    @task.bash
    def dag_operator():
        return "echo https://airflow.apache.org/"
    

    dag_operator()
    

first_dag()