from airflow.sdk import dag, task
@dag
def xcom_dag():
    @task.python
    def first_task(**kwargs):
        print("extracting data")
        ti=kwargs['ti']
        fetched_data={"data":[1,2,3,4,5]}
        ti.xcom_push(key="return_value", value=fetched_data)

    @task.python
    def second_task(**kwargs):
        ti=kwargs['ti']
        fetched_data=ti.xcom_pull(task_ids='first_task'  ,key="return_value")['data']
        transformed_data=fetched_data*2
        transformed_data_dict={"transf_data":transformed_data}
        ti.xcom_push(key="return_value", value=transformed_data_dict)

    @task.python
    def third_task(** kwargs):
        ti=kwargs['ti']
        load_data=ti.xcom_pull(task_ids='second_task', key="return_value")
        return load_data

    first=first_task()
    second=second_task()
    third=third_task()
    first>>second>>third

xcom_dag()