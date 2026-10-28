from airflow.sdk import dag, task
@dag(
    dag_id="parallel_dag",
)
def parallel_dag():
    @task.python
    def extract_task(**kwargs):
        print("extracting data")
        ti=kwargs['ti']
        extracted_data={"api_extracted_data":[1,2,3],
                        "db_extracted_data":[4,5,6],
                        "s3_extracted_data":[7,8,9]}
        ti.xcom_push(key="return_value", value=extracted_data)


    @task.python
    def transform_task_api(**kwargs):
        ti=kwargs['ti']
        fetched_data=ti.xcom_pull(task_ids='extract_task')['api_extracted_data']
        print("transforming api name")
        transformed_api_data=[i*10 for i in fetched_data]

        ti.xcom_push(key="return_value", value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        ti=kwargs['ti']
        fetched_data=ti.xcom_pull(task_ids='extract_task')['db_extracted_data']
        print("transforming db name")
        transformed_db_data=[i*100 for i in fetched_data]
        ti.xcom_push(key="return_value", value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        ti=kwargs['ti']
        fetched_data=ti.xcom_pull(task_ids='extract_task')['s3_extracted_data']
        print("transforming s3 name")
        transformed_s3_data=[i*1000 for i in fetched_data]
        ti.xcom_push(key="return_value", value=transformed_s3_data)


    @task.bash
    def load_task(**kwargs):
        print("loading data to destination")
        api_data=kwargs['ti'].xcom_pull(task_ids='transform_task_api')
        db_data=kwargs['ti'].xcom_pull(task_ids='transform_task_db')
        s3_data=kwargs['ti'].xcom_pull(task_ids='transform_task_s3')
        return f"echo 'loaded data: {api_data}, {db_data}, {s3_data}'"

    extract=extract_task()
    transform_api=transform_task_api()
    transform_db=transform_task_db()
    transform_s3=transform_task_s3()
    load=load_task()

    extract>>[transform_api, transform_db, transform_s3]>>load

parallel_dag()






    