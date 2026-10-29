from airflow.sdk import dag,task
from first_dag import first_dag
from branches import conditional_dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
@dag
def orchestration_parent():
        trigger_first_dag=TriggerDagRunOperator(
            task_id="trigger_first_dag",
            trigger_dag_id="first_dag",
            wait_for_completion=True
        )

        trigger_second_dag=TriggerDagRunOperator(
            task_id="trigger_sec_dag",
            trigger_dag_id="conditional_dag",
            wait_for_completion=True
        )

    

        trigger_first_dag>>trigger_second_dag


orchestration_parent()