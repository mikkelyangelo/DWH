from datetime import datetime
from airflow.decorators import dag
from airflow.operators.dagrun_operator import TriggerDagRunOperator

DAG_ID = 'metadag_mudrichenko_mikhail'

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule_interval=None, 
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko"],
)

def init_metadag():
    trigger_first_dag = TriggerDagRunOperator(
        task_id='trigger_deploy',
        trigger_dag_id='deploy_database_mikhail_mudrichenko',
        wait_for_completion=True
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_ods',
        trigger_dag_id='upload_ods_tables_mikhail_mudrichenko',
        wait_for_completion=True,
        trigger_rule="all_done" 
    )

    trigger_third_dag = TriggerDagRunOperator(
        task_id='trigger_dds',
        trigger_dag_id='upload_dds_dm_layers_mikhail_mudrichenko',
        wait_for_completion=True,
        trigger_rule="all_done" 
    )

   
    trigger_first_dag >> trigger_second_dag >> trigger_third_dag

init_metadag()






