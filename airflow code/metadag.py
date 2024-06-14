from datetime import datetime
from airflow.decorators import dag
from airflow.operators.dagrun_operator import TriggerDagRunOperator


DAG_ID = 'dag_metadag_mudrichenko_mikhail_pp'

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule_interval='0 8 * * *', 
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko","Personal project"],
)

def init_metadag():

    trigger_first_dag = TriggerDagRunOperator(
        task_id='trigger_stg_ods',
        trigger_dag_id='dag_upload_ods_stg_tables_indiv_mikhail_mudrichenko',
        wait_for_completion=True,
        trigger_rule="all_done" 
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_dds_dm',
        trigger_dag_id='dag_upload_dds_dm_layers_mikhail_mudrichenko_indiv',
        wait_for_completion=True,
        trigger_rule="all_done" 
    )

   
    trigger_first_dag >> trigger_second_dag

init_metadag()






