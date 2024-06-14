from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "upload_dm_layers_mikhail_mudrichenko"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko"],
)
def load_to_dm_layer():
  def execute_function(
        func_name: str,
        dwh_conn_id: str = "mikhail_mudrichenko_db",
        dwh_db: str = "mikhail_mudrichenko_db",
    ):
    pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    pg_hook.run(sql=f"select * from  {func_name}();")
  
    
  @task_group(group_id='insert_dm')  
  def insert_dm_group():   
    
          
    @task
    def insert_report_move_balance():
          execute_function(func_name="dds.insert_dds_pricelist")
          return True
               
    insert_report_move_balance()
    
  insert_dm_group()
    
init_dag = load_to_dm_layer()
