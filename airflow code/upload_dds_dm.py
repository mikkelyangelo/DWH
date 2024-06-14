from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_upload_dds_dm_layers_mikhail_mudrichenko_indiv"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko","Personal project"],
)
def load_to_dds_layer():
  def execute_function(
        func_name: str,
        dwh_conn_id: str = "mikhail_mudrichenko_db",
        dwh_db: str = "mikhail_mudrichenko_db",
    ):
    pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    pg_hook.run(sql=f"select * from  {func_name}();")
    
  @task_group(group_id="insert_hubs_pp")
  def insert_hubs_pp():    
    @task
    def insert_dds_hub_supplier():
          execute_function(func_name="dds_pp.insert_dds_hub_supplier ")
          return True
                   
    @task
    def insert_dds_hub_contact():
          execute_function(func_name="dds_pp.insert_dds_hub_contact")
          return True    
    
    @task
    def insert_dds_hub_goods():
          execute_function(func_name="dds_pp.insert_dds_hub_goods")
          return True    
    
    @task
    def insert_dds_hub_goods_type():
          execute_function(func_name="dds_pp.insert_dds_hub_goods_type")
          return True    
    
    @task
    def insert_dds_hub_measure():
          execute_function(func_name="dds_pp.insert_dds_hub_measure")
          return True    
          
    @task
    def insert_dds_hub_goods_supply():
          execute_function(func_name="dds_pp.insert_dds_hub_goods_supply")
          return True 
    
    
    insert_dds_hub_supplier()     
    insert_dds_hub_contact()
    insert_dds_hub_goods()
    insert_dds_hub_goods_type()
    insert_dds_hub_measure()
    insert_dds_hub_goods_supply()
    
  @task_group(group_id="insert_sats_pp")
  def insert_dds_pp():    
    @task
    def insert_dds_supplier():
          execute_function(func_name="dds_pp.insert_dds_supplier ")
          return True
                   
    @task
    def insert_dds_contact():
          execute_function(func_name="dds_pp.insert_dds_contact")
          return True    
    
    @task
    def insert_dds_goods():
          execute_function(func_name="dds_pp.insert_dds_goods")
          return True    
    
    @task
    def insert_dds_goods_type():
          execute_function(func_name="dds_pp.insert_dds_goods_type")
          return True    
    
    @task
    def insert_dds_measure():
          execute_function(func_name="dds_pp.insert_dds_measure")
          return True    
          
    @task
    def insert_dds_goods_supply():
          execute_function(func_name="dds_pp.insert_dds_goods_supply")
          return True 
    
    
    insert_dds_supplier()     
    insert_dds_contact()
    insert_dds_goods()
    insert_dds_goods_type()
    insert_dds_measure()
    insert_dds_goods_supply()
    
  @task_group(group_id="insert_dm_pp")
  def insert_dm_pp():    
    @task
    def insert_report_goods_supply_info():
          execute_function(func_name="dm_pp.insert_report_goods_supply_info")
          return True
    
    
    insert_report_goods_supply_info()
    
  insert_hubs_pp()>>insert_dds_pp()>>insert_dm_pp()
    
init_dag = load_to_dds_layer()
