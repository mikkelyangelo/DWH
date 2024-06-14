from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "upload_dds_dm_layers_mikhail_mudrichenko"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko"],
)
def load_to_dds_layer():
  def execute_function(
        func_name: str,
        dwh_conn_id: str = "mikhail_mudrichenko_db",
        dwh_db: str = "mikhail_mudrichenko_db",
    ):
    pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    pg_hook.run(sql=f"select * from  {func_name}();")
  @task_group(group_id="insert_hubs")
  def insert_hubs():    
    @task
    def insert_dds_hub_counterparty():
          execute_function(func_name="dds.insert_dds_hub_counterparty")
          return True
                   
    @task
    def insert_dds_hub_deal():
          execute_function(func_name="dds.insert_dds_hub_deal")
          return True    
    
    @task
    def insert_dds_hub_contact():
          execute_function(func_name="dds.insert_dds_hub_contact")
          return True    
    
    @task
    def insert_dds_hub_goods():
          execute_function(func_name="dds.insert_dds_hub_goods")
          return True    
    
    @task
    def insert_dds_hub_item():
          execute_function(func_name="dds.insert_dds_hub_item")
          return True    
          
    @task
    def insert_dds_hub_measure():
          execute_function(func_name="dds.insert_dds_hub_measure")
          return True 
    
    @task
    def insert_dds_hub_item_type():
          execute_function(func_name="dds.insert_dds_hub_item_type")
          return True    
    
    @task
    def insert_dds_hub_manufacture():
          execute_function(func_name="dds.insert_dds_hub_manufacture")
          return True  
    
    @task
    def insert_dds_hub_region():
          execute_function(func_name="dds.insert_dds_hub_region")
          return True
    
    @task
    def insert_dds_hub_warehouse():
          execute_function(func_name="dds.insert_dds_hub_warehouse")
          return True
    
    insert_dds_hub_counterparty()     
    insert_dds_hub_deal()
    insert_dds_hub_contact()
    insert_dds_hub_goods()
    insert_dds_hub_item()
    insert_dds_hub_item_type()
    insert_dds_hub_warehouse()
    insert_dds_hub_region()
    insert_dds_hub_manufacture()
    insert_dds_hub_measure()
    
  @task_group(group_id='insert_dds')  
  def insert_dds():   
    @task
    def insert_dds_deal():
          execute_function(func_name="dds.insert_dds_deal")
          return True
          
    @task
    def insert_dds_warehouse():
          execute_function(func_name="dds.insert_dds_warehouse")
          return True
          
    @task
    def insert_dds_item():
          execute_function(func_name="dds.insert_dds_item")
          return True
          
    @task
    def insert_dds_item_type():
          execute_function(func_name="dds.insert_dds_item_type")
          return True
          
    @task
    def insert_dds_region():
          execute_function(func_name="dds.insert_dds_region")
          return True
          
    @task
    def insert_dds_manufacture():
          execute_function(func_name="dds.insert_dds_manufacture")
          return True
          
    @task
    def insert_dds_client():
          execute_function(func_name="dds.insert_dds_client")
          return True
          
    @task
    def insert_dds_contact():
          execute_function(func_name="dds.insert_dds_contact")
          return True
          
    @task
    def insert_dds_counterparty_x_contact():
          execute_function(func_name="dds.insert_dds_counterparty_x_contact")
          return True
          
    @task
    def insert_dds_goods():
          execute_function(func_name="dds.insert_dds_goods")
          return True
          
    @task
    def insert_dds_deal_detail():
          execute_function(func_name="dds.insert_dds_deal_detail")
          return True
          
    @task
    def insert_dds_measure():
          execute_function(func_name="dds.insert_dds_measure")
          return True
          
    
    @task
    def insert_dds_stock():
          execute_function(func_name="dds.insert_dds_stock")
          return True
          
    @task
    def insert_dds_pricelist():
          execute_function(func_name="dds.insert_dds_pricelist")
          return True
               
    insert_dds_deal()
    insert_dds_warehouse()
    insert_dds_item()
    insert_dds_item_type()
    insert_dds_region()
    insert_dds_manufacture()
    insert_dds_client()
    insert_dds_contact()
    insert_dds_counterparty_x_contact()
    insert_dds_goods()
    insert_dds_measure()
    insert_dds_stock()
    insert_dds_pricelist()
    insert_dds_deal_detail()
    
    
  @task_group(group_id='insert_dm')  
  def insert_dm():   
    
          
    @task
    def insert_report_move_balance():
          execute_function(func_name="dm.insert_report_move_balance")
          return True
               
    insert_report_move_balance()
    
  insert_hubs()>>insert_dds()>>insert_dm()
    
init_dag = load_to_dds_layer()
