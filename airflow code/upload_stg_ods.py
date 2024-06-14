from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_upload_ods_stg_tables_indiv_mikhail_mudrichenko" 


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko","Personal project"],
)



def load_to_stg_ods_layer():
  def execute_procedure(
      proc_name: str,
      dwh_conn_id: str = "mikhail_mudrichenko_db",
      dwh_db: str = "mikhail_mudrichenko_db",
    ):
    pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    pg_hook.run(sql=f"call  {proc_name}();")

  @task_group(group_id='insert_stg')
  def insert_stg():   
    @task
    def insert_stg_client():
          execute_procedure(proc_name="stg_pp.client_load")
          return True
          
    @task
    def insert_stg_goods():
          execute_procedure(proc_name="stg_pp.goods_load")
          return True
          
    @task
    def insert_stg_goods_supply():
          execute_procedure(proc_name="stg_pp.goods_supply_load")
          return True
          
    @task
    def insert_stg_order_detail():
          execute_procedure(proc_name="stg_pp.order_detail_load")
          return True
          
    @task
    def insert_stg_orders():
          execute_procedure(proc_name="stg_pp.orders_load")
          return True
          
    @task
    def insert_stg_product():
          execute_procedure(proc_name="stg_pp.product_load")
          return True
          
    @task
    def insert_stg_store():
          execute_procedure(proc_name="stg_pp.store_load")
          return True
          
    @task
    def insert_stg_supplier():
          execute_procedure(proc_name="stg_pp.supplier_load")
          return True
    @task
    def insert_stg_suppy_details():
          execute_procedure(proc_name="stg_pp.supply_details_load")
          return True
    @task
    def insert_stg_supply():
          execute_procedure(proc_name="stg_pp.supply_load")
          return True
    
    insert_stg_client()
    insert_stg_goods()
    insert_stg_goods_supply()
    insert_stg_order_detail()
    insert_stg_orders()
    insert_stg_product()
    insert_stg_store()
    insert_stg_supplier()
    insert_stg_suppy_details()
    insert_stg_supply()
     
  @task_group(group_id='insert_ods')
  def insert_ods():
    def upload_table_inc_delete(
        source_conn_id: str,
        source_db: str,
        source_table: str,
        target_table: str,
        source_schema: str,
        dwh_schema: str,
        fields: list,
        dwh_conn_id: str = "mikhail_mudrichenko_db",
        dwh_db: str = "mikhail_mudrichenko_db",
    ):
        source_db_pg_hook = PostgresHook(
            postgres_conn_id=source_conn_id, schema=source_db
        )
        dwh_pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id, schema=dwh_db)
        set_fields = ', '.join([f'{field} = {source_schema}.{source_table}.{field}' for field in fields if field != '"ID"'])
        qualified_fields = [f'{source_schema}.{source_table}.{field}' for field in fields]
        update_query = f"UPDATE {dwh_schema}.{target_table} SET {set_fields}, upload_stg_ts = TIMEZONE('utc', NOW()) FROM {source_schema}.{source_table} WHERE {dwh_schema}.{target_table}.{fields[0]} = {source_schema}.{source_table}.{fields[0]};"
        dwh_pg_hook.run(update_query)
        query = f"SELECT {', '.join(qualified_fields)} FROM {source_schema}.{source_table} left join {dwh_schema}.{target_table} ON  {source_schema}.{source_table}.{fields[0]} = {dwh_schema}.{target_table}.{fields[0]} WHERE {dwh_schema}.{target_table}.{fields[0]} IS NULL;"
        curr_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        records = [
            record + (curr_dt,) for record in source_db_pg_hook.get_records(query)
        ]
        fields.append("upload_stg_ts")
        dwh_pg_hook.insert_rows(
            table=f'"{dwh_schema}"."{target_table}"',
            rows=records,
            target_fields=fields,
        )
        delete_query = f"DELETE FROM {dwh_schema}.{target_table} o USING {source_schema}.{source_table} s WHERE s.{fields[0]} = o.{fields[0]} AND deleted_flg = '1';"
        dwh_pg_hook.run(delete_query)
        
    def upload_table_full_reload(
        source_conn_id: str,
        source_db: str,
        source_table: str,
        target_table: str,
        source_schema: str,
        dwh_schema: str,
        fields: list,
        dwh_conn_id: str = "mikhail_mudrichenko_db",
        dwh_db: str = "mikhail_mudrichenko_db",
    ):
        source_db_pg_hook = PostgresHook(
            postgres_conn_id=source_conn_id, schema=source_db
        )


        dwh_pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id, schema=dwh_db)
        qualified_fields = [f'{source_schema}.{source_table}.{field}' for field in fields]
        query = f"SELECT {', '.join(qualified_fields)} FROM {source_schema}.{source_table} left join {dwh_schema}.{target_table} ON  {source_schema}.{source_table}.{fields[0]} = {dwh_schema}.{target_table}.{fields[0]} WHERE {dwh_schema}.{target_table}.{fields[0]} IS NULL;"
        curr_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        records = [
            record + (curr_dt,) for record in source_db_pg_hook.get_records(query)
        ]
        fields.append("upload_stg_ts")
        dwh_pg_hook.insert_rows(
            table=f'"{dwh_schema}"."{target_table}"',
            rows=records,
            target_fields=fields,
        )

    @task
    def upload_client():
        upload_table_full_reload(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="client",
            target_table="client",
            fields=['"ID"', 'name', 'name_short', 'address', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
    
    @task
    def upload_orders():
        upload_table_full_reload(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="orders",
            target_table="orders",
            fields=['"ID"', 'id_client', 'order_dttm', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
        
    @task
    def upload_product():
        upload_table_full_reload(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="product",
            target_table="product",
            fields=['"ID"', 'id_store', 'price', 'product_name', 'quantity', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
        
    @task
    def upload_order_detail():
        upload_table_full_reload(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="order_detail",
            target_table="order_detail",
            fields=['"ID"', 'id_order', 'id_product', 'quantity', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
        
    @task
    def upload_store():
        upload_table_full_reload(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="store",
            target_table="store",
            fields=['"ID"', 'store_name', 'address', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
        
    @task
    def upload_supply():
        upload_table_full_reload(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="supply",
            target_table="supply",
            fields=['"ID"', 'id_sup_goods', 'date_sup_start', 'date_sup_stop', 'supply_status', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
        
    @task
    def upload_supply_details():
        upload_table_full_reload(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="supply_details",
            target_table="supply_details",
            fields=['"ID"', 'quantity', 'id_supply', 'id_sup_goods', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
    @task
    def upload_goods():
        upload_table_inc_delete(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="goods",
            target_table="goods",
            fields=['"ID"', 'food_name', 'food_type', '"Specification"', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
    @task
    def upload_supplier():
        upload_table_inc_delete(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="supplier",
            target_table="supplier",
            fields=['id', '"name"', 'region', 'email','website','phone','address', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
    @task
    def upload_goods_supply():
        upload_table_inc_delete(
            source_conn_id="mikhail_mudrichenko_db",
            source_db="mikhail_mudrichenko_db",
            source_table="goods_supply",
            target_table="goods_supply",
            fields=['"ID"', 'item_sup_id', 'supplier_id', 'avaibl_count','price_per_measure', 'update_dttm',
            ],
            source_schema="stg_pp",
            dwh_schema="ods_pp",
        )
        return True
    upload_client()
    upload_orders()
    upload_product()
    upload_store()
    upload_order_detail()
    upload_supply()
    upload_supply_details()
    upload_goods()
    upload_supplier()
    upload_goods_supply()
      
  insert_stg()>>insert_ods()
    
init_dag = load_to_stg_ods_layer()
