from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "upload_ods_tables_mikhail_mudrichenko"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko"],
)
def load_to_staging_layer(
    depends_on_past=True):

    def upload_table(
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
        query = f"SELECT {', '.join(fields)} FROM {source_schema}.{source_table};"
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
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="client",
            target_table="client",
            fields=[
                '"ID"',
                "name",
                "name_short",
                "address",
                "phone",
                "email",
                "website",
                "update_dttm",
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
        
        
    @task
    def upload_item():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="item",
            target_table="item",
            fields=[
                '"ID"',
                'item_type',
                '"name"',
                'specification',
                'material',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
        
    @task    
    def upload_stock():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="stock",
            target_table="stock",
            fields=[
                'id',
                'item_id',
                'wh_id',
                'resource_id',
                'stock_date',
                'stock_measure',
                'stock_quantity',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
    
    @task
    def upload_deal():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="deal",
            target_table="deal",
            fields=[
                '"ID"',
                '"Production_item_id"',
                '"Client_id"',
                '"Wh_id"',
                'item_measure',
                'item_quantity',
                'price_basic',
                'price_discount',
                'price_final',
                'deal_value',
                'parent_id',
                '"date"',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
            
    @task   
    def upload_warehouse():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="warehouse",
            target_table="warehouse",
            fields=[
                '"ID"',
                '"Prod_ID"',
                '"Name"',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
    
    @task    
    def upload_price():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="price",
            target_table="price",
            fields=[
                '"ID"',
                '"Production_item_id"',
                'measure',
                'quantity',
                'price',
                'price_date',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
    
    @task    
    def upload_supplier():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="supplier",
            target_table="supplier",
            fields=[
                'id',
                '"name"',
                'region',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
    
    @task    
    def upload_resource():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="resource",
            target_table="resource",
            fields=[
                '"ID"',
                '"name"',
                '"Specification"',
                'supplier_id',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
    
    @task    
    def upload_production():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="production",
            target_table="production",
            fields=[
                '"ID"',
                '"Name"',
                '"Region"',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
    
    @task    
    def upload_production_items():
        upload_table(
            source_conn_id="ERP",
            source_db="ERP",
            source_table="production_items",
            target_table="production_items",
            fields=[
                '"ID"',
                '"Production_id"',
                '"Item_id"',
                'update_dttm',
            ],
            source_schema="public",
            dwh_schema="ods",
        )
        return True
    upload_client()
    upload_item()
    upload_stock()
    upload_deal()
    upload_warehouse()
    upload_price()
    upload_supplier()
    upload_resource()
    upload_production()
    upload_production_items()
init_dag = load_to_staging_layer()
