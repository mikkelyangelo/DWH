from datetime import datetime
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = 'deploy_database_mikhail_mudrichenko'




@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko"],
)
def deploy_database():
  create_schemas_task = PostgresOperator(
        task_id="create_schemas",
        postgres_conn_id="mikhail_mudrichenko_db",
        sql="""CREATE SCHEMA IF NOT EXISTS ods;
               CREATE SCHEMA IF NOT EXISTS dds;
               CREATE SCHEMA IF NOT EXISTS dm;
               """,
    )
  create_tables_and_funcs_task = PostgresOperator(
        task_id="create_tables_and_funcs",
        postgres_conn_id="mikhail_mudrichenko_db",
        sql ="""
        
        
DROP TABLE IF EXISTS dm.report_move_balance;

CREATE TABLE if not exists dm.report_move_balance (
    item_id BIGINT not null
    , item_name VARCHAR(128) NULL
    , manufacture_id BIGINT not null
    , manufacture_name VARCHAR(128) NULL
    , warehouse_id BIGINT not null
    , warehouse_name VARCHAR(128) NULL
    , report_date DATE not null
    , start_balance_quantity int8 null
    , start_balance_cost decimal(21,4) null
    , receipt_quantity int8 null
    , receipt_cost decimal(21,4) null
    , release_quantity int8 null
    , release_cost decimal(21,4) null
    , end_balance_cost decimal(21,4) null
    , end_balance_quantity int8 null
    
    , load_id BIGINT not null
    , hash uuid not null
    , start_ts TIMESTAMP not null
    , end_ts TIMESTAMP not null
);

DROP TABLE IF EXISTS ods.client;
CREATE TABLE ods.client (
"ID" int8 NULL,
"name" varchar(255) NULL,
name_short varchar(128) NULL,
address varchar(255) NULL,
phone varchar(128) NULL,
email varchar(128) NULL,
website varchar(128) NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL
, upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc' , NOW())
);   

DROP TABLE IF EXISTS ods.deal;
CREATE TABLE ods.deal (
"ID" int8 NULL,
"Production_item_id" int8 NULL,
"Client_id" int8 NULL,
"Wh_id" int8 NULL,
item_measure varchar(128) NULL,
item_quantity int8 NULL,
price_basic int8 NULL,
price_discount int8 NULL,
price_final int8 NULL,
deal_value int8 NULL,
parent_id int8 NULL,
"date" date NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);   

DROP TABLE IF EXISTS ods.item;
CREATE TABLE ods.item (
"ID" int8 NULL,
item_type varchar(128) NULL,
"name" varchar(128) NULL,
specification varchar(128) NULL,
material varchar(128) NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS ods.price;
CREATE TABLE ods.price (
"ID" int8 NULL,
"Production_item_id" int8 NULL,
measure varchar(128) NULL,
quantity varchar(128) NULL,
price int8 NULL,
price_date date NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS ods.production;
CREATE TABLE ods.production (
"ID" int8 NULL,
"Name" varchar(128) NULL,
"Region" varchar(128) NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS ods.production_items;
CREATE TABLE ods.production_items (
"ID" int8 NULL,
"Production_id" int8 NULL,
"Item_id" int8 NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS ods.resource;
CREATE TABLE ods.resource (
"ID" int8 NULL,
"name" varchar(128) NULL,
"Specification" varchar(128) NULL,
supplier_id int8 NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS ods.stock;
CREATE TABLE ods.stock (
id int8 NULL,
item_id int8 NULL,
wh_id int8 NULL,
resource_id int8 NULL,
stock_date date NULL,
stock_measure varchar(32) NULL,
stock_quantity int8 NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS ods.supplier;
CREATE TABLE ods.supplier (
id int8 NULL,
"name" varchar(128) NULL,
region varchar(128) NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS ods.warehouse;
CREATE TABLE ods.warehouse (
"ID" int8 NULL,
"Prod_ID" int8 NULL,
"Name" varchar(128) NULL,
update_dttm timestamp DEFAULT timezone('utc'::text, now()) NULL,
upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);
DROP TABLE IF EXISTS dds.hub_counterparty;

CREATE TABLE IF NOT EXISTS dds.hub_counterparty (
    counterparty_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_contact;

CREATE TABLE IF NOT EXISTS dds.hub_contact (
    contact_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(225) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_manufacture;


CREATE TABLE IF NOT EXISTS dds.hub_manufacture (
    manufacture_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_warehouse;

CREATE TABLE IF NOT EXISTS dds.hub_warehouse (
    warehouse_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_region;

CREATE TABLE IF NOT EXISTS dds.hub_region (
    region_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_deal;

CREATE TABLE IF NOT EXISTS dds.hub_deal (
    deal_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_item;

CREATE TABLE IF NOT EXISTS dds.hub_item (
    item_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_goods;

CREATE TABLE IF NOT EXISTS dds.hub_goods (
    goods_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_measure;

CREATE TABLE IF NOT EXISTS dds.hub_measure (
    measure_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128)[] NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds.hub_item_type;

CREATE TABLE IF NOT EXISTS dds.hub_item_type (
    item_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NULL,
    src_table VARCHAR(128) NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NULL,
    src_system_cd CHAR(3) NULL,
    load_id BIGINT DEFAULT 1 NULL
);

DROP TABLE IF EXISTS dds.deal;

CREATE TABLE IF NOT EXISTS dds.deal (
    deal_id bigint NOT NULL,
    client_id bigint NOT NULL,
    date timestamp NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds.goods;

CREATE TABLE IF NOT EXISTS dds.goods (
    goods_id BIGINT NOT NULL
    , manufacture_id BIGINT NOT NULL
    , item_id BIGINT NOT NULL
    , material VARCHAR(128) NULL
    , start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL
    , end_ts timestamp DEFAULT '2999-12-31' NOT NULL
    , src_system_cd char(3) NOT NULL
    , is_deleted boolean DEFAULT 'false' NOT NULL
    , load_id bigint NOT NULL
    , hash uuid NOT NULL
);
    
DROP TABLE IF EXISTS dds.item_type;

CREATE TABLE IF NOT EXISTS dds.item_type (
  item_type_id bigint NOT NULL,
  item_type_name varchar(128) NOT NULL,
  start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
  end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
  src_system_cd char(3) NOT NULL,
  is_deleted boolean DEFAULT 'false' NOT NULL,
  load_id bigint NOT NULL,
  hash uuid NOT NULL);

DROP TABLE IF EXISTS dds.warehouse;

CREATE TABLE IF NOT EXISTS dds.warehouse (
    warehouse_id bigint NOT NULL,
    name varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);
  
DROP TABLE IF EXISTS dds.item;

CREATE TABLE IF NOT EXISTS dds.item (
    item_id bigint NOT NULL,
    item_type_id bigint NOT NULL,
    name varchar(128) NULL,
    specification varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);
  
DROP TABLE IF EXISTS dds.region;

CREATE TABLE IF NOT EXISTS dds.region (
    region_id bigint NOT NULL,
    name varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);
  
DROP TABLE IF EXISTS dds.manufacture;

CREATE TABLE IF NOT EXISTS dds.manufacture (
    manufacture_id bigint NOT NULL,
    region_id bigint NOT NULL,
    name varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds.contact;

CREATE TABLE IF NOT EXISTS dds.contact (
    contact_id bigint NOT NULL,
    contact_type_id bigint NOT NULL,
    contact_info varchar(255) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds.counterparty_x_contact;

CREATE TABLE IF NOT EXISTS dds.counterparty_x_contact (
    counterparty_id bigint NOT NULL,
    contact_id bigint NOT NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);
    
DROP TABLE IF EXISTS dds.client;

CREATE TABLE IF NOT EXISTS dds.client (
    client_id bigint NOT NULL,
    name_short varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);
  
DROP TABLE IF EXISTS dds.deal_detail;

CREATE TABLE IF NOT EXISTS dds.deal_detail (
    deal_id bigint NOT NULL,
    goods_id bigint NOT NULL,
    warehouse_id bigint NOT NULL,
    measure_id bigint NOT NULL,
    discount decimal(8, 4) NULL,
    quantity int8 NOT NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds.measure;

CREATE TABLE IF NOT EXISTS dds.measure (
    measure_id bigint NOT NULL,
    name varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint DEFAULT 1 NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds.stock;

CREATE TABLE IF NOT EXISTS dds.stock (
    warehouse_id bigint NOT NULL,
    stock_measure_id bigint NOT NULL,
    item_id bigint NOT NULL,
    stock_start_date timestamp NULL,
    stock_end_date timestamp NULL,
    stock_quantity integer NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint DEFAULT 1 NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds.pricelist;
    CREATE TABLE IF NOT EXISTS dds.pricelist (
    goods_id bigint NOT NULL,
    measurement_id bigint NOT NULL,
    price_start_date timestamp NOT NULL,
    price_end_date timestamp NULL,
    price_value decimal(21, 4) NOT NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    is_deleted boolean DEFAULT 'false' NOT NULL,
    load_id bigint DEFAULT 1 NOT NULL,
    hash uuid NOT NULL);

DROP SEQUENCE IF EXISTS dds.pricelist_load_id;

CREATE SEQUENCE dds.pricelist_load_id;
          
DROP FUNCTION IF EXISTS dds.insert_dds_pricelist ();

CREATE OR REPLACE FUNCTION dds.insert_dds_pricelist ()
RETURNS INTEGER
LANGUAGE plpgsql
AS $function$
  BEGIN
    PERFORM NEXTVAL('dds.pricelist_load_id');
    WITH first_ranked AS (
      SELECT RANK() OVER (PARTITION BY "Production_item_id" ORDER BY price_date) AS ranked,
        "Production_item_id" AS pid,
        measure AS msr,
        price_date,
        price
        FROM ods.price),
        dated AS (
      SELECT
        pid,msr, CASE WHEN ranked = 1 THEN '1970-01-01'
        ELSE price_date END AS p_date,
        CASE WHEN ranked = (SELECT COUNT(DISTINCT price_date) FROM ods.price) THEN '2999-12-31'
        ELSE (DATE_TRUNC('day', price_date)::DATE - INTERVAL '1 second' + interval '1 month')::DATE END AS end_date,
        price
        FROM first_ranked)
      INSERT INTO dds.pricelist (
        goods_id,
        measurement_id,
        price_start_date,
        price_end_date,
        price_value,
        src_system_cd,
        load_id,
        hash)
      SELECT dds.hub_goods.goods_id, dds.hub_measure.measure_id AS measurement_id,
        dated.p_date AS price_start_date,
        dated.end_date AS price_end_date,
        dated.price AS price_value,
        '1' AS src_system_cd,
        CURRVAL('dds.pricelist_load_id') AS load_id,
        md5(concat(dds.hub_goods.goods_id::text, '#', dds.hub_measure.measure_id::text, '#', dated.p_date::text))::uuid AS hash
      FROM ods.production_items join dds.hub_goods on ods.production_items."ID" = dds.hub_goods.src_key::int8 JOIN dated 
      ON ods.production_items."ID" = dated.pid JOIN dds.hub_measure 
      ON dated.msr = dds.hub_measure.src_key;
    RETURN 0;
    END;
$function$;
          
DROP SEQUENCE IF EXISTS dds.stock_load_id;

CREATE SEQUENCE dds.stock_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_stock ();

CREATE OR REPLACE FUNCTION dds.insert_dds_stock ()
        
RETURNS INTEGER
LANGUAGE plpgsql
AS $function$
  BEGIN
    PERFORM NEXTVAL('dds.stock_load_id');
    WITH rank1 AS (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY item_id, wh_id ORDER BY stock_date) AS rnk,
        wh_id,
        stock_measure,
        item_id,
        stock_date,
        stock_quantity
      FROM ods.stock
    ),
    other_ranks AS (
      SELECT
        wh_id, item_id, rnk,
        CASE WHEN rnk = 1 THEN '1970-01-01' ELSE stock_date END AS sdate,
        CASE WHEN rnk = (select max(rnk) from rank1) THEN '2999-12-31' ELSE (DATE_TRUNC('day', stock_date)::DATE - INTERVAL '1 second' + interval '1 month')::DATE END AS edate,
        stock_quantity
      FROM rank1
    ),
    rank_1 AS (
      SELECT
        wh_id, item_id, rnk,
        CASE WHEN rnk = 1 THEN '1970-01-01' ELSE stock_date END AS sdate,
        CASE WHEN rnk = (select max(rnk) from rank1) THEN '2999-12-31' ELSE (DATE_TRUNC('day', stock_date)::DATE - INTERVAL '1 second' + interval '1 month')::DATE END AS edate,
        stock_quantity
      FROM rank1
      WHERE rnk = 1
    )
    INSERT INTO dds.stock (
      warehouse_id,
      stock_measure_id,
      item_id,
      stock_start_date,
      stock_end_date,
      stock_quantity,
      src_system_cd, load_id, hash
    )
    SELECT 
      dds.hub_warehouse.warehouse_id,
      dds.hub_measure.measure_id,
      dds.hub_item.item_id,
      other_ranks.sdate,
      other_ranks.edate,
      other_ranks.stock_quantity,
      '1', CURRVAL('dds.stock_load_id') AS load_id,
      md5(concat(dds.hub_warehouse.warehouse_id::text, '#', dds.hub_measure.measure_id::text,'#', dds.hub_item.item_id::text, '#', edate::text))::uuid AS hash
    FROM ods.stock JOIN dds.hub_item 
    ON ods.stock.item_id = dds.hub_item.src_key::bigint JOIN dds.hub_warehouse 
    ON ods.stock.wh_id = dds.hub_warehouse.src_key::bigint JOIN other_ranks 
    ON ods.stock.wh_id = other_ranks.wh_id AND ods.stock.item_id = other_ranks.item_id AND ods.stock.stock_date = other_ranks.sdate JOIN dds.hub_measure 
    ON 'price' = ANY(dds.hub_measure.src_table)
    WHERE dds.hub_measure.src_key = '\u0448\u0442'
    UNION
    SELECT dds.hub_warehouse.warehouse_id,
      dds.hub_measure.measure_id,
      dds.hub_item.item_id,
      rank_1.sdate,
      rank_1.edate,
      rank_1.stock_quantity,
      '1' AS src_system_cd,
      CURRVAL('dds.stock_load_id') AS load_id,
      md5(concat(dds.hub_warehouse.warehouse_id::text, '#', dds.hub_measure.measure_id::text,'#', dds.hub_item.item_id::text, '#', edate::text))::uuid AS hash          
    FROM ods.stock JOIN dds.hub_item 
    ON ods.stock.item_id = dds.hub_item.src_key::bigint JOIN dds.hub_warehouse 
    ON ods.stock.wh_id = dds.hub_warehouse.src_key::bigint JOIN rank_1 
    ON ods.stock.wh_id = rank_1.wh_id AND ods.stock.item_id = rank_1.item_id AND ods.stock.stock_date = rank_1.sdate JOIN ods.price 
    ON ods.price."Production_item_id" = ods.stock.item_id JOIN dds.hub_measure 
    ON 'price' = ANY(dds.hub_measure.src_table)
    WHERE dds.hub_measure.src_key = '\u0448\u0442';
  RETURN 0;
  END;
$function$;           

DROP SEQUENCE IF EXISTS dds.measure_load_id;

CREATE SEQUENCE dds.measure_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_measure ();

CREATE OR REPLACE FUNCTION dds.insert_dds_measure ()
RETURNS INTEGER
LANGUAGE plpgsql
AS $function$
  BEGIN
    PERFORM NEXTVAL('dds.measure_load_id');
    INSERT INTO dds.measure (measure_id, name, src_system_cd, load_id, hash)
    SELECT
       dds.hub_measure.measure_id AS measure_id,
       dds.hub_measure.src_key AS name,
       '1' AS src_system_cd,
       CURRVAL('dds.measure_load_id') AS load_id,
       md5(concat(measure_id::text, '#', start_ts::text))::uuid AS hash
    FROM dds.hub_measure;
    RETURN 0;
  END;
$function$;

DROP SEQUENCE IF EXISTS dds.deal_detail_load_id; 
 
CREATE SEQUENCE dds.deal_detail_load_id; 
 
DROP FUNCTION IF EXISTS dds.insert_dds_deal_detail (); 
CREATE OR REPLACE FUNCTION dds.insert_dds_deal_detail () 
RETURNS INTEGER 
LANGUAGE plpgsql 
AS $function$ 
  BEGIN 
    PERFORM NEXTVAL('dds.deal_detail_load_id'); 
    INSERT INTO dds.deal_detail ( 
          deal_id, 
          goods_id, 
          warehouse_id, 
          measure_id, 
          discount, 
          quantity, 
          src_system_cd, 
          load_id, 
          hash) 
    SELECT dds.hub_deal.deal_id AS deal_id, 
      dds.hub_goods.goods_id AS goods_id, 
      dds.hub_warehouse.warehouse_id AS warehouse_id, 
      dds.hub_measure.measure_id AS measure_id, 
      ods.deal.price_discount AS discount, 
      ods.deal.item_quantity AS quantity, 
      '1' AS src_system_cd, 
      CURRVAL('dds.deal_detail_load_id') AS load_id, 
      md5(concat(dds.hub_deal.deal_id::text, '#', dds.hub_goods.goods_id::text, '#', dds.hub_warehouse.warehouse_id::text, '#', dds.hub_measure.measure_id::text))::uuid as hash 
    FROM ods.deal JOIN dds.hub_deal  
    ON ods.deal.parent_id  = dds.hub_deal.src_key::bigint JOIN dds.hub_goods  
    ON ods.deal."Production_item_id" = dds.hub_goods.src_key::bigint JOIN dds.hub_warehouse  
    ON ods.deal."Wh_id" = dds.hub_warehouse.src_key::bigint  JOIN dds.hub_measure  
    ON ods.deal.item_measure = dds.hub_measure.src_key 
    WHERE ods.deal.parent_id is NOT NULL; 
  RETURN 0; 
  END; 
$function$;

DROP SEQUENCE IF EXISTS dds.counterparty_x_contact_load_id;

CREATE SEQUENCE dds.counterparty_x_contact_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_counterparty_x_contact;

CREATE OR REPLACE FUNCTION dds.insert_dds_counterparty_x_contact ()
RETURNS INTEGER
LANGUAGE plpgsql
AS $function$
  BEGIN
    PERFORM NEXTVAL('dds.counterparty_x_contact_load_id');
      INSERT INTO dds.counterparty_x_contact (counterparty_id, contact_id, src_system_cd, load_id, hash)
      SELECT dds.hub_counterparty.counterparty_id AS counterparty_id,
        dds.hub_contact.contact_id AS contact_id,
        '1' AS src_system_cd,
        CURRVAL('dds.counterparty_x_contact_load_id') AS load_id,
        md5(concat(dds.hub_counterparty.counterparty_id::text, '#', dds.hub_contact.contact_id::text))::uuid as hash
      FROM dds.hub_counterparty, dds.hub_contact;
    RETURN 0;
  END;
$function$;

DROP SEQUENCE IF EXISTS dm.report_move_balance_load_id;

CREATE SEQUENCE dm.report_move_balance_load_id;

CREATE OR REPLACE FUNCTION dm.insert_report_move_balance ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('report_move_balance_load_id');
    INSERT INTO dm.report_move_balance  (item_id , item_name , manufacture_id , manufacture_name , warehouse_id , warehouse_name , report_date , start_balance_quantity , start_balance_cost , receipt_quantity , receipt_cost , release_quantity ,release_cost, end_balance_cost , end_balance_quantity , load_id , hash , start_ts , end_ts)
    WITH igw AS ( 
 SELECT g.item_id, 
        g.manufacture_id, 
        w.warehouse_id, 
        g.goods_id 
    FROM dds.item i CROSS JOIN dds.warehouse w JOIN dds.goods g 
    ON g.item_id = i.item_id), 
s_prev_month AS ( 
 SELECT warehouse_id, 
     item_id, 
     stock_quantity 
    FROM dds.stock 
    WHERE (DATE_TRUNC('month' , '2018-07-02'::DATE)::DATE - interval '1 second')::DATE BETWEEN stock_start_date::DATE AND stock_end_date::DATE),
s_price AS ( 
    SELECT goods_id, 
        measurement_id, 
        price_value 
    FROM dds.pricelist 
    WHERE (DATE_TRUNC('month' , '2018-07-02'::DATE)::DATE - interval '1 second')::DATE BETWEEN price_start_date::DATE AND price_end_date::DATE), 
s_last_date AS ( 
 SELECT DISTINCT  d.deal_id, 
    d."date", 
    dd.goods_id, 
    dd.warehouse_id, 
    dd.quantity AS quantity 
FROM dds.deal d 
JOIN dds.deal_detail dd ON d.deal_id = dd.deal_id  
WHERE d."date" BETWEEN DATE_TRUNC('month', '2018-07-02'::DATE)  
                AND (DATE_TRUNC('month', '2018-07-02'::DATE) + INTERVAL '1 month - 1 day')::DATE 
 
), 
s_end_date AS ( 
 SELECT warehouse_id, 
     item_id, 
     stock_quantity 
 FROM dds.stock 
 WHERE DATE_TRUNC('month', stock_end_date::DATE) = DATE_TRUNC('month', '2018-07-02'::DATE) + INTERVAL '1 month'),
 p_end_date AS ( 
 SELECT goods_id, 
     measurement_id, 
     price_value 
 FROM dds.pricelist 
 WHERE DATE_TRUNC('month', price_end_date::DATE) = DATE_TRUNC('month', '2018-07-02'::DATE) + INTERVAL '1 month'),
 s_last_day_price AS ( 
    SELECT goods_id, 
        measurement_id, 
        price_value 
    FROM dds.pricelist 
    WHERE (DATE_TRUNC('month' , '2018-07-02'::DATE)::DATE - interval '1 second')::DATE BETWEEN price_start_date::DATE AND price_end_date::DATE),
indicators AS ( 
    SELECT igw.item_id, 
        igw.manufacture_id, 
        igw.warehouse_id, 
        '2018-07-02' AS report_date, 
        SUM(spm.stock_quantity) AS start_balance_quantity, 
        SUM(spm.stock_quantity * sp.price_value) as start_balance_cost, 
        SUM(spm.stock_quantity) - SUM(sed.stock_quantity) + SUM(sld.quantity) AS receipt_quantity,
        SUM(sed.stock_quantity * ped.price_value) - SUM(spm.stock_quantity * sp.price_value) + SUM(sld.quantity) as receipt_cost,
        SUM(sld.quantity) as release_quantity,
        sld.quantity * sp.price_value as release_cost,
        spm.stock_quantity as end_balance_quantity,
        spm.stock_quantity * sp.price_value as end_balance_cost
    FROM igw JOIN s_prev_month spm 
    ON spm.item_id = igw.item_id AND igw.warehouse_id = spm.warehouse_id LEFT JOIN s_price sp 
    on sp.goods_id = igw.goods_id LEFT JOIN s_end_date sed 
    ON sed.item_id = igw.item_id AND igw.warehouse_id = sed.warehouse_id LEFT JOIN p_end_date ped  
    on ped.goods_id = igw.goods_i LEFT JOIN s_last_date sld 
    ON sld.goods_id = igw.goods_id 
    GROUP BY( igw.item_id, 
        igw.manufacture_id, 
        igw.warehouse_id,
        igw.goods_id,
        spm.stock_quantity,
        sp.price_value,
        sld.quantity)) 
SELECT
    indicators.item_id,
    i."name" AS item_name,
    indicators.manufacture_id,
    m."name" AS manufacture_name,
    indicators.warehouse_id,
    w."name" AS warehouse_name,
    '2018-07-02' AS report_date,
    indicators.start_balance_quantity as start_balance_quantity,
    indicators.start_balance_cost as start_balance_cost,
    indicators.receipt_quantity as receipt_quantity,
    indicators.receipt_cost as receipt_cost,
    indicators.release_quantity as release_quantity,
    indicators.release_cost as release_cost,
    indicators.end_balance_quantity as end_balance_quantity,
    indicators.end_balance_cost as end_balance_cost,
    CURRVAL('report_move_balance_load_id') AS load_id,
    MD5(indicators.item_id || '#' || indicators.manufacture_id || '#' || indicators.warehouse_id || '#' || '2018-07-02')::UUID AS hash,
    NOW() AS start_ts,
    '2999-12-31'::TIMESTAMP AS end_ts
    from indicators JOIN ( SELECT DISTINCT
                manufacture_id,
                "name" 
                from dds.manufacture) m ON indicators.manufacture_id = m.manufacture_id
        JOIN ( SELECT DISTINCT
                item_id
                , "name"
            from dds.item) i ON indicators.item_id = i.item_id
        JOIN ( SELECT DISTINCT
                warehouse_id
                , "name"
            from dds.warehouse) w ON indicators.warehouse_id = w.warehouse_id;
    RETURN 0;
END;
$function$;
 
DROP SEQUENCE IF EXISTS dds.goods_load_id; 
 
CREATE SEQUENCE dds.goods_load_id; 
 
DROP FUNCTION IF EXISTS dds.insert_dds_goods (); 
 
CREATE OR REPLACE FUNCTION dds.insert_dds_goods () 
RETURNS INTEGER 
LANGUAGE plpgsql 
AS $function$ 
  BEGIN 
    PERFORM NEXTVAL('dds.goods_load_id'); 
    INSERT INTO dds.goods (goods_id, manufacture_id, item_id, material, src_system_cd, load_id, hash) 
    SELECT dds.hub_goods.goods_id AS goods_id, 
      dds.hub_manufacture.manufacture_id AS manufacture_id, 
      dds.hub_item.item_id AS item_id, 
      ods.item.material AS material, 
      '1' AS src_system_cd, 
      CURRVAL('dds.goods_load_id') AS load_id, 
      md5(dds.hub_goods.goods_id::text)::uuid as hash 
    FROM ods.item JOIN ods.production_items 
    ON ods.item."ID" = ods.production_items."Item_id" JOIN dds.hub_item 
    ON ods.item."ID" = dds.hub_item.src_key::bigint JOIN dds.hub_manufacture 
    ON ods.production_items."Production_id" = dds.hub_manufacture.src_key::bigint JOIN dds.hub_goods 
    ON ods.production_items."ID" = dds.hub_goods.src_key::bigint; 
  RETURN 0; 
  END; 
$function$;


DROP SEQUENCE IF EXISTS dds.contact_load_id;

CREATE SEQUENCE dds.contact_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_contact ();

CREATE OR REPLACE FUNCTION dds.insert_dds_contact ()
RETURNS INTEGER
LANGUAGE plpgsql
AS $function$
  BEGIN
    PERFORM NEXTVAL('dds.contact_load_id');
    INSERT INTO dds.contact (contact_id, contact_type_id, contact_info, src_system_cd, load_id, hash)
    SELECT hub_contact.contact_id AS contact_id,
      '1' AS contact_type_id,
      CONCAT(ods.client.address,ods.client.phone,ods.client.email,ods.client.website) AS contact_id,
      '1' AS src_system_cd,
      CURRVAL('dds.contact_load_id') AS load_id,
      md5(dds.hub_contact.contact_id::text)::uuid as hash
      FROM ods.client JOIN dds.hub_contact 
      ON ods.client."ID" = dds.hub_contact.src_key::bigint;
    RETURN 0;
  END;
$function$;

DROP SEQUENCE IF EXISTS dds.client_load_id;

CREATE SEQUENCE dds.client_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_client ();

CREATE OR REPLACE FUNCTION dds.insert_dds_client ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds.client_load_id');
  INSERT INTO dds.client (client_id, name_short, src_system_cd, load_id, hash)
  SELECT
     hub_counterparty.counterparty_id AS client_id,
     ods.client.name_short AS name_short,
     '1' AS src_system_cd,
     CURRVAL('dds.client_load_id') AS load_id,
     md5(dds.hub_counterparty.counterparty_id::text)::uuid as hash
  FROM ods.client JOIN dds.hub_counterparty ON ods.client."ID" = dds.hub_counterparty.src_key::bigint;
 
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.manufacture_load_id;

CREATE SEQUENCE dds.manufacture_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_manufacture ();

CREATE OR REPLACE FUNCTION dds.insert_dds_manufacture ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds.manufacture_load_id');
  INSERT INTO dds.manufacture (manufacture_id, region_id, name, src_system_cd, load_id, hash)
  SELECT
     hub_manufacture.manufacture_id AS manufacture_id,
     hub_region.region_id AS region_id,
     ods.production."Name" AS name,
     '1' AS src_system_cd,
     CURRVAL('dds.manufacture_load_id') AS load_id,
     md5(dds.hub_manufacture.manufacture_id::text)::uuid as hash
  FROM ods.production JOIN dds.hub_manufacture ON ods.production."ID" = dds.hub_manufacture.src_key::bigint
  JOIN dds.hub_region ON ods.production."Region" = dds.hub_region.src_key;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.region_load_id;

CREATE SEQUENCE dds.region_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_region ();

CREATE OR REPLACE FUNCTION dds.insert_dds_region ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds.region_load_id');
  INSERT INTO dds.region (region_id, name, src_system_cd, load_id, hash)
  SELECT
     hub_region.region_id AS region_id,
     ods.production."Region" AS name,
     '1' AS src_system_cd,
     CURRVAL('dds.region_load_id') AS load_id,
     md5(dds.hub_region.region_id::text)::uuid as hash
  FROM ods.production JOIN dds.hub_region ON ods.production."Region" = dds.hub_region.src_key;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.item_type_load_id;

CREATE SEQUENCE dds.item_type_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_item_type ();

CREATE OR REPLACE FUNCTION dds.insert_dds_item_type ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds.item_type_load_id');
  INSERT INTO dds.item_type (item_type_id, item_type_name, src_system_cd, load_id, hash)
  SELECT
     hub_item_type.item_id AS item_type_id,
     hub_item_type.src_key AS item_type_name,
     '1' AS src_system_cd,
     CURRVAL('dds.item_type_load_id') AS load_id,
     md5(dds.hub_item_type.item_id::text)::uuid as hash
  FROM dds.hub_item_type;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.item_load_id;

CREATE SEQUENCE dds.item_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_item ();

CREATE OR REPLACE FUNCTION dds.insert_dds_item ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds.item_load_id');
  INSERT INTO dds.item (item_id, item_type_id, name, specification, src_system_cd, load_id, hash)
  SELECT
     hub_item.item_id AS item_id,
     hub_item_type.item_id AS item_type_id,
     ods.item."name" AS name,
     ods.item.specification AS specification,
     '1' AS src_system_cd,
     CURRVAL('dds.item_load_id') AS load_id,
     md5(dds.hub_item.item_id::text)::uuid as hash
  FROM ods.item JOIN dds.hub_item ON ods.item."ID" = dds.hub_item.src_key::bigint
  JOIN dds.hub_item_type ON ods.item.item_type = dds.hub_item_type.src_key;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.warehouse_load_id;

CREATE SEQUENCE dds.warehouse_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_warehouse ();

CREATE OR REPLACE FUNCTION dds.insert_dds_warehouse ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds.warehouse_load_id');
  INSERT INTO dds.warehouse (warehouse_id, name, end_ts, src_system_cd, load_id, hash)
  SELECT
     hub_warehouse.warehouse_id AS warehouse_id,
     ods.warehouse."Name" AS name,
     '2999-12-31' as end_ts,
     '1' AS src_system_cd,
     CURRVAL('dds.warehouse_load_id') AS load_id,
     md5(dds.hub_warehouse.warehouse_id::text)::uuid as hash
  FROM ods.warehouse JOIN dds.hub_warehouse
  ON ods.warehouse."ID" = dds.hub_warehouse.src_key::bigint;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.hub_item_type_load_id;

CREATE SEQUENCE dds.hub_item_type_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_item_type ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_item_type ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_item_type_load_id');
    INSERT INTO dds.hub_item_type (src_key , src_table , src_system_cd , load_id)
    SELECT
        "item_type" AS src_key
        , 'item' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_item_type_load_id') AS load_id
    FROM
        ods.item;
    RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.deal_load_id;

CREATE SEQUENCE dds.deal_load_id;
  
DROP FUNCTION IF EXISTS dds.insert_dds_deal ();

CREATE OR REPLACE FUNCTION dds.insert_dds_deal ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM NEXTVAL('dds.deal_load_id');
    INSERT INTO dds.deal (deal_id, client_id, date, src_system_cd, load_id,hash)
    SELECT
        hub_deal.deal_id AS deal_id,
        counterparty_id AS client_id,
        "date" AS date,
        '1' AS src_system_cd,
        CURRVAL('dds.deal_load_id') AS load_id,
        md5(dds.hub_deal.deal_id::text)::uuid as hash
    FROM ods.deal  JOIN dds.hub_deal  
    ON ods.deal."ID" = dds.hub_deal.src_key::bigint JOIN dds.hub_counterparty  
    ON ods.deal."Client_id" = dds.hub_counterparty.src_key::bigint
    WHERE parent_id is NULL;
  RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds.hub_measure_load_id;

CREATE SEQUENCE dds.hub_measure_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_measure ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_measure ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM NEXTVAL('dds.hub_measure_load_id');
    WITH ms AS ( 
        SELECT "Production_item_id", measure AS msr
        FROM ods.price 
        UNION 
        SELECT "Production_item_id", item_measure AS msr
        FROM ods.deal 
        WHERE "Client_id" is NOT NULL) 
    INSERT INTO dds.hub_measure ( 
        src_key, 
        src_table, 
        src_system_cd, 
        load_id) 
    SELECT distinct
        msr, 
        Array['deal','price'], 
        '1' AS src_system_cd, 
        CURRVAL('dds.hub_measure_load_id') AS load_id
    FROM ms;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.hub_contact_load_id;

CREATE SEQUENCE dds.hub_contact_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_contact ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_contact ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_contact_load_id');
    INSERT INTO dds.hub_contact (src_key , src_table , src_system_cd , load_id)
    SELECT
        (case
              WHEN (trim(phone) <> '' ) THEN phone 
              WHEN (trim(email) <> '') THEN email
              WHEN (trim(address) <> '') THEN address
              WHEN (trim(website) <> '') THEN website
              ELSE NULL
            end) AS src_key
        , 'client' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_contact_load_id') AS load_id
    FROM
        ods.client;
    RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.hub_goods_load_id;

CREATE SEQUENCE dds.hub_goods_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_goods ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_goods ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_goods_load_id');
    INSERT INTO dds.hub_goods (src_key , src_table , src_system_cd , load_id)
    SELECT
        "ID" AS src_key
        , 'production_items' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_goods_load_id') AS load_id
    FROM
        ods.production_items;
    RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.hub_item_load_id;

CREATE SEQUENCE dds.hub_item_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_item ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_item ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_item_load_id');
    INSERT INTO dds.hub_item (src_key , src_table , src_system_cd , load_id)
    SELECT
        "ID" AS src_key
        , 'item' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_item_load_id') AS load_id
    FROM
        ods.item;
    RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds.hub_deal_load_id;

CREATE SEQUENCE dds.hub_deal_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_deal ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_deal ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_deal_load_id');
    INSERT INTO dds.hub_deal (src_key , src_table , src_system_cd , load_id)
    SELECT
        "ID" AS src_key
        , 'deal' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_deal_load_id') AS load_id
    FROM
        ods.deal
    WHERE parent_id is NULL;
    RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds.hub_region_load_id;

CREATE SEQUENCE dds.hub_region_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_region ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_region ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_region_load_id');
    INSERT INTO dds.hub_region (src_key , src_table , src_system_cd , load_id)
    SELECT
        "Region" AS src_key
        , 'production' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_region_load_id') AS load_id
    FROM
        ods.production;
    RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds.hub_contact_load_id;

CREATE SEQUENCE dds.hub_contact_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_contact ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_contact ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_contact_load_id');
    INSERT INTO dds.hub_contact (src_key , src_table , src_system_cd , load_id)
    SELECT
        (case
              WHEN (trim(phone) <> '' ) THEN phone 
              WHEN (trim(email) <> '') THEN email
              WHEN (trim(address) <> '') THEN address
              WHEN (trim(website) <> '') THEN website
              ELSE NULL
            end) AS src_key
        , 'client' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_contact_load_id') AS load_id
    FROM
        ods.client;
    RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds.hub_warehouse_load_id;

CREATE SEQUENCE dds.hub_warehouse_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_warehouse ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_warehouse ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_warehouse_load_id');
    INSERT INTO dds.hub_warehouse (src_key , src_table , src_system_cd , load_id)
    SELECT
        "ID" AS src_key
        , 'warehouse' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_warehouse_load_id') AS load_id
    FROM
        ods.warehouse;
    RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds.hub_counterparty_load_id;

CREATE SEQUENCE dds.hub_counterparty_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_counterparty ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_counterparty ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_counterparty_load_id');
    INSERT INTO dds.hub_counterparty (src_key , src_table , src_system_cd , load_id)
    SELECT
        "ID" AS src_key
        , 'client' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_counterparty_load_id') AS load_id
    FROM
        ods.client;
    RETURN 0;
END;
$function$; 

DROP SEQUENCE IF EXISTS dds.hub_manufacture_load_id;

CREATE SEQUENCE dds.hub_manufacture_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_manufacture ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_manufacture ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_manufacture_load_id');
    INSERT INTO dds.hub_manufacture (src_key , src_table , src_system_cd , load_id)
    SELECT
        "ID" AS src_key
        , 'production' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_manufacture_load_id') AS load_id
    FROM
        ods.production;
    RETURN 0;
END;
$function$; 

DROP SEQUENCE IF EXISTS dds.hub_countact_load_id;

CREATE SEQUENCE dds.hub_countact_load_id;

DROP FUNCTION IF EXISTS dds.insert_dds_hub_contact ();

CREATE OR REPLACE FUNCTION dds.insert_dds_hub_contact ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds.hub_contact_load_id');
    INSERT INTO dds.hub_contact (src_key , src_table , src_system_cd , load_id)
    SELECT
        "ID" AS src_key
        , 'client' AS src_table
        , '1' AS src_system_cd
        , CURRVAL('dds.hub_contact_load_id') AS load_id
    FROM
        ods.client;
    RETURN 0;
END;
$function$;   



"""
        )
  
  create_schemas_task >> create_tables_and_funcs_task
  
  
init_dag = deploy_database()
