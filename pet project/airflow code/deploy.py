from datetime import datetime
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = 'deploy_database_mikhail_mudrichenko_project'




@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["Mudrichenko","Personal project"],
)
def deploy_database():
  create_schemas_task_project = PostgresOperator(
        task_id="create_schemas_project",
        postgres_conn_id="mikhail_mudrichenko_db",
        sql="""CREATE SCHEMA IF NOT EXISTS stg_pp;
               CREATE SCHEMA IF NOT EXISTS ods_pp;
               CREATE SCHEMA IF NOT EXISTS dds_pp;
               CREATE SCHEMA IF NOT EXISTS dm_pp;
               """,
    )
  create_tables_and_funcs_task_project = PostgresOperator(
        task_id="create_tables_and_funcs_project",
        postgres_conn_id="mikhail_mudrichenko_db",
        sql ="""

drop function if exists update_timestamp_goods() cascade;
CREATE OR REPLACE FUNCTION update_timestamp_goods()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_dttm = TIMEZONE('utc', NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timestamp_trigger_goods ON source_pp.goods;
CREATE TRIGGER update_timestamp_trigger_goods
BEFORE UPDATE ON source_pp.goods
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_goods();

drop function if exists update_timestamp_supplier() cascade;
CREATE OR REPLACE FUNCTION update_timestamp_supplier()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_dttm = TIMEZONE('utc', NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

drop trigger if exists update_timestamp_trigger_supplier ON source_pp.supplier;
CREATE TRIGGER update_timestamp_trigger_supplier
BEFORE UPDATE ON source_pp.supplier
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_supplier();

drop function if exists update_timestamp_goods_supply() cascade;
CREATE OR REPLACE FUNCTION update_timestamp_goods_supply()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_dttm = TIMEZONE('utc', NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

drop trigger if exists update_timestamp_trigger_goods_supply ON source_pp.goods_supply;
CREATE TRIGGER update_timestamp_trigger_goods_supply
BEFORE UPDATE ON source_pp.goods_supply
FOR EACH ROW
EXECUTE FUNCTION update_timestamp_goods_supply();

drop table if exists tech_pp.last_update;
CREATE TABLE tech_pp.last_update (
	table_name varchar(50) NOT NULL,
	update_dt timestamp NOT NULL
);
        
drop table if exists stg_pp.supply_details;
CREATE TABLE stg_pp.supply_details(
	"ID" int8 NOT NULL,
	quantity int8 NULL,
	ID_supply int8 NULL,
	ID_sup_goods int8 NULL,
	update_dttm timestamp null
);

drop table if exists stg_pp.supply;
CREATE TABLE stg_pp.supply(
	"ID" int8 NOT NULL,
	ID_sup_goods int8 NULL,
	Date_sup_start timestamp NULL,
	Date_sup_stop timestamp NULL,
	supply_status bit null,
	update_dttm timestamp null
);

drop table if exists stg_pp.store;
CREATE TABLE stg_pp.store(
	"ID" int8  NOT NULL,
	store_name varchar(128) NULL,
	address varchar(128) NULL,
	update_dttm timestamp null
);

drop table if exists stg_pp.product;
CREATE TABLE stg_pp.product(
	"ID" int8  NOT NULL,
	ID_store int8 NULL,
	price decimal(10,2),
	product_name varchar(128) NULL,
	quantity int8 null,
	update_dttm timestamp null
);

drop table if exists stg_pp.order_detail;
CREATE TABLE stg_pp.order_detail(
	"ID" int8  NOT NULL,
	ID_order int8 NULL,
	ID_product int8 NULL,
	quantity int8 NULL,
	update_dttm timestamp null
);

drop table if exists stg_pp.orders;
CREATE TABLE stg_pp.orders(
	"ID" int8  NOT NULL,
	ID_client int8 NULL,
	order_dttm timestamp NULL,
	update_dttm timestamp null
);

drop table if exists stg_pp.client;
CREATE TABLE stg_pp.client(
	"ID" int8 NOT NULL,
	"name" varchar(128) NULL,
	name_short varchar NULL,
	address varchar(128) NULL,
	update_dttm timestamp null
);

drop table if exists stg_pp.goods;
CREATE TABLE stg_pp.goods(
	"ID" int8 not NULL,
	food_name varchar(128) NULL,
	food_type varchar null,
	"Specification" varchar(128) NULL,
	update_dttm timestamp NULL,
	deleted_flg bit null
);

DROP TABLE if exists stg_pp.goods_supply;

CREATE TABLE stg_pp.goods_supply (
	"ID" int8 NOT NULL,
	item_sup_id int8 NULL,
	supplier_id int8 NULL,
	avaibl_count int8 NULL,
  price_per_measure decimal(10,2) null,
	update_dttm timestamp NULL,
	deleted_flg bit null
);
drop table if exists stg_pp.supplier;
CREATE TABLE stg_pp.supplier (
	id int8  NULL,
	"name" varchar(128) NULL,
	region varchar(128) NULL,
	email varchar(128) NULL,
	website varchar(128) NULL,
	phone varchar(128) NULL,
	address varchar(128) NULL,
	update_dttm timestamp NULL,
	deleted_flg bit null
);



drop table if exists ods_pp.supply_details;
CREATE TABLE ods_pp.supply_details(
	"ID" int8  NOT NULL,
	quantity int8 NULL,
	ID_supply int8 NULL,
	ID_sup_goods int8 NULL,
	update_dttm timestamp null,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table if exists ods_pp.supply;
CREATE TABLE ods_pp.supply(
	"ID" int8 NOT NULL,
	ID_sup_goods int8 NULL,
	Date_sup_start timestamp NULL,
	Date_sup_stop timestamp NULL,
	supply_status bit null,
	update_dttm timestamp null,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table if exists ods_pp.store;
CREATE TABLE ods_pp.store(
	"ID" int8  NOT NULL,
	store_name varchar(128) NULL,
	address varchar(128) NULL,
	update_dttm timestamp null,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table if exists ods_pp.product;
CREATE TABLE ods_pp.product(
	"ID" int8  NOT NULL,
	ID_store int8 NULL,
	price decimal(10,2),
	product_name varchar(128) NULL,
	quantity int8 null,
	update_dttm timestamp null,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table if exists ods_pp.order_detail;
CREATE TABLE ods_pp.order_detail(
	"ID" int8 NOT NULL,
	ID_order int8 NULL,
	ID_product int8 NULL,
	quantity int8 NULL,
	update_dttm timestamp null,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table if exists ods_pp.orders;
CREATE TABLE ods_pp.orders(
	"ID"int8 NOT NULL,
	ID_client int8 NULL,
	order_dttm timestamp NULL,
	update_dttm timestamp null,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table if exists ods_pp.client;
CREATE TABLE ods_pp.client(
	"ID" int8 NOT NULL,
	"name" varchar(128) NULL,
	name_short varchar NULL,
	address varchar(128) NULL,
	update_dttm timestamp null,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table ods_pp.goods;
CREATE TABLE ods_pp.goods(
	"ID" int8 not NULL,
	food_name varchar(128) NULL,
	food_type varchar null,
	"Specification" varchar(128) NULL,
	update_dttm timestamp NULL,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE if exists ods_pp.goods_supply;

CREATE TABLE ods_pp.goods_supply (
	"ID" int8 NOT NULL,
	item_sup_id int8 NULL,
	supplier_id int8 NULL,
	avaibl_count int8 NULL,
  price_per_measure decimal(10,2) null,
	update_dttm timestamp NULL,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

drop table if exists ods_pp.supplier;
CREATE TABLE ods_pp.supplier (
	id int8 NULL,
	"name" varchar(128) NULL,
	region varchar(128) NULL,
	email varchar(128) NULL,
	website varchar(128) NULL,
	phone varchar(128) NULL,
	address varchar(128) NULL,
	update_dttm timestamp NULL,
	upload_stg_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

DROP TABLE IF EXISTS dds_pp.hub_supplier;

CREATE TABLE IF NOT EXISTS dds_pp.hub_supplier (
    supplier_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds_pp.hub_contact;

CREATE TABLE IF NOT EXISTS dds_pp.hub_contact (
    contact_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds_pp.hub_measure;

CREATE TABLE IF NOT EXISTS dds_pp.hub_measure (
    measure_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds_pp.hub_goods;

CREATE TABLE IF NOT EXISTS dds_pp.hub_goods (
    goods_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds_pp.hub_goods_type;

CREATE TABLE IF NOT EXISTS dds_pp.hub_goods_type (
    goods_type_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds_pp.hub_goods_supply;

CREATE TABLE IF NOT EXISTS dds_pp.hub_goods_supply (
    goods_supply_id BIGSERIAL PRIMARY KEY,
    src_key VARCHAR(128) NOT NULL,
    src_table VARCHAR(128) NOT NULL,
    start_ts TIMESTAMP DEFAULT TIMEZONE('utc'::TEXT , NOW()) NOT NULL,
    src_system_cd CHAR(3) NOT NULL,
    load_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS dds_pp.supplier;

CREATE TABLE IF NOT EXISTS dds_pp.supplier (
    supplier_id int8 NOT NULL,
    supplier_name varchar(128) NULL,
    contact_id int8 NOT NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds_pp.contact;

CREATE TABLE IF NOT EXISTS dds_pp.contact (
    contact_id int8 NOT NULL,
    contact_type_id int8 NOT NULL,
    contact_info varchar(255) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);
    
DROP TABLE IF EXISTS dds_pp.contact_type;

CREATE TABLE IF NOT EXISTS dds_pp.contact_type (
    contact_type_id int8 NOT NULL,
    contact_type_code int8 NOT NULL,
    contact_type_name varchar(255) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds_pp.measure;

CREATE TABLE IF NOT EXISTS dds_pp.measure (
    measure_id int8 NOT NULL,
    name_measure varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds_pp.goods;

CREATE TABLE IF NOT EXISTS dds_pp.goods (
    goods_id int8 NOT NULL,
    goods_type_id int8 NOT NULL,
    goods_name varchar(128) NULL,
    goods_measure_id int8 NOT NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds_pp.goods_type;

CREATE TABLE IF NOT EXISTS dds_pp.goods_type (
    goods_type_id int8 NOT NULL,
    goods_type_name varchar(128) NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);

DROP TABLE IF EXISTS dds_pp.goods_supply;

CREATE TABLE IF NOT EXISTS dds_pp.goods_supply (
    goods_supply_id int8 NOT NULL,
    supplier_id int8 NOT NULL,
    goods_id int8 NOT NULL,
    price_per_measure decimal(10,2) NULL,
    quantity int8 NULL,
    start_ts timestamp DEFAULT timezone('utc'::text , now()) NOT NULL,
    end_ts timestamp DEFAULT '2999-12-31' NOT NULL,
    src_system_cd char(3) NOT NULL,
    load_id bigint NOT NULL,
    hash uuid NOT NULL);
    
DROP TABLE IF EXISTS dm_pp.report_goods_supply_info;

CREATE TABLE if not exists dm_pp.report_goods_supply_info(
	supplier_id int8 not null,
    supplier_name varchar(128) null,
    supplier_contact_info varchar(128)null,
    goods_name varchar(128) null,
    goods_type varchar(128) null,
    price_per_measure decimal(10,2) null,
    goods_quantity decimal(10,2) null,
    goods_measure varchar(128) null,
    total_quantity decimal(10,2) null,
    total_goods_per_supplier int8 null,
    total_suppliers_per_goods int8 null,
    load_id BIGINT not null,
    start_ts TIMESTAMP not null,
    end_ts TIMESTAMP not null,
    hash uuid not null
);
    
INSERT INTO dds_pp.contact_type (contact_type_id, contact_type_code, contact_type_name,src_system_cd, load_id, hash)
  VALUES  ('1','1', 'website', '1','1',  MD5('1'::TEXT)::UUID),
          ('2','2', 'phone', '1', '1', MD5('2'::TEXT)::UUID),
          ('3', '3','email', '1', '1', MD5('3'::TEXT)::UUID),
          ('4', '4','address','1', '1', MD5('4'::TEXT)::UUID),
          ('5', '5','region', '1', '1', MD5('5'::TEXT)::UUID);

DROP SEQUENCE IF EXISTS dm_pp.report_goods_supply_info_load_id;

CREATE SEQUENCE dm_pp.report_goods_supply_info_load_id;

drop function if exists dm_pp.insert_report_goods_supply_info();
CREATE OR REPLACE FUNCTION dm_pp.insert_report_goods_supply_info ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dm_pp.report_goods_supply_info_load_id');
       
    delete from dm_pp.report_goods_supply_info;
    INSERT INTO dm_pp.report_goods_supply_info (supplier_id, supplier_name, supplier_contact_info, goods_name, goods_type, price_per_measure,
	    goods_quantity, goods_measure, total_quantity, total_goods_per_supplier, 
	    total_suppliers_per_goods, load_id, start_ts, end_ts, hash)
	WITH goods_supply_data AS (
	        SELECT dds_pp.goods_supply.supplier_id,
	            dds_pp.goods_supply.goods_id,
	            dds_pp.goods_supply.price_per_measure,
	            dds_pp.goods_supply.quantity,
	            SUM(dds_pp.goods_supply.quantity) OVER (PARTITION BY dds_pp.goods_supply.supplier_id, dds_pp.goods.goods_type_id, dds_pp.goods.goods_measure_id) AS total_quantity
	        FROM dds_pp.goods_supply JOIN dds_pp.hub_goods
	        ON dds_pp.goods_supply.goods_id = dds_pp.hub_goods.src_key::bigint join dds_pp.goods 
	        on dds_pp.hub_goods.goods_id = dds_pp.goods.goods_id 
          where goods_supply.price_per_measure > 0       
	     
	    ),
	    supplier_goods_count AS (
	        SELECT dds_pp.goods_supply.supplier_id, COUNT(DISTINCT dds_pp.goods.goods_id) AS total_goods_per_supplier
	        FROM dds_pp.goods_supply JOIN dds_pp.hub_goods
	        ON dds_pp.goods_supply.goods_id = dds_pp.hub_goods.src_key::bigint join dds_pp.goods 
	        on dds_pp.hub_goods.goods_id = dds_pp.goods.goods_id 
	        where goods_supply.price_per_measure > 0
	        GROUP BY dds_pp.goods_supply.supplier_id
	        
	    ),
	    goods_suppliers_count AS (
	        SELECT dds_pp.goods_supply.goods_id, COUNT(DISTINCT dds_pp.goods_supply.supplier_id) AS total_suppliers_per_goods
	        FROM dds_pp.goods_supply 
	        where goods_supply.price_per_measure > 0
	        GROUP BY dds_pp.goods_supply.goods_id
	    )
	SELECT DISTINCT 
	    dds_pp.supplier.supplier_id,
	    dds_pp.supplier.supplier_name,
	    dds_pp.contact.contact_info AS supplier_contact_info,
	    dds_pp.goods.goods_name,
	    dds_pp.goods_type.goods_type_name,
	    goods_supply_data.price_per_measure,
	    goods_supply_data.quantity,
	    dds_pp.measure.name_measure,
	    goods_supply_data.total_quantity,
	    supplier_goods_count.total_goods_per_supplier,
	    goods_suppliers_count.total_suppliers_per_goods,
	    CURRVAL('dm_pp.report_goods_supply_info_load_id') AS load_id,
	    NOW() AS start_ts,
	    '2999-12-31'::TIMESTAMP AS end_ts,
	    MD5(CONCAT(supplier.supplier_id::text, '#', goods.goods_id::text, '#'))::UUID AS hash
	FROM goods_supply_data JOIN dds_pp.hub_supplier  
	ON goods_supply_data.supplier_id = dds_pp.hub_supplier.src_key::bigint JOIN dds_pp.supplier
	on dds_pp.hub_supplier.supplier_id = dds_pp.supplier.supplier_id join dds_pp.hub_goods  
	ON goods_supply_data.goods_id = dds_pp.hub_goods.src_key::bigint join dds_pp.goods 
	on dds_pp.hub_goods.goods_id = dds_pp.goods.goods_id JOIN dds_pp.contact  
	ON dds_pp.supplier.contact_id = dds_pp.contact.contact_id JOIN dds_pp.goods_type 
	ON dds_pp.goods.goods_type_id = dds_pp.goods_type.goods_type_id JOIN dds_pp.measure  
	ON dds_pp.goods.goods_measure_id = dds_pp.measure.measure_id JOIN supplier_goods_count 
	ON goods_supply_data.supplier_id = supplier_goods_count.supplier_id JOIN goods_suppliers_count 
	ON goods_supply_data.goods_id = goods_suppliers_count.goods_id 
  where goods_supply_data.price_per_measure > 0
	;
    RETURN 0;
END;
$function$;

DROP PROCEDURE IF EXISTS stg_pp.supply_details_load();
CREATE PROCEDURE stg_pp.supply_details_load()
AS $$
BEGIN
  DELETE FROM stg_pp.supply_details;

  INSERT INTO stg_pp.supply_details ("ID",quantity, ID_supply, ID_sup_goods, update_dttm)
  SELECT "ID", quantity, ID_supply, ID_sup_goods, update_dttm
  FROM source_pp.supply_details; 
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS stg_pp.supply_load();
CREATE PROCEDURE stg_pp.supply_load()
AS $$
BEGIN
  DELETE FROM stg_pp.supply;

  INSERT INTO stg_pp.supply ("ID",ID_sup_goods, Date_sup_start, Date_sup_stop, supply_status, update_dttm)
  SELECT "ID", ID_sup_goods, Date_sup_start, Date_sup_stop, supply_status, update_dttm
  FROM source_pp.supply; 
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS stg_pp.store_load();
CREATE PROCEDURE stg_pp.store_load()
AS $$
BEGIN
  DELETE FROM stg_pp.store;

  INSERT INTO stg_pp.store ("ID" ,
	store_name,
	address,
	update_dttm)
  SELECT "ID" ,
	store_name,
	address,
	update_dttm
  FROM source_pp.store; 
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS stg_pp.product_load();
CREATE PROCEDURE stg_pp.product_load()
AS $$
BEGIN
  DELETE FROM stg_pp.product;

  INSERT INTO stg_pp.product ("ID",
	ID_store,
	price,
	product_name,
	quantity,
	update_dttm)
  SELECT "ID",
	ID_store,
	price,
	product_name,
	quantity,
	update_dttm
  FROM source_pp.product; 
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS stg_pp.order_detail_load();
CREATE PROCEDURE stg_pp.order_detail_load()
AS $$
BEGIN
  DELETE FROM stg_pp.order_detail;

  INSERT INTO stg_pp.order_detail ("ID", id_order, id_product, quantity, update_dttm)
  SELECT "ID", id_order, id_product, quantity, update_dttm
  FROM source_pp.order_detail; 
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS stg_pp.orders_load();
CREATE PROCEDURE stg_pp.orders_load()
AS $$
BEGIN
  DELETE FROM stg_pp.orders;

  INSERT INTO stg_pp.orders("ID", id_client, order_dttm, update_dttm)
  SELECT "ID", id_client, order_dttm, update_dttm
  FROM source_pp.orders; 
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS stg_pp.client_load();
CREATE PROCEDURE stg_pp.client_load()
AS $$
BEGIN
  DELETE FROM stg_pp.client;

  INSERT INTO stg_pp.client("ID", "name", name_short, address, update_dttm)
  SELECT "ID", "name", name_short, address, update_dttm
  FROM source_pp.client; 
END;
$$ LANGUAGE plpgsql;

drop procedure if exists stg_pp.goods_load();

create procedure stg_pp.goods_load()
as $$
declare 
		last_update_dt timestamp;
	begin
		last_update_dt = coalesce (
	(select max(update_dt)
		from tech_pp.last_update
	where table_name = 'stg_pp.goods'
), '1900-01-01'::date
);
	delete from stg_pp.goods;
	insert into stg_pp.goods
				(	
					"ID",
					food_name,
					food_type,
					"Specification",
					update_dttm,
					deleted_flg
				)
	select 
					"ID",
					food_name,
					food_type,
					"Specification",
					update_dttm,
					'0'
	from source_pp.goods
	where update_dttm>last_update_dt; 

	insert 	into stg_pp.goods
				(	"ID",
					food_name,
					food_type,
					"Specification",
					update_dttm,
					deleted_flg
				)
	select 
					o."ID",
					o.food_name,
					o.food_type,
					o."Specification",
					TIMEZONE('utc', NOW()),
					'1'
	from ods_pp.goods o
	left join source_pp.goods s on o."ID" = s."ID"
	where s."ID" is null;

	insert 	into tech_pp.last_update
			(table_name,
			update_dt)
		values
			('stg_pp.goods',
			TIMEZONE('utc', NOW()));
end;
$$ language plpgsql;

drop procedure if exists stg_pp.supplier_load();

create procedure stg_pp.supplier_load()
as $$
declare 
		last_update_dt timestamp;
	begin
		last_update_dt = coalesce (
	(select max(update_dt)
		from tech_pp.last_update
	where table_name = 'stg_pp.supplier'
), '1900-01-01'::date
);
	delete from stg_pp.supplier;
	insert into stg_pp.supplier
				(	
					id, "name", region, email, website, phone, address, update_dttm, deleted_flg
				)
	select 
					id, "name", region, email, website, phone, address, update_dttm, '0'
	from source_pp.supplier
	where update_dttm>last_update_dt; 

	insert 	into stg_pp.supplier
				(	id, "name", region, email, website, phone, address, update_dttm, deleted_flg
				)
	select 
					o.id,
					o."name",
					o.region,
					o.email,
					o.website,
					o.phone,
					o.address,
					TIMEZONE('utc', NOW()),
					'1'
	from ods_pp.supplier o
	left join source_pp.supplier s on o.id = s.id
	where s.id is null;

	insert 	into tech_pp.last_update
			(table_name,
			update_dt)
		values
			('stg_pp.supplier',
			TIMEZONE('utc', NOW()));
end;
$$ language plpgsql;

drop procedure if exists stg_pp.goods_supply_load();

create procedure stg_pp.goods_supply_load()
as $$
declare 
		last_update_dt timestamp;
	begin
		last_update_dt = coalesce (
	(select max(update_dt)
		from tech_pp.last_update
	where table_name = 'stg_pp.goods_supply'
), '1900-01-01'::date
);
	delete from stg_pp.goods_supply;
	insert into stg_pp.goods_supply
				(	
					"ID", item_sup_id, supplier_id, avaibl_count, price_per_measure,  update_dttm, deleted_flg
				)
	select 
					"ID", item_sup_id, supplier_id, avaibl_count, price_per_measure, update_dttm, '0'
	from source_pp.goods_supply
	where update_dttm>last_update_dt; 

	insert 	into stg_pp.goods_supply
				(	"ID", item_sup_id, supplier_id, avaibl_count, price_per_measure, update_dttm, deleted_flg
				)
	select 
					o."ID",
					o.item_sup_id,
					o.supplier_id,
					o.avaibl_count,
     o.price_per_measure, 
					TIMEZONE('utc', NOW()),
					'1'
	from ods_pp.goods_supply o
	left join source_pp.goods_supply s on o."ID" = s."ID"
	where s."ID" is null;

	insert 	into tech_pp.last_update
			(table_name,
			update_dt)
		values
			('stg_pp.goods_supply',
			TIMEZONE('utc', NOW()));
end;
$$ language plpgsql;

DROP SEQUENCE IF EXISTS dds_pp.hub_supplier_load_id;

CREATE SEQUENCE dds_pp.hub_supplier_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_hub_supplier ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_hub_supplier ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds_pp.hub_supplier_load_id');
    
    delete from dds_pp.hub_supplier
    where dds_pp.hub_supplier.src_key::bigint in (SELECT dds_pp.hub_supplier.src_key::bigint
    FROM
        ods_pp.supplier left join dds_pp.hub_supplier
    on ods_pp.supplier.id = dds_pp.hub_supplier.src_key::bigint
   	where ods_pp.supplier.upload_stg_ts  >  dds_pp.hub_supplier.start_ts);
   	
    INSERT INTO dds_pp.hub_supplier (src_key , src_table , src_system_cd , load_id)
    SELECT
        ods_pp.supplier.id AS src_key,
        'supplier' AS src_table,
        '1' AS src_system_cd,
        CURRVAL('dds_pp.hub_supplier_load_id') AS load_id
    FROM
        ods_pp.supplier left join dds_pp.hub_supplier
    on ods_pp.supplier.id = dds_pp.hub_supplier.src_key::bigint
    where dds_pp.hub_supplier.src_key ::bigint is null;
   
   
   
    delete from dds_pp.hub_supplier where dds_pp.hub_supplier.src_key::bigint not in (select ods_pp.supplier.id  from ods_pp.supplier );
    RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.hub_contact_load_id;

CREATE SEQUENCE dds_pp.hub_contact_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_hub_contact ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_hub_contact ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds_pp.hub_contact_load_id');
       
       
    delete  from dds_pp.hub_contact where dds_pp.hub_contact.src_key not in 
   (select CONCAT(
        COALESCE(address, ''), ', ',
        COALESCE(phone, ''), ', ',
        COALESCE(email, ''), ', ',
        COALESCE(website, ''), ', ',
        COALESCE(region, '')
    ) AS src_key from ods_pp.supplier );  
    INSERT INTO dds_pp.hub_contact (src_key , src_table , src_system_cd , load_id)
    select 
        CONCAT(
        COALESCE(address, ''), ', ',
        COALESCE(phone, ''), ', ',
        COALESCE(email, ''), ', ',
        COALESCE(website, ''), ', ',
        COALESCE(region, '')
    ) AS src_key,
        'supplier' AS src_table,
        '1' AS src_system_cd,
        CURRVAL('dds_pp.hub_contact_load_id') AS load_id
    FROM
         ods_pp.supplier left join dds_pp.hub_contact 
    on  CONCAT(
        COALESCE(address, ''), ', ',
        COALESCE(phone, ''), ', ',
        COALESCE(email, ''), ', ',
        COALESCE(website, ''), ', ',
        COALESCE(region, '')) = dds_pp.hub_contact.src_key
    where dds_pp.hub_contact.src_key is null;
    RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.hub_goods_load_id;

CREATE SEQUENCE dds_pp.hub_goods_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_hub_goods ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_hub_goods ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds_pp.hub_goods_load_id');
    
    delete from dds_pp.hub_goods
    where dds_pp.hub_goods.src_key::bigint in (SELECT dds_pp.hub_goods.src_key::bigint
    FROM
        ods_pp.goods left join dds_pp.hub_goods
    on ods_pp.goods."ID" = dds_pp.hub_goods.src_key::bigint
   	where ods_pp.goods.upload_stg_ts  >  dds_pp.hub_goods.start_ts);
   	
    INSERT INTO dds_pp.hub_goods (src_key , src_table , src_system_cd , load_id)
    SELECT
        ods_pp.goods."ID" AS src_key,
        'goods' AS src_table,
        '1' AS src_system_cd,
        CURRVAL('dds_pp.hub_goods_load_id') AS load_id
    FROM
        ods_pp.goods left join dds_pp.hub_goods
    on ods_pp.goods."ID" = dds_pp.hub_goods.src_key::bigint
    where dds_pp.hub_goods.goods_id::bigint is null;
    delete from dds_pp.hub_goods where dds_pp.hub_goods.src_key::bigint not in (select ods_pp.goods."ID"  from ods_pp.goods );
    RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds_pp.hub_goods_type_load_id;

CREATE SEQUENCE dds_pp.hub_goods_type_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_hub_goods_type ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_hub_goods_type ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds_pp.hub_goods_type_load_id');
    
    delete from dds_pp.hub_goods_type
    where dds_pp.hub_goods_type.src_key in (SELECT dds_pp.hub_goods_type.src_key
    FROM
        ods_pp.goods left join dds_pp.hub_goods_type
    on ods_pp.goods.food_type = dds_pp.hub_goods_type.src_key
   	where ods_pp.goods.upload_stg_ts  >  dds_pp.hub_goods_type.start_ts);
   	
    INSERT INTO dds_pp.hub_goods_type (src_key , src_table , src_system_cd , load_id)
    select distinct 
        ods_pp.goods.food_type  AS src_key,
        'goods' AS src_table,
        '1' AS src_system_cd,
        CURRVAL('dds_pp.hub_goods_type_load_id') AS load_id
    FROM
        ods_pp.goods left join dds_pp.hub_goods_type
    on ods_pp.goods.food_type = dds_pp.hub_goods_type.src_key
    where dds_pp.hub_goods_type.src_key is null;
    delete from dds_pp.hub_goods_type where dds_pp.hub_goods_type.src_key not in (select ods_pp.goods.food_type  from ods_pp.goods );
    RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds_pp.hub_measure_load_id;

CREATE SEQUENCE dds_pp.hub_measure_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_hub_measure ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_hub_measure ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds_pp.hub_measure_load_id');
    
    delete from dds_pp.hub_measure
    where dds_pp.hub_measure.src_key in (SELECT dds_pp.hub_measure.src_key
    FROM
        ods_pp.goods left join dds_pp.hub_measure
    on ods_pp.goods."Specification" = dds_pp.hub_measure.src_key
   	where ods_pp.goods.upload_stg_ts  >  dds_pp.hub_measure.start_ts);
   	
    INSERT INTO dds_pp.hub_measure (src_key , src_table , src_system_cd , load_id)
    select distinct 
        ods_pp.goods."Specification"  AS src_key,
        'goods' AS src_table,
        '1' AS src_system_cd
        ,
        CURRVAL('dds_pp.hub_measure_load_id') AS load_id
    FROM
        ods_pp.goods left join dds_pp.hub_measure
    on ods_pp.goods."Specification" = dds_pp.hub_measure.src_key
    where dds_pp.hub_measure.src_key is null;
    delete from dds_pp.hub_measure where dds_pp.hub_measure.src_key not in (select ods_pp.goods."Specification"  from ods_pp.goods );
    RETURN 0;
END;
$function$;


DROP SEQUENCE IF EXISTS dds_pp.hub_goods_supply_load_id;

CREATE SEQUENCE dds_pp.hub_goods_supply_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_hub_goods_supply ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_hub_goods_supply ()
    RETURNS INTEGER
    LANGUAGE plpgsql
    AS $function$
BEGIN
    PERFORM
        NEXTVAL('dds_pp.hub_goods_supply_load_id');
    
    delete from dds_pp.hub_goods_supply
    where dds_pp.hub_goods_supply.src_key::bigint in (SELECT dds_pp.hub_goods_supply.src_key::bigint 
    FROM
        ods_pp.goods_supply  left join dds_pp.hub_goods_supply
    on ods_pp.goods_supply."ID" = dds_pp.hub_goods_supply.src_key::bigint 
   	where ods_pp.goods_supply.upload_stg_ts  >  dds_pp.hub_goods_supply.start_ts);
   	
    INSERT INTO dds_pp.hub_goods_supply (src_key , src_table , src_system_cd , load_id)
    select distinct 
        ods_pp.goods_supply."ID"  AS src_key,
        'goods_supply' AS src_table,
        '1' AS src_system_cd
        ,
        CURRVAL('dds_pp.hub_goods_supply_load_id') AS load_id
    FROM
        ods_pp.goods_supply left join dds_pp.hub_goods_supply
    on ods_pp.goods_supply."ID" = dds_pp.hub_goods_supply.src_key::bigint
    where dds_pp.hub_goods_supply.src_key::bigint is null;
    delete from dds_pp.hub_goods_supply where dds_pp.hub_goods_supply.src_key::bigint not in (select ods_pp.goods_supply."ID"  from ods_pp.goods_supply );
    RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.supplier_load_id;

CREATE SEQUENCE dds_pp.supplier_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_supplier ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_supplier()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds_pp.supplier_load_id');
 
  delete from dds_pp.supplier where supplier_id not in (select dds_pp.hub_supplier.supplier_id from dds_pp.hub_supplier);
  INSERT INTO dds_pp.supplier (supplier_id, supplier_name, contact_id, src_system_cd, load_id, hash)
  SELECT
     dds_pp.hub_supplier.supplier_id AS supplier_id,
     ods_pp.supplier."name" AS supplier_name,
     dds_pp.hub_contact.contact_id as contact_id,
     '1' AS src_system_cd,
     CURRVAL('dds_pp.supplier_load_id') AS load_id,
     md5(concat(dds_pp.hub_supplier.supplier_id::text,'#',dds_pp.hub_contact.contact_id::text))::uuid as hash
  FROM ods_pp.supplier 
  JOIN dds_pp.hub_supplier 
  ON ods_pp.supplier.id = dds_pp.hub_supplier.src_key::bigint join dds_pp.hub_contact 
  on CONCAT(
        COALESCE(ods_pp.supplier.address, ''), ', ',
        COALESCE(ods_pp.supplier.phone, ''), ', ',
        COALESCE(ods_pp.supplier.email, ''), ', ',
        COALESCE(ods_pp.supplier.website, ''), ', ',
        COALESCE(ods_pp.supplier.region, '')
    ) = hub_contact.src_key
  left join dds_pp.supplier
  on dds_pp.supplier.supplier_id  = dds_pp.hub_supplier.supplier_id 
  where dds_pp.supplier.supplier_id is null;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.contact_load_id;

CREATE SEQUENCE dds_pp.contact_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_contact ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_contact ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds_pp.contact_load_id');
 
  delete from dds_pp.contact where contact_id not in (select dds_pp.hub_contact.contact_id from dds_pp.hub_contact);
  INSERT INTO dds_pp.contact (contact_id, contact_type_id,contact_info, src_system_cd, load_id, hash)
  SELECT
     dds_pp.hub_contact.contact_id AS contact_id ,
     '1',
     dds_pp.hub_contact.src_key as contact_info,
     '1' AS src_system_cd,
     CURRVAL('dds_pp.contact_load_id') AS load_id,
     md5(concat(dds_pp.hub_contact.contact_id::text))::uuid as hash
  FROM dds_pp.hub_contact left join dds_pp.contact  
  on dds_pp.contact.contact_id = dds_pp.hub_contact.contact_id 
  where dds_pp.contact.contact_id is null;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.measure_load_id;

CREATE SEQUENCE dds_pp.measure_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_measure ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_measure ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds_pp.measure_load_id');

 
  delete from dds_pp.measure where measure_id not in (select dds_pp.hub_measure.measure_id from dds_pp.hub_measure);
 
  INSERT INTO dds_pp.measure (measure_id, name_measure, src_system_cd, load_id, hash)
  SELECT
     dds_pp.hub_measure.measure_id AS measure_id,
     dds_pp.hub_measure.src_key as name_measure,
     '1' AS src_system_cd,
     CURRVAL('dds_pp.measure_load_id') AS load_id,
     md5(concat(dds_pp.hub_measure.measure_id::text))::uuid as hash
  FROM dds_pp.hub_measure left join dds_pp.measure
  on dds_pp.measure.measure_id = dds_pp.hub_measure.measure_id 
  where dds_pp.measure.measure_id is null;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.goods_load_id;

CREATE SEQUENCE dds_pp.goods_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_goods ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_goods ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds_pp.goods_load_id');
 
  delete from dds_pp.goods where goods_id not in (select dds_pp.hub_goods.goods_id from dds_pp.hub_goods);
  INSERT INTO dds_pp.goods (goods_id, goods_type_id,goods_name, goods_measure_id, src_system_cd, load_id, hash)
  SELECT
     dds_pp.hub_goods.goods_id AS goods_id,
     dds_pp.hub_goods_type.goods_type_id as goods_type_id,
     ods_pp.goods.food_name as goods_name,
     dds_pp.hub_measure.measure_id as goods_measure_id,
     '1' AS src_system_cd,
     CURRVAL('dds_pp.goods_load_id') AS load_id,
     md5(concat(dds_pp.hub_goods.goods_id::text,'#', dds_pp.hub_goods_type.goods_type_id::text,'#',dds_pp.hub_measure.measure_id::text))::uuid as hash
  FROM ods_pp.goods join dds_pp.hub_goods 
  on ods_pp.goods."ID" = dds_pp.hub_goods.src_key::bigint join dds_pp.hub_goods_type
  on ods_pp.goods.food_type = dds_pp.hub_goods_type.src_key join dds_pp.hub_measure 
  on ods_pp.goods."Specification" = dds_pp.hub_measure.src_key left join dds_pp.goods
  on dds_pp.goods.goods_id = dds_pp.hub_goods.goods_id 
  where dds_pp.goods.goods_id is null;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.goods_type_load_id;

CREATE SEQUENCE dds_pp.goods_type_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_goods_type ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_goods_type ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds_pp.goods_type_load_id');

  delete from dds_pp.goods_type where goods_type_id not in (select dds_pp.hub_goods_type.goods_type_id from dds_pp.hub_goods_type);
  INSERT INTO dds_pp.goods_type (goods_type_id, goods_type_name, src_system_cd, load_id, hash)
  SELECT
     dds_pp.hub_goods_type.goods_type_id AS goods_id,
     dds_pp.hub_goods_type.src_key as goods_type_name,
     '1' AS src_system_cd,
     CURRVAL('dds_pp.goods_type_load_id') AS load_id,
     md5(concat(dds_pp.hub_goods_type.goods_type_id::text))::uuid as hash
  FROM dds_pp.hub_goods_type left join dds_pp.goods_type
  on dds_pp.hub_goods_type.goods_type_id = dds_pp.goods_type.goods_type_id 
  where dds_pp.goods_type.goods_type_id is null;
  RETURN 0;
END;
$function$;

DROP SEQUENCE IF EXISTS dds_pp.goods_supply_load_id;

CREATE SEQUENCE dds_pp.goods_supply_load_id;

DROP FUNCTION IF EXISTS dds_pp.insert_dds_goods_supply ();

CREATE OR REPLACE FUNCTION dds_pp.insert_dds_goods_supply ()
  RETURNS INTEGER
  LANGUAGE plpgsql
  AS $function$
BEGIN
  PERFORM NEXTVAL('dds_pp.goods_supply_load_id');

  delete from dds_pp.goods_supply where goods_supply_id not in (select dds_pp.hub_goods_supply.goods_supply_id from dds_pp.hub_goods_supply);
 
  INSERT INTO dds_pp.goods_supply (goods_supply_id, supplier_id, goods_id, price_per_measure,quantity, src_system_cd, load_id, hash)
  SELECT
     dds_pp.hub_goods_supply.goods_supply_id  AS goods_supply_id,
     dds_pp.hub_supplier.src_key::bigint AS supplier_id,
     dds_pp.hub_goods.src_key::bigint AS goods_id,
     ods_pp.goods_supply.price_per_measure as price_per_measure,
     ods_pp.goods_supply.avaibl_count as quantity,
     '1' AS src_system_cd,
     CURRVAL('dds_pp.goods_supply_load_id') AS load_id,
     md5(concat(dds_pp.hub_goods.goods_id::text,'#', dds_pp.hub_goods_supply.goods_supply_id::text,'#',dds_pp.hub_supplier.supplier_id::text))::uuid as hash
  FROM ods_pp.goods_supply join dds_pp.hub_goods_supply 
  on ods_pp.goods_supply."ID" = dds_pp.hub_goods_supply.src_key::bigint join dds_pp.hub_goods
  on ods_pp.goods_supply.item_sup_id = dds_pp.hub_goods.src_key::bigint join dds_pp.hub_supplier
  on ods_pp.goods_supply.supplier_id = dds_pp.hub_supplier.src_key::bigint left join dds_pp.goods_supply
  on dds_pp.hub_goods_supply.goods_supply_id = dds_pp.goods_supply.goods_supply_id 
  where  dds_pp.goods_supply.goods_supply_id is null;
  RETURN 0;
END;
$function$;
"""
        )
  
  create_schemas_task_project >> create_tables_and_funcs_task_project
  
  
init_dag = deploy_database()

