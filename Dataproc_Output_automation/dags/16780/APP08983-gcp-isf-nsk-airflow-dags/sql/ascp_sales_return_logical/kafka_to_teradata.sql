--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_sales_return_logical_kafka_to_teradata.sql
-- Author            : Tetiana Ivchyk
-- Description       : Reading Data from  Source Kafka Topic Name=ascp-rms-salereturn-event-avro using SQL API CODE
-- Source topic      : ascp-rms-salereturn-event-avro
-- Object model      : InventoryEvent
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author     Description
--*************************************************************************************************************************************
-- 2023-09-22  Tetiana Ivchyk  FA-9655: sales_return_logical to ISF
--*************************************************************************************************************************************
-- 2024-02-20  Josh Roter  FA-11691: sales_return_logical to NSK
--*************************************************************************************************************************************


create temporary view sales_return_logical as
 select
	header,
	receipt,
	explode_outer(receipt.receiptDetails) as receipt_receiptDetails,
	inventoryAdjustment,
	shipment,
	transfer,
	rtv,
	stockLedgerCorrection
from kafka_sales_return_logical_avro;


create temporary view sales_return_cnt as
select
	count(*) as total_cnt
from kafka_sales_return_logical_avro;


create temporary view sales_returns_events_stg_explode1 as
 select
	header,
	explode_outer(inventoryadjustment.inventoryadjustmentdetails) as inventoryadjustment_inventoryadjustmentdetails,
	inventoryAdjustment
from sales_return_logical;


create temporary view sales_returns_events_stg_explode2 as
 select
	header,
	inventoryadjustment_inventoryadjustmentdetails,
	inventoryadjustment,
	explode_outer(inventoryadjustment_inventoryadjustmentdetails.product) as inventoryadjustment_inventoryadjustmentdetails_product
from sales_returns_events_stg_explode1;


create temporary view sales_returns_events_stg_explode3 as
select
	header.eventid as event_id,
	header.correlationid as correlation_id,
	header.eventtimestamp as event_timestamp,
	header.eventtype as event_type,
	header.requesttimestamp as request_timestamp,
	header.mode as event_mode,
	header.source as partner_source,
	header.fromlocation.facility.id as from_location_facility_id,
	header.fromlocation.facility.type as from_location_facility_type,
	header.fromlocation.logical.id as from_location_logical_id,
	header.fromlocation.logical.type as from_location_logical_type,
	header.tolocation.facility.id as to_location_facility_id,
	header.tolocation.facility.type as to_location_facility_type,
	header.tolocation.logical.id as to_location_logical_id,
	header.tolocation.logical.type as to_location_logical_type,
	inventoryadjustment.channel as channel,
	inventoryadjustment.operationno as tran_code,
	inventoryadjustment.operationtype as program_name,
	inventoryadjustment_inventoryadjustmentdetails.quantity as quantity,
	inventoryadjustment_inventoryadjustmentdetails.fromdisposition as from_disposition,
	inventoryadjustment_inventoryadjustmentdetails.todisposition as to_disposition,
	inventoryadjustment_inventoryadjustmentdetails.reasoncode as reason_code,
	inventoryadjustment_inventoryadjustmentdetails_product.productid as product_id,
	inventoryadjustment_inventoryadjustmentdetails_product.producttype as product_type
from sales_returns_events_stg_explode2;


---Writing Error Data to S3 in csv format
insert overwrite table sales_return_logical_err
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
	event_id,
	correlation_id,
	event_timestamp,
	event_type,
	request_timestamp,
	event_mode,
	partner_source,
	from_location_facility_id,
	from_location_facility_type,
	from_location_logical_id,
	from_location_logical_type,
	to_location_facility_id,
	to_location_facility_type,
	to_location_logical_id,
	to_location_logical_type,
	channel,
	tran_code,
	program_name,
	quantity,
	from_disposition,
	to_disposition,
	reason_code,
	product_id,
	product_type,
	'SALES_RETURN_LOGICAL' as error_table,
	3 as error_code,
	'Date/Mandatory Field Validation Failed in SALES_RETURN_LOGICAL Table' as error_desc,
	substr(current_date,1,4) as year,
	substr(current_date,6,2) as month,
	substr(current_date,9,2) as day,
	substr(current_timestamp,12,2) as hour
from sales_returns_events_stg_explode3
where from_location_facility_id is null
	or product_id is null or product_id = '' or product_id = '""';


---Writing Kafka Data to Teradata staging table
insert overwrite table sales_return_logical_ldg_table_csv
select
	event_id,
	correlation_id,
	cast(event_timestamp as string) as event_timestamp,
	event_type,
	cast(request_timestamp as string) as request_timestamp,
	event_mode,
	partner_source,
	from_location_facility_id,
	from_location_facility_type,
	from_location_logical_id,
	from_location_logical_type,
	to_location_facility_id,
	to_location_facility_type,
	to_location_logical_id,
	to_location_logical_type,
	channel,
	tran_code,
	program_name,
	quantity,
	from_disposition,
	to_disposition,
	reason_code,
	product_id,
	product_type
from sales_returns_events_stg_explode3
where from_location_facility_id is not null
	and product_id <> '' and product_id <> '""';


 ---Writing Spark job count metrics
insert into table processed_data_spark_log_table
select
    isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	metric_value,
	metric_tmstp
from (
	select
		'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
		'Loading to csv' as step_nm,
		'SALES_RETURN_LOGICAL_LDG' as tbl_nm,
		'PROCESSED_ROWS_CNT' as metric_nm,
		cast(count(*) as string) as metric_value,
		cast(current_timestamp() as string) as metric_tmstp
	from sales_returns_events_stg_explode3
	where from_location_facility_id is not null
		and product_id <> '' and product_id <> '""'
	union all
	select
		'ascp_sales_return_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
		'Reading from kafka' as step_nm,
		cast (null as string) as tbl_nm,
		'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
		cast(total_cnt as string) as metric_value,
		cast(current_timestamp() as string) as metric_tmstp
	from sales_return_cnt
	 ) a;
