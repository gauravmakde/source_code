--Source
--Reading Data from  Source Kafka Topic Name=ascp-inventory-adjustment-logical-event-avro using SQL API CODE

--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_inventory_adjustment_logical_from_kafka_to_teradata.sql
-- Author            : Tetiana Ivchyk
-- Description       : Reading Data from  Source Kafka Topic Name=ascp-inventory-adjustment-logical-event-avro using SQL API CODE
-- Source topic      : ascp-inventory-adjustment-logical-event-avro
-- Object model      : InventoryEvent
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author     Description
--*************************************************************************************************************************************
-- 2023-09-02  Tetiana Ivchyk    FA-9651: TECH_SC_NAP_Engg_Metamorph_inventory_adjustment_logical to ISF
-- 2023-11-06  Andrew Ivchuk     FA-10496: App ID Migration- RMS 14 GG Inventory Adjustments - ISF Components.
-- 2024-02-19  Stefanos Stoikos  FA-11684: TECH_SC_NAP_Engg_Metamorph_inventory_adjustment_logical to NSK
--*************************************************************************************************************************************

create temporary view inventory_adjustment as
	select
		header,
		inventoryAdjustment
	from kafka_inventory_adjustment_logical_avro;



create temporary view inventory_adjustment_logical_cnt as
select
	count(*) as total_cnt
from kafka_inventory_adjustment_logical_avro;


create temporary view inventory_adjustment_rms_stg as
	select
		header,
		inventoryAdjustment,
   		explode_outer(inventoryAdjustment.inventoryadjustmentdetails) as inventoryadjustment_inventoryadjustmentdetails
	from inventory_adjustment;


create temporary view inventory_adjustment_rms_stg_explode1 as
	select
		header,
		inventoryAdjustment,
		inventoryadjustment_inventoryadjustmentdetails,
		explode_outer(inventoryadjustment_inventoryadjustmentdetails.product) inventoryadjustment_inventoryadjustmentdetails_product
	from inventory_adjustment_rms_stg;


create temporary view inventory_adjustment_rms_stg_explode2 as
	select
		header.eventid event_id,
		header.correlationid correlation_id,
		header.eventtimestamp event_timestamp,
		header.eventtype event_type,
		header.requesttimestamp request_timestamp,
		header.mode event_mode,
		header.source partner_source,
		header.fromlocation.facility.id from_location_facility_id,
		header.fromlocation.facility.type from_location_facility_type,
		header.fromlocation.logical.id from_location_logical_id,
		header.fromlocation.logical.type from_location_logical_type,
		header.tolocation.facility.id to_location_facility_id,
		header.tolocation.facility.type to_location_facility_type,
		header.tolocation.logical.id to_location_logical_id,
		header.tolocation.logical.type to_location_logical_type,
		inventoryadjustment.channel channel,
		inventoryadjustment.operationno tran_code,
		inventoryadjustment.operationtype program_name,
		inventoryadjustment_inventoryadjustmentdetails.quantity quantity,
		inventoryadjustment_inventoryadjustmentdetails.fromdisposition from_disposition,
		inventoryadjustment_inventoryadjustmentdetails.todisposition to_disposition,
		inventoryadjustment_inventoryadjustmentdetails.reasoncode reason_code,
		inventoryadjustment_inventoryadjustmentdetails_product.productid product_id,
		inventoryadjustment_inventoryadjustmentdetails_product.producttype product_type
	from inventory_adjustment_rms_stg_explode1;


---Writing Error Data to S3 in csv format
insert overwrite table inventory_adjustment_logical_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
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
	substr(current_date, 1, 4) as year,
	substr(current_date, 6, 2) as month,
	substr(current_date, 9, 2) as day,
	substr(current_timestamp, 12, 2) as hour
from inventory_adjustment_rms_stg_explode2
where
	event_id is null or event_id = '' or event_id = '""'
	or product_id is null or product_id = '' or product_id = '""'
	or to_location_facility_id is null
	;

	
---Writing Kafka Data to Teradata staging table
insert overwrite table inventory_adjustment_logical_ldg_table_csv
select
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
	product_type
from inventory_adjustment_rms_stg_explode2
where
	event_id <> '' and event_id <> '""'
	and product_id <> '' and product_id <> '""'
	and to_location_facility_id is not null
	;



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
		'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
		'Loading to csv' as step_nm,
		'INVENTORY_ADJUSTMENT_LOGICAL_LDG' as tbl_nm,
		'PROCESSED_ROWS_CNT' as metric_nm,
		count(*) as metric_value,
		current_timestamp() as metric_tmstp
	from inventory_adjustment_rms_stg_explode2
	where
		event_id <> '' and event_id <> '""'
		and product_id <> '' and product_id <> '""'
		and to_location_facility_id is not null
	union all
	select
		'ascp_inventory_adjustment_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
		'Reading from kafka' as step_nm,
		cast (null as string) as tbl_nm,
		'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
		total_cnt as metric_value,
		current_timestamp() as metric_tmstp
	from inventory_adjustment_logical_cnt
	 ) a
;

	