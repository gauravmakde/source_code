--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_rms_cost_transfers_from_kafka_to_teradata.sql
-- Author            : Tetiana Ivchyk
-- Description       : Reading Data from  Source Kafka Topic Name=inventory-merchandise-transfer-analytical-avro using SQL API CODE
-- Source topic      : inventory-merchandise-transfer-analytical-avro
-- Object model      : MerchandiseTransfer
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author     Description
--*************************************************************************************************************************************
-- 2023-08-16  Tetiana Ivchyk  FA-9659: Migrate TECH_SC_NAP_Engg_Metamorph_rms_cost_transfers to ISF
-- 2023-11-24  Alexis Ding     FA-10425: Migrate to new APP ID
-- 2024-03-04  Maksym Pochepets     FA-11689: Migrate to NSK Airflow
--*************************************************************************************************************************************

-- main data

create temporary view staging AS
	select
		transfernumber,
		externalreferencenumber,
		systemid,
		userid,
		fromlocation,
		tolocation,
		transfertype,
		transfercontexttype,
		routingcode,
		freightcode,
		deliverydate,
		comments,
		createdtime,
		latestupdatetime,
		latestcanceltime,
		headers.SystemTime,
		explode_outer(transferdetails) detail
	from kafka_inventory_merchandise_transfer_analytical_avro;


create temporary view rms_cost_transfers_cnt as
select
    count(*) as total_cnt
from kafka_inventory_merchandise_transfer_analytical_avro;


create temporary view ranked_data as
	select
		transfernumber,
		externalreferencenumber,
		systemid,
		userid,
		fromlocation,
		tolocation,
		transfertype,
		transfercontexttype,
		routingcode,
		freightcode,
		deliverydate,
		comments,
		createdtime,
		latestupdatetime,
		latestcanceltime,
		detail,
		row_number() OVER (partition by transfernumber, detail.productdetail.product.id order by SystemTime desc) row_num
	from staging;


create temporary view rms_cost_transfers as
	select
		transfernumber transfer_number,
		externalreferencenumber external_reference_number,
		systemid system_id,
		userid user_id,
		fromlocation.facility from_location_facility,
		fromlocation.logical from_location_logical,
		tolocation.facility to_location_facility,
		tolocation.logical to_location_logical,
		transfertype transfer_type,
		transfercontexttype transfer_context_type,
		routingcode routing_code,
		freightcode freight_code,
		deliverydate delivery_date,
		regexp_replace(comments, '[\\r\\n]', ' ') comments,
		createdtime created_time,
		latestupdatetime latest_update_time,
		latestcanceltime latest_cancel_time,
		detail.productdetail.product.id detail_product_id,
		detail.productdetail.product.idtype detail_product_id_type,
		detail.productdetail.quantity detail_quantity,
		detail.productdetail.disposition detail_disposition,
		detail.supplierpacksize detail_supplier_pack_size,
		detail.latestupdatetime detail_latest_update_time,
		detail.latestcanceledtime detail_latest_cancel_time
	from ranked_data
	where row_num = 1;


-- canceled_details

create temporary view staging_canceled_details as
	select
		transfernumber,
		headers.SystemTime,
		explode_outer(transfercanceleddetails) canceleddetail
	from kafka_inventory_merchandise_transfer_analytical_avro;


create temporary view ranked_data_canceled_details as
	select
		transfernumber,
		canceleddetail,
		row_number() OVER (partition by transfernumber, canceleddetail.productdetail.product.id order by SystemTime desc) row_num
	from staging_canceled_details;


create temporary view exploded_data_canceled_details as
	select
		transfernumber transfer_number,
		canceleddetail.productdetail.product.id canceled_detail_product_id,
		canceleddetail.productdetail.quantity canceled_detail_quantity,
		canceleddetail.latestcanceledtime canceled_detail_latest_cancel_time
	from ranked_data_canceled_details
	where row_num = 1;


create temporary view rms_cost_transfers_canceled_details as
	select
		transfer_number transfer_number,
		canceled_detail_product_id canceled_detail_product_id,
		canceled_detail_quantity canceled_detail_quantity,
		canceled_detail_latest_cancel_time canceled_detail_latest_cancel_time
	from exploded_data_canceled_details
	where canceled_detail_latest_cancel_time is not null
	;


--Writing Error Data to S3 in csv format -- main

insert overwrite table rms_cost_transfers_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
		transfer_number,
		external_reference_number,
		system_id,
		user_id,
		from_location_facility,
		from_location_logical,
		to_location_facility,
		to_location_logical,
		transfer_type,
		transfer_context_type,
		routing_code,
		freight_code,
		delivery_date,
		comments,
		created_time,
		latest_update_time,
		latest_cancel_time,
		detail_product_id,
		detail_product_id_type,
		detail_quantity,
		detail_disposition,
		detail_supplier_pack_size,
		detail_latest_update_time,
		detail_latest_cancel_time,
		substr(current_date, 1, 4) as year,
		substr(current_date, 6, 2) as month,
		substr(current_date, 9, 2) as day,
		substr(current_timestamp, 12, 2) as hour
from rms_cost_transfers
where
	transfer_number is null or transfer_number = '' or transfer_number = '""'
	or detail_product_id is null or detail_product_id = '' or detail_product_id = '""';


--Writing Error Data to S3 in csv format -- details

insert overwrite table rms_cost_transfers_canceled_details_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	transfer_number,
	canceled_detail_product_id,
	canceled_detail_quantity,
	canceled_detail_latest_cancel_time,
	substr(current_date, 1, 4) as year,
	substr(current_date, 6, 2) as month,
	substr(current_date, 9, 2) as day,
	substr(current_timestamp, 12, 2) as hour
from rms_cost_transfers_canceled_details
where
	transfer_number is null or transfer_number = '' or transfer_number = '""'
	or canceled_detail_product_id is null or canceled_detail_product_id = '' or canceled_detail_product_id = '""';



--Writing Kafka Data to Teradata staging table -- main

insert overwrite table rms_cost_transfers_ldg_table_csv
select
	transfer_number,
	external_reference_number,
	system_id,
	user_id,
	from_location_facility,
	from_location_logical,
	to_location_facility,
	to_location_logical,
	transfer_type,
	transfer_context_type,
	routing_code,
	freight_code,
	cast(delivery_date as string),
	comments,
	cast(created_time as string),
	cast(latest_update_time as string),
	cast(latest_cancel_time as string),
	detail_product_id,
	detail_product_id_type,
	cast(detail_quantity as string),
	detail_disposition,
	detail_supplier_pack_size,
	cast(detail_latest_update_time as string),
	cast(detail_latest_cancel_time as string)
from rms_cost_transfers
where
	transfer_number <> '' and transfer_number <> '""'
	and detail_product_id <> '' and detail_product_id <> '""';


--Writing Kafka Data to Teradata staging table - details

insert overwrite table rms_cost_transfers_canceled_details_ldg_table_csv
select
	transfer_number,
	canceled_detail_product_id,
	canceled_detail_quantity,
	cast(canceled_detail_latest_cancel_time as string)
from rms_cost_transfers_canceled_details
where
	transfer_number <> '' and transfer_number <> '""'
	and canceled_detail_product_id <> '' and canceled_detail_product_id <> '""';


--insert Spark metrics into log table
insert into table processed_data_spark_log_table
select
    isf_dag_nm,
	step_nm,
	tbl_nm,
	cast(metric_nm as string),
	cast(metric_value as string),
	cast(metric_tmstp as string)
from (
		select
			'ascp_rms_cost_transfers_16780_TECH_SC_NAP_insights' as isf_dag_nm,
			'Loading to csv' as step_nm,
			'RMS_COST_TRANSFERS_LDG' as tbl_nm,
			'PROCESSED_ROWS_CNT' as metric_nm,
			count(*) as metric_value,
			current_timestamp() as metric_tmstp
		from rms_cost_transfers
		where
			transfer_number <> '' and transfer_number <> '""'
			and detail_product_id <> '' and detail_product_id <> '""'
		union all
			select
			'ascp_rms_cost_transfers_16780_TECH_SC_NAP_insights' as isf_dag_nm,
			'Loading to csv' as step_nm,
			'RMS_COST_TRANSFERS_CANCELED_DETAILS_LDG' as tbl_nm,
			'PROCESSED_ROWS_CNT' as metric_nm,
			count(*) as metric_value,
			current_timestamp() as metric_tmstp
		from rms_cost_transfers_canceled_details
		where
			transfer_number <> '' and transfer_number <> '""'
			and canceled_detail_product_id <> '' and canceled_detail_product_id <> '""'
		union all
		select
			'ascp_rms_cost_transfers_16780_TECH_SC_NAP_insights' as isf_dag_nm,
			'Reading from kafka' as step_nm,
			cast (null as string) as tbl_nm,
			'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
			total_cnt as metric_value,
			current_timestamp() as metric_tmstp
		from rms_cost_transfers_cnt
		) a
;

