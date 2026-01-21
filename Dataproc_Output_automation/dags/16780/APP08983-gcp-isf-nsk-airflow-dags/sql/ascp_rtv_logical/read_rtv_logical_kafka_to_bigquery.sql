--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_rtv_logical_kafka_to_teradata.sql
-- Author            : Alexander Doroshevich
-- Description       : Reading Data from  Source Kafka Topic Name=ascp-rms-rtv-avro using SQL API CODE
-- Source topic      : ascp-rms-rtv-avro
-- Object model      : RTV
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-07-31  Alexander Doroshevich   FA-9653:  Migrate TECH_SC_NAP_Engg_Metamorph_rtv_logical to ISF
-- 2023-10-17  Tetiana Ivchyk          FA-10219: Add metrics to released rtv_logical
-- 2023-11-10 Josh Roter               FA-10503: Migrate DAG to new AppId 08983
-- 2024-02-26  Oleksandr Chaichenko  FA-11960: Migration to Airflow NSK
--*************************************************************************************************************************************


create temporary view rtv_rms_rn as
	select
		id,
		createTime,
		shipTime,
		cancelTime,
		updateTime,
		items,
		row_number() over (partition by id order by headers.SystemTime desc) as rn
	from rtv_logical_avro;


create temporary view rtv_logical_cnt as
select
    count(*) as total_cnt
from rtv_logical_avro;


create temporary view rtv_rms_stg as
	select
		id,
		createTime,
		shipTime,
		cancelTime,
		updateTime,
		explode_outer(items) as items_
	from rtv_rms_rn
	where rn = 1;


create temporary view rtv_rms_main_stg as
	select
		id as RETURNTOVENDORNUMBER,
		status as STATUS,
		initiatedBy as INITIATEDBY,
		createTime as CREATETIME,
		shipTime as SHIPTIME,
		cancelTime as CANCELTIME,
		updateTime as UPDATETIME,
		source as RTV_SOURCE,
		fromLocation.id as FROMLOCATION_ID,
		fromLocation.type as FROMLOCATION_TYPE,
		fromLogicalLocation.id as FROMLOGICAL_LOCATION_ID,
		fromLogicalLocation.type as FROMLOGICAL_LOCATION_TYPE,
		channel as CHANNEL,
		regexp_replace(returnToVendorComments,'[\\r\\n]', ' ') as COMMENTS,
		externalReferenceNumber as EXTERNALREFERENCENUMBER,
		vendor.vendorNumber as VENDORNUMBER,
		vendor.returnsAuthorizationNumber as RETURNSAUTHORIZATIONNUMBER,
		row_number() over (partition by id order by headers.SystemTime desc) as rn
	from rtv_logical_avro;


create temporary view rtv_rms_item_stg as
	select
		id as RETURNTOVENDORNUMBER,
		createTime as CREATETIME,
		shipTime as SHIPTIME,
		cancelTime as CANCELTIME,
		updateTime as UPDATETIME,
		items_.item.product.id as PRODUCT_ID,
		items_.item.product.type as PRODUCT_TYPE,
		items_.item.product.upc as UPC,
		items_.item.quantity as QUANTITY,
		items_.fromDisposition as FROMDISPOSITION,
		items_.toDisposition as TODISPOSITION,
		items_.reasonCode as REASONCODE,
		items_.unitCost  as UNITCOST
	from rtv_rms_stg;



---Writing Error Data to S3 in csv format -- main
insert overwrite table rtv_rms_main_stg_err
partition(year, month, day, hour)
	select /*+ COALESCE(1) */
		RETURNTOVENDORNUMBER,
		STATUS,
		INITIATEDBY,
		CAST(CREATETIME AS STRING),
		CAST(SHIPTIME AS STRING),
		CAST(CANCELTIME AS STRING),
		CAST(UPDATETIME AS STRING),
		RTV_SOURCE,
		FROMLOCATION_ID,
		FROMLOCATION_TYPE,
		FROMLOGICAL_LOCATION_ID,
		FROMLOGICAL_LOCATION_TYPE,
		CHANNEL,
		COMMENTS,
		EXTERNALREFERENCENUMBER,
		VENDORNUMBER,
		RETURNSAUTHORIZATIONNUMBER,
	 	substr(current_date, 1, 4) as year,
		substr(current_date, 6, 2) as month,
		substr(current_date, 9, 2) as day,
		substr(current_timestamp, 12, 2) as hour
	from rtv_rms_main_stg
	where RETURNTOVENDORNUMBER is null or RETURNTOVENDORNUMBER = '' or RETURNTOVENDORNUMBER = '""'
		or VENDORNUMBER is null or VENDORNUMBER = '' or VENDORNUMBER = '""'
		or FROMLOCATION_ID is null or FROMLOCATION_ID = '' or FROMLOCATION_ID = '""'
		or FROMLOCATION_TYPE is null or FROMLOCATION_TYPE = '' or FROMLOCATION_TYPE = '""';


---Writing Error Data to S3 in csv format -- item
insert overwrite table rtv_rms_item_stg_err
partition(year, month, day, hour)
	select /*+ COALESCE(1) */
		RETURNTOVENDORNUMBER,
		CAST(CREATETIME AS STRING),
		CAST(SHIPTIME AS STRING),
		CAST(CANCELTIME AS STRING),
		CAST(UPDATETIME AS STRING),
		PRODUCT_ID,
		PRODUCT_TYPE,
		UPC,
		CAST(QUANTITY AS STRING),
		FROMDISPOSITION,
		TODISPOSITION,
		REASONCODE,
		UNITCOST,
		substr(current_date, 1, 4) as year,
		substr(current_date, 6, 2) as month,
		substr(current_date, 9, 2) as day,
		substr(current_timestamp, 12, 2) as hour
	from rtv_rms_item_stg
	where RETURNTOVENDORNUMBER is null or RETURNTOVENDORNUMBER = '' or RETURNTOVENDORNUMBER = '""'
		or PRODUCT_ID is null or PRODUCT_ID = '' or PRODUCT_ID = '""';



---Writing Kafka Data to Teradata staging table -- main
insert overwrite table rtv_logical_main_csv
	select
		RETURNTOVENDORNUMBER,
		STATUS,
		INITIATEDBY,
		CAST(CREATETIME AS STRING),
		CAST(SHIPTIME AS STRING),
		CAST(CANCELTIME AS STRING),
		CAST(UPDATETIME AS STRING),
		RTV_SOURCE,
		FROMLOCATION_ID,
		FROMLOCATION_TYPE,
		FROMLOGICAL_LOCATION_ID,
		FROMLOGICAL_LOCATION_TYPE,
		CHANNEL,
		COMMENTS,
		EXTERNALREFERENCENUMBER,
		VENDORNUMBER,
		RETURNSAUTHORIZATIONNUMBER
	from rtv_rms_main_stg
	where RETURNTOVENDORNUMBER <> '' and RETURNTOVENDORNUMBER <> '""'
	and VENDORNUMBER <> '' and VENDORNUMBER <> '""'
	and rn = 1;


---Writing Kafka Data to Teradata staging table -- item
	insert overwrite table rtv_logical_item_csv
	select
		RETURNTOVENDORNUMBER,
		CAST(CREATETIME AS STRING),
		CAST(SHIPTIME AS STRING),
		CAST(CANCELTIME AS STRING),
		CAST(UPDATETIME AS STRING),
		PRODUCT_ID,
		PRODUCT_TYPE,
		UPC,
		CAST(QUANTITY AS STRING),
		FROMDISPOSITION,
		TODISPOSITION,
		REASONCODE,
		UNITCOST
	from rtv_rms_item_stg
	where RETURNTOVENDORNUMBER <> '' and RETURNTOVENDORNUMBER <> '""'
		and PRODUCT_ID <> '' and PRODUCT_ID <> '""';


--insert Spark metrics into log table
insert into table processed_data_spark_log_table
select
    isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	CAST(metric_value AS STRING),
	CAST(metric_tmstp AS STRING)
from (
		select
			'ascp_rtv_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
			'Loading to csv' as step_nm,
			'RTV_LOGICAL_LDG' as tbl_nm,
			'PROCESSED_ROWS_CNT' as metric_nm,
			count(*) as metric_value,
			current_timestamp() as metric_tmstp
		from rtv_rms_main_stg
		where
			RETURNTOVENDORNUMBER <> '' and RETURNTOVENDORNUMBER <> '""'
			and VENDORNUMBER <> '' and VENDORNUMBER <> '""'
			and rn = 1
		union all
			select
			'ascp_rtv_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
			'Loading to csv' as step_nm,
			'RTV_LOGICAL_ITEM_LDG' as tbl_nm,
			'PROCESSED_ROWS_CNT' as metric_nm,
			count(*) as metric_value,
			current_timestamp() as metric_tmstp
		from rtv_rms_item_stg
		where
			RETURNTOVENDORNUMBER <> '' and RETURNTOVENDORNUMBER <> '""'
			and PRODUCT_ID <> '' and PRODUCT_ID <> '""'
		union all
		select
			'ascp_rtv_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
			'Reading from kafka' as step_nm,
			cast (null as string) as tbl_nm,
			'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
			total_cnt as metric_value,
			current_timestamp() as metric_tmstp
		from rtv_logical_cnt
		) a
;

