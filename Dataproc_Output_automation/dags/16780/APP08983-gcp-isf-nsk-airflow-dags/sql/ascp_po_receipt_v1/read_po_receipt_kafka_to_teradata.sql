--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_po_receipt_from_kafka_to_teradata.sql
-- Author            : Maksym Pochepets
-- Description       : Reading Data from  Source Kafka Topic Name=ascp-rms-shipment-po-receipt-v1-object-model-avro using SQL API CODE
-- Source topic      : ascp-rms-shipment-po-receipt-v1-object-model-avro
-- Object model      : Shipment
-- ETL Run Frequency : Every day
-- Version :         : 0.1
-- Change Log: Date Author     Description
--*************************************************************************************************************************************
-- 2023-10-04 Maksym Pochepets [FA-9856] deploy shipemnt_ibs_logical_v1
-- 2023-11-14 Oleksandr Chaichenko FA-10593 add GG fields
-- 2023-12-12 Oleksandr Chaichenko FA-11012 migration under app08983 (git repo 15850)
-- 2023-12-21 Oleksandr Chaichenko FA-10993 add headers.SystemTime to ldg table
-- 2024-02-25 Stefanos Stoikos [FA-11685] Migration to NSK
-- 2024-03-26 Oleksandr Chaichenko [FA-10993] ISF PO_RECEIPT_V1 flow scripts optimization
--*************************************************************************************************************************************

create temporary view po_receipt_rn AS
	select
		shipmentId,
		purchaseOrderNumber,
		externalReferenceId,
		receivedAt,
		cartonId,
		vendorAdvanceShipNotice,
		operationDetails,
		items,
		toFacility,
		toLogical,
		headers.SystemTime,
		row_number() over (partition by shipmentId, cartonId ORDER BY headers.SystemTime desc) as rn
	from kafka_rms_shipment_po_receipt_avro;


create temporary view po_receipt_cnt as
select
	count(*) as total_cnt
from kafka_rms_shipment_po_receipt_avro;


create temporary view po_receipt as
	select
		shipmentId,
		purchaseOrderNumber,
		externalReferenceId,
		receivedAt,
		cartonId,
		vendorAdvanceShipNotice,
		operationDetails,
		items,
		toFacility,
		toLogical,
		from_unixtime((CAST(DECODE(SystemTime, 'UTF-8') AS LONG)) / 1000) as SystemTime
		from po_receipt_rn
	where rn = 1;


create temporary view ascp_rms_shipment_po_receipt_object as
	select
		shipmentId,
		purchaseOrderNumber,
		externalReferenceId,
		receivedAt,
		cartonId,
		vendorAdvanceShipNotice,
		operationDetails,
		explode_outer(items) exploded_items,
		toFacility,
		toLogical,
		SystemTime
	from po_receipt;


create temporary view explode_items_expanded as
	select
		shipmentId,
		purchaseOrderNumber,
		externalReferenceId,
		receivedAt,
		cartonId                                  cartonid,
		vendorAdvanceShipNotice  		          vendoradvanceshipnotice,
		operationDetails.operationNumber          operationdetails_operationnumber,
		operationDetails.operationType            operationdetails_operationtype,
		exploded_items.product.id                 items_product_id,
		exploded_items.product.type               items_product_type,
		exploded_items.product.upc                items_product_upc,
		exploded_items.seqNo                      items_seq_num,
		exploded_items.quantity                   items_quantity,
		exploded_items.csn                        items_csn,
		exploded_items.opSeqNo                    items_opSeqNo,
		exploded_items.position                   items_position,
		toFacility.id 							  tofacility_id,
		toFacility.type                           tofacility_type,
		toLogical.id                              tological_id,
		toLogical.type                            tological_type,
		SystemTime
	from ascp_rms_shipment_po_receipt_object;


---Writing Error Data to S3 in csv format
insert overwrite table po_receipt_v1_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	shipmentId,
	purchaseOrderNumber,
	externalReferenceId,
	receivedAt,
	cartonid,
	vendoradvanceshipnotice,
	operationdetails_operationnumber,
	operationdetails_operationtype,
	items_product_id,
	items_product_type,
	items_product_upc,
	items_seq_num,
	items_quantity,
	items_csn,
	items_opSeqNo,
	items_position,
	tofacility_id,
	tofacility_type,
	tological_id,
	tological_type,
	SystemTime,
	substr(current_date, 1, 4) as year,
	substr(current_date, 6, 2) as month,
	substr(current_date, 9, 2) as day,
	substr(current_timestamp, 12, 2) as hour
from explode_items_expanded
where
	shipmentId is null or shipmentId = '' or shipmentId = '""'
	or cartonid is null or cartonid = '' or cartonid = '""'
	or items_product_id is null or items_product_id = '' or items_product_id = '""'
;

---Writing Kafka Data to Teradata staging table
insert overwrite table po_receipt_v1_ldg_table_csv
select
	shipmentId,
	purchaseOrderNumber,
	externalReferenceId,
	receivedAt,
	cartonid,
	vendoradvanceshipnotice,
	operationdetails_operationnumber,
	operationdetails_operationtype,
	items_product_id,
	items_product_type,
	items_product_upc,
	items_seq_num,
	items_quantity,
	items_csn,
	items_opSeqNo,
	items_position,
	tofacility_id,
	tofacility_type,
	tological_id,
	tological_type,
	SystemTime
from explode_items_expanded
where
	shipmentId <> '' and shipmentId <> '""'
	and cartonid <> '' and cartonid <> '""'
	and items_product_id <> '' and items_product_id <> '""'
;

---Writing Spark job count metrics
insert into table processed_data_spark_log_table
select
	isf_dag_nm,
	step_nm,
	cast(tbl_nm as string),
	cast(metric_nm as string),
	cast(metric_value as string),
	cast(metric_tmstp as string) 
from (
	select
		'{dag_name}' as isf_dag_nm,
		'Loading to csv' as step_nm,
		'PO_RECEIPT_V1_LDG' as tbl_nm,
		'PROCESSED_ROWS_CNT' as metric_nm,
		count(*) as metric_value,
		current_timestamp() as metric_tmstp
	from explode_items_expanded
    where
        shipmentId <> '' and shipmentId <> '""'
        and cartonid <> '' and cartonid <> '""'
        and items_product_id <> '' and items_product_id <> '""'
	union all
	select
		'{dag_name}' as isf_dag_nm,
		'Reading from kafka' as step_nm,
		cast (null as string) as tbl_nm,
		'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
		total_cnt as metric_value,
		current_timestamp() as metric_tmstp
	from po_receipt_cnt
	 ) a
;
