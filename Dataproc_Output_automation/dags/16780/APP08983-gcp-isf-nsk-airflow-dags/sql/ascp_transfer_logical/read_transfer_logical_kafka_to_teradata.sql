--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_transfer_logical_from_kafka_to_teradata.sql
-- Author            : Dmytro Utte
-- Description       : Reading Data from  Source Kafka Topic Name=ascp-rms-transfer-object-model-avro using SQL API CODE
-- Source topic      : ascp-rms-transfer-object-model-avro
-- Object model      : Transfer
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-07-31  Utte Dmytro   FA-9657: Migrate TECH_SC_NAP_Engg_Metamorph_transfer_logical to ISF
-- 2023-08-09  Utte Dmytro   added source into deduplcation key
-- 2024-02-26 Roter Josh     FA-11692: Migrate ascp_transfer_logical to NSK
--*************************************************************************************************************************************

create temporary view transfer_logical_rn AS
select
	operationDetails,
    source,
    transfercreated,
    shipments,
    receipts,
	row_number() over (partition by operationDetails.operationnumber, operationDetails.operationtype, source ORDER BY headers.SystemTime desc) as rn
from kafka_transfer_logical_avro;

create temporary view transfer_logical_cnt AS
select
    count(*) as total_cnt
from kafka_transfer_logical_avro;

create temporary view transfer_logical as
select
	operationDetails,
    source as src,
    transfercreated,
    shipments,
    receipts
from transfer_logical_rn
where rn = 1;


create temporary view transfer_created_stg as
select
	operationDetails,
    src,
    transfercreated,
    shipments,
    receipts,
    explode(transfercreated.items) transfercreated_items
from transfer_logical;

create temporary view shipments_stg_explode1 as
select
	operationDetails,
    src,
    explode_outer(shipments) shipments
from transfer_logical;

create temporary view shipments_stg_explode2 as
select
	operationDetails,
    src,
    shipments,
    explode_outer(shipments.cartons) shipments_cartons
from shipments_stg_explode1;

create temporary view shipments_stg_explode3 as
select
	operationDetails,
    src,
    shipments,
    shipments_cartons,
    explode(shipments_cartons.items) shipments_cartons_items
from shipments_stg_explode2;

create temporary view shipments_final as
select
	operationdetails.operationnumber operationdetails_operationnumber,
    operationdetails.operationtype operationdetails_operationtype,
    src,
    shipments.id shipments_id,
    shipments.eventTimestamp shipments_eventtimestamp,
    shipments.billoflading shipments_billoflading,
    shipments.shiptime shipments_shiptime,
    shipments.expectedarrivaltime shipments_expectedarrivaltime,
    shipments.asnnumber shipments_asnnumber,
    shipments.fromlocation.id shipments_fromlocation_id,
    shipments.fromlocation.type shipments_fromlocation_type,
    shipments.fromlogicallocation.id shipments_fromlogicallocation_id,
    shipments.fromlogicallocation.type shipments_fromlogicallocation_type,
    shipments.tolocation.id shipments_tolocation_id,
    shipments.tolocation.type shipments_tolocation_type,
    shipments.tologicallocation.id shipments_tologicallocation_id,
    shipments.tologicallocation.type shipments_tologicallocation_type,
    shipments_cartons.id shipments_cartons_cartonid,
    shipments_cartons_items.product.id items_product_id,
    shipments_cartons_items.product.type items_product_type,
    shipments_cartons_items.product.upc items_product_upc,
    shipments_cartons_items.quantity items_quantity,
    shipments_cartons_items.cancelledquantity items_cancelledquantity
from shipments_stg_explode3;

create temporary view receipts_stg_explode1 as
select
	operationDetails,
    src,
    explode_outer(receipts) receipts
from transfer_logical;

create temporary view receipts_stg_explode2 as
select
	operationDetails,
    src,
    receipts,
    explode_outer(receipts.cartons) receipts_cartons
from receipts_stg_explode1;

create temporary view receipts_stg_explode3 as
select
	operationDetails,
    src,
    receipts,
    receipts_cartons,
    explode(receipts_cartons.items) receipts_cartons_items
from receipts_stg_explode2;

create temporary view receipts_final as
select
	operationdetails.operationnumber operationdetails_operationnumber,
    operationdetails.operationtype operationdetails_operationtype,
    src,
    receipts.id receipts_id,
    receipts.eventTimestamp receipts_eventtimestamp,
    receipts.asnnumber receipts_asnnumber,
    receipts.receiptTime receipts_receipttime,
    receipts.fromlocation.id receipts_fromlocation_id,
    receipts.fromlocation.type receipts_fromlocation_type,
    receipts.fromlogicallocation.id receipts_fromlogicallocation_id,
    receipts.fromlogicallocation.type receipts_fromlogicallocation_type,
    receipts.tolocation.id receipts_tolocation_id,
    receipts.tolocation.type receipts_tolocation_type,
    receipts.tologicallocation.id receipts_tologicallocation_id,
    receipts.tologicallocation.type receipts_tologicallocation_type,
    receipts_cartons.id receipts_cartons_cartonid,
    receipts_cartons_items.product.id items_product_id,
    receipts_cartons_items.product.type items_product_type,
    receipts_cartons_items.product.upc items_product_upc,
    receipts_cartons_items.quantity items_quantity,
    receipts_cartons_items.cancelledquantity items_cancelledquantity
from receipts_stg_explode3;

---Writing Error Data to S3 in csv format
insert overwrite table transfer_created_logical_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	operationdetails.operationnumber operationdetails_operationnumber,
    operationdetails.operationtype operationdetails_operationtype,
    src,
    transfercreated.transferstatus transfercreated_transferstatus,
    transfercreated.createtimestamp transfercreated_createtimestamp,
    transfercreated.requestuserid transfercreated_requestuserid,
    transfercreated.transfertype transfercreated_transfertype,
    transfercreated.transfercontextvalue transfercreated_transfercontextvalue,
    transfercreated.transactionid transfercreated_transactionid,
    transfercreated.routingcode transfercreated_routingcode,
    transfercreated.freightcode transfercreated_freightcode,
    transfercreated.deliverydate transfercreated_deliverydate,
    transfercreated.department transfercreated_department,
    transfercreated.eventtimestamp transfercreated_eventtimestamp,
    transfercreated.fromlocation.id transfercreated_fromlocation_id,
    transfercreated.fromlocation.type transfercreated_fromlocation_type,
    transfercreated.fromlogicallocation.id transfercreated_fromlogicallocation_id,
    transfercreated.fromlogicallocation.type transfercreated_fromlogicallocation_type,
    transfercreated.tolocation.id transfercreated_tolocation_id,
    transfercreated.tolocation.type transfercreated_tolocation_type,
    transfercreated.tologicallocation.id transfercreated_tologicallocation_id,
    transfercreated.tologicallocation.type transfercreated_tologicallocation_type,
    transfercreated_items.product.id transfercreated_items_product_id,
    transfercreated_items.product.type transfercreated_items_product_type,
    transfercreated_items.product.upc transfercreated_items_product_upc,
    transfercreated_items.quantity transfercreated_items_quantity,
    transfercreated_items.cancelledquantity transfercreated_items_cancelledquantity,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from transfer_created_stg
where
	-- Primary key null check
    operationdetails.operationnumber is null
    or operationdetails.operationtype is null
    or transfercreated_items.product.id is null
    -- Primary key empty string check
    or operationdetails.operationnumber = ''
    or operationdetails.operationtype = ''
    or transfercreated_items.product.id = ''
    -- Primary key empty quotes check
    or operationdetails.operationnumber = '""'
    or operationdetails.operationtype = '""'
    or transfercreated_items.product.id = '""';

insert overwrite table transfer_shipment_logical_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	operationdetails_operationnumber,
    operationdetails_operationtype,
    src,
    shipments_id,
    shipments_eventtimestamp,
    shipments_billoflading,
    cast(shipments_shiptime as string),
    cast(shipments_expectedarrivaltime as string),
    shipments_asnnumber,
    shipments_fromlocation_id,
    shipments_fromlocation_type,
    shipments_fromlogicallocation_id,
    shipments_fromlogicallocation_type,
    shipments_tolocation_id,
    shipments_tolocation_type,
    shipments_tologicallocation_id,
    shipments_tologicallocation_type,
    shipments_cartons_cartonid,
    items_product_id,
    items_product_type,
    items_product_upc,
    items_quantity,
    items_cancelledquantity,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from shipments_final
where operationdetails_operationnumber is null
      or operationdetails_operationtype is null
      or shipments_id is null
      or shipments_cartons_cartonid is null
      or items_product_id is null
      -- Primary key empty string check
      or operationdetails_operationnumber = ''
      or operationdetails_operationtype = ''
      or shipments_id = ''
      or shipments_cartons_cartonid = ''
      or items_product_id = ''
      -- Primary key empty quotes check
      or operationdetails_operationnumber = '""'
      or operationdetails_operationtype = '""'
      or shipments_id  = '""'
      or shipments_cartons_cartonid = '""'
      or items_product_id  = '""';

insert overwrite table transfer_receipt_logical_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	operationdetails_operationnumber,
    operationdetails_operationtype,
    src,
    receipts_id,
    receipts_eventtimestamp,
    receipts_asnnumber,
    cast(receipts_receipttime as string),
    receipts_fromlocation_id,
    receipts_fromlocation_type,
    receipts_fromlogicallocation_id,
    receipts_fromlogicallocation_type,
    receipts_tolocation_id,
    receipts_tolocation_type,
    receipts_tologicallocation_id,
    receipts_tologicallocation_type,
    receipts_cartons_cartonid,
    items_product_id,
    items_product_type,
    items_product_upc,
    items_quantity,
    items_cancelledquantity,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from receipts_final
where operationdetails_operationnumber is null
      or operationdetails_operationtype is null
      or receipts_id is null
      or receipts_cartons_cartonid is null
      or items_product_id is null
      -- Primary key empty string check
      or operationdetails_operationnumber = ''
      or operationdetails_operationtype = ''
      or receipts_id = ''
      or receipts_cartons_cartonid = ''
      or items_product_id = ''
      -- Primary key empty quotes check
      or operationdetails_operationnumber = '""'
      or operationdetails_operationtype = '""'
      or receipts_id  = '""'
      or receipts_cartons_cartonid = '""'
      or items_product_id  = '""';

---Writing Kafka Data to Teradata staging tables
insert overwrite table transfer_shipment_receipt_logical_ldg_table_csv
select
   coalesce(tsll.operationdetails_operationnumber, trll.operationdetails_operationnumber) as operation_num,
   coalesce(tsll.operationdetails_operationtype, trll.operationdetails_operationtype) as operation_type,
   coalesce(tsll.shipments_id, trll.receipts_id) as shipment_id,
   coalesce(tsll.shipments_cartons_cartonid, trll.receipts_cartons_cartonid) as carton_id,
   coalesce(tsll.items_product_id, trll.items_product_id) as rms_sku_num,
   tsll.src as shipment_transfer_source,
   trll.src as receipt_transfer_source,
   cast(tsll.shipments_shiptime as string),
   tsll.shipments_billoflading,
   cast(tsll.shipments_expectedarrivaltime as string),
   tsll.shipments_asnnumber,
   tsll.shipments_fromlocation_id,
   tsll.shipments_fromlocation_type,
   tsll.shipments_fromlogicallocation_id,
   tsll.shipments_fromlogicallocation_type,
   tsll.shipments_tolocation_id,
   tsll.shipments_tolocation_type,
   tsll.shipments_tologicallocation_id,
   tsll.shipments_tologicallocation_type,
   tsll.items_product_upc as shipments_items_product_upc,
   trll.receipts_asnnumber,
   trll.receipts_fromlocation_id,
   trll.receipts_fromlocation_type,
   trll.receipts_fromlogicallocation_id,
   trll.receipts_fromlogicallocation_type,
   trll.receipts_tolocation_id,
   trll.receipts_tolocation_type,
   trll.receipts_tologicallocation_id,
   trll.receipts_tologicallocation_type,
   trll.items_product_upc as receipts_items_product_upc,
   cast(tsll.items_quantity as string) as shipments_items_quantity,
   cast(tsll.items_cancelledquantity as string) as shipments_items_cancelledquantity,
   cast(trll.receipts_receipttime as string),
   cast(trll.items_quantity as string) as receipts_items_quantity,
   cast(trll.items_cancelledquantity as string) as receipts_items_cancelledquantity
from (select * from shipments_final
      where operationdetails_operationnumber is not null
      and operationdetails_operationtype is not null
      and shipments_id is not null
      and shipments_cartons_cartonid is not null
      and items_product_id is not null
      and operationdetails_operationnumber <> ''
      and operationdetails_operationtype <> ''
      and shipments_id <> ''
      and shipments_cartons_cartonid <> ''
      and items_product_id <> ''
      and operationdetails_operationnumber <> '""'
      and operationdetails_operationtype <> '""'
      and shipments_id  <> '""'
      and shipments_cartons_cartonid <> '""'
      and items_product_id  <> '""') tsll
full join (select * from receipts_final
      where operationdetails_operationnumber is not null
      and operationdetails_operationtype is not null
      and receipts_id is not null
      and receipts_cartons_cartonid is not null
      and items_product_id is not null
      and operationdetails_operationnumber <> ''
      and operationdetails_operationtype <> ''
      and receipts_id <> ''
      and receipts_cartons_cartonid <> ''
      and items_product_id <> ''
      and operationdetails_operationnumber <> '""'
      and operationdetails_operationtype <> '""'
      and receipts_id  <> '""'
      and receipts_cartons_cartonid <> '""'
      and items_product_id  <> '""') trll ON (trll.operationdetails_operationnumber = tsll.operationdetails_operationnumber
  AND trll.operationdetails_operationtype = tsll.operationdetails_operationtype
  AND trll.receipts_id = tsll.shipments_id
  AND trll.receipts_cartons_cartonid = tsll.shipments_cartons_cartonid
  AND trll.items_product_id = tsll.items_product_id);

insert overwrite table transfer_created_logical_ldg_table_csv
select
	operationdetails.operationnumber operationdetails_operationnumber,
    operationdetails.operationtype operationdetails_operationtype,
    src,
    transfercreated.transferstatus transfercreated_transferstatus,
    cast(transfercreated.createtimestamp as string) transfercreated_createtimestamp,
    transfercreated.requestuserid transfercreated_requestuserid,
    transfercreated.transfertype transfercreated_transfertype,
    transfercreated.transfercontextvalue transfercreated_transfercontextvalue,
    transfercreated.transactionid transfercreated_transactionid,
    transfercreated.routingcode transfercreated_routingcode,
    transfercreated.freightcode transfercreated_freightcode,
    cast(transfercreated.deliverydate as string) transfercreated_deliverydate,
    transfercreated.department transfercreated_department,
    cast(transfercreated.eventtimestamp as string) transfercreated_eventtimestamp,
    transfercreated.fromlocation.id transfercreated_fromlocation_id,
    transfercreated.fromlocation.type transfercreated_fromlocation_type,
    transfercreated.fromlogicallocation.id transfercreated_fromlogicallocation_id,
    transfercreated.fromlogicallocation.type transfercreated_fromlogicallocation_type,
    transfercreated.tolocation.id transfercreated_tolocation_id,
    transfercreated.tolocation.type transfercreated_tolocation_type,
    transfercreated.tologicallocation.id transfercreated_tologicallocation_id,
    transfercreated.tologicallocation.type transfercreated_tologicallocation_type,
    transfercreated_items.product.id transfercreated_items_product_id,
    transfercreated_items.product.type transfercreated_items_product_type,
    transfercreated_items.product.upc transfercreated_items_product_upc,
    cast(transfercreated_items.quantity as int) transfercreated_items_quantity,
    cast(transfercreated_items.cancelledquantity as int) transfercreated_items_cancelledquantity
from transfer_created_stg
where
	-- Primary key null check
    operationdetails.operationnumber is not null
    and operationdetails.operationtype is not null
    and transfercreated_items.product.id is not null
    -- Primary key empty string check
    and operationdetails.operationnumber <> ''
    and operationdetails.operationtype <> ''
    and transfercreated_items.product.id <> ''
    ---- Primary key empty quotes check
    and operationdetails.operationnumber <> '""'
    and operationdetails.operationtype <> '""'
    and transfercreated_items.product.id <> '""'
    ;

--insert Spark metrics into log table
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
          'ascp_transfer_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Loading to csv' as step_nm,
	      'TRANSFER_CREATED_LOGICAL_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      cast(count(*) as string) as metric_value,
		  cast(current_timestamp() as string) as metric_tmstp
       from transfer_created_stg
       where
       	-- Primary key null check
           operationdetails.operationnumber is not null
           and operationdetails.operationtype is not null
           and transfercreated_items.product.id is not null
           -- Primary key empty string check
           and operationdetails.operationnumber <> ''
           and operationdetails.operationtype <> ''
           and transfercreated_items.product.id <> ''
           -- Primary key empty quotes check
           and operationdetails.operationnumber <> '""'
           and operationdetails.operationtype <> '""'
           and transfercreated_items.product.id <> '""'
	   union all
	   select
          'ascp_transfer_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Loading to csv' as step_nm,
	      'TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      cast(count(*) as string) as metric_value,
		  cast(current_timestamp() as string) as metric_tmstp
       from (select * from shipments_final
             where operationdetails_operationnumber is not null
             and operationdetails_operationtype is not null
             and shipments_id is not null
             and shipments_cartons_cartonid is not null
             and items_product_id is not null
             and operationdetails_operationnumber <> ''
             and operationdetails_operationtype <> ''
             and shipments_id <> ''
             and shipments_cartons_cartonid <> ''
             and items_product_id <> ''
             and operationdetails_operationnumber <> '""'
             and operationdetails_operationtype <> '""'
             and shipments_id  <> '""'
             and shipments_cartons_cartonid <> '""'
             and items_product_id  <> '""') tsll
       full join (select * from receipts_final
             where operationdetails_operationnumber is not null
             and operationdetails_operationtype is not null
             and receipts_id is not null
             and receipts_cartons_cartonid is not null
             and items_product_id is not null
             and operationdetails_operationnumber <> ''
             and operationdetails_operationtype <> ''
             and receipts_id <> ''
             and receipts_cartons_cartonid <> ''
             and items_product_id <> ''
             and operationdetails_operationnumber <> '""'
             and operationdetails_operationtype <> '""'
             and receipts_id  <> '""'
             and receipts_cartons_cartonid <> '""'
             and items_product_id  <> '""') trll
       ON (trll.operationdetails_operationnumber = tsll.operationdetails_operationnumber
       AND trll.operationdetails_operationtype = tsll.operationdetails_operationtype
       AND trll.receipts_id = tsll.shipments_id
       AND trll.receipts_cartons_cartonid = tsll.shipments_cartons_cartonid
       AND trll.items_product_id = tsll.items_product_id)
	   union all
	   select
	      'ascp_transfer_logical_16780_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Reading from kafka' as step_nm,
	      cast (null as string) as tbl_nm,
	      'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
	      cast(total_cnt as string) as metric_value,
	      cast(current_timestamp() as string) as metric_tmstp
	   from transfer_logical_cnt
	 ) a
;
