--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_fc_product_in_storage_change_kafka_to_teradata.sql
-- Author            : Alexander Doroshevich
-- Description       : Reading Data from  Source Kafka Topic Name=inventory-fulfillment-center-product-in-storage-change-v2-analytical-avro using SQL API CODE
-- Source topic      : inventory-fulfillment-center-product-in-storage-change-v2-analytical-avro
-- Object model      : ProductInStorageChange
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-07-20  Alexander Doroshevich   FA-9650: Migrate TECH_SC_NAP_Engg_Metamorph_fc_product_in_storage_change to ISF
-- 2024-02-08  Valeriy Borysyuk        FA-11445: New PutAway attributes: entity ID
-- 2024-06-13  Roman Tymchik           FA-13108: remove headers.SystemTime
--*************************************************************************************************************************************

create temporary view fc_product_in_storage_change_rn AS
 select
    productSku,
    cartonLpn,
    toStorageId,
    warehouseEvent,
    adjustmentReasonCodes,
    inventoryStateQuantities,
	logicalItemInventoryStateQuantities,
	WarehouseInboundOrderId,
    row_number() over (partition by productSku, warehouseEvent.warehouseEventContext.originatingEventId order by objectMetadata.lastUpdatedTime desc) as rn
from kafka_fc_product_in_storage_change_avro;

create temporary view fc_product_in_storage_change_cnt AS
select
    count(*) as total_cnt
from kafka_fc_product_in_storage_change_avro;


create temporary view fc_product_in_storage_change AS
 select
    warehouseEvent.warehouseEventContext.originatingEventId as originating_event_id,
    productSku.id as sku_id,
    productSku.idType as sku_type,
    cartonLpn as carton_lpn,
    toStorageId.id as to_storage_id,
    toStorageId.idType as to_storage_type,
    warehouseEvent.warehouseEventContext.locationId as event_location_id,
	to_json(warehouseEvent.warehouseEventContext.logicalLocationSellingBrandMappings) as selling_brands_logical_locations_array,
    warehouseEvent.warehouseEventContext.userId.id as user_id,
    warehouseEvent.warehouseEventContext.userId.idType as user_id_type,
    warehouseEvent.warehouseEventType as event_type,
    warehouseEvent.eventTime as event_time,
    to_json(adjustmentReasonCodes) as reason_code,
    to_json(inventoryStateQuantities) as inventory_states_array,
	explode(inventoryStateQuantities) as inventory_states_array_,
	WarehouseInboundOrderId.id        as inbound_order_id,
	WarehouseInboundOrderId.type      as inbound_order_id_type_code
from fc_product_in_storage_change_rn
where rn = 1;

create temporary view fc_product_in_storage_change_logical AS
select
   originating_event_id,
   sku_id,
   carton_lpn,
   logical_inventory_states_array.logicallocationid as logical_location_id,
   inventory_states_array_.inventorystate as sku_inventory_state_code,
   inventory_states_array_.quality as sku_quality_code,
   sum(inventory_states_array_.quantity) as sku_qty
from
 (select
    warehouseEvent.warehouseEventContext.originatingEventId as originating_event_id,
    productSku.id as sku_id,
    cartonLpn as carton_lpn,
    explode(logicalItemInventoryStateQuantities) as logical_inventory_states_array,
    explode(logical_inventory_states_array.inventoryStateQuantities) as inventory_states_array_
  from fc_product_in_storage_change_rn
  where rn = 1
 ) a
group by
   originating_event_id,
   sku_id,
   carton_lpn,
   logical_inventory_states_array.logicallocationid,
   inventory_states_array_.inventorystate,
   inventory_states_array_.quality;


---Writing Error Data to S3 in csv format
insert overwrite table fc_product_in_storage_change_err
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
  originating_event_id,
  sku_id,
  sku_type,
  carton_lpn,
  to_storage_id,
  to_storage_type,
  event_location_id,
  selling_brands_logical_locations_array,
  user_id,
  user_id_type,
  event_type,
  event_time,
  reason_code,
  inventory_states_array,
  substr(current_date,1,4) as year,
  substr(current_date,6,2) as month,
  substr(current_date,9,2) as day,
  substr(current_timestamp,12,2) as hour
FROM fc_product_in_storage_change
where originating_event_id is null or originating_event_id = '' or originating_event_id = '""'
 or sku_id is null or sku_id = '' or sku_id = '""'
 or sku_type is null or sku_type = '' or sku_type = '""'
 or carton_lpn is null;

---Writing Kafka Data to Teradata staging table
insert overwrite table fc_product_in_storage_change_ldg_table_csv
SELECT
  a.originating_event_id,
  a.sku_id,
  a.carton_lpn,
  coalesce(b.sku_inventory_state_code, a.inventory_states_array_.inventoryState) as sku_inventory_state_code,
  coalesce(b.sku_quality_code, a.inventory_states_array_.quality) as sku_quality_code,
  cast(coalesce(b.sku_qty, a.inventory_states_array_.quantity) as string) as sku_qty,
  a.to_storage_id,
  a.to_storage_type,
  a.event_location_id,
  b.logical_location_id,
  a.event_type,
  cast(a.event_time as string) as event_time,
  a.selling_brands_logical_locations_array,
  a.inventory_states_array,
  a.reason_code,
  a.sku_type,
  a.user_id,
  a.user_id_type,
  a.inbound_order_id,
  a.inbound_order_id_type_code
FROM fc_product_in_storage_change a left join fc_product_in_storage_change_logical b
     on a.originating_event_id = b.originating_event_id
	 and a.sku_id = b.sku_id
	 and a.carton_lpn = b.carton_lpn
	 and a.inventory_states_array_.inventoryState = b.sku_inventory_state_code
where a.originating_event_id <> '' and a.originating_event_id <> '""'
 and a.sku_id <> '' and a.sku_id <> '""'
 and a.sku_type <> '' and a.sku_type <> '""'
 and a.carton_lpn is not null;

 ---Writing Spark job count metrics
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
        'ascp_fc_product_in_storage_change_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Loading to csv' as step_nm,
	      'FC_PRODUCT_IN_STORAGE_CHANGE_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      count(*) as metric_value,
		  current_timestamp() as metric_tmstp
       from fc_product_in_storage_change
	   where originating_event_id <> '' and originating_event_id <> '""'
 and sku_id <> '' and sku_id <> '""'
 and sku_type <> '' and sku_type <> '""'
 and carton_lpn is not null
	   union all
	   select
	      'ascp_fc_product_in_storage_change_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Reading from kafka' as step_nm,
	      cast (null as string) as tbl_nm,
	      'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
	      total_cnt as metric_value,
	      current_timestamp() as metric_tmstp
	   from fc_product_in_storage_change_cnt
	 ) a
;
