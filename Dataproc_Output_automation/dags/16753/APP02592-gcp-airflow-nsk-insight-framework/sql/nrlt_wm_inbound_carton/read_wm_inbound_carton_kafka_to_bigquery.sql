--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_wm_inbound_carton_from_kafka_to_teradata.sql
-- Author            : Dmytro Utte
-- Description       : Reading Data from  Source Kafka Topic Name=inventory-distribution-center-inbound-carton-v2-analytical-avro using SQL API CODE
-- Source topic      : inventory-distribution-center-inbound-carton-v2-analytical-avro
-- Object model      : DistributionCenterInboundCartonV2
-- ETL Run Frequency : Every hour
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-17  Utte Dmytro   FA-9913: Migrate NRLT DAG for WM_INBOUND_CARTON to IsF
--*************************************************************************************************************************************

create temporary view wm_inbound_carton_rn AS
select
	inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	warehouseEvents,
	initialReceivedItems,
	latestReceivedItems,
	itemQuantityAdjustmentDetails,
	vendorBillOfLading,
	carrierBillOfLading,
	auditDetails,
	allAuditsCompleted,
	allAuditsCompletedTime,
	lastUpdatedSystemTime,
	row_number() OVER (partition by inboundCartonId order by objectMetadata.lastUpdatedTime desc) rn
from kafka_wm_inbound_carton_avro;

create temporary view wm_inbound_carton_cnt AS
select
	count(*) as total_cnt
from kafka_wm_inbound_carton_avro;

create temporary view wm_inbound_carton_lifecycle as
select
	inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	warehouseEvents,
	initialReceivedItems,
	latestReceivedItems,
	itemQuantityAdjustmentDetails,
	vendorBillOfLading,
	carrierBillOfLading,
	auditDetails,
	allAuditsCompleted,
	allAuditsCompletedTime,
	lastUpdatedSystemTime
from wm_inbound_carton_rn
where rn = 1;


create temporary view inbound_carton_explode as
select
	inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	vendorBillOfLading,
	carrierBillOfLading,
	explode(initialReceivedItems) items
from wm_inbound_carton_lifecycle;

create temporary view inbound_carton_qty_explode as
select
	inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	vendorBillOfLading,
	carrierBillOfLading,
	items,
	explode(items.item.inventoryStateQuantities) qty_states
from inbound_carton_explode;

create temporary view inbound_carton_final as
select
	inboundCartonId as inbound_carton_id,
	receivedTime as received_time,
	cartonLpn as carton_lpn,
	purchaseOrderNumber as purchase_order_number,
	locationId as location_id,
	vendorBillOfLading.number as vendor_number,
	vendorBillOfLading.shipmentNumberType as vendor_number_type,
	carrierBillOfLading.number as carrier_number,
	carrierBillOfLading.shipmentNumberType as carrier_number_type,
	items.item.productSku.id as sku_id,
	items.item.productSku.idType as sku_type,
	array_agg(qty_states) as initial_inventory_states_array,
	sum(qty_states.quantity) as qty,
	to_json(items.reasonCodes) as reason_codes
from inbound_carton_qty_explode
group by
    inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	vendorBillOfLading.number,
	vendorBillOfLading.shipmentNumberType,
	carrierBillOfLading.number,
	carrierBillOfLading.shipmentNumberType,
	items.item.productSku.id,
	items.item.productSku.idType,
	to_json(items.reasonCodes);

create temporary view inbound_carton_latest_carton_explode as
select
	inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	vendorBillOfLading,
	carrierBillOfLading,
	explode_outer(latestReceivedItems) items
from wm_inbound_carton_lifecycle;

create temporary view inbound_carton_latest_qty_explode as
select
	inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	vendorBillOfLading,
	carrierBillOfLading,
	items,
	explode_outer(items.item.inventoryStateQuantities) qty_states
from inbound_carton_latest_carton_explode;

create temporary view inbound_carton_latest_final as
select
	inboundCartonId as inbound_carton_id,
	receivedTime as received_time,
	cartonLpn as carton_lpn,
	purchaseOrderNumber as purchase_order_number,
	locationId as location_id,
	vendorBillOfLading.number as vendor_number,
	vendorBillOfLading.shipmentNumberType as vendor_number_type,
	carrierBillOfLading.number as carrier_number,
	carrierBillOfLading.shipmentNumberType as carrier_number_type,
	items.item.productSku.id as sku_id,
	items.item.productSku.idType as sku_type,
	sum(qty_states.quantity) as qty,
	to_json(items.reasonCodes) as reason_codes
from inbound_carton_latest_qty_explode
group by inboundCartonId,
	receivedTime,
	cartonLpn,
	purchaseOrderNumber,
	locationId,
	vendorBillOfLading.number,
	vendorBillOfLading.shipmentNumberType,
	carrierBillOfLading.number,
	carrierBillOfLading.shipmentNumberType,
	items.item.productSku.id,
	items.item.productSku.idType,
	to_json(items.reasonCodes);

create temporary view inbound_carton_adjustments_explode as
select
	inboundCartonId,
	locationId,
	explode(itemQuantityAdjustmentDetails) items
from wm_inbound_carton_lifecycle;

create temporary view inbound_carton_adjustments_qty_explode as
select
	inboundCartonId,
	locationId,
	items,
	explode(items.inventoryAdjustmentQuantity.inventoryStateQuantities) qty_states
from inbound_carton_adjustments_explode;

create temporary view inbound_carton_warehouse_events_explode as
select
	inboundCartonId,
	allAuditsCompleted,
	allAuditsCompletedTime,
	lastUpdatedSystemTime,
	explode(warehouseEvents) warehouseEvent
from wm_inbound_carton_lifecycle;

create temporary view inbound_carton_audit_events_explode as
select
	inboundCartonId,
	allAuditsCompleted,
	allAuditsCompletedTime,
	lastUpdatedSystemTime,
	explode(auditDetails) auditDetails
from wm_inbound_carton_lifecycle;

---Writing Error Data to S3 in csv format
insert overwrite table wm_inbound_carton_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	inboundCartonId as inbound_carton_id,
	receivedTime as received_time,
	cartonLpn as carton_lpn,
	purchaseOrderNumber as purchase_order_number,
	locationId as location_id,
	vendorBillOfLading.number as vendor_number,
	vendorBillOfLading.shipmentNumberType as vendor_number_type,
	carrierBillOfLading.number as carrier_number,
	carrierBillOfLading.shipmentNumberType as carrier_number_type,
	items.item.productSku.id as sku_id,
	items.item.productSku.idType as sku_type,
	qty_states.inventoryState as inventory_state,
	qty_states.quality as sku_quality_code,
	qty_states.quantity as qty,
	to_json(items.reasonCodes) as reason_codes,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from inbound_carton_qty_explode
where inboundCartonId is null or items.item.productSku.id is null
      or inboundCartonId = '' or items.item.productSku.id = ''
	  or inboundCartonId = '""' or items.item.productSku.id = '""';

insert overwrite table wm_inbound_carton_details_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	inboundCartonId as inbound_carton_id,
	receivedTime as received_time,
	cartonLpn as carton_lpn,
	purchaseOrderNumber as purchase_order_number,
	locationId as location_id,
	vendorBillOfLading.number as vendor_number,
	vendorBillOfLading.shipmentNumberType as vendor_number_type,
	carrierBillOfLading.number as carrier_number,
	carrierBillOfLading.shipmentNumberType as carrier_number_type,
	items.item.productSku.id as sku_id,
	items.item.productSku.idType as sku_type,
	qty_states.inventoryState as inventory_state,
	qty_states.quantity as qty,
	to_json(items.reasonCodes) as reason_codes,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from inbound_carton_latest_qty_explode
where inboundCartonId is null
      or inboundCartonId = '' or items.item.productSku.id = ''
	  or inboundCartonId = '""' or items.item.productSku.id = '""';

insert overwrite table wm_inbound_carton_sku_adjustment_events_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	inboundCartonId as inbound_carton_id,
	locationId as location_id,
	items.triggeringAdjustmentEventType as adjustment_event_type,
	items.inventoryAdjustmentQuantity.productSku.id as sku_id,
	items.inventoryAdjustmentQuantity.productSku.idType as sku_type,
	qty_states.inventoryState as inventory_state,
	qty_states.quality as sku_quality_code,
	qty_states.quantity as qty,
	items.adjustmentDate as adjustment_date,
	items.adjustmentUser.id as user_id,
	items.adjustmentUser.idType as user_type,
	items.toCartonLpn as to_carton_lpn,
	items.toCartonPurchaseOrderNumber as to_carton_purchase_order_number,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from inbound_carton_adjustments_qty_explode
where inboundCartonId is null or items.inventoryAdjustmentQuantity.productSku.id is null
      or items.adjustmentDate is null or qty_states.inventoryState is null or items.triggeringAdjustmentEventType is null
	  or inboundCartonId = '' or items.inventoryAdjustmentQuantity.productSku.id = ''
	  or qty_states.inventoryState = '' or items.triggeringAdjustmentEventType = ''
	  or inboundCartonId = '""' or items.inventoryAdjustmentQuantity.productSku.id = '""'
	  or qty_states.inventoryState = '""' or items.triggeringAdjustmentEventType = '""'
	  or items.adjustmentDate rlike('[\\+]+');

insert overwrite table wm_inbound_carton_warehouse_events_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	inboundCartonId as inbound_carton_id,
	warehouseEvent.warehouseEventContext.originatingEventId as originating_event_id,
	warehouseEvent.warehouseEventContext.locationId as event_location_num,
	case when warehouseEvent.warehouseEventContext.user.type = 'EMPLOYEE' then warehouseEvent.warehouseEventContext.user.employee.id when warehouseEvent.warehouseEventContext.user.type = 'SYSTEM' then warehouseEvent.warehouseEventContext.user.systemId when warehouseEvent.warehouseEventContext.user.type = 'EXTERNAL' then warehouseEvent.warehouseEventContext.user.externalUser.id.value else warehouseEvent.warehouseEventContext.userId.id end as user_id,
	coalesce(warehouseEvent.warehouseEventContext.user.type, warehouseEvent.warehouseEventContext.userId.idType) as user_type,
	to_json(warehouseEvent.warehouseEventContext.logicalLocationSellingBrandMappings) as selling_brands_logical_locations_array,
	warehouseEvent.warehouseEventType as warehouse_event_type,
	cast(null as string) as audit_event_type,
	warehouseEvent.eventTime as event_tmstp,
    cast(null as timestamp) as event_start_tmstp,
	cast(null as string) as audit_result,
	case when allAuditsCompleted = false then 'N' else 'Y' end as all_audit_completed,
	allAuditsCompletedTime as audit_completed_tmstp,
	lastUpdatedSystemTime as last_updt_tmstp,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from inbound_carton_warehouse_events_explode
where inboundCartonId is null or warehouseEvent.warehouseEventContext.originatingEventId is null
    or inboundCartonId = '' or warehouseEvent.warehouseEventContext.originatingEventId = ''
    or inboundCartonId = '""' or warehouseEvent.warehouseEventContext.originatingEventId = '""'
	or warehouseEvent.eventTime rlike('[\\+]+');

insert overwrite table wm_inbound_carton_audit_events_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	inboundCartonId as inbound_carton_id,
	auditDetails.auditStartTime as event_start_tmstp,
	auditDetails.auditEndTime as event_tmstp,
	auditDetails.warehouseEventContext.originatingEventId as originating_event_id,
	auditDetails.warehouseEventContext.locationId as event_location_num,
	case when auditDetails.warehouseEventContext.user.type = 'EMPLOYEE' then auditDetails.warehouseEventContext.user.employee.id when auditDetails.warehouseEventContext.user.type = 'SYSTEM' then auditDetails.warehouseEventContext.user.systemId when auditDetails.warehouseEventContext.user.type = 'EXTERNAL' then auditDetails.warehouseEventContext.user.externalUser.id.value else auditDetails.warehouseEventContext.userId.id end as user_id,
	coalesce(auditDetails.warehouseEventContext.user.type, auditDetails.warehouseEventContext.userId.idType) as user_type,
	to_json(auditDetails.warehouseEventContext.logicalLocationSellingBrandMappings) as selling_brands_logical_locations_array,
	cast(null as string) as warehouse_event_type,
	auditDetails.auditType as audit_event_type,
	auditDetails.auditResult as audit_result,
	case when allAuditsCompleted = false then 'N' else 'Y' end as all_audit_completed,
	allAuditsCompletedTime as audit_completed_tmstp,
	lastUpdatedSystemTime as last_updt_tmstp,
	substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from inbound_carton_audit_events_explode
where auditDetails.auditEndTime is not null and
     (inboundCartonId is null or auditDetails.warehouseEventContext.originatingEventId is null
	  or inboundCartonId = '' or auditDetails.warehouseEventContext.originatingEventId = ''
	  or inboundCartonId = '""' or auditDetails.warehouseEventContext.originatingEventId = '""');

---Writing Kafka Data to Teradata staging tables
insert overwrite table wm_inbound_carton_ldg_table
select
	coalesce(a.inbound_carton_id, b.inbound_carton_id) as inbound_carton_id,
    cast(coalesce(a.received_time, b.received_time)as string) as received_time,
    coalesce(a.carton_lpn, b.carton_lpn) as carton_lpn,
    coalesce(a.purchase_order_number, b.purchase_order_number) as purchase_order_number,
    coalesce(a.location_id, b.location_id) as location_id,
    coalesce(a.vendor_number, b.vendor_number) as vendor_number,
    coalesce(a.vendor_number_type, b.vendor_number_type) as vendor_number_type,
    coalesce(a.carrier_number, b.carrier_number) as carrier_number,
    coalesce(a.carrier_number_type, b.carrier_number_type) as carrier_number_type,
    coalesce(b.sku_id, a.sku_id) as sku_id,
    coalesce(b.sku_type, a.sku_type) as sku_type,
    cast(a.qty as string)as initial_qty,
	cast(b.qty as string)as latest_qty,
    coalesce(b.reason_codes, a.reason_codes) as reason_codes,
	to_json(a.initial_inventory_states_array) as initial_inventory_states_array
from (select * from inbound_carton_final
      where inbound_carton_id <> '' and sku_id <> ''
	  and inbound_carton_id <> '""' and sku_id <> '""') a
full join (select * from inbound_carton_latest_final
           where inbound_carton_id <> '' and sku_id <> ''
	       and inbound_carton_id <> '""' and sku_id <> '""') b ON
    a.inbound_carton_id = b.inbound_carton_id
    AND a.sku_id = b.sku_id
;

insert overwrite table wm_inbound_carton_sku_adjustment_events_ldg_table
select
    inboundCartonId as inbound_carton_id,
	locationId as location_id,
	items.triggeringAdjustmentEventType as adjustment_event_type,
	items.inventoryAdjustmentQuantity.productSku.id as sku_id,
	items.inventoryAdjustmentQuantity.productSku.idType as sku_type,
	qty_states.inventoryState as inventory_state,
	cast(sum(qty_states.quantity)as string) as qty,
	cast(items.adjustmentDate as string) as adjustment_date,
	items.adjustmentUser.id as user_id,
	items.adjustmentUser.idType as user_type,
	items.toCartonLpn as to_carton_lpn,
	items.toCartonPurchaseOrderNumber as to_carton_purchase_order_number,
	qty_states.quality as sku_quality_code
	from inbound_carton_adjustments_qty_explode
where inboundCartonId is not null and items.inventoryAdjustmentQuantity.productSku.id is not null
      and items.adjustmentDate is not null and qty_states.inventoryState is not null and items.triggeringAdjustmentEventType is not null
	  and inboundCartonId <> '' and items.inventoryAdjustmentQuantity.productSku.id <> ''
	  and qty_states.inventoryState <> '' and items.triggeringAdjustmentEventType <> ''
	  and inboundCartonId <> '""' and items.inventoryAdjustmentQuantity.productSku.id <> '""'
	  and qty_states.inventoryState <> '""' and items.triggeringAdjustmentEventType <> '""'
	  and items.adjustmentDate not rlike('[\\+]+')
group by 
    inboundCartonId,
	locationId,
	items.triggeringAdjustmentEventType,
	items.inventoryAdjustmentQuantity.productSku.id,
	items.inventoryAdjustmentQuantity.productSku.idType,
	qty_states.inventoryState,
	items.adjustmentDate,
	items.adjustmentUser.id,
	items.adjustmentUser.idType,
	items.toCartonLpn,
	items.toCartonPurchaseOrderNumber,
	qty_states.quality;

insert overwrite table wm_inbound_carton_events_ldg_table
select
    'WAREHOUSE' as event_source,
	inboundCartonId as inbound_carton_id,
	warehouseEvent.warehouseEventType as warehouse_event_type,
	cast(null as string) as audit_event_type,
	cast(null as string) as event_start_tmstp,
	cast(warehouseEvent.eventTime as string) as event_tmstp,
	case when warehouseEvent.warehouseEventContext.user.type = 'EMPLOYEE' then warehouseEvent.warehouseEventContext.user.employee.id when warehouseEvent.warehouseEventContext.user.type = 'SYSTEM' then warehouseEvent.warehouseEventContext.user.systemId when warehouseEvent.warehouseEventContext.user.type = 'EXTERNAL' then warehouseEvent.warehouseEventContext.user.externalUser.id.value else warehouseEvent.warehouseEventContext.userId.id end as user_id,
	coalesce(warehouseEvent.warehouseEventContext.user.type, warehouseEvent.warehouseEventContext.userId.idType) as user_type,
	warehouseEvent.warehouseEventContext.originatingEventId as originating_event_id,
	warehouseEvent.warehouseEventContext.locationId as event_location_num,
	cast(null as string) as audit_result,
	case when allAuditsCompleted = false then 'N' else 'Y' end as all_audit_completed,
	cast(allAuditsCompletedTime as string) as audit_completed_tmstp,
	cast(lastUpdatedSystemTime as string) as last_updt_tmstp,
	to_json(warehouseEvent.warehouseEventContext.logicalLocationSellingBrandMappings) as selling_brands_logical_locations_array
    from inbound_carton_warehouse_events_explode
where warehouseEvent.warehouseEventType <> 'WAREHOUSE_PURCHASE_ORDER_CARTON_AUDITED'
      and inboundCartonId is not null and warehouseEvent.warehouseEventContext.originatingEventId is not null
	  and inboundCartonId <> '' and warehouseEvent.warehouseEventContext.originatingEventId <> ''
	  and inboundCartonId <> '""' and warehouseEvent.warehouseEventContext.originatingEventId <> '""'
	  and warehouseEvent.eventTime not rlike('[\\+]+')
union all
select
    'AUDIT' as event_source,
	inboundCartonId as inbound_carton_id,
	'WAREHOUSE_PURCHASE_ORDER_CARTON_AUDITED' as warehouse_event_type,
	auditDetails.auditType as audit_event_type,
	auditDetails.auditStartTime as event_start_tmstp,
	auditDetails.auditEndTime as event_tmstp,
	case when auditDetails.warehouseEventContext.user.type = 'EMPLOYEE' then auditDetails.warehouseEventContext.user.employee.id when auditDetails.warehouseEventContext.user.type = 'SYSTEM' then auditDetails.warehouseEventContext.user.systemId when auditDetails.warehouseEventContext.user.type = 'EXTERNAL' then auditDetails.warehouseEventContext.user.externalUser.id.value else auditDetails.warehouseEventContext.userId.id end as user_id,
	coalesce(auditDetails.warehouseEventContext.user.type, auditDetails.warehouseEventContext.userId.idType) as user_type,
	auditDetails.warehouseEventContext.originatingEventId as originating_event_id,
	auditDetails.warehouseEventContext.locationId as event_location_num,
	auditDetails.auditResult as audit_result,
	case when allAuditsCompleted = false then 'N' else 'Y' end as all_audit_completed,
	allAuditsCompletedTime as audit_completed_tmstp,
	lastUpdatedSystemTime as last_updt_tmstp,
    to_json(auditDetails.warehouseEventContext.logicalLocationSellingBrandMappings) as selling_brands_logical_locations_array
from inbound_carton_audit_events_explode
where auditDetails.auditEndTime is not null
      and inboundCartonId is not null and auditDetails.warehouseEventContext.originatingEventId is not null
	  and inboundCartonId <> '' and auditDetails.warehouseEventContext.originatingEventId <> ''
	  and inboundCartonId <> '""' and auditDetails.warehouseEventContext.originatingEventId <> '""';

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
          'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Loading to ldg' as step_nm,
	      'WM_INBOUND_CARTON_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      count(*) as metric_value,
		  current_timestamp() as metric_tmstp
      from
	    (select * from inbound_carton_final
         where inbound_carton_id is not null and sku_id is not null
	       and inbound_carton_id <> '' and sku_id <> ''
	       and inbound_carton_id <> '""' and sku_id <> '""') a
      full join
	    (select * from inbound_carton_latest_final
         where inbound_carton_id is not null
	       and inbound_carton_id <> '' and sku_id <> ''
	       and inbound_carton_id <> '""' and sku_id <> '""') b ON
               a.inbound_carton_id = b.inbound_carton_id
               AND a.sku_id = b.sku_id
	  union all
	  select
          'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Loading to ldg' as step_nm,
	      'WM_INBOUND_CARTON_SKU_ADJUSTMENT_EVENTS_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      count(*) as metric_value,
		  current_timestamp() as metric_tmstp
      from (select
	            inboundCartonId,
            	locationId,
            	items.triggeringAdjustmentEventType,
            	items.inventoryAdjustmentQuantity.productSku.id,
            	items.inventoryAdjustmentQuantity.productSku.idType,
            	qty_states.inventoryState,
            	items.adjustmentDate,
            	items.adjustmentUser.id,
            	items.adjustmentUser.idType,
            	items.toCartonLpn,
            	items.toCartonPurchaseOrderNumber,
            	qty_states.quality
            from inbound_carton_adjustments_qty_explode
            where inboundCartonId is not null and items.inventoryAdjustmentQuantity.productSku.id is not null
                  and items.adjustmentDate is not null and qty_states.inventoryState is not null and items.triggeringAdjustmentEventType is not null
            	  and inboundCartonId <> '' and items.inventoryAdjustmentQuantity.productSku.id <> ''
            	  and qty_states.inventoryState <> '' and items.triggeringAdjustmentEventType <> ''
            	  and inboundCartonId <> '""' and items.inventoryAdjustmentQuantity.productSku.id <> '""'
            	  and qty_states.inventoryState <> '""' and items.triggeringAdjustmentEventType <> '""'
            	  and items.adjustmentDate not rlike('[\\+]+')
            group by 1,2,3,4,5,6,7,8,9,10,11,12  
		) agg
	  union all
	  select
          'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Loading to ldg' as step_nm,
	      'WM_INBOUND_CARTON_EVENTS_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      count(*) as metric_value,
		  current_timestamp() as metric_tmstp
      from (
	    select inboundCartonId from inbound_carton_warehouse_events_explode
        where warehouseEvent.warehouseEventType <> 'WAREHOUSE_PURCHASE_ORDER_CARTON_AUDITED'
           and inboundCartonId is not null and warehouseEvent.warehouseEventContext.originatingEventId is not null
	       and inboundCartonId <> '' and warehouseEvent.warehouseEventContext.originatingEventId <> ''
	       and inboundCartonId <> '""' and warehouseEvent.warehouseEventContext.originatingEventId <> '""'
		   and warehouseEvent.eventTime not rlike('[\\+]+')
	    union all
	    select inboundCartonId from inbound_carton_audit_events_explode
        where auditDetails.auditEndTime is not null
           and inboundCartonId is not null and auditDetails.warehouseEventContext.originatingEventId is not null
	       and inboundCartonId <> '' and auditDetails.warehouseEventContext.originatingEventId <> ''
	       and inboundCartonId <> '""' and auditDetails.warehouseEventContext.originatingEventId <> '""'
		) events
	  union all
	  select
	      'nrlt_wm_inbound_carton_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Reading from kafka' as step_nm,
	      cast (null as string) as tbl_nm,
	      'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
	      total_cnt as metric_value,
	      current_timestamp() as metric_tmstp
	  from wm_inbound_carton_cnt) a
;
