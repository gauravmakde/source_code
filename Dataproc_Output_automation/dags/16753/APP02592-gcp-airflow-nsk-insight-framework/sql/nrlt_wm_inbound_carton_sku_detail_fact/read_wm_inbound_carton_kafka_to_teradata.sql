--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_wm_inbound_carton_from_kafka_to_teradata.sql
-- Author            : Valeriy Borysyuk
-- Description       : Reading Data from  Source Kafka Topic Name=inventory-distribution-center-inbound-carton-v2-analytical-avro using SQL API CODE
-- Source topic      : inventory-distribution-center-inbound-carton-v2-analytical-avro
-- Object model      : DistributionCenterInboundCartonV2
-- ETL Run Frequency : Every hour
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2024-03-25  Valeriy Borysyuk  FA-11902: New topic schema - redesign nrlt_wm_inbound_carton
-- 2024-04-30  Valeriy Borysyuk  FA-12666: Fix: allow noll sku number
--*************************************************************************************************************************************

create temporary view wm_inbound_carton_rn AS
select
 	objectMetadata.lastUpdatedTime as objectMetadata_lastUpdatedTime,
 	cartonLpn,
 	purchaseOrderNumber,
 	cartonReceivedDetails,
 	cartonSplitDetails,
 	inventoryAdjustedDetails,
 	inventorySplitDetails,
 	cartonAuditedDetails,
 	allAuditsCompletedDetails,
 	row_number() OVER (partition by cartonLpn, purchaseOrderNumber order by objectMetadata.lastUpdatedTime desc) rn
   from kafka_wm_inbound_carton_avro;

CACHE TABLE kafka_cnt OPTIONS ('storageLevel' 'MEMORY_AND_DISK') AS
select
	count(*) as total_cnt
from kafka_wm_inbound_carton_avro;

--Cache delta deduped as it will be used in several queries
CACHE TABLE wm_inbound_carton_lifecycle OPTIONS ('storageLevel' 'MEMORY_AND_DISK') AS
select
	cartonLpn,
	purchaseOrderNumber,
	cartonReceivedDetails,
	cartonSplitDetails,
	inventoryAdjustedDetails,
	inventorySplitDetails,
	cartonAuditedDetails,
	allAuditsCompletedDetails,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_rn
where rn = 1;

------------------------------------------
-- Extract all events
------------------------------------------
-------WarehousePurchaseOrderCartonReceived
create temporary view wm_inbound_carton_received_sku_state_quality_event_fact AS
select
	cartonReceivedDetails_.eventTime                                as event_time,
	l.cartonLpn                                                     as inbound_carton_num,
	l.purchaseOrderNumber                                           as purchase_order_num,
	cartonReceivedDetails_.warehouseEventContext.locationId         as location_num,
	cartonReceivedDetails_.warehouseEventContext.originatingEventId as originating_event_id,
	cartonReceivedDetails_.warehouseEventContext.rescindedDetails.originalEventId                as rescinded_event_id,
	cartonReceivedDetails_.warehouseEventContext.rescindedDetails.warehouseEventRescindedEventId as corresponding_rescinded_event_id,
	cartonReceivedDetails_.cartonShipmentRelationType                                            as carton_shipment_relation_type_code,
	case cartonReceivedDetails_.warehouseEventContext.user.type
	     when 'EMPLOYEE' then cartonReceivedDetails_.warehouseEventContext.user.employee.id
	     when 'SYSTEM'   then cartonReceivedDetails_.warehouseEventContext.user.systemId
         when 'EXTERNAL' then cartonReceivedDetails_.warehouseEventContext.user.externalUser.id.value
         else cartonReceivedDetails_.warehouseEventContext.userId.id
     end                                                                                         as user_id,
	coalesce(cartonReceivedDetails_.warehouseEventContext.user.type, cartonReceivedDetails_.warehouseEventContext.userId.idType) as user_id_type_code,
	cartonReceivedDetails_.vendorBillOfLading.number              as vendor_bill_of_lading_num,
	cartonReceivedDetails_.vendorBillOfLading.shipmentNumberType  as vendor_bill_of_Lading_type_code,
	cartonReceivedDetails_.carrierBillOfLading.number             as carrier_bill_of_lading_num,
	cartonReceivedDetails_.carrierBillOfLading.shipmentNumberType as carrier_bill_of_lading_type_code,
	receivedItems_.item.productSku.id                             as sku_num,
	receivedItems_.item.productSku.idType                         as sku_type,
	inventoryStateQuantities_.quantity                            as sku_qty,
	inventoryStateQuantities_.quality                             as sku_quality_code,
	inventoryStateQuantities_.inventorystate                      as sku_inventory_state_code,
	case when size(receivedItems_.reasonCodes) = 0
	         then cast(null as string)
	     else to_json(receivedItems_.reasonCodes) end             as reason_codes_array,
	case when size(cartonReceivedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings) = 0
	           then cast(null as string)
	     else to_json(cartonReceivedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings)
	 end                                                          as selling_brands_logical_locations_array,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_lifecycle l
     lateral view explode(cartonReceivedDetails) d                              as cartonReceivedDetails_
     lateral view outer explode(cartonReceivedDetails_.receivedItems) i         as receivedItems_
     lateral view outer explode(receivedItems_.item.inventoryStateQuantities) s as inventoryStateQuantities_
where trim(l.cartonLpn)!='' and  trim(l.purchaseOrderNumber)!=''
      and cartonReceivedDetails_.eventTime not rlike('$[\\+]+') ;

------WarehousePurchaseOrderCartonSplit
create temporary view wm_inbound_carton_split_sku_event_fact AS
select
	cartonSplitDetails_.eventTime                                                             as event_time,
	l.cartonLpn                                                                               as inbound_carton_num,
	l.purchaseOrderNumber                                                                     as purchase_order_num,
	cartonSplitDetails_.warehouseEventContext.locationId                                      as location_num,
	cartonSplitDetails_.warehouseEventContext.originatingEventId                              as originating_event_id,
	cartonSplitDetails_.warehouseEventContext.rescindedDetails.originalEventId                as rescinded_event_id,
	cartonSplitDetails_.warehouseEventContext.rescindedDetails.warehouseEventRescindedEventId as corresponding_rescinded_event_id,
	case cartonSplitDetails_.warehouseEventContext.user.type
	     when 'EMPLOYEE' then cartonSplitDetails_.warehouseEventContext.user.employee.id
	     when 'SYSTEM'   then cartonSplitDetails_.warehouseEventContext.user.systemId
         when 'EXTERNAL' then cartonSplitDetails_.warehouseEventContext.user.externalUser.id.value
         else cartonSplitDetails_.warehouseEventContext.userId.id
     end                                                                                      as user_id,
	coalesce(cartonSplitDetails_.warehouseEventContext.user.type, cartonSplitDetails_.warehouseEventContext.userId.idType) as user_id_type_code,
	cartonSplitDetails_.splitItem.productSku.id                                                    as sku_num,
	cartonSplitDetails_.splitItem.productSku.idType                                                as sku_type,
	cartonSplitDetails_.splitItem.quantity * coalesce(cartonSplitDetails_.splitItem.multiplier,1)  as sku_qty,
	cartonSplitDetails_.fromCartonLpn                                                              as from_Carton_num,
	cartonSplitDetails_.toCartonLpn                                                                as to_Carton_num,
    case when size(cartonSplitDetails_.warehouseEventContext.logicalLocationSellingBrandMappings) = 0
	           then cast(null as string)
	     else to_json(cartonSplitDetails_.warehouseEventContext.logicalLocationSellingBrandMappings)
	 end                                                                                       as selling_brands_logical_locations_array,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_lifecycle l
     lateral view explode(cartonSplitDetails) d  as cartonSplitDetails_
where trim(l.cartonLpn)!='' and  trim(l.purchaseOrderNumber)!=''
      and cartonSplitDetails_.eventTime not rlike('$[\\+]+') ;

-------WarehousePurchaseOrderInventoryAdjusted
create temporary view wm_inbound_carton_adjusted_sku_state_quality_event_fact AS
select
	inventoryAdjustedDetails_.eventTime                                as event_time,
	l.cartonLpn                                                        as inbound_carton_num,
	l.purchaseOrderNumber                                              as purchase_order_num,
	inventoryAdjustedDetails_.warehouseEventContext.locationId         as location_num,
	inventoryAdjustedDetails_.warehouseEventContext.originatingEventId as originating_event_id,
	inventoryAdjustedDetails_.warehouseEventContext.rescindedDetails.originalEventId                as rescinded_event_id,
	inventoryAdjustedDetails_.warehouseEventContext.rescindedDetails.warehouseEventRescindedEventId as corresponding_rescinded_event_id,
	case inventoryAdjustedDetails_.warehouseEventContext.user.type
	     when 'EMPLOYEE' then inventoryAdjustedDetails_.warehouseEventContext.user.employee.id
	     when 'SYSTEM'   then inventoryAdjustedDetails_.warehouseEventContext.user.systemId
         when 'EXTERNAL' then inventoryAdjustedDetails_.warehouseEventContext.user.externalUser.id.value
         else inventoryAdjustedDetails_.warehouseEventContext.userId.id
     end                                                                                         as user_id,
	coalesce(inventoryAdjustedDetails_.warehouseEventContext.user.type, inventoryAdjustedDetails_.warehouseEventContext.userId.idType) as user_id_type_code,
	inventoryAdjustedDetails_.adjustedItem.item.productSku.id                                    as sku_num,
	inventoryAdjustedDetails_.adjustedItem.item.productSku.idType                                as sku_type,
	inventoryStateQuantities_.quantity                            as sku_qty,
	inventoryStateQuantities_.quality                             as sku_quality_code,
	inventoryStateQuantities_.inventorystate                      as sku_inventory_state_code,
	case when size(inventoryAdjustedDetails_.adjustedItem.reasonCodes) = 0
	          then cast(null as string)
	     else to_json(inventoryAdjustedDetails_.adjustedItem.reasonCodes)
	 end                                                          as reason_codes_array,
	case when size(inventoryAdjustedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings) = 0
	          then cast(null as string)
	     else to_json(inventoryAdjustedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings)
	 end                                                          as selling_brands_logical_locations_array,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_lifecycle l
     lateral view explode(inventoryAdjustedDetails) d                        as inventoryAdjustedDetails_
     lateral view outer explode(inventoryAdjustedDetails_.adjustedItem.item.inventoryStateQuantities)  s as inventoryStateQuantities_
where trim(l.cartonLpn)!='' and  trim(l.purchaseOrderNumber)!=''
      and inventoryAdjustedDetails_.eventTime not rlike('$[\\+]+') ;

-----WarehousePurchaseOrderInventorySplit
create temporary view wm_inbound_carton_inventory_split_cross_po_sku_event_fact AS
select
	inventorySplitDetails_.eventTime                                as event_time,
	l.cartonLpn                                                     as inbound_carton_num,
	l.purchaseOrderNumber                                           as purchase_order_num,
	inventorySplitDetails_.newPurchaseOrderNumber                   as new_purchase_order_num,
	inventorySplitDetails_.warehouseEventContext.locationId         as location_num,
	inventorySplitDetails_.warehouseEventContext.originatingEventId as originating_event_id,
	inventorySplitDetails_.warehouseEventContext.rescindedDetails.originalEventId                as rescinded_event_id,
	inventorySplitDetails_.warehouseEventContext.rescindedDetails.warehouseEventRescindedEventId as corresponding_rescinded_event_id,
	case inventorySplitDetails_.warehouseEventContext.user.type
	     when 'EMPLOYEE' then inventorySplitDetails_.warehouseEventContext.user.employee.id
	     when 'SYSTEM'   then inventorySplitDetails_.warehouseEventContext.user.systemId
         when 'EXTERNAL' then inventorySplitDetails_.warehouseEventContext.user.externalUser.id.value
         else inventorySplitDetails_.warehouseEventContext.userId.id
     end                                                                                         as user_id,
	coalesce(inventorySplitDetails_.warehouseEventContext.user.type, inventorySplitDetails_.warehouseEventContext.userId.idType) as user_id_type_code,
	inventorySplitDetails_.vendorBillOfLading.number              as vendor_bill_of_lading_num,
	inventorySplitDetails_.vendorBillOfLading.shipmentNumberType  as vendor_bill_of_Lading_type_code,
	inventorySplitDetails_.carrierBillOfLading.number             as carrier_bill_of_lading_num,
	inventorySplitDetails_.carrierBillOfLading.shipmentNumberType as carrier_bill_of_lading_type_code,
	items_.item.productSku.id                                     as sku_num,
	items_.item.productSku.idType                                 as sku_type,
    items_.item.quantity*coalesce(items_.item.multiplier,1)       as sku_qty,
    case when size(items_.reasonCodes) = 0
             then cast(null as string)
         else to_json(items_.reasonCodes)
     end                                                          as reason_codes_array,
	case when size(inventorySplitDetails_.warehouseEventContext.logicalLocationSellingBrandMappings) = 0
	           then cast(null as string)
	     else to_json(inventorySplitDetails_.warehouseEventContext.logicalLocationSellingBrandMappings)
	 end                                                          as selling_brands_logical_locations_array,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_lifecycle l
    lateral view explode(inventorySplitDetails) d                        as inventorySplitDetails_
    lateral view outer explode(inventorySplitDetails_.items) i           as items_
    where trim(l.cartonLpn)!='' and  trim(l.purchaseOrderNumber)!=''
      and inventorySplitDetails_.eventTime not rlike('$[\\+]+') ;

-----WarehousePurchaseOrderCartonAudited
create temporary view wm_inbound_carton_audit_event_fact AS
select
	cartonAuditedDetails_.eventTime                                 as event_time,
	l.cartonLpn                                                     as inbound_carton_num,
	l.purchaseOrderNumber                                           as purchase_order_num,
	cartonAuditedDetails_.warehouseEventContext.locationId          as location_num,
	cartonAuditedDetails_.warehouseEventContext.originatingEventId  as originating_event_id,
	cartonAuditedDetails_.warehouseEventContext.rescindedDetails.originalEventId                as rescinded_event_id,
	cartonAuditedDetails_.warehouseEventContext.rescindedDetails.warehouseEventRescindedEventId as corresponding_rescinded_event_id,
	case cartonAuditedDetails_.warehouseEventContext.user.type
	     when 'EMPLOYEE' then cartonAuditedDetails_.warehouseEventContext.user.employee.id
	     when 'SYSTEM'   then cartonAuditedDetails_.warehouseEventContext.user.systemId
         when 'EXTERNAL' then cartonAuditedDetails_.warehouseEventContext.user.externalUser.id.value
         else cartonAuditedDetails_.warehouseEventContext.userId.id
     end                                                                                         as user_id,
	coalesce(cartonAuditedDetails_.warehouseEventContext.user.type, cartonAuditedDetails_.warehouseEventContext.userId.idType) as user_id_type_code,
	case when size(cartonAuditedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings) = 0
	           then cast(null as string)
	     else to_json(cartonAuditedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings)
	 end                                                          as selling_brands_logical_locations_array,
	cartonAuditedDetails_.auditStartTime                                                       as audit_start_time,
	cartonAuditedDetails_.auditType                                                            as audit_type_code,
	cartonAuditedDetails_.auditResult                                                          as audit_result_code,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_lifecycle l
    lateral view explode(cartonAuditedDetails) d as cartonAuditedDetails_
    where trim(l.cartonLpn)!='' and  trim(l.purchaseOrderNumber)!=''
      and cartonAuditedDetails_.eventTime not rlike('$[\\+]+') ;

------WarehousePurchaseOrderAllCartonAuditsCompleted
create temporary view wm_inbound_carton_all_audits_completed_event_fact AS
select
	allAuditsCompletedDetails_.eventTime                                 as event_time,
	l.cartonLpn                                                          as inbound_carton_num,
	l.purchaseOrderNumber                                                as purchase_order_num,
	allAuditsCompletedDetails_.warehouseEventContext.locationId          as location_num,
	allAuditsCompletedDetails_.warehouseEventContext.originatingEventId  as originating_event_id,
	allAuditsCompletedDetails_.warehouseEventContext.rescindedDetails.originalEventId                as rescinded_event_id,
	allAuditsCompletedDetails_.warehouseEventContext.rescindedDetails.warehouseEventRescindedEventId as corresponding_rescinded_event_id,
	case allAuditsCompletedDetails_.warehouseEventContext.user.type
	     when 'EMPLOYEE' then allAuditsCompletedDetails_.warehouseEventContext.user.employee.id
	     when 'SYSTEM'   then allAuditsCompletedDetails_.warehouseEventContext.user.systemId
         when 'EXTERNAL' then allAuditsCompletedDetails_.warehouseEventContext.user.externalUser.id.value
         else allAuditsCompletedDetails_.warehouseEventContext.userId.id
     end                                                                                         as user_id,
	coalesce(allAuditsCompletedDetails_.warehouseEventContext.user.type, allAuditsCompletedDetails_.warehouseEventContext.userId.idType) as user_id_type_code,
	case when size(allAuditsCompletedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings) = 0
	           then cast(null as string)
	     else to_json(allAuditsCompletedDetails_.warehouseEventContext.logicalLocationSellingBrandMappings)
	 end                                                          as selling_brands_logical_locations_array,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_lifecycle l
    lateral view explode(allAuditsCompletedDetails) d as allAuditsCompletedDetails_
        where trim(l.cartonLpn)!='' and  trim(l.purchaseOrderNumber)!=''
              and allAuditsCompletedDetails_.eventTime not rlike('$[\\+]+');

------------------------------------------
-- Union all events together to load into landing table
------------------------------------------
CACHE TABLE wm_inbound_carton_lifecycle_event_fact_ldg_table_stg OPTIONS ('storageLevel' 'MEMORY_AND_DISK') AS
select
    'WarehousePurchaseOrderCartonReceived' as event_type,
	event_time,
	inbound_carton_num,
	purchase_order_num,
    cast(null as string) as new_purchase_order_num,
	location_num,
	originating_event_id,
	rescinded_event_id,
	corresponding_rescinded_event_id,
	carton_shipment_relation_type_code,
	user_id,
	user_id_type_code,
	vendor_bill_of_lading_num,
	vendor_bill_of_Lading_type_code,
	carrier_bill_of_lading_num,
	carrier_bill_of_lading_type_code,
	sku_num,
	sku_type,
	sku_qty,
	sku_quality_code,
	sku_inventory_state_code,
	reason_codes_array,
	selling_brands_logical_locations_array,
    cast(null as timestamp) as audit_start_time,
	cast(null as string)    as audit_type_code,
	cast(null as string)    as audit_result_code,
    cast(null as string)    as from_Carton_num,
	cast(null as string)    as to_Carton_num,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_received_sku_state_quality_event_fact
UNION ALL
select
    'WarehousePurchaseOrderCartonSplit' as event_type,
	event_time,
	inbound_carton_num,
	purchase_order_num,
    cast(null as string) as new_purchase_order_num,
	location_num,
	originating_event_id,
	rescinded_event_id,
	corresponding_rescinded_event_id,
    cast(null as string) as carton_shipment_relation_type_code,
	user_id,
	user_id_type_code,
    cast(null as string) as vendor_bill_of_lading_num,
	cast(null as string) as vendor_bill_of_Lading_type_code,
	cast(null as string) as carrier_bill_of_lading_num,
	cast(null as string) as carrier_bill_of_lading_type_code,
	sku_num,
	sku_type,
	sku_qty,
    cast(null as string) as sku_quality_code,
    cast(null as string) as sku_inventory_state_code,
    cast(null as string) as reason_codes_array,
    selling_brands_logical_locations_array,
    cast(null as timestamp) as audit_start_time,
	cast(null as string) as audit_type_code,
	cast(null as string) as audit_result_code,
	from_Carton_num,
	to_Carton_num,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_split_sku_event_fact
UNION ALL
select
    'WarehousePurchaseOrderInventoryAdjusted' as event_type,
	event_time,
	inbound_carton_num,
	purchase_order_num,
    cast(null as string) as new_purchase_order_num,
	location_num,
	originating_event_id,
	rescinded_event_id,
	corresponding_rescinded_event_id,
    cast(null as string) as carton_shipment_relation_type_code,
	user_id,
	user_id_type_code,
    cast(null as string) as vendor_bill_of_lading_num,
	cast(null as string) as vendor_bill_of_Lading_type_code,
	cast(null as string) as carrier_bill_of_lading_num,
	cast(null as string) as carrier_bill_of_lading_type_code,
	sku_num,
	sku_type,
	sku_qty,
	sku_quality_code,
	sku_inventory_state_code,
	reason_codes_array,
	selling_brands_logical_locations_array,
    cast(null as timestamp) as audit_start_time,
	cast(null as string) as audit_type_code,
	cast(null as string) as audit_result_code,
    cast(null as string) as from_Carton_num,
	cast(null as string) as to_Carton_num,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_adjusted_sku_state_quality_event_fact
UNION ALL
select
    'WarehousePurchaseOrderInventorySplit' as event_type,
	event_time,
	inbound_carton_num,
	purchase_order_num,
	new_purchase_order_num,
	location_num,
	originating_event_id,
	rescinded_event_id,
	corresponding_rescinded_event_id,
    cast(null as string) as carton_shipment_relation_type_code,
	user_id,
	user_id_type_code,
	vendor_bill_of_lading_num,
	vendor_bill_of_Lading_type_code,
	carrier_bill_of_lading_num,
	carrier_bill_of_lading_type_code,
	sku_num,
	sku_type,
    sku_qty,
    cast(null as string) as sku_quality_code,
    cast(null as string) as sku_inventory_state_code,
    reason_codes_array,
	selling_brands_logical_locations_array,
    cast(null as timestamp) as audit_start_time,
	cast(null as string) as audit_type_code,
	cast(null as string) as audit_result_code,
    cast(null as string) as from_Carton_num,
	cast(null as string) as to_Carton_num,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_inventory_split_cross_po_sku_event_fact
UNION ALL
select
    'WarehousePurchaseOrderCartonAudited' event_type,
	event_time,
	inbound_carton_num,
	purchase_order_num,
    cast(null as string) as new_purchase_order_num,
	location_num,
	originating_event_id,
	rescinded_event_id,
	corresponding_rescinded_event_id,
    cast(null as string) as carton_shipment_relation_type_code,
	user_id,
	user_id_type_code,
    cast(null as string) as vendor_bill_of_lading_num,
	cast(null as string) as vendor_bill_of_Lading_type_code,
	cast(null as string) as carrier_bill_of_lading_num,
	cast(null as string) as carrier_bill_of_lading_type_code,
    cast(null as string) as sku_num,
    cast(null as string) as sku_type,
    cast(null as int) as sku_qty,
    cast(null as string) as sku_quality_code,
    cast(null as string) as sku_inventory_state_code,
    cast(null as string) as reason_codes_array,
	selling_brands_logical_locations_array,
	audit_start_time,
	audit_type_code,
	audit_result_code,
    cast(null as string) as from_Carton_num,
	cast(null as string) as to_Carton_num,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_audit_event_fact
UNION ALL
select
    'WarehousePurchaseOrderAllCartonAuditsCompleted' as event_type,
	event_time,
	inbound_carton_num,
	purchase_order_num,
    cast(null as string) as new_purchase_order_num,
	location_num,
	originating_event_id,
	rescinded_event_id,
	corresponding_rescinded_event_id,
    cast(null as string) as carton_shipment_relation_type_code,
	user_id,
	user_id_type_code,
    cast(null as string) as vendor_bill_of_lading_num,
	cast(null as string) as vendor_bill_of_Lading_type_code,
	cast(null as string) as carrier_bill_of_lading_num,
	cast(null as string) as carrier_bill_of_lading_type_code,
    cast(null as string) as sku_num,
    cast(null as string) as sku_type,
    cast(null as int) as sku_qty,
    cast(null as string) as sku_quality_code,
    cast(null as string) as sku_inventory_state_code,
    cast(null as string) as reason_codes_array,
	selling_brands_logical_locations_array,
    cast(null as timestamp) as audit_start_time,
	cast(null as string) as audit_type_code,
	cast(null as string) as audit_result_code,
    cast(null as string) as from_Carton_num,
	cast(null as string) as to_Carton_num,
	objectMetadata_lastUpdatedTime
from wm_inbound_carton_all_audits_completed_event_fact ;

--free cache
UNCACHE TABLE wm_inbound_carton_lifecycle ;

--Writing Kafka Data to Teradata landing table
--Trying to reduce jdbc parallelism using coalesce
--to prevent Socket communication failure for Packet transmit in JDBC
insert  overwrite table wm_inbound_carton_lifecycle_event_fact_ldg_tbl
select /*+ COALESCE(1) */
    event_type,
	cast(event_time as string) as event_tmstp,
	inbound_carton_num,
	purchase_order_num,
    new_purchase_order_num,
	location_num,
	originating_event_id,
	rescinded_event_id,
	corresponding_rescinded_event_id,
	carton_shipment_relation_type_code,
	user_id,
	user_id_type_code,
	vendor_bill_of_lading_num,
	vendor_bill_of_Lading_type_code,
	carrier_bill_of_lading_num,
	carrier_bill_of_lading_type_code,
	sku_num,
	sku_type,
	sku_qty,
	sku_quality_code,
	sku_inventory_state_code,
	reason_codes_array,
	selling_brands_logical_locations_array,
    cast(audit_start_time as string) as audit_start_tmstp,
	audit_type_code,
	audit_result_code,
    from_Carton_num,
	to_Carton_num,
	cast(objectMetadata_lastUpdatedTime as string) as objectMetadata_lastUpdatedTime
from wm_inbound_carton_lifecycle_event_fact_ldg_table_stg ;

--refresh partitions in the connector data table
--MSCK REPAIR TABLE ascp.warehouse_inbound_carton_lifecycle_model_parquet ;

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
          '{dag_name}' as isf_dag_nm,
	      'Loading to ldg' as step_nm,
	      'WM_INBOUND_CARTON_LIFECYCLE_EVENT_FACT_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      cast(count(*) as string) as metric_value,
		  cast(current_timestamp() as string) as metric_tmstp
      from wm_inbound_carton_lifecycle_event_fact_ldg_table_stg
	  union all
	  select
	      '{dag_name}' as isf_dag_nm,
	      'Reading from kafka' as step_nm,
	      cast (null as string) as tbl_nm,
	      'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
	      cast(total_cnt as string) as metric_value,
	      cast(current_timestamp() as string) as metric_tmstp
	  from kafka_cnt) a
;