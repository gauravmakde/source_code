--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_dc_internal_transfer_kafka_to_teradata.sql
-- Author            : Dmytro Utte
-- Description       : Reading Data from  Source Kafka Topic Name=inventory-distribution-center-internal-transfer-analytical-avro using SQL API CODE
-- Source topic      : inventory-distribution-center-internal-transfer-analytical-avro
-- Object model      : DistributionCenterInternalTransfer
-- ETL Run Frequency : Every hour
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-25  Utte Dmytro    FA-9925:  Migrate NRLT DAG for DC_INTERNAL_TRANSFER to IsF
-- 2023-11-29  Tetiana Ivchyk FA-10573: Filter Out Logical Transfers For DC Internal Transfer
--*************************************************************************************************************************************


create temporary view dc_internal_transfer_rn AS
select
	transferId,
	transferType,
	purchaseOrderNumber,
	warehouseEvents,
	transferReceipt,
	transferShipment,
	row_number() OVER (partition by transferId, transferType order by objectMetadata.lastUpdatedTime desc) rn
from kafka_dc_internal_transfer_avro;

create temporary view dc_internal_transfer_cnt AS
select
	count(*) as total_cnt
from kafka_dc_internal_transfer_avro;


create temporary view dc_internal_transfer as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	warehouseEvents,
	transferReceipt,
	transferShipment
from dc_internal_transfer_rn
where rn = 1;

create temporary view receipt_cartons_explode as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	explode(transferReceipt) receipt_,
	explode(receipt_.receiptCartons) cartons
from dc_internal_transfer;

create temporary view receipt_cartons_allocation_received_items_explode as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	receipt_.shipmentNumber,
	cartons.cartonLpn,
	cartons.receiptTime ,
	cartons.cartonShipmentLocationDetails,
	explode(cartons.allocationReceivedItems) allocation_received_items
from receipt_cartons_explode;

create temporary view receipt_cartons_allocation_received_items_inventory_state_quantities as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber,
	cartonLpn,
	receiptTime,
	cartonShipmentLocationDetails,
	allocation_received_items.item.productSku,
	to_json(allocation_received_items.reasonCodes) reason_codes,
	explode(allocation_received_items.item.inventoryStateQuantities) inventory_state_quantity
from receipt_cartons_allocation_received_items_explode;

create temporary view receipt_cartons_transfer_received_items_explode as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	receipt_.shipmentNumber,
	cartons.cartonLpn,
	cartons.receiptTime ,
	cartons.cartonShipmentLocationDetails,
	explode(cartons.transferReceivedItems) transfer_received_items
from receipt_cartons_explode;

create temporary view receipt_cartons_transfer_received_items_inventory_state_quantities as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber,
	cartonLpn,
	receiptTime,
	cartonShipmentLocationDetails,
	transfer_received_items.item.productSku,
	to_json(transfer_received_items.reasonCodes) reason_codes,
	explode(transfer_received_items.item.inventoryStateQuantities) inventory_state_quantity
from receipt_cartons_transfer_received_items_explode;

create temporary view receipt_consolidated as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber,
	cartonLpn,
	receiptTime,
	cartonShipmentLocationDetails,
	productSku,
	reason_codes,
	inventory_state_quantity.quantity
from receipt_cartons_transfer_received_items_inventory_state_quantities
union all 
select
    transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber,
	cartonLpn,
	receiptTime,
	cartonShipmentLocationDetails,
	productSku,
	reason_codes,
	inventory_state_quantity.quantity
from receipt_cartons_allocation_received_items_inventory_state_quantities
;

create temporary view receipt_consolidated_final as
select
	transferId as transfer_id,
	transferType as transfer_type,
	purchaseOrderNumber as purchase_order_num,
	shipmentNumber.number as shipment_num,
	shipmentNumber.shipmentNumberType as shipment_num_type,
	cartonLpn as carton_lpn,
	receiptTime as receipt_time,
	cartonShipmentLocationDetails.shipFromLocationId as ship_from_location_id,
	cartonShipmentLocationDetails.shipToLocationId as ship_to_location_id,
	cartonShipmentLocationDetails.originatingLocationId as originating_location_id,
	cartonShipmentLocationDetails.destinationLocationId as destination_location_id,
	cartonShipmentLocationDetails.warehouseShipmentStage as warehouse_shipment_stage,
	productSku.id as sku_id,
	productSku.idType as sku_type,
	reason_codes as reason_codes,
	sum(quantity) as receipt_qty
from receipt_consolidated
where transferId <> '' and transferType <> '' and cartonLpn <> '' and productSku.id <> '' 
	  and transferId <> '""' and transferType <> '""' and cartonLpn <> '""' and productSku.id  <> '""'
group by 
    transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber.number,
	shipmentNumber.shipmentNumberType,
	cartonLpn,
	receiptTime,
	cartonShipmentLocationDetails.shipFromLocationId,
	cartonShipmentLocationDetails.shipToLocationId,
	cartonShipmentLocationDetails.originatingLocationId,
	cartonShipmentLocationDetails.destinationLocationId,
	cartonShipmentLocationDetails.warehouseShipmentStage,
	productSku.id,
	productSku.idType,
	reason_codes;
	
create temporary view receipt_consolidated_final_rn as
select
	transfer_id,
	transfer_type,
	purchase_order_num,
	shipment_num,
	shipment_num_type,
	carton_lpn,
	receipt_time,
	ship_from_location_id,
	ship_to_location_id,
	originating_location_id,
	destination_location_id,
	warehouse_shipment_stage,
	sku_id,
	sku_type,
	reason_codes,
	receipt_qty,
	row_number() over(partition by transfer_id, transfer_type, carton_lpn, sku_id, ship_from_location_id order by receipt_time desc) as rn,
	row_number() over(partition by transfer_id, transfer_type, receipt_time, carton_lpn, sku_id, ship_from_location_id order by receipt_time desc) as dedup_rn
from receipt_consolidated_final
where shipment_num_type <> 'LOGICAL_TRANSFER_NUMBER'
	or shipment_num_type is null;

create temporary view transfer_shipment_carton_explode as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	explode(transferShipment) as transfer_shipment,
	explode(transfer_shipment.shipmentCartons) cartons
from dc_internal_transfer;

create temporary view transfer_shipment_carton_item_explode as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	transfer_shipment.shipmentNumber,
	transfer_shipment.routeNumber,
	transfer_shipment.laneNumber,
	transfer_shipment.loadNumber,
	cartons.cartonLpn,
	cartons.shippedTime,
	cartons.palletLpn,
	cartons.cartonShipmentLocationDetails,
	explode(cartons.shippedItems) items
from transfer_shipment_carton_explode;

create temporary view transfer_shipment_qty_explode as
select
	transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber.number,
	shipmentNumber.shipmentNumberType,
	routeNumber,
	laneNumber,
	loadNumber,
	cartonLpn,
	shippedTime,
	palletLpn,
	cartonShipmentLocationDetails.shipFromLocationId,
	cartonShipmentLocationDetails.shipToLocationId,
	cartonShipmentLocationDetails.originatingLocationId,
	cartonShipmentLocationDetails.destinationLocationId,
	cartonShipmentLocationDetails.warehouseShipmentStage,
	items.productSku.id,
	items.productSku.idType,
	explode(items.inventoryStateQuantities) as inventory_state_quantities
from transfer_shipment_carton_item_explode;

create temporary view transfer_shipment_final as
select
	transferId as transfer_id,
	transferType as transfer_type,
	purchaseOrderNumber as purchase_order_num,
	number as shipment_num,
	shipmentNumberType as shipment_num_type,
	routeNumber as route_num,
	laneNumber as lane_num,
	loadNumber as load_num,
	cartonLpn as carton_lpn,
	shippedTime as shipped_time,
	palletLpn as pallet_lpn,
	shipFromLocationId as ship_from_location_id,
	shipToLocationId as ship_to_location_id,
	originatingLocationId as originating_location_id,
	destinationLocationId as destination_location_id,
	warehouseShipmentStage as warehouse_shipment_stage,
	id as sku_id,
	idType as sku_type,
	sum(inventory_state_quantities.quantity) as shipment_qty
from transfer_shipment_qty_explode
where transferId <> '' and transferType <> '' and cartonLpn <> '' and id <> '' 
	  and transferId <> '""' and transferType <> '""' and cartonLpn <> '""' and id  <> '""'
group by 
    transferId,
	transferType,
	purchaseOrderNumber,
	number,
	shipmentNumberType,
	routeNumber,
	laneNumber,
	loadNumber,
	cartonLpn,
	shippedTime,
	palletLpn,
	shipFromLocationId,
	shipToLocationId,
	originatingLocationId,
	destinationLocationId,
	warehouseShipmentStage,
	id,
	idType;
	
create temporary view transfer_shipment_final_rn as
select
	transfer_id,
	transfer_type,
	purchase_order_num,
	shipment_num,
	shipment_num_type,
	route_num,
	lane_num,
	load_num,
	carton_lpn,
	shipped_time,
	pallet_lpn,
	ship_from_location_id,
	ship_to_location_id,
	originating_location_id,
	destination_location_id,
	warehouse_shipment_stage,
	sku_id,
	sku_type,
	shipment_qty,
	row_number() over(partition by transfer_id, transfer_type, carton_lpn, sku_id, ship_from_location_id order by shipped_time desc) as rn,
	row_number() over(partition by transfer_id, transfer_type, carton_lpn, shipped_time, sku_id, ship_from_location_id order by shipped_time desc) as dedup_rn
from transfer_shipment_final
where shipment_num_type <> 'LOGICAL_TRANSFER_NUMBER'
	or shipment_num_type is null;

---Writing Error Data to S3 in csv format
insert overwrite table dc_internal_transfer_shipments_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	transferId,
	transferType,
	purchaseOrderNumber,
	number,
	shipmentNumberType,
	routeNumber,
	laneNumber,
	loadNumber,
	cartonLpn,
	shippedTime,
	palletLpn,
	shipFromLocationId,
	shipToLocationId,
	originatingLocationId,
	destinationLocationId,
	warehouseShipmentStage,
	id as sku_id,
	idType as sku_type,
	inventory_state_quantities.quantity as shipment_qty,
	substr(current_date,1,4) as year,
	substr(current_date,6,2) as month,
	substr(current_date,9,2) as day,
	substr(current_timestamp,12,2) as hour
from transfer_shipment_qty_explode
where transferId is null or transferType is null or cartonLpn is null or id is null 
      or transferId = '' or transferType = '' or cartonLpn = '' or id = '' 
	  or transferId = '""' or transferType = '""' or cartonLpn = '""' or id  = '""';
	
insert overwrite table dc_internal_transfer_receipts_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber.number as shipment_num,
	shipmentNumber.shipmentNumberType as shipment_num_type,
	cartonLpn,
	receiptTime,
	cartonShipmentLocationDetails.shipFromLocationId as ship_from_location_id,
	cartonShipmentLocationDetails.shipToLocationId as ship_to_location_id,
	cartonShipmentLocationDetails.originatingLocationId as originating_location_id,
	cartonShipmentLocationDetails.destinationLocationId as destination_location_id,
	cartonShipmentLocationDetails.warehouseShipmentStage as warehouse_shipment_stage,
	productSku.id as sku_id,
	productSku.idType as sku_type,
	reason_codes,
	inventory_state_quantity.quantity,
	substr(current_date,1,4) as year,
	substr(current_date,6,2) as month,
	substr(current_date,9,2) as day,
	substr(current_timestamp,12,2) as hour
from receipt_cartons_transfer_received_items_inventory_state_quantities
where transferId is null or transferType is null or cartonLpn is null or productSku.id is null 
      or transferId = '' or transferType = '' or cartonLpn = '' or productSku.id = '' 
	  or transferId = '""' or transferType = '""' or cartonLpn = '""' or productSku.id  = '""';
	  
insert overwrite table dc_internal_transfer_allocation_receipts_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	transferId,
	transferType,
	purchaseOrderNumber,
	shipmentNumber.number as shipment_num,
	shipmentNumber.shipmentNumberType as shipment_num_type,
	cartonLpn,
	receiptTime,
	cartonShipmentLocationDetails.shipFromLocationId as ship_from_location_id,
	cartonShipmentLocationDetails.shipToLocationId as ship_to_location_id,
	cartonShipmentLocationDetails.originatingLocationId as originating_location_id,
	cartonShipmentLocationDetails.destinationLocationId as destination_location_id,
	cartonShipmentLocationDetails.warehouseShipmentStage as warehouse_shipment_stage,
	productSku.id as sku_id,
	productSku.idType as sku_type,
	reason_codes,
	inventory_state_quantity.quantity,
	substr(current_date,1,4) as year,
	substr(current_date,6,2) as month,
	substr(current_date,9,2) as day,
	substr(current_timestamp,12,2) as hour
from receipt_cartons_allocation_received_items_inventory_state_quantities
where transferId is null or transferType is null or cartonLpn is null or productSku.id is null 
      or transferId = '' or transferType = '' or cartonLpn = '' or productSku.id = '' 
	  or transferId = '""' or transferType = '""' or cartonLpn = '""' or productSku.id  = '""';

---Writing Kafka Data to Teradata staging tables
insert overwrite table dc_internal_transfer_shipment_receipt_ldg_table
select
	coalesce(ts.transfer_id, tr.transfer_id) as transfer_id,
    coalesce(ts.transfer_type, tr.transfer_type) as transfer_type,
    coalesce(ts.purchase_order_num, tr.purchase_order_num) as purchase_order_num,
    coalesce(ts.shipment_num, tr.shipment_num) as shipment_num,
    coalesce(ts.shipment_num_type, tr.shipment_num_type) as shipment_num_type,
    ts.route_num,
    ts.lane_num,
    ts.load_num,
    coalesce(ts.carton_lpn, tr.carton_lpn) as carton_lpn,
	cast(shipped_time as string),
	cast(receipt_time as string),
    ts.pallet_lpn,
    coalesce(ts.ship_from_location_id, tr.ship_from_location_id) as ship_from_location_id,
    coalesce(tr.ship_to_location_id, ts.ship_to_location_id) as ship_to_location_id,
    coalesce(ts.originating_location_id, tr.originating_location_id) as originating_location_id,
    coalesce(ts.destination_location_id, tr.destination_location_id) as destination_location_id,
	ts.warehouse_shipment_stage,
	tr.warehouse_shipment_stage as warehouse_receipt_stage,
    coalesce(ts.sku_id, tr.sku_id) as sku_id,
    coalesce(ts.sku_type, tr.sku_type) as sku_type,
	tr.reason_codes,
    ts.shipment_qty,
	tr.receipt_qty
from (
  select * from transfer_shipment_final_rn
  where dedup_rn = 1
) ts
full outer join (
  select * from receipt_consolidated_final_rn
  where dedup_rn = 1
) tr ON ts.transfer_id = tr.transfer_id
    AND ts.transfer_type = tr.transfer_type
    AND ts.carton_lpn = tr.carton_lpn
    AND ts.sku_id = tr.sku_id
	AND coalesce(ts.ship_from_location_id,'') = coalesce(tr.ship_from_location_id,'')
	AND 
    (
     (ts.ship_to_location_id <> '' and 
      tr.ship_to_location_id = ts.ship_to_location_id)
     or 
     (ts.ship_to_location_id = '' and 
      tr.rn = ts.rn and tr.receipt_time > ts.shipped_time)
    )
;

---Writing Spark job count metrics 
insert into table processed_data_spark_log_table
select
    isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	cast(metric_value as string),
	cast(metric_tmstp as string)
from (
      select 
          'nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Loading to ldg' as step_nm,
	      'DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      count(*) as metric_value,
		  current_timestamp() as metric_tmstp
      from 
	    (
         select * from transfer_shipment_final_rn
         where dedup_rn = 1
        ) ts
      full outer join 
	    (
         select * from receipt_consolidated_final_rn
         where dedup_rn = 1
        ) tr ON ts.transfer_id = tr.transfer_id
             AND ts.transfer_type = tr.transfer_type
             AND ts.carton_lpn = tr.carton_lpn
             AND ts.sku_id = tr.sku_id
    	     AND coalesce(ts.ship_from_location_id,'') = coalesce(tr.ship_from_location_id,'')
			 AND 
             (
              (ts.ship_to_location_id <> '' and 
               tr.ship_to_location_id = ts.ship_to_location_id)
              or 
              (ts.ship_to_location_id = '' and 
               tr.rn = ts.rn and tr.receipt_time > ts.shipped_time)
             )
	  union all 
	  select
	      'nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
	      'Reading from kafka' as step_nm,
	      cast (null as string) as tbl_nm,
	      'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
	      total_cnt as metric_value,
	      current_timestamp() as metric_tmstp
	  from dc_internal_transfer_cnt) a 
;
