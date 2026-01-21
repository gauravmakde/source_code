--Source
--Reading Data from  Source Kafka Topic Name=store-inbound-carton-lifecycle-avro using SQL API CODE
create temporary view store_inbound_carton_rn AS select 
purchaseOrderNumber, cartonNumber, vendorCartonNumber, locationId, advanceShipmentNumber, directStoreDelivery, receiptDetails, adjustmentDetails, 
row_number() over (partition by purchaseOrderNumber, cartonNumber order by headers.SystemTime desc) as rn  
from kafka_inventory_store_inbound_carton_object_model_avro;

create temporary view store_inbound_carton AS select 
cartonNumber, purchaseOrderNumber, vendorCartonNumber, advanceShipmentNumber, locationId, directStoreDelivery, receiptDetails, adjustmentDetails 
from store_inbound_carton_rn
where rn = 1;

create temporary view store_inbound_carton_extract_receipt as select cartonNumber,
purchaseOrderNumber, vendorCartonNumber, locationId, advanceShipmentNumber, explode(receiptDetails) receiptDetails  
from store_inbound_carton;

create temporary view store_inbound_carton_extract_receipt_items as
select cartonNumber, purchaseOrderNumber, vendorCartonNumber, locationId, advanceShipmentNumber, receiptDetails.id as eventId, receiptDetails.eventTime,
explode(receiptDetails.productDetails) productDetails 
from store_inbound_carton_extract_receipt;

create temporary view store_inbound_carton_extract_adjustment as select cartonNumber,
purchaseOrderNumber, vendorCartonNumber, locationId, advanceShipmentNumber, explode(adjustmentDetails) adjustmentDetails  
from store_inbound_carton;

create temporary view store_inbound_carton_extract_adjustment_items as
select cartonNumber, purchaseOrderNumber, vendorCartonNumber, locationId, advanceShipmentNumber, adjustmentDetails.id as eventId, adjustmentDetails.eventTime,
explode(adjustmentDetails.productDetails) productDetails 
from store_inbound_carton_extract_adjustment;

create temporary view kafka_store_inbound_carton_events as
select 
 cartonNumber as carton_num, 
 purchaseOrderNumber as purchase_order_num,
 vendorCartonNumber as vendor_carton_num,
 advanceShipmentNumber as advance_shipment_num,
 locationId as location_num, 
 eventId as event_id, 
 'STORE_PURCHASE_ORDER_RECEIVED_ADJUSTED' as event_type,
 eventTime as event_tmstp,
 productDetails.product.id as sku_num,
 productDetails.product.idType as sku_type,
 productDetails.quantity as sku_qty,
 productDetails.disposition as disposition_code
from store_inbound_carton_extract_adjustment_items
union all
select 
 cartonNumber as carton_num, 
 purchaseOrderNumber as purchase_order_num,
 vendorCartonNumber as vendor_carton_num,
 advanceShipmentNumber as advance_shipment_num,
 locationId as location_num, 
 eventId as event_id, 
 'STORE_PURCHASE_ORDER_RECEIVED' as event_type,
 eventTime as event_tmstp,
 productDetails.product.id as sku_num,
 productDetails.product.idType as sku_type,
 productDetails.quantity as sku_qty,
 productDetails.disposition as disposition_code
from store_inbound_carton_extract_receipt_items;

--Sink

--save to pre-created table to make Spark use table schema
insert overwrite table ascp.ascp_store_inbound_carton_lifecycle_kafka
select 
   cartonNumber, 
   purchaseOrderNumber, 
   advanceShipmentNumber,
   locationId, 
   directStoreDelivery, 
   receiptDetails, 
   adjustmentDetails,
   vendorCartonNumber
from store_inbound_carton;
  
---Writing Kafka Data to S3 in ORC format
-- sink data to S3
insert overwrite table ascp.ascp_store_inbound_carton_lifecycle_tmp
select 
   cartonNumber, 
   purchaseOrderNumber, 
   advanceShipmentNumber,
   locationId, 
   directStoreDelivery, 
   receiptDetails, 
   adjustmentDetails,
   vendorCartonNumber
 FROM ( SELECT u.*
             , Row_Number() Over (PARTITION BY u.purchaseOrderNumber, u.cartonNumber ORDER BY u.SystemTime DESC) AS rn
         FROM ( SELECT Cast(1 AS BIGINT) as SystemTime,
                     purchaseOrderNumber, 
                     cartonNumber, 
                     locationId, 
                     advanceShipmentNumber, 
                     directStoreDelivery, 
                     receiptDetails, 
                     adjustmentDetails,
					 vendorCartonNumber
                FROM ascp.ascp_store_inbound_carton_lifecycle_kafka
                UNION ALL
                SELECT Cast(0 AS BIGINT) as SystemTime,
                     purchaseOrderNumber, 
                     cartonNumber, 
                     locationId, 
                     advanceShipmentNumber, 
                     directStoreDelivery, 
                     receiptDetails, 
                     adjustmentDetails,
					 vendorCartonNumber
                FROM ascp.ascp_store_inbound_carton_lifecycle_v1
              ) AS u
      ) as r
where r.rn = 1;

insert overwrite table ascp.ascp_store_inbound_carton_lifecycle_v1
SELECT
   cartonNumber, 
   purchaseOrderNumber, 
   advanceShipmentNumber,
   locationId, 
   directStoreDelivery, 
   receiptDetails, 
   adjustmentDetails,
   vendorCartonNumber
FROM ascp.ascp_store_inbound_carton_lifecycle_tmp;

---Writing Error Data to S3 in csv format
insert overwrite table store_inbound_carton_events_err
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
  purchase_order_num,
  carton_num,
  vendor_carton_num,
  advance_shipment_num,
  location_num,
  event_id,
  event_type,
  event_tmstp,
  sku_num,
  sku_type,
  disposition_code,
  sku_qty,
  cast(year(current_date) as string) as year, 
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month, 
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM kafka_store_inbound_carton_events
where purchase_order_num is null or purchase_order_num = '' or purchase_order_num = '""'
 or carton_num is null or carton_num = '' or carton_num = '""'
 or event_id is null or event_id = '' or event_id = '""'
 or sku_num is null or sku_num = '' or sku_num = '""';

---Writing Kafka Data to Teradata staging table
insert overwrite table store_inbound_carton_events_ldg_table
SELECT
  purchase_order_num,
  carton_num,
  vendor_carton_num,
  advance_shipment_num,
  location_num,
  event_id,
  event_type,
  event_tmstp,
  sku_num,
  sku_type,
  disposition_code,
  sku_qty,
  current_timestamp as dw_sys_load_tmstp
FROM kafka_store_inbound_carton_events
where purchase_order_num is not null and purchase_order_num <> '' and purchase_order_num <> '""'
 and carton_num is not null and carton_num <> '' and carton_num <> '""'
 and event_id is not null and event_id <> '' and event_id <> '""'
 and sku_num is not null and sku_num <> '' and sku_num <> '""'
 and (event_tmstp is not null and to_timestamp(event_tmstp) is not null);
