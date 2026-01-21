--Source
--Reading Data from  Source Kafka Topic Name=inventory-store-inventory-allocation-analytical-avro using SQL API CODE
create temporary view store_inventory_allocation_rn AS select 
allocationNumber as inventoryTransferId, "ALLOCATION" as inventoryTransferIdType, allocationReceivedDetails, 
row_number() over (partition by allocationNumber order by headers.SystemTime desc) as rn  
from kafka_inventory_store_inventory_allocation_object_model_avro;

create temporary view store_inventory_allocation AS select 
inventoryTransferId, inventoryTransferIdType, allocationReceivedDetails
from store_inventory_allocation_rn
where rn = 1;

create temporary view store_inventory_transfer_extract_allocation_receipt as select inventoryTransferId, inventoryTransferIdType, 
explode(allocationReceivedDetails) allocationReceivedDetails  
from store_inventory_allocation;

create temporary view store_inventory_transfer_extract_allocation_receipt_items as
select inventoryTransferId, inventoryTransferIdType, allocationReceivedDetails.cartonNumber, allocationReceivedDetails.shipmentNumber,
allocationReceivedDetails.billOfLading, allocationReceivedDetails.locationId, allocationReceivedDetails.id as eventId, allocationReceivedDetails.eventTime,
explode(allocationReceivedDetails.productDetails) productDetails 
from store_inventory_transfer_extract_allocation_receipt;

create temporary view kafka_store_inventory_transfer_allocation_receipt as
select 
inventoryTransferId as transfer_num,
inventoryTransferIdType as transfer_type,
cartonNumber as carton_num, 
shipmentNumber as shipment_num,
billOfLading as bill_of_lading,
locationId as location_num, 
eventId as event_id, 
eventTime as event_tmstp,
productDetails.product.id as sku_num,
productDetails.product.idType as sku_type,
productDetails.quantity as sku_qty,
productDetails.disposition as disposition_code
from store_inventory_transfer_extract_allocation_receipt_items;

-- read from S3
create temporary view orc_vw using ORC
OPTIONS (path "s3://{s3_bucket_orc}/data/ascp-store-inventory-allocation-lifecycle-v1");

--Sink

---Writing Kafka Data to S3 in ORC format
insert overwrite table `ascp-store-inventory-allocation-lifecycle-tmp`
select 
   inventoryTransferId, 
   inventoryTransferIdType, 
   allocationReceivedDetails
 FROM ( SELECT u.*
             , Row_Number() Over (PARTITION BY u.inventoryTransferId ORDER BY u.SystemTime DESC) AS rn
         FROM ( SELECT Cast(0 AS BIGINT) as SystemTime,
                     inventoryTransferId, 
					 inventoryTransferIdType, 
					 allocationReceivedDetails
                FROM orc_vw
                UNION ALL
                SELECT Cast(headers.SystemTime AS BIGINT) as SystemTime,
                     allocationNumber as inventoryTransferId, 
					 "ALLOCATION" as inventoryTransferIdType, 
					 allocationReceivedDetails
                FROM kafka_inventory_store_inventory_allocation_object_model_avro
              ) AS u
      ) as r
where r.rn = 1;

---Writing Error Data to S3 in csv format
 
insert overwrite table store_inventory_transfer_allocation_receipt
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
  transfer_num,
  transfer_type,
  carton_num, 
  shipment_num,
  bill_of_lading,
  location_num, 
  event_id, 
  event_tmstp,
  sku_num,
  sku_type,
  sku_qty,
  disposition_code,
  cast(year(current_date) as string) as year, 
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month, 
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM kafka_store_inventory_transfer_allocation_receipt
where transfer_num is null or transfer_num = '' or transfer_num = '""'
 or transfer_type is null or transfer_type = '' or transfer_type = '""'
 or event_id is null or event_id = '' or event_id = '""'
 or carton_num is null or carton_num = '' or carton_num = '""'
 or sku_num is null or sku_num = '' or sku_num = '""'
 or disposition_code = 'UNKNOWN';

---Writing Kafka Data to Teradata staging table

insert overwrite table store_inventory_transfer_allocation_receipt_ldg_table
SELECT
  transfer_num,
  transfer_type,
  event_id,
  event_tmstp,
  'STORE_ALLOCATION_RECEIVED' as event_type,
  shipment_num,
  location_num,
  bill_of_lading,
  carton_num,
  sku_num,
  sku_type,
  disposition_code,
  sku_qty 
FROM kafka_store_inventory_transfer_allocation_receipt;
