--Source
--Reading Data from  Source Kafka Topic Name=inventory-store-inventory-transfer-analytical-avro using SQL API CODE
create temporary view store_inventory_transfer_rn AS select
transferNumber as inventoryTransferId, "TRANSFER" as inventoryTransferIdType, createdDetail, canceledDetail, shippedDetails, receivedDetails,
row_number() over (partition by transferNumber order by headers.SystemTime desc) as rn
from kafka_inventory_store_inventory_transfer_object_model_avro;

create temporary view store_inventory_transfer AS select
inventoryTransferId, inventoryTransferIdType, createdDetail, canceledDetail, shippedDetails, receivedDetails
from store_inventory_transfer_rn
where rn = 1;

create temporary view store_inventory_transfer_extract_receipt as select inventoryTransferId, inventoryTransferIdType,
explode(receivedDetails) receivedDetails
from store_inventory_transfer;

create temporary view store_inventory_transfer_extract_receipt_items as
select inventoryTransferId, inventoryTransferIdType, receivedDetails.cartonNumber, receivedDetails.shipmentNumber,
receivedDetails.billOfLading, receivedDetails.locationId, receivedDetails.id as eventId, receivedDetails.eventTime,
explode(receivedDetails.productDetails) productDetails
from store_inventory_transfer_extract_receipt;

create temporary view kafka_store_inventory_transfer_receipt as
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
from store_inventory_transfer_extract_receipt_items;

create temporary view store_inventory_transfer_extract_shipment as select inventoryTransferId, inventoryTransferIdType,
explode(shippedDetails) shippedDetails
from store_inventory_transfer;

create temporary view store_inventory_transfer_extract_shipment_items as
select inventoryTransferId, inventoryTransferIdType, shippedDetails.cartonNumber, shippedDetails.shipmentNumber,
shippedDetails.billOfLading, shippedDetails.locationId, shippedDetails.id as eventId, shippedDetails.eventTime,
shippedDetails.toLocation.facility, shippedDetails.toLocation.logical, explode(shippedDetails.productDetails) productDetails
from store_inventory_transfer_extract_shipment;

create temporary view kafka_store_inventory_transfer_shipment as
select
inventoryTransferId as transfer_num,
inventoryTransferIdType as transfer_type,
cartonNumber as carton_num,
shipmentNumber as shipment_num,
billOfLading as bill_of_lading,
locationId as location_num,
facility as to_location_num,
logical as to_logical_location_num,
eventId as event_id,
eventTime as event_tmstp,
productDetails.product.id as sku_num,
productDetails.product.idType as sku_type,
productDetails.quantity as sku_qty,
productDetails.disposition as disposition_code
from store_inventory_transfer_extract_shipment_items;

create temporary view store_inventory_transfer_extract_created_items as
select inventoryTransferId, inventoryTransferIdType, createdDetail.userId, createdDetail.systemId,
createdDetail.locationId, createdDetail.id as eventId, createdDetail.eventTime,
createdDetail.externalReferenceNumber, createdDetail.transferType, createdDetail.transferContextType,
createdDetail.toLocation.facility, createdDetail.toLocation.logical, explode(createdDetail.productDetails) productDetails
from store_inventory_transfer;

create temporary view kafka_store_inventory_transfer_created as
select
inventoryTransferId as transfer_num,
inventoryTransferIdType as transfer_type,
userId as user_id,
systemId as system_id,
externalReferenceNumber as external_refence_num,
locationId as location_num,
facility as to_location_num,
logical as to_logical_location_num,
eventId as event_id,
eventTime as event_tmstp,
transferType as transfer_type_code,
transferContextType as transfer_context_type,
productDetails.product.id as sku_num,
productDetails.product.idType as sku_type,
productDetails.quantity as sku_qty,
productDetails.disposition as disposition_code
from store_inventory_transfer_extract_created_items;

create temporary view store_inventory_transfer_extract_canceled_items as
select inventoryTransferId, inventoryTransferIdType, canceledDetail.userId, canceledDetail.systemId,
canceledDetail.locationId, canceledDetail.id as eventId, canceledDetail.eventTime,
explode(canceledDetail.productDetails) productDetails
from store_inventory_transfer;

create temporary view kafka_store_inventory_transfer_canceled as
select
inventoryTransferId as transfer_num,
inventoryTransferIdType as transfer_type,
userId as user_id,
systemId as system_id,
locationId as location_num,
eventId as event_id,
eventTime as event_tmstp,
productDetails.product.id as sku_num,
productDetails.product.idType as sku_type,
productDetails.quantity as sku_qty,
productDetails.disposition as disposition_code
from store_inventory_transfer_extract_canceled_items;

-- read from S3
create temporary view orc_vw using ORC
OPTIONS (path "s3://{s3_bucket_orc}/data/ascp-store-inventory-transfer-lifecycle-v1");

--Sink

---Writing Kafka Data to S3 in ORC format
insert overwrite table `ascp-store-inventory-transfer-lifecycle-tmp`
select 
   inventoryTransferId,
   inventoryTransferIdType,
   createdDetail, 
   canceledDetail, 
   shippedDetails, 
   receivedDetails
 FROM ( SELECT u.*
             , Row_Number() Over (PARTITION BY u.inventoryTransferId ORDER BY u.SystemTime DESC) AS rn
         FROM ( SELECT Cast(0 AS BIGINT) as SystemTime,
                     inventoryTransferId,
					 inventoryTransferIdType,
					 createdDetail, 
					 canceledDetail, 
					 shippedDetails, 
					 receivedDetails
                FROM orc_vw
                UNION ALL
                SELECT Cast(headers.SystemTime AS BIGINT) as SystemTime,
                     transferNumber as inventoryTransferId,
					 "TRANSFER" as inventoryTransferIdType,
					 createdDetail, 
					 canceledDetail, 
					 shippedDetails, 
					 receivedDetails
                FROM kafka_inventory_store_inventory_transfer_object_model_avro
              ) AS u
      ) as r
where r.rn = 1;

---Writing Error Data to S3 in csv format
insert overwrite table store_inventory_transfer_receipt
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
FROM kafka_store_inventory_transfer_receipt
where transfer_num is null or transfer_num = '' or transfer_num = '""'
 or transfer_type is null or transfer_type = '' or transfer_type = '""'
 or carton_num is null or carton_num = '' or carton_num = '""'
 or event_id is null or event_id = '' or event_id = '""'
 or sku_num is null or sku_num = '' or sku_num = '""'
 or disposition_code = 'UNKNOWN';

insert overwrite table store_inventory_transfer_shipment
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
  transfer_num,
  transfer_type,
  carton_num,
  shipment_num,
  bill_of_lading,
  location_num,
  to_location_num,
  to_logical_location_num,
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
FROM kafka_store_inventory_transfer_shipment
where transfer_num is null or transfer_num = '' or transfer_num = '""'
 or transfer_type is null or transfer_type = '' or transfer_type = '""'
 or carton_num is null or carton_num = '' or carton_num = '""'
 or event_id is null or event_id = '' or event_id = '""'
 or sku_num is null or sku_num = '' or sku_num = '""'
 or disposition_code = 'UNKNOWN';

insert overwrite table store_inventory_transfer_created
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
  transfer_num,
  transfer_type,
  user_id,
  system_id,
  external_refence_num,
  location_num,
  to_location_num,
  to_logical_location_num,
  event_id,
  event_tmstp,
  transfer_type_code,
  transfer_context_type,
  sku_num,
  sku_type,
  sku_qty,
  disposition_code,
  cast(year(current_date) as string) as year,
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month,
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM kafka_store_inventory_transfer_created
where transfer_num is null or transfer_num = '' or transfer_num = '""'
 or transfer_type is null or transfer_type = '' or transfer_type = '""'
 or event_id is null or event_id = '' or event_id = '""'
 or sku_num is null or sku_num = '' or sku_num = '""'
 or disposition_code = 'UNKNOWN';

insert overwrite table store_inventory_transfer_canceled
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
  transfer_num,
  transfer_type,
  user_id,
  system_id,
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
FROM kafka_store_inventory_transfer_canceled
where transfer_num is null or transfer_num = '' or transfer_num = '""'
 or transfer_type is null or transfer_type = '' or transfer_type = '""'
 or event_id is null or event_id = '' or event_id = '""'
 or sku_num is null or sku_num = '' or sku_num = '""'
 or disposition_code = 'UNKNOWN';

---Writing Kafka Data to Teradata staging table
insert overwrite table store_inventory_transfer_events_ldg_table
SELECT
  transfer_num,
  transfer_type,
  event_id,
  event_tmstp,
  'STORE_TRANSFER_RECEIVED' as event_type,
  shipment_num,
  bill_of_lading,
  carton_num,
  location_num,
  null as to_location_num,
  null as to_logical_location_num,
  null as transfer_type_code,
  null as transfer_context_type,
  sku_num,
  sku_type,
  disposition_code,
  sku_qty,
  null as user_id,
  null as external_refence_num,
  null as system_id
FROM kafka_store_inventory_transfer_receipt
union all
SELECT
  transfer_num,
  transfer_type,
  event_id,
  event_tmstp,
  'STORE_TRANSFER_SHIPPED' as event_type,
  shipment_num,
  bill_of_lading,
  carton_num,
  location_num,
  to_location_num,
  to_logical_location_num,
  null as transfer_type_code,
  null as transfer_context_type,
  sku_num,
  sku_type,
  disposition_code,
  sku_qty,
  null as user_id,
  null as external_refence_num,
  null as system_id
FROM kafka_store_inventory_transfer_shipment
union all
SELECT
  transfer_num,
  transfer_type,
  event_id,
  event_tmstp,
  'STORE_TRANSFER_CREATED' as event_type,
  null as shipment_num,
  null as bill_of_lading,
  null as carton_num,
  location_num,
  to_location_num,
  to_logical_location_num,
  transfer_type_code,
  transfer_context_type,
  sku_num,
  sku_type,
  disposition_code,
  sku_qty,
  user_id,
  external_refence_num,
  system_id
FROM kafka_store_inventory_transfer_created
union all
SELECT
  transfer_num,
  transfer_type,
  event_id,
  event_tmstp,
  'STORE_TRANSFER_CANCELED' as event_type,
  null as shipment_num,
  null as bill_of_lading,
  null as carton_num,
  location_num,
  null as to_location_num,
  null as to_logical_location_num,
  null as transfer_type_code,
  null as transfer_context_type,
  sku_num,
  sku_type,
  disposition_code,
  sku_qty,
  user_id,
  null as external_refence_num,
  system_id
FROM kafka_store_inventory_transfer_canceled;
