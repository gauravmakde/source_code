--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : read_store_return_to_vendor_from_kafka_to_teradata.sql
-- Description             : Reading Data from Source Kafka Topic and writing to STORE_RETURN_TO_VENDOR_DETAIL_LDG table
-- Data Source             : Order Object model kafka topic "inventory-store-return-to-vendor-lifecycle-analytical-avro"
-- Reference Documentation : https://confluence.nordstrom.com/display/SCNAP/SIM+16+Store+Return+To+Vendor+Design+and+Events+Mapping
--*************************************************************************************************************************************
/*
ROW_NUMBER() in the preceding CREATE TABLE logic is used to identify duplicate records and select only the most recent record
*/

create temporary view store_inventory_return_to_vendor_rn AS select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
returnToVendorCreatedDetail,
returnToVendorCanceledDetail,
returnToVendorShippedDetails,
row_number() over (partition by returnToVendorNumber order by headers.SystemTime desc) as rn
from kafka_inventory_store_return_to_vendor_object_model_avro;

create temporary view store_inventory_return_to_vendor AS select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
returnToVendorCreatedDetail,
returnToVendorCanceledDetail,
returnToVendorShippedDetails
from store_inventory_return_to_vendor_rn
where rn = 1;

--Created event
create temporary view store_inventory_return_to_vendor_extract_created_items as
select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
returnToVendorCreatedDetail.eventId as eventId,
returnToVendorCreatedDetail.eventTime as eventTime,
returnToVendorCreatedDetail.locationId as locationId,
returnToVendorCreatedDetail.vendorNumber as vendorNumber,
returnToVendorCreatedDetail.returnAuthorizationNumber as returnAuthorizationNumber,
explode(returnToVendorCreatedDetail.productDetails) createdReturnToVendorItemDetails
from store_inventory_return_to_vendor;

create temporary view kafka_store_inventory_return_to_vendor_created as
select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
eventId as CreatedDetail_eventId,
eventTime as CreatedDetail_eventTime,
locationId as CreatedDetail_locationId,
vendorNumber as CreatedDetail_vendorNumber,
returnAuthorizationNumber as CreatedDetail_returnAuthorizationNumber,
createdReturnToVendorItemDetails.product.id as CreatedDetail_productDetails_product_num,
createdReturnToVendorItemDetails.product.idType as CreatedDetail_productDetails_product_idType,
createdReturnToVendorItemDetails.quantity as CreatedDetail_productDetails_quantity,
createdReturnToVendorItemDetails.disposition as CreatedDetail_productDetails_disposition,
createdReturnToVendorItemDetails.reasonCode as CreatedDetail_productDetails_reasonCode
from store_inventory_return_to_vendor_extract_created_items;

--Cancelled event
create temporary view store_inventory_return_to_vendor_extract_canceled_items as
select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
returnToVendorCanceledDetail.eventId as eventId,
returnToVendorCanceledDetail.eventTime as eventTime,
returnToVendorCanceledDetail.locationId as locationId,
explode(returnToVendorCanceledDetail.productDetails) canceledReturnToVendorItemDetails
from store_inventory_return_to_vendor;

create temporary view kafka_store_inventory_return_to_vendor_canceled as
select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
eventId as CanceledDetail_eventId,
eventTime as CanceledDetail_eventTime,
locationId as CanceledDetail_locationId,
canceledReturnToVendorItemDetails.product.id as CanceledDetail_productDetails_product_num,
canceledReturnToVendorItemDetails.product.idType as CanceledDetail_productDetails_product_idType,
canceledReturnToVendorItemDetails.quantity as CanceledDetail_productDetails_quantity,
canceledReturnToVendorItemDetails.disposition as CanceledDetail_productDetails_disposition,
canceledReturnToVendorItemDetails.reasonCode as CanceledDetail_productDetails_reasonCode
from store_inventory_return_to_vendor_extract_canceled_items;

--Shipped event
create temporary view store_inventory_return_to_vendor_extract_shipped as
select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
returnToVendorShippedDetails.eventId as eventId,
returnToVendorShippedDetails.eventTime as eventTime,
returnToVendorShippedDetails.locationId as locationId,
explode(returnToVendorShippedDetails) returnToVendorShippedDetails
from store_inventory_return_to_vendor;

create temporary view store_inventory_return_to_vendor_extract_shipped_items as
select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
returnToVendorShippedDetails.eventId as eventId,
returnToVendorShippedDetails.eventTime as eventTime,
returnToVendorShippedDetails.locationId as locationId,
explode(returnToVendorShippedDetails.productDetails) shippedReturnToVendorItemDetails
from store_inventory_return_to_vendor_extract_shipped;

create temporary view kafka_store_inventory_return_to_vendor_shipped as
select
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
eventId as ShippedDetail_eventId,
eventTime as ShippedDetail_eventTime,
locationId as ShippedDetail_locationId,
shippedReturnToVendorItemDetails.product.id as ShippedDetail_productDetails_product_num,
shippedReturnToVendorItemDetails.product.idType as ShippedDetail_productDetails_product_idType,
shippedReturnToVendorItemDetails.quantity as ShippedDetail_productDetails_quantity,
shippedReturnToVendorItemDetails.disposition as ShippedDetail_productDetails_disposition
from store_inventory_return_to_vendor_extract_shipped_items;

---Writing Error Data to S3 in csv format
insert overwrite table store_return_to_vendor_created
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
CreatedDetail_eventId,
CreatedDetail_eventTime,
CreatedDetail_locationId,
CreatedDetail_vendorNumber,
CreatedDetail_returnAuthorizationNumber,
CreatedDetail_productDetails_product_num ,
CreatedDetail_productDetails_product_idType,
CreatedDetail_productDetails_quantity ,
CreatedDetail_productDetails_disposition,
CreatedDetail_productDetails_reasonCode,
  cast(year(current_date) as string) as year,
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month,
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM kafka_store_inventory_return_to_vendor_created
where returnToVendorNumber is null or returnToVendorNumber = '' or returnToVendorNumber = '""'
 or CreatedDetail_eventId is null or CreatedDetail_eventId = '' or CreatedDetail_eventId = '""'
 or CreatedDetail_productDetails_product_num is null or CreatedDetail_productDetails_product_num = '' or CreatedDetail_productDetails_product_num = '""'
 or CreatedDetail_productDetails_disposition = 'UNKNOWN';

--canceled event
insert overwrite table store_return_to_vendor_canceled
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
CanceledDetail_eventId,
CanceledDetail_eventTime,
CanceledDetail_locationId,
CanceledDetail_productDetails_product_num ,
CanceledDetail_productDetails_product_idType,
CanceledDetail_productDetails_quantity ,
CanceledDetail_productDetails_disposition,
CanceledDetail_productDetails_reasonCode,
  cast(year(current_date) as string) as year,
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month,
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM kafka_store_inventory_return_to_vendor_canceled
where returnToVendorNumber is null or returnToVendorNumber = '' or returnToVendorNumber = '""'
 or CanceledDetail_eventId is null or CanceledDetail_eventId = '' or CanceledDetail_eventId = '""'
 or CanceledDetail_productDetails_product_num is null or CanceledDetail_productDetails_product_num = '' or CanceledDetail_productDetails_product_num = '""'
 or CanceledDetail_productDetails_disposition = 'UNKNOWN';

--shipped event
insert overwrite table store_return_to_vendor_shipped
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
returnToVendorNumber,
createdSystemTime,
lastUpdatedSystemTime,
lastTriggeringEventType,
ShippedDetail_eventId,
ShippedDetail_eventTime,
ShippedDetail_locationId,
ShippedDetail_productDetails_product_num,
ShippedDetail_productDetails_product_idType,
ShippedDetail_productDetails_quantity,
ShippedDetail_productDetails_disposition,
  cast(year(current_date) as string) as year,
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month,
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM kafka_store_inventory_return_to_vendor_shipped
where returnToVendorNumber is null or returnToVendorNumber = '' or returnToVendorNumber = '""'
 or ShippedDetail_eventId is null or ShippedDetail_eventId = '' or ShippedDetail_eventId = '""'
 or ShippedDetail_productDetails_product_num is null or ShippedDetail_productDetails_product_num = '' or ShippedDetail_productDetails_product_num = '""'
 or ShippedDetail_productDetails_disposition = 'UNKNOWN';

---Writing Kafka Data to Teradata staging table
insert overwrite table store_return_to_vendor_detail_ldg_table
SELECT
      u.return_to_vendor_num,
      u.sku_num,
      u.sku_type,
      u.location_num,
      u.created_return_authorization_num,
      u.created_event_id,
      u.created_event_tmstp,
      u.created_vendor_num,
      u.created_qty,
      u.created_disposition_code,
      u.created_reason_code,
      u.canceled_event_id,
      u.canceled_event_tmstp,
      u.canceled_qty,
      u.canceled_disposition_code,
      u.canceled_reason_code,
      u.shipped_event_id,
      u.shipped_event_tmstp,
      u.shipped_qty,
      u.shipped_disposition_code,
      u.created_sys_tmstp,
      u.last_updated_sys_tmstp,
      u.last_triggering_event_type,
      current_timestamp as dw_sys_load_tmstp
FROM
      (
            SELECT
                  coalesce(created.returnToVendorNumber, canceled.returnToVendorNumber, shipped.returnToVendorNumber) as return_to_vendor_num,
                  coalesce(CreatedDetail_productDetails_product_num, CanceledDetail_productDetails_product_num, ShippedDetail_productDetails_product_num) as sku_num,
                  coalesce(CreatedDetail_productDetails_product_idType, CanceledDetail_productDetails_product_idType,ShippedDetail_productDetails_product_idType) as sku_type,
                  coalesce(CreatedDetail_locationId, CanceledDetail_locationId, ShippedDetail_locationId) as location_num,
                  CreatedDetail_returnAuthorizationNumber as created_return_authorization_num,
                  CreatedDetail_eventId as created_event_id,
                  CreatedDetail_eventTime as created_event_tmstp,
                  CreatedDetail_vendorNumber as created_vendor_num,
                  CreatedDetail_productDetails_quantity as created_qty,
                  CreatedDetail_productDetails_disposition as created_disposition_code,
                  CreatedDetail_productDetails_reasonCode as created_reason_code,
                  CanceledDetail_eventId as canceled_event_id,
                  CanceledDetail_eventTime as canceled_event_tmstp,
                  CanceledDetail_productDetails_quantity as canceled_qty,
                  CanceledDetail_productDetails_disposition as canceled_disposition_code,
                  CanceledDetail_productDetails_reasonCode as canceled_reason_code,
                  ShippedDetail_eventId as shipped_event_id,
                  ShippedDetail_eventTime as shipped_event_tmstp,
                  ShippedDetail_productDetails_quantity as shipped_qty,
                  ShippedDetail_productDetails_disposition as shipped_disposition_code,
                  coalesce(created.createdSystemTime, canceled.createdSystemTime, shipped.createdSystemTime) as created_sys_tmstp,
                  coalesce(created.lastUpdatedSystemTime, canceled.lastUpdatedSystemTime, shipped.lastUpdatedSystemTime) as last_updated_sys_tmstp,
                  coalesce(created.lastTriggeringEventType, canceled.lastTriggeringEventType, shipped.lastTriggeringEventType) as last_triggering_event_type,
                  ROW_NUMBER() OVER (PARTITION BY coalesce(created.returnToVendorNumber, canceled.returnToVendorNumber, shipped.returnToVendorNumber),
                        coalesce(canceled.CanceledDetail_productDetails_product_num, created.CreatedDetail_productDetails_product_num, shipped.ShippedDetail_productDetails_product_num),
                        coalesce(canceled.CanceledDetail_productDetails_product_idType, created.CreatedDetail_productDetails_product_idType, shipped.ShippedDetail_productDetails_product_idType),
                        coalesce(canceled.CanceledDetail_locationId, created.CreatedDetail_locationId, shipped.ShippedDetail_locationId)
                        ORDER BY coalesce(created.lastUpdatedSystemTime, canceled.lastUpdatedSystemTime, shipped.lastUpdatedSystemTime)
                  ) AS rn
            FROM
                  kafka_store_inventory_return_to_vendor_created created
                  FULL OUTER JOIN kafka_store_inventory_return_to_vendor_shipped shipped
                  ON created.returnToVendorNumber = shipped.returnToVendorNumber
                  AND created.CreatedDetail_productDetails_product_num = shipped.ShippedDetail_productDetails_product_num
                  AND created.CreatedDetail_productDetails_product_idType = shipped.ShippedDetail_productDetails_product_idType
                  AND created.CreatedDetail_locationId = shipped.ShippedDetail_locationId
                  FULL OUTER JOIN kafka_store_inventory_return_to_vendor_canceled canceled
                  ON coalesce(created.returnToVendorNumber, shipped.returnToVendorNumber) = canceled.returnToVendorNumber
                  AND coalesce(created.CreatedDetail_productDetails_product_num, shipped.ShippedDetail_productDetails_product_num) = canceled.CanceledDetail_productDetails_product_num
                  AND coalesce(created.CreatedDetail_productDetails_product_idType, shipped.ShippedDetail_productDetails_product_idType) = canceled.CanceledDetail_productDetails_product_idType
                  AND coalesce(created.CreatedDetail_locationId, shipped.ShippedDetail_locationId) = canceled.CanceledDetail_locationId
      ) AS u
WHERE
      u.rn = 1;
