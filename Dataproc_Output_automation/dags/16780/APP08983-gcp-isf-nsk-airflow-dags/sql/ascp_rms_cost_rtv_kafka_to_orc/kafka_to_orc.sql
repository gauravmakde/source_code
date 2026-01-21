create temporary view kafka_vw as
SELECT
    Cast(headers['SystemTime'] AS BIGINT) last_updated_millis
   ,returnToVendorNumber
   ,returnToVendorEventIds
   ,externalReferenceNumber
   ,fromLocation
   ,vendorNumber
   ,vendorAddress
   ,returnAuthorizationNumber
   ,comments
   ,createTime
   ,latestUpdateTime
   ,latestCancelTime
   ,returnToVendorDetails
   ,returnToVendorCanceledDetails
   ,returnToVendorShippedDetails
FROM kafka_src;

insert overwrite table ascp_inventory_merchandise_return_to_vendor_v0_kafka
select
    last_updated_millis
   ,returnToVendorNumber
   ,returnToVendorEventIds
   ,externalReferenceNumber
   ,fromLocation
   ,vendorNumber
   ,vendorAddress
   ,returnAuthorizationNumber
   ,comments
   ,createTime
   ,latestUpdateTime
   ,latestCancelTime
   ,returnToVendorDetails
   ,returnToVendorCanceledDetails
   ,returnToVendorShippedDetails
from kafka_vw;
