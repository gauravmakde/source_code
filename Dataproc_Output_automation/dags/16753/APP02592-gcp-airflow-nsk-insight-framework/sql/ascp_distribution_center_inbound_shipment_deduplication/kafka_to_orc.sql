-- read from kafka
create temporary view kafka_vw as
SELECT unix_millis(objectMetadata.lastUpdatedTime) last_updated_millis
     , inboundShipmentId
     , receivedTime
     , purchaseOrderNumber
     , warehouseEvents
     , vendorBillOfLading
     , carrierBillOfLading
FROM kafka_src;
--save to pre-created table to make Spark use table schema
insert overwrite table ascp.inventory_distribution_center_inbound_shipment_orc_kafka
select last_updated_millis
     , inboundShipmentId
     , receivedTime
     , purchaseOrderNumber
     , warehouseEvents
     , vendorBillOfLading
     , carrierBillOfLading
from kafka_vw;
--deduplicate and write to s3 temp:
insert overwrite table ascp.inventory_distribution_center_inbound_shipment_orc_tmp
select inboundShipmentId
     , receivedTime
     , purchaseOrderNumber
     , warehouseEvents
     , vendorBillOfLading
     , carrierBillOfLading
FROM (
      SELECT u.*
           , Row_Number() Over (PARTITION BY u.inboundShipmentId ORDER BY u.last_updated_millis DESC) AS rn
      FROM (
            SELECT Cast(0 AS BIGINT) last_updated_millis
                 , inboundShipmentId
                 , receivedTime
                 , purchaseOrderNumber
                 , warehouseEvents
                 , vendorBillOfLading
                 , carrierBillOfLading
              FROM ascp.inventory_distribution_center_inbound_shipment_orc_v1
            UNION ALL
            SELECT last_updated_millis
                 , inboundShipmentId
                 , receivedTime
                 , purchaseOrderNumber
                 , warehouseEvents
                 , vendorBillOfLading
                 , carrierBillOfLading
             FROM ascp.inventory_distribution_center_inbound_shipment_orc_kafka
            ) AS u
     ) as t
where rn = 1;
--save to target table
insert overwrite table ascp.inventory_distribution_center_inbound_shipment_orc_v1
SELECT inboundShipmentId
     , receivedTime
     , purchaseOrderNumber
     , warehouseEvents
     , vendorBillOfLading
     , carrierBillOfLading
FROM ascp.inventory_distribution_center_inbound_shipment_orc_tmp;