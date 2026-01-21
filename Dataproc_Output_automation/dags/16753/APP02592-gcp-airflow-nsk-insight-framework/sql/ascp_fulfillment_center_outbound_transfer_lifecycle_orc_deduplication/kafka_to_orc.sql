-- read from kafka
create temporary view kafka_vw as
SELECT unix_millis(objectMetadata.lastUpdatedTime) last_updated_millis
     , transferid
     , transfertype
     , purchaseordernumber
     , transferdetails
FROM kafka_src;
--save to pre-created table to make Spark use table schema
insert overwrite table ascp.fulfillment_center_outbound_transfer_lifecycle_orc_kafka
SELECT last_updated_millis
     , transferid
     , transfertype
     , purchaseordernumber
     , transferdetails
FROM kafka_vw;
--deduplicate and write to s3 temp:
insert overwrite table ascp.fulfillment_center_outbound_transfer_lifecycle_orc_tmp
select transferid
     , transfertype
     , purchaseordernumber
     , transferdetails
from (
      SELECT u.*
           , Row_Number() Over (PARTITION BY u.transferid,u.transfertype ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT Cast(0 AS BIGINT) last_updated_millis
                , transferid
                , transfertype
                , purchaseordernumber
                , transferdetails
             FROM ascp.fulfillment_center_outbound_transfer_lifecycle_orc_v1
           UNION ALL
           SELECT last_updated_millis
                , transferid
                , transfertype
                , purchaseordernumber
                , transferdetails
            FROM ascp.fulfillment_center_outbound_transfer_lifecycle_orc_kafka
           ) AS u
      ) as t
where rn = 1 ;
--save to target table
insert overwrite table ascp.fulfillment_center_outbound_transfer_lifecycle_orc_v1
SELECT transferid
     , transfertype
     , purchaseordernumber
     , transferdetails
FROM ascp.fulfillment_center_outbound_transfer_lifecycle_orc_tmp;