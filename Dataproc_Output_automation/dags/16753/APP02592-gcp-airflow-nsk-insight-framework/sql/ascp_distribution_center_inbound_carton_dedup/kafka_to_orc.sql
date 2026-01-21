-- read from kafka
create temporary view kafka_vw as
SELECT unix_millis(objectMetadata.lastUpdatedTime) last_updated_millis
      , inboundcartonid
      , receivedtime
      , cartonlpn
      , purchaseordernumber
      , locationid
      , warehouseevents
      , initialreceiveditems
      , latestreceiveditems
      , itemquantityadjustmentdetails
      , vendorbilloflading
      , carrierbilloflading
      , auditdetails
      , allAuditsCompleted
      , allAuditsCompletedTime
      , lastUpdatedSystemTime
      , inventorySplitDetails
FROM kafka_src;

--save to pre-created table to make Spark use table schema
insert overwrite table ascp.inventory_distribution_center_inbound_carton_orc_kafka
select last_updated_millis
     , inboundcartonid
     , receivedtime
     , cartonlpn
     , purchaseordernumber
     , locationid
     , warehouseevents
     , initialreceiveditems
     , latestreceiveditems
     , itemquantityadjustmentdetails
     , vendorbilloflading
     , carrierbilloflading
     , auditdetails
     , allAuditsCompleted
     , allAuditsCompletedTime
     , lastUpdatedSystemTime
     , inventorySplitDetails
  from kafka_vw;
-- deduplicate and write to s3 temp:
insert overwrite table ascp.inventory_distribution_center_inbound_carton_orc_tmp
SELECT inboundcartonid
     , receivedtime
     , cartonlpn
     , purchaseordernumber
     , locationid
     , warehouseevents
     , initialreceiveditems
     , latestreceiveditems
     , itemquantityadjustmentdetails
     , vendorbilloflading
     , carrierbilloflading
     , auditdetails
     , allAuditsCompleted
     , allAuditsCompletedTime
     , lastUpdatedSystemTime
     , inventorySplitDetails
FROM (
       SELECT u.*
             , Row_Number() Over (PARTITION BY u.inboundcartonid ORDER BY u.last_updated_millis DESC) AS rn
         FROM (
             SELECT Cast(0 AS BIGINT) last_updated_millis
                  , inboundcartonid
                  , receivedtime
                  , cartonlpn
                  , purchaseordernumber
                  , locationid
                  , warehouseevents
                  , initialreceiveditems
                  , latestreceiveditems
                  , itemquantityadjustmentdetails
                  , vendorbilloflading
                  , carrierbilloflading
                  , auditdetails
                  , allAuditsCompleted
                  , allAuditsCompletedTime
                  , lastUpdatedSystemTime
                  , inventorySplitDetails
               FROM ascp.inventory_distribution_center_inbound_carton_orc_v1
             UNION ALL
             SELECT last_updated_millis
                  , inboundcartonid
                  , receivedtime
                  , cartonlpn
                  , purchaseordernumber
                  , locationid
                  , warehouseevents
                  , initialreceiveditems
                  , latestreceiveditems
                  , itemquantityadjustmentdetails
                  , vendorbilloflading
                  , carrierbilloflading
                  , auditdetails
                  , allAuditsCompleted
                  , allAuditsCompletedTime
                  , lastUpdatedSystemTime
                  , inventorySplitDetails
              FROM ascp.inventory_distribution_center_inbound_carton_orc_kafka
             ) AS u
) as t
where rn = 1;
--save to target table
insert overwrite table ascp.inventory_distribution_center_inbound_carton_orc_v1
SELECT inboundcartonid
     , receivedtime
     , cartonlpn
     , purchaseordernumber
     , locationid
     , warehouseevents
     , initialreceiveditems
     , latestreceiveditems
     , itemquantityadjustmentdetails
     , vendorbilloflading
     , carrierbilloflading
     , auditdetails
     , allAuditsCompleted
     , allAuditsCompletedTime
     , lastUpdatedSystemTime
     , inventorySplitDetails
FROM ascp.inventory_distribution_center_inbound_carton_orc_tmp;