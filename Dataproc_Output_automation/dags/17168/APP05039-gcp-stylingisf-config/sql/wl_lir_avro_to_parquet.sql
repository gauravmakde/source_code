-- --- read from s3 avro:
-- create temporary view lir_event_view as select *, year(eventtime) as year, lpad(month(eventtime),2,'0') as month, lpad(day(eventtime),2,'0') as day, lpad(hour(eventtime),2,'0') as hour from kafka_tbl;

-- --- sink to s3 orc:
-- insert into acp_event_list_item_removed_parquet
-- -- partition(year, month, day, hour)
-- select /*+ REPARTITION(10,year, month, day, hour) */
-- (
--     struct (headers['key'] , headers['value']) as headers, 
--   eventtime, 
--   struct(channelcountry,channelbrand,sellingchannel) as channel , 
--   experience , 
--    struct(idtype,id) as customer, 
--   struct(idtype,id) as listid , 
--   itemid, 
--    struct((struct(idtype,id))) as item,
--    year,
--    month,
--    day,
--    hour
-- )
-- from lir_event_view;

-- Read from S3 Avro
CREATE TEMPORARY VIEW lir_event_view AS 
SELECT 
    *, 
    YEAR(eventtime) AS year, 
    LPAD(MONTH(eventtime), 2, '0') AS month, 
    LPAD(DAY(eventtime), 2, '0') AS day, 
    LPAD(HOUR(eventtime), 2, '0') AS hour 
FROM kafka_tbl;

-- Sink to S3 Parquet (with partitioning hint)
INSERT INTO acp_event_list_item_removed_parquet
SELECT /*+ REPARTITION(10, year, month, day, hour) */
    STRUCT(headers['key'] as key, headers['value'] as value) AS headers, 
    eventtime, 
    STRUCT(channel.channelCountry as channelCountry, channel.channelBrand AS channelBrand, channel.sellingChannel AS sellingChannel) AS channel, 
    experience, 
    STRUCT(customer.idType AS idType, customer.id AS id) AS customer, 
    STRUCT(listId.idType AS idType, listId.id AS id) AS listid, 
    itemid, 
    STRUCT(item.productSku.idType AS idType, item.productSku.id AS id) AS item,
    year,
    month,
    day,
    hour
FROM lir_event_view;
