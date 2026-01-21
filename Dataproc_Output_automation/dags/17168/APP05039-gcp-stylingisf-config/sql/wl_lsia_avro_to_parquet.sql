--- read from kafka avro:
create temporary view lsia_event_view as select *, year(eventtime) as year, lpad(month(eventtime),2,'0') as month, lpad(day(eventtime),2,'0') as day, lpad(hour(eventtime),2,'0') as hour from kafka_tbl;

--- sink to s3 orc:
insert into acp_event_list_sku_item_added_parquet
-- partition(year, month, day, hour)
select /*+ REPARTITION(10,year, month, day, hour) */
*
from lsia_event_view;