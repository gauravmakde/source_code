SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sku_item_added_11521_ACE_ENG;
     Task_Name=sku_item_added_load;'
     FOR SESSION VOLATILE;

create table if not exists ace_etl.sku_item_added
(
    channelcountry string
    , channelbrand string
    , experience string
    , rms_sku_num string
    , quantity int
    , event_date_pacific date
) 
using ORC
location 's3://ace-etl/sku_item_added/'
partitioned by (event_date_pacific);

create or replace temporary view sku_item_added_res as
select event_date_pacific
       , channelcountry
       , channelbrand
       , experience
       , rms_sku_num
       , quantity
from ( select date(from_utc_timestamp(eventtime, 'US/Pacific')) as event_date_pacific
              , channel.channelcountry
              , channel.channelbrand
              , experience
              , item.productsku.id as rms_sku_num
              , sum(quantity) as quantity
       from acp_event.list_sku_item_added_parquet 
       group by 1,2,3,4,5) a
where event_date_pacific between '2024-06-01' and '2024-06-18'
;

-- Writing output to new Hive table
insert overwrite table ace_etl.sku_item_added partition (event_date_pacific)
select 
channelcountry,
channelbrand,
experience,
rms_sku_num,
quantity,
event_date_pacific
from sku_item_added_res
;

msck repair table ace_etl.sku_item_added;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table sku_item_added_output
select
event_date_pacific,
channelcountry,
channelbrand,
experience,
rms_sku_num,
quantity
from ace_etl.sku_item_added
where event_date_pacific between '2024-06-01' and '2024-06-18'
; 