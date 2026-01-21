SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sku_item_added_11521_ACE_ENG;
     Task_Name=sku_item_added_load;'
     FOR SESSION VOLATILE;

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
where event_date_pacific between {start_date} and {end_date}
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table sku_item_added_ldg_output
select
event_date_pacific,
channelcountry,
channelbrand,
experience,
rms_sku_num,
quantity
from sku_item_added_res
where event_date_pacific between {start_date} and {end_date}
; 