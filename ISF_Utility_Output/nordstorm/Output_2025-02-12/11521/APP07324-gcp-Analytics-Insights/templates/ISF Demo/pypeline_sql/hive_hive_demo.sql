--- Reading data from internal hive tables is straightforward
create or replace temp view add_to_bag_events as
select * from acp_event.customer_activity_added_to_bag 
where year = 2022 and month = 6 and day = 1
limit 100;

-- This would be analagous to the above code, BUT not as performant as reading from Hive:
--- CREATE TEMPORARY VIEW add_to_bag USING orc OPTIONS (path "s3://tf-nap-prod-acp-presentation/event/customer-activity-addedtobag-v1/year=2022/month=06/day=01");
--- CREATE TEMPORARY VIEW add_to_bag_slim as select * from add_to_bag where year = 2022 and month = 6 and day = 1 limit 100;


--- Creating our own hive tables is no different than how it was done in Beetle
--- {s3_bucket_root_var} is interpolated based on environment CFG
--- this means in dev it will be `acedev-etl` and in prod it will be `ace-etl`
--- similarly, {hive_schema} is inerpolated the same way, and will either be `acedev_etl` or `ace_etl`

create table if not exists {hive_schema}.demo_add_to_bag
(
    funnel_event_key string
    , event_id string
    , event_timestamp_utc timestamp 
    , event_type string 
    , header_keys array<string>
    , identified_bot string
    , app_id string
    , cust_id_type string
    , cust_id string 
    , icon_id string
    , shopper_id string
    , channelcountry string
    , channel string
    , platform string
    , source_feature string
    , style_id_type string 
    , style_id string 
    , sku_id_type string
    , sku_id string 
    , bag_type string
    , quantity integer
    , event_date_pacific date
)
using PARQUET
location 's3://{s3_bucket_root_var}/demo_add_to_bag'
partitioned by (event_date_pacific);

--- now the ETL step
create or replace temporary view add_to_bag_funnel_out as
select distinct
    cast(stg.headers.Id as string) as funnel_event_key
    , cast(stg.headers.Id as string) as event_id
    ,cast(from_unixtime(cast(cast(stg.headers.EventTime as string) as double)/1000) as timestamp) as event_timestamp_utc
    , 'add_to_bag' as event_type
    , map_keys(stg.headers) as header_keys
    , cast(stg.headers['identified-bot'] as string) as identified_bot
    , cast(stg.headers['AppId'] as string) as app_id
    , stg.customer.idType as cust_id_type
    , stg.customer.id as cust_id
    , stg.credentials.iconId as icon_id
    , stg.credentials.shopperId as shopper_id
    , stg.source.channelcountry as channelcountry
    , stg.source.channel as channel
    , stg.source.platform as platform
    , stg.source.feature as source_feature
    , stg.productstyle.idType as style_id_type
    , stg.productstyle.id as style_id
    , stg.product.idType as sku_id_type
    , stg.product.id as sku_id
    , stg.bagType as bag_type
    , stg.quantity as quantity
    , date(
        from_utc_timestamp(
        from_unixtime(
            cast(cast(stg.headers.EventTime as string) as double)/1000
        ), 'US/Pacific')
    ) as event_date_pacific
from add_to_bag_events stg
--checks against existing data in sink so that no duplicate events are added
left join {hive_schema}.demo_add_to_bag prd
on cast(stg.headers.Id as string) = prd.funnel_event_key
where prd.funnel_event_key is null;

--- now the INSERT into our hive table
--- the Hive table will have the data queryable from presto and hive cli
insert into table {hive_schema}.demo_add_to_bag partition (event_date_pacific)
select *
from add_to_bag_funnel_out;

