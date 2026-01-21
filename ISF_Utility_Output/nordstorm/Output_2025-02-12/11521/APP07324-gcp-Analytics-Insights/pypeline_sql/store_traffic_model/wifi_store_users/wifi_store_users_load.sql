SET QUERY_BAND = 'App_ID=APP08158;
     DAG_ID=wifi_store_users_11521_ACE_ENG;
     Task_Name=wifi_store_users_load;'
     FOR SESSION VOLATILE;

--Wifi users by store and date
--Data sourced from two vendors: RetailNext and NSG
--Business support: Store Traffic  
--ETL support: analytics-engineering in Slack

-- Define new Hive table for output
create table if not exists {hive_schema}.wifi_store_users
(
    store string
    , wifi_users integer
    , wifi_source string
    , business_date date
) 
using ORC
location 's3://{s3_bucket_root_var}/wifi_store_users/'
partitioned by (business_date);

--The -interval '8' hour part will be replaced by the local timezone offset information.
--Local timezone offset handled in CTE before additional transformations in UNION ALL

create or replace temporary view nsg_wifi as
  select
    servername
    , store
    , mac
    , type
    , essid
    , date(from_utc_timestamp(timestamp, store.timezonename)) as local_business_date
    , cast(timestamp as timestamp) as utc_timestamp
  from
    nsp.nap_nsg_wifi_events_orc wifi
  inner join org.store store 
  on cast(cast(wifi.store as int) as varchar(5))= store.storenumber
  where cast(business_date as date) between cast({start_date} as date) and date_add(cast({end_date} as date), 1)
    and date(from_utc_timestamp(timestamp, store.timezonename)) between cast({start_date} as date) and cast({end_date} as date);

create or replace temporary view aruba_wifi as
  select
    accesscontrolmethod
    , store
    , mac
    , type
    , essid
    , date(from_utc_timestamp(timestamp, store.timezonename)) as local_business_date
    , cast(timestamp as timestamp) as utc_timestamp
  from
    nsp.nap_aruba_wifi_events_orc wifi
  inner join org.store store
  on cast(cast(wifi.store as int) as varchar(5))= store.storenumber
  where cast(business_date as date) between cast({start_date} as date) and date_add(cast({end_date} as date), 1)
    and date(from_utc_timestamp(timestamp, store.timezonename)) between cast({start_date} as date) and cast({end_date} as date);

--RetailNext Wifi Aggregated at Store-Day Level with Date
--based on Store's Local Timezone  (For 1Hop Layer)
--RN data available up to March 2020. Date variables added for backfill purposes.
--NSG Raw Wifi Aggregated at Store-Day Level with Date based on UTC Timezone (For 1Hop Layer)
--Will need to pull timezone offset details from store dim table  (hivescno.org.store) IN Presto layer - the 'interval '8' hour' in the following code will be replaced by local timezone offset information
--NSG Cleaned Wifi Aggregated at Store-Day Level with Date based on UTC Timezone  (For 1Hop Layer).
--The query below filters to only include authenticated wifi users who are on the network for more than 5 minutes and less than 5 hours (to filter out employees)

create or replace temporary view wifi_store_users_output_view as 
-- select
--     storeid as store,
--     cast(business_date as date) as business_date,
--     sum(value) as wifi_users,
--     'retailnext' as wifi_source
-- from nsp.nap_rn_wifi_events_orc a
-- where lower(location_type) = 'store'
--   and lower(source) = 'retailnext'
--   and cast(business_date as date) between cast({start_date} as date) and cast({end_date} as date)
-- group by 1, 2
--
-- union all

select
  store,
  cast(local_business_date as date) as business_date,
  count(distinct mac) as wifi_users,
  'nsg_raw_wifi' AS wifi_source
from nsg_wifi
where (lower(serverName) in ('clearpass_guest_vip','retailnext', 'retailnext-gcp') or essid='Nordstrom_Wi-Fi')
group by 1, 2

union all

select
  store,
  local_business_date as business_date,
  count(distinct mac) as wifi_users,
  'nsg_cleaned_wifi' as wifi_source

from
  (
  select
    (max(unix_millis(utc_timestamp)) over (partition by local_business_date, store, mac) - min(unix_millis(utc_timestamp)) over (partition by local_business_date, store, mac))/60000 as diff_timestamp
    , max(case when (lower(servername)  in  ('retailnext', 'retailnext-gcp', 'clearpass_guest_vip') or servername  like 'acp%') then 1 else 0 end) over (partition by mac, local_business_date, store) as  max_retailflag
    , store
    , local_business_date
    , mac
    , servername
    , max(case when lower(type) = 'deviceloggedin' then 1 else 0 end) over (partition by local_business_date, store, mac) as is_logged
  from
    nsg_wifi
  )

where max_retailflag = 1
  and is_logged = 1
 and (diff_timestamp >= 5 and diff_timestamp < 300)
group by 1, 2

union all

select
  store,
  cast(local_business_date as date) as business_date,
  count(distinct mac) as wifi_users,
  'aruba_raw_wifi' AS wifi_source
from aruba_wifi
where accesscontrolmethod in ('MAC based authentication','WEB') OR essid='Nordstrom_Wi-Fi'
group by 1, 2

union all

select
  store,
  local_business_date as business_date,
  count(distinct mac) as wifi_users,
  'aruba_cleaned_wifi' as wifi_source

from
  (
  select
    (max(unix_millis(utc_timestamp)) over (partition by local_business_date, store, mac) - min(unix_millis(utc_timestamp)) over (partition by local_business_date, store, mac))/60000 as diff_timestamp
    , max(case when accesscontrolmethod in ('MAC based authentication','WEB') then 1 else 0 end) over (partition by mac, local_business_date, store) as  max_retailflag
    , store
    , local_business_date
    , mac
    , accesscontrolmethod
    , max(case when lower(type) = 'deviceloggedin' then 1 else 0 end) over (partition by local_business_date, store, mac) as is_logged
  from
    aruba_wifi
  )

where max_retailflag = 1
  and is_logged = 1
 and (diff_timestamp >= 5 and diff_timestamp < 300)
group by 1, 2
;

-- Writing output to new Hive table
insert overwrite table {hive_schema}.wifi_store_users partition (business_date)
select 
store,
wifi_users,
wifi_source,
business_date
from wifi_store_users_output_view
;

msck repair table {hive_schema}.wifi_store_users;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table wifi_store_users_output
select
store,
business_date,
wifi_users,
wifi_source
from {hive_schema}.wifi_store_users
where cast(business_date as date) between cast({start_date} as date) and cast({end_date} as date)
;