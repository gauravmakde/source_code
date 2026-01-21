SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_store_traffic_11521_ACE_ENG;
     Task_Name=rack_traffic_daily;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_daily
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 07/25/2023

Notes: 
-- Purpose: Applies business rules & cleans up final traffic number. This table supports the view that's used by the end-users.
   This job updates the Rack Store Traffic production table.

-- Update cadence: daily

*/


--drop table rack_traffic_estimation;
create multiset volatile TABLE rack_traffic_estimation as
(
select tr.store_number
    , store_name
    , tr.day_date
    , day_454_num
    , store_open_date
    , store_close_date
    , region
    , dma
    , reopen_date
    , covid_store_closure_flag
    , COALESCE(cmt.corrected_wifi, tr.wifi_users) as wifi_users
    , holiday_flag
    , unplanned_closure
    , purchase_trips
    , net_sales
    , gross_sales
    , camera_flag
    , camera_traffic
    , round(CASE WHEN camera_flag=1 THEN camera_traffic else coalesce(rgt.traffic, estimated_traffic) end,0) as traffic
from {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily tr
      LEFT JOIN {fls_traffic_model_t2_schema}.corrected_modeled_traffic cmt on tr.store_number=cmt.store_number and cmt.day_date=tr.day_date
      LEFT JOIN {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic rgt on tr.store_number=rgt.store_number AND tr.day_date =rgt.day_dt and rgt.day_dt<=date'2019-03-30'
where tr.day_date BETWEEN {start_date} AND {end_date}
)
with data
unique primary index(store_number, day_date)
on commit preserve rows;


/*
Clean up traffic tables
Sets all traffic metrics to NULL where traffic,puchase trips or netsales is NULL or less than 100 or traffic is less than 10. Given we might see some traffic either due to passerbys or employees on days
when stores are closed, applying the following conditions to force traffic to be NULL to
Included <800 store number logic to exclude Canada stores that have conversion as null due to unavailability of Canada traffic data
*/

update rack_traffic_estimation
set traffic = NULL
 -- , purchase_trips = NULL
where ((traffic is NULL or purchase_trips is NULL) and store_number<800) or ((purchase_trips/NULLIF(traffic, 0) < 0.05 or purchase_trips/NULLIF(traffic, 0)> 1.0))
or traffic <10 or net_sales<=100
;

update rack_traffic_estimation
set purchase_trips = NULL
where traffic <10 or net_sales<=100
;
-- store 539 is closed on Sundays
update rack_traffic_estimation
set traffic = NULL
   , purchase_trips = NULL
where store_number in (539) and day_454_num = 1;

--sets traffic metrics to NULL on days with closure closure due to holidays, unplanned fleet-wide & individual store closures
update rack_traffic_estimation
set traffic = NULL
  , purchase_trips = NULL
where covid_store_closure_flag= 1
    or holiday_flag = 1
    or unplanned_closure = 1
    or day_date in (select distinct closure_date from {fls_traffic_model_t2_schema}.fls_unplanned_closures where all_store_flag=1);


--Insert cleaned up traffic data into final prod table
delete from {fls_traffic_model_t2_schema}.rack_traffic_daily where day_date between {start_date} and {end_date};

insert into {fls_traffic_model_t2_schema}.rack_traffic_daily
    select
        store_number
        , store_name
        , region
        , dma
        , case when coalesce(store_close_date,date'2099-12-31')<= CURRENT_DATE then 1 else 0 end as store_closure_flag  -- tags permanent store closure
        , day_date
        --COVID Closure Details
        , reopen_date
        , covid_store_closure_flag
        --Traffic Metrics
        , wifi_users
        , purchase_trips
        , traffic
        , case when camera_flag = 1 then 'Camera' else 'Wifi' end as traffic_source
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    from  rack_traffic_estimation;

--COLLECT STATS
COLLECT STATISTICS COLUMN(store_number), COLUMN(day_date) ON {fls_traffic_model_t2_schema}.rack_traffic_daily;
COLLECT STATISTICS COLUMN (PARTITION , day_date) ON {fls_traffic_model_t2_schema}.rack_traffic_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON {fls_traffic_model_t2_schema}.rack_traffic_daily;
 
SET QUERY_BAND = NONE FOR SESSION;