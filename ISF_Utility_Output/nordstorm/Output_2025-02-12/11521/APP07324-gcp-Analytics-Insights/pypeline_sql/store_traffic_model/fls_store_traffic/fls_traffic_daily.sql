SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_store_traffic_11521_ACE_ENG;
     Task_Name=fls_traffic_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_fls_traffic_model.fls_traffic_daily
Team/Owner: TECH_FFP_ANALYTICS/Agnes Bao
Date Created/Modified: 02/02/2023

Notes:
-- This job updates the FLS Store Traffic production table
-- Update cadence: daily
-- Lookback window: 15 days

 */

CREATE multiset volatile TABLE fls_traffic_estimation as
(
select tr.store_number
    , store_name
    , tr.day_date
    , fiscal_week
    , fiscal_month
    , fiscal_year
    , day_num
    , day_454_num
    , store_open_date
    , store_close_date
    , region
    , dma
    , reopen_date
    , covid_store_closure_flag
    , COALESCE(cmt.corrected_wifi, tr.wifi_users) as wifi_users
    , thanksgiving
    , christmas
    , easter
    , unplanned_closure
    , purchase_trips
    , net_sales
    , gross_sales
    , positive_merch_custs_bopus
    , camera_flag
    , camera_traffic
    , estimated_traffic
    , CASE WHEN camera_flag=1 THEN round(camera_traffic, 0) else round(estimated_traffic, 0) end as traffic
FROM {fls_traffic_model_t2_schema}.fls_traffic_estimation_daily tr
LEFT JOIN t2dl_das_fls_traffic_model.corrected_modeled_traffic cmt on tr.store_number=cmt.store_number and cmt.day_date=tr.day_date
where tr.day_date between {start_date} and {end_date}
)
WITH DATA
UNIQUE PRIMARY index(store_number, day_date)
ON COMMIT PRESERVE ROWS;


/*
Clean up traffic tables
Sets all traffic metrics to NULL where traffic,puchase trips or netsales is NULL or less than 0.
Included <800 store number logic to exclude Canada stores that have conversion as null due to unavailability of Canada traffic data
*/
update fls_traffic_estimation
set traffic        = NULL
--   , purchase_trips = NULL
where ((traffic is NULL or purchase_trips is NULL) and store_number < 800)
   or (net_sales < 0 and (purchase_trips / NULLIF(traffic, 0) < 0.05 or purchase_trips / NULLIF(traffic, 0) > 0.6));

--store 520 & 73 are closed on Sundays
update fls_traffic_estimation
set traffic        = NULL
  , purchase_trips = NULL
where store_number in (520, 73)
  and day_454_num = 1;

--sets traffic metrics to NULL on days with closure
--closure due to holidays, unplanned fleet-wide & individual store closures
update fls_traffic_estimation
set traffic        = NULL
  , purchase_trips = NULL
where covid_store_closure_flag = 1
   or christmas = 1
   or thanksgiving = 1
   or easter = 1
   or unplanned_closure = 1
   or day_date in
      (select distinct closure_date from t2dl_das_fls_traffic_model.fls_unplanned_closures where all_store_flag = 1);

--Insert cleaned up traffic data into final prod table
DELETE
FROM {fls_traffic_model_t2_schema}.fls_traffic_daily
WHERE day_date between {start_date} and {end_date};

insert into {fls_traffic_model_t2_schema}.fls_traffic_daily
select store_number
     , store_name
     , region
     , dma
     , case
           when coalesce(store_close_date, date '2099-12-31') <= CURRENT_DATE then 1
           else 0 end                                          as store_closure_flag -- tags permanent store closure
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
from fls_traffic_estimation;


--COLLECT STATS
COLLECT STATISTICS COLUMN (store_number), COLUMN (day_date) ON {fls_traffic_model_t2_schema}.fls_traffic_daily;
COLLECT STATISTICS COLUMN (PARTITION , day_date) ON {fls_traffic_model_t2_schema}.fls_traffic_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON {fls_traffic_model_t2_schema}.fls_traffic_daily;

SET QUERY_BAND = NONE FOR SESSION;