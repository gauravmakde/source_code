SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_store_traffic_11521_ACE_ENG;
     Task_Name=fls_traffic_estimation_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_fls_traffic_model.fls_traffic_estimation_daily
Team/Owner: TECH_FFP_ANALYTICS/Agnes Bao
Date Created/Modified: 02/02/2023

Notes:
Supports validation & reporting for FLS store traffic
This job supports the daily reporting for FLS store traffic - with data coming FROM two sources
    - Actual traffic FROM RetailNext (vendor)
    - Estimated traffic FROM wifi-based regression model
Update cadence: daily
Lookback window: 15 days

*/


/* Create store-day base table*/
CREATE MULTISET VOLATILE TABLE fls_traffic_store_day_base_tmp AS
(
    SELECT
        st.store_number
        , st.store_name
        , rl.day_date
        , rl.fiscal_week
        , rl.fiscal_month
        , rl.fiscal_year
        , rl.ly_day_date
        , rl.day_num
        , rl.day_454_num
        , st.store_open_date
        , st.store_close_date
        , st.region
        , st.dma
        , st.reopen_dt
    FROM
        (SELECT
            a.store_num as store_number
            , INITCAP(a.store_name) as store_name
            , CASE WHEN a.store_num = 57 THEN date'2022-05-20' ELSE a.store_open_date END as store_open_date
            , a.store_close_date
            , INITCAP(a.region_desc) as region
            , INITCAP(b.store_dma_desc) as dma
            , reopen_dt
        FROM prd_nap_usr_vws.STORE_DIM a
            LEFT JOIN PRD_NAP_BASE_VWS.JWN_STORE_DIM_VW b on a.store_num = b.store_num
            LEFT JOIN t2dl_das_fls_traffic_model.store_covid_reopen_dt c  ON a.store_num = c.store_number
        WHERE a.store_type_code = 'FL'
            AND coalesce(a.store_close_date, date'2099-12-31') >= current_date()-20
            AND a.store_open_date <= current_date()-1
            AND a.store_num NOT IN (996,813,926,992,922,994,1443,993,387,206,923,925,1446)
        ) st
        CROSS JOIN
        -- The cross is used to create a wireframe with combination of every open fls store & date with the set date range.
        -- Helps avoid dropping any stores or days due to issues with one of the upstream data sources (especially wifi & RetailNext traffic
        (SELECT
            day_date
            , day_num
            , day_desc
            , day_454_num
            , week_num
            , week_454_num as fiscal_week
            , month_num
            , month_454_num as fiscal_month
            , year_num as fiscal_year
            , last_year_day_date_realigned as ly_day_date
        FROM prd_nap_usr_vws.DAY_CAL
        WHERE day_date BETWEEN current_date()-20 AND current_date()-1
        ) rl
    WHERE st.store_open_date <= day_date AND coalesce(st.store_close_date,date'2099-12-31') >= rl.day_date
)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, day_date)
ON COMMIT PRESERVE ROWS;

/* Purchase trips calculation*/

--Merch store-day-customer driver table
CREATE MULTISET VOLATILE TABLE traffic_merch_qualifier_dtl AS
(
    SELECT DISTINCT
        dtl.acp_id
        , business_day_date
        , intent_store_num
    FROM prd_nap_vws.retail_tran_detail_fact_vw dtl
        LEFT JOIN prd_nap_usr_vws.store_dim st ON dtl.intent_store_num = st.store_num
        LEFT JOIN prd_nap_usr_vws.department_dim div on dtl.merch_dept_num = div.dept_num
    WHERE dtl.business_day_date BETWEEN current_date()-20 AND current_date()-1
      AND st.store_type_code IN ('FL')
      AND dtl.acp_id IS NOT NULL
      AND dtl.line_item_merch_nonmerch_ind = 'MERCH'
      AND division_num <> 70
)
WITH DATA
UNIQUE PRIMARY INDEX(acp_id,business_day_date,intent_store_num)
ON COMMIT PRESERVE ROWS;

--This table helps identify trips AND associated net & gross sales
CREATE MULTISET VOLATILE TABLE traffic_trips_dtl AS
(
    SELECT
        dtl.acp_id
        , dtl.intent_store_num as storenumber
        , dtl.business_day_date
        , month_num
        , MAX(CASE WHEN line_item_order_type LIKE 'CustInit%' AND line_item_fulfillment_type = 'StorePickUp' AND data_source_code = 'COM' THEN 1 ELSE 0 END)  bopus_trip
        , MAX(CASE WHEN dtl.line_net_usd_amt>0 THEN 1 ELSE 0 END) trip_qualifier
        , SUM(line_net_usd_amt) netsales
        , SUM(CASE WHEN line_net_usd_amt>0 THEN line_net_usd_amt ELSE 0 END) grosssales
    FROM prd_nap_vws.retail_tran_detail_fact_vw dtl
        JOIN prd_nap_usr_vws.store_dim st ON dtl.intent_store_num = st.store_num
        join prd_nap_usr_vws.day_cal dt on dtl.business_day_date=dt.day_date
    WHERE dtl.business_day_date BETWEEN current_date()-20 AND current_date()-1
        AND st.store_type_code IN ('FL')
        AND dtl.acp_id IS NOT NULL
    GROUP BY 1,2,3,4
)
WITH DATA
UNIQUE PRIMARY INDEX(acp_id,storenumber, business_day_date)
ON COMMIT PRESERVE ROWS;

--Joining merch driver & trips table to identify daily positive merch customers by store
CREATE MULTISET VOLATILE TABLE kinds_of_trips_dtl AS
(
    SELECT
        a.storeNumber
        , a.business_day_date
        , month_num
        , COUNT(DISTINCT a.acp_id) transactors
        , COUNT(DISTINCT CASE WHEN trip_qualifier=1 THEN a.acp_id ELSE NULL END) positive_custs
        , COUNT(DISTINCT CASE WHEN trip_qualifier=1 AND a.acp_id=b.acp_id AND a.business_day_date=b.business_day_date  AND a.storenumber=b.intent_store_num THEN a.acp_id ELSE NULL END) positive_merch_custs --purchase _trip
        , COUNT(DISTINCT CASE WHEN trip_qualifier=1 AND bopus_trip =1 AND a.acp_id=b.acp_id AND a.business_day_date=b.business_day_date  AND a.storenumber=b.intent_store_num THEN a.acp_id ELSE NULL END) positive_merch_custs_bopus --purchase _trip
        , SUM(netsales) as netsales
        , SUM(grosssales) as grosssales
    FROM traffic_trips_dtl a
        LEFT JOIN traffic_merch_qualifier_dtl b on a.acp_id = b.acp_id AND a.business_day_date = b.business_day_date
    GROUP BY 1,2,3
)
WITH DATA
UNIQUE PRIMARY INDEX(storenumber, business_day_date)
ON COMMIT PRESERVE ROWS;

--This table helps identify positive merch transaction. This will be used for estimating purchase trips for current date-2 dates
CREATE MULTISET VOLATILE TABLE traffic_pm_tran_dtl AS
(
    SELECT
          dtl.global_tran_id
        , dtl.intent_store_num as storenumber
        , dtl.business_day_date
        , CASE WHEN dtl.line_net_usd_amt>0 THEN dtl.global_tran_id ELSE NULL END as postive_merch_trx
        , SUM(line_net_usd_amt) netsales
        , SUM(CASE WHEN line_net_usd_amt>0 THEN line_net_usd_amt ELSE 0 END) grosssales
    FROM prd_nap_vws.retail_tran_detail_fact_vw dtl
        JOIN prd_nap_usr_vws.store_dim st ON dtl.intent_store_num = st.store_num
        LEFT JOIN prd_nap_usr_vws.department_dim div on dtl.merch_dept_num = div.dept_num
    WHERE dtl.business_day_date BETWEEN CURRENT_DATE-2 AND CURRENT_DATE
        AND st.store_type_code IN ('FL')
        AND dtl.line_item_merch_nonmerch_ind = 'MERCH'
        AND division_num <> 70
    GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX(global_tran_id,storenumber, business_day_date)
ON COMMIT PRESERVE ROWS;

CREATE
MULTISET VOLATILE TABLE traffic_pm_tran_agg AS
(
    SELECT
          a.storeNumber
        , a.business_day_date
        , agg_merch_trx * COALESCE(trans_to_trips_mltplr,1) as estimated_positive_merch_custs
        , netsales
        , grosssales
    FROM (SELECT  storeNumber
		        , business_day_date
		        , COUNT(DISTINCT postive_merch_trx) as agg_merch_trx
		        , SUM(netsales) as netsales
		        , SUM(grosssales) as grosssales
          FROM traffic_pm_tran_dtl
          GROUP BY 1,2) a
    LEFT JOIN T2DL_DAS_FLS_Traffic_Model.fls_tran_to_trip_estimate_coeff b on a.storenumber = b.store_number AND b.time_period_type='COVID'
)
WITH DATA
UNIQUE PRIMARY INDEX(storenumber, business_day_date)
ON COMMIT PRESERVE ROWS;

--For a ~5 months period starting June 2020, store 4 has different set of cameras going offline at different times
--because of this, we can not use the store-level traffic counts for modeling
--traffic from bad camera and only have to limit the traffic count from cameras that were working all the time during this time period
CREATE MULTISET VOLATILE TABLE good_camera_traffic_store4 as(
     SELECT store_num, business_date, sum(traffic_in) as traffic
     FROM prd_nap_usr_vws.store_traffic_vw a
     JOIN t2dl_das_fls_traffic_model.fls_camera_store_details b on a.store_num=b.store_number AND a.business_date between b.start_date and b.end_date
     WHERE measurement_location_type = 'entrance' -- filter on this to pull store-level numbers
        AND source_type ='RetailNext'
        AND store_num = 4
        AND measurement_location_name like'SD-%' and measurement_location_name not in ('SD-04','SD-05','SD-06')
    GROUP BY 1,2)
with data
unique primary index(store_num, business_date)
on commit preserve rows;


CREATE MULTISET VOLATILE TABLE fls_traffic_base_data AS
(
    SELECT
          bs.store_number
        , bs.store_name
        , bs.day_date
        , bs.fiscal_week
        , bs.fiscal_month
        , bs.fiscal_year
        --, bs.ly_day_date
        , bs.day_num
        , bs.day_454_num
        , bs.store_open_date
        , bs.store_close_date
        , bs.region
        , bs.dma

        --COVID dates & flags
        , bs.reopen_dt
        , CASE WHEN bs.day_date BETWEEN reopen_dt AND date'2021-12-31' THEN 1 ELSE 0 END AS covid_flag
        , CASE WHEN bs.day_date BETWEEN date'2020-03-17' AND coalesce(reopen_dt-1, store_close_date) THEN 1 ELSE 0 END AS covid_store_closure_flag

        --The following case statement applies adjustment to RetailNext wifi for it to align with the new source of wifi data (NSG).
        --The wifi adjustment has 3 parts to it - Base Adjustment+Event/Holiday Based Adjustment+Weekend Adjustment
        --Except for store 342, 353, the NSG wifi data becomes available starting 2019-06-30. For store 342 & 353 it becomes available after March 2020
        , CASE WHEN bs.store_number in (342, 353) AND bs.day_date < date'2020-03-17'
                       THEN (rn_adj.intercept+rn_adj.rn_wifi_slope*rn.wifi_users
                           +(CASE WHEN wifi_event_date IS NOT NULL THEN holiday_dt+holiday_rn_wifi_slope*rn.wifi_users ELSE 0 END)
                           +(CASE WHEN day_454_num in (7,1) THEN weekend_dt+weekend_rn_wifi_slope*rn.wifi_users ELSE 0 END))

               WHEN bs.day_date<date'2019-06-30'
                       THEN (rn_adj.intercept+rn_adj.rn_wifi_slope*rn.wifi_users
                           +(CASE WHEN wifi_event_date IS NOT NULL THEN holiday_dt+holiday_rn_wifi_slope*rn.wifi_users ELSE 0 END)
                           +(CASE WHEN day_454_num in (7,1) THEN weekend_dt+weekend_rn_wifi_slope*rn.wifi_users ELSE 0 END))
               end as rn_adj_wifi_users

           --There are certain cases where the retailnext wifi numbers are too low to apply any adjustment (less than 10), in those cases we directly use the RetailNext numbers
        , COALESCE(wg.wifi_count, CASE WHEN ((bs.store_number in (342, 353) AND bs.day_date<date'2020-03-17') or bs.day_date<date'2019-06-30') AND rn_adj_wifi_users<10
          		 	   THEN rn.wifi_users

               WHEN (bs.store_number in (342, 353) AND bs.day_date<date'2020-03-17') OR bs.day_date<date'2019-06-30'
               		   THEN  rn_adj_wifi_users

               ELSE nsg.wifi_users END) * COALESCE(wccc.level_correction,1) AS wifi_users

        --List of holiday store closures & unplanned closures
        --holidays are moved to fls_traffic_model_mlp
        , CASE WHEN pc.closure_date IS NOT NULL THEN 1 ELSE 0 END AS unplanned_closure

        --Purchase trips & sales information
        , CASE WHEN bs.day_date between CURRENT_DATE-2 AND CURRENT_DATE then est_pt.estimated_positive_merch_custs ELSE pt.positive_merch_custs END as purchase_trips
        , CASE WHEN bs.day_date between CURRENT_DATE-2 AND CURRENT_DATE then est_pt.netsales ELSE pt.netsales END as netsales
        , CASE WHEN bs.day_date between CURRENT_DATE-2 AND CURRENT_DATE then est_pt.grosssales ELSE pt.grosssales END as grosssales
        , pt.positive_merch_custs_bopus

        --Traffic camera details
        --The first case statement tags for stores with working or partially working cameras
            -- 0: good camera;
            -- 1: impute camera traffic with lr;
            -- 2: start or end of model estimation
        --The second case statement applies regression model to estimate traffic for stores with partially working cameras
        , CASE WHEN imputation_flag = 2 AND bs.day_date>=coalesce(bce.end_date, date'1901-01-01') AND bs.day_date<=coalesce(bce.start_date, date'2099-12-31') THEN 1
        	   WHEN imputation_flag in (1,0) THEN 1
        	   ELSE 0 END AS camera_flag
        , CASE WHEN imputation_flag = 1  AND bs.day_date BETWEEN bce.start_date AND coalesce(bce.end_date, date'2099-12-31')
               THEN bce.intercept+bce.slope*(case when bs.store_number = 4 then s4_gc.traffic else ctr.traffic end)
               ELSE ctr.traffic END AS camera_traffic

    FROM fls_traffic_store_day_base_tmp bs
    LEFT JOIN (
                SELECT cast(store as integer) as store_num
                        , business_date
                        , wifi_users
                FROM t2dl_das_fls_traffic_model.wifi_store_users
                WHERE wifi_source='retailnext'
                    AND store NOT IN ('3lab','706t','CEC')
                    AND business_date BETWEEN current_date()-20 AND current_date()-1
               ) rn on bs.store_number = rn.store_num AND bs.day_date = rn.business_date
    LEFT JOIN (
                SELECT cast(store as integer) as store_num
                        , business_date
                        , wifi_users
                FROM t2dl_das_fls_traffic_model.wifi_store_users
                WHERE wifi_source='nsg_cleaned_wifi'
                    AND store NOT IN ('3lab','706t','CEC')
                    AND business_date BETWEEN current_date()-20 AND current_date()-1
               ) nsg on bs.store_number=nsg.store_num AND bs.day_date = nsg.business_date
    LEFT JOIN t2dl_das_fls_traffic_model.fls_rn_nsg_wifi_coef rn_adj on bs.store_number = rn_adj.store_number
    LEFT JOIN t2dl_das_fls_traffic_model.fls_rn_nsg_wifi_event_dt rn_adj_et on bs.day_date = rn_adj_et.wifi_event_date
    LEFT JOIN (
                SELECT closure_date
                    , store_number
                FROM t2dl_das_fls_traffic_model.fls_unplanned_closures
                WHERE all_store_flag <> 1
               ) pc ON bs.store_number = pc.store_number AND bs.day_date = pc.closure_date
    LEFT JOIN (
                SELECT store_num, business_date, sum(traffic_in) as traffic
                FROM prd_nap_usr_vws.store_traffic_vw a
                WHERE measurement_location_type = 'store' -- filter on this to pull store-level numbers
                    AND source_type in ('RetailNext', 'Internal')
                    AND business_date BETWEEN current_date()-20 AND current_date()-1
                GROUP BY 1,2
                ) ctr on bs.store_number = ctr.store_num AND bs.day_date = ctr.business_date
    LEFT JOIN kinds_of_trips_dtl pt on bs.store_number=pt.storenumber AND bs.day_date = pt.business_day_date
    LEFT JOIN traffic_pm_tran_agg est_pt on bs.store_number=est_pt.storenumber AND bs.day_date = est_pt.business_day_date
    LEFT JOIN t2dl_das_fls_traffic_model.fls_camera_store_details bce on bs.store_number = bce.store_number AND bs.day_date between coalesce(bce.start_date, date'1901-01-01') and coalesce(bce.end_date, date'2099-12-31')
    LEFT JOIN good_camera_traffic_store4 s4_gc on bs.store_number=s4_gc.store_num and bs.day_date=s4_gc.business_date
    LEFT JOIN T2DL_DAS_FLS_Traffic_Model.rack_wifi_gap_traffic wg on bs.store_number=wg.store_number and bs.day_date=wg.day_dt
    LEFT JOIN T2DL_DAS_FLS_TRAFFIC_MODEL.wifi_config_change_correction wccc on bs.store_number= wccc.store_number and bs.day_date>=wccc.correction_start_date
)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, day_date)
ON COMMIT PRESERVE ROWS;


/* Calculate traffic*/
CREATE MULTISET VOLATILE TABLE fls_traffic_estimation as
(
   SELECT tr.store_number
       , store_name
       , tr.day_date
       , fiscal_week
       , fiscal_month
       , fiscal_year
       , day_num
       , day_454_num
       , store_open_date
       , store_close_date
       , tr.region
       , dma
       , reopen_dt
       , covid_store_closure_flag
       , wifi_users
       , v2.thanksgiving
       , v2.christmas
       , v2.easter
       , unplanned_closure
       , purchase_trips
       , netsales
       , grosssales
       , positive_merch_custs_bopus
       , camera_flag
       , camera_traffic
       , v2.estimated_traffic
   FROM fls_traffic_base_data tr
   LEFT JOIN T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_model_mlp v2 on tr.store_number=v2.store_number and tr.day_date = v2.day_date
)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, day_date)
ON COMMIT PRESERVE ROWS;

DELETE
FROM T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_estimation_daily
WHERE day_date BETWEEN current_date()-20 AND current_date()-1;

INSERT INTO T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_estimation_daily
SELECT store_number
     , store_name
     , day_date
     , fiscal_week
     , fiscal_month
     , fiscal_year
     , day_num
     , day_454_num
     , store_open_date
     , store_close_date
     , region
     , dma
     , reopen_dt         as reopen_date
     , covid_store_closure_flag
     , thanksgiving
     , christmas
     , easter
     , unplanned_closure
     , wifi_users
     , purchase_trips
     , netsales          as net_sales
     , grosssales        as gross_sales
     , positive_merch_custs_bopus
     , camera_flag
     , camera_traffic
     , estimated_traffic
     , current_timestamp as dw_sys_load_tmstp
FROM fls_traffic_estimation;


--COLLECT STATS
COLLECT STATISTICS COLUMN (store_number), COLUMN (day_date) ON T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_estimation_daily;
COLLECT STATISTICS COLUMN (PARTITION , day_date) ON T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_estimation_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_estimation_daily;

SET QUERY_BAND = NONE FOR SESSION;