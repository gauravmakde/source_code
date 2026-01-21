SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_store_traffic_11521_ACE_ENG;
     Task_Name=rack_traffic_estimation_daily;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_estimation_daily
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 07/25/2023

Notes: 
-- Purpose: Data Pre-processing, collation + base model traffic estimation for the rack store traffic model
-- Update cadence: daily

*/
 

/* Step 1:  Create store-day base table*/
CREATE MULTISET VOLATILE TABLE rack_traffic_store_day_base_tmp AS
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
        , CASE WHEN st.store_number = 120 then 'Seattle-Tacoma Wa' else st.dma end dma
        , st.reopen_dt
    FROM
        (SELECT
            a.store_num as store_number
            , initcap(a.store_name) as store_name
            , a.store_open_date
            , a.store_close_date
            , INITCAP(a.region_desc) as region
            , INITCAP(b.store_dma_desc) as dma
            , reopen_dt
        FROM prd_nap_usr_vws.STORE_DIM a
        LEFT JOIN PRD_NAP_BASE_VWS.JWN_STORE_DIM_VW b on a.store_num = b.store_num
        --LEFT JOIN prd_nap_usr_vws.STORE b ON a.store_num = b.store_num -- this table is a legacy table and will be dropped once all the columns are populated in the prd_nap_usr_vws.STORE_DIM table
        LEFT JOIN {fls_traffic_model_t2_schema}.store_covid_reopen_dt c  ON a.store_num = c.store_number
        WHERE a.store_type_code = 'RK'
            AND coalesce(a.store_close_date, date'2099-12-31') >= {start_date}
            AND a.store_open_date <= {end_date}
            AND lower(a.store_name) not like '%unassigned%'
            AND LOWER(a.group_desc) like 'rack%'
            AND a.store_num NOT IN (924, 919, 928)
        ) st
        CROSS JOIN
        -- The cross is used to create a wireframe with combination of every open fls store & date with the set date range.
        -- Helps avoid dropping any stores or days due to issues with one of the upstream data sources (especially wifi & LP traffic)
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

        WHERE day_date BETWEEN {start_date} AND {end_date}
        ) rl
    WHERE st.store_open_date <= day_date AND coalesce(st.store_close_date,date'2099-12-31') >= rl.day_date
)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, day_date)
ON COMMIT PRESERVE ROWS;



/* Step 2:  Calculate Rack purchase trips by store-day  */
CREATE MULTISET VOLATILE TABLE rack_purchase_trips AS(
    SELECT    dtl.intent_store_num as store_number
                , dtl.business_day_date
                , count(distinct case when line_net_usd_amt>0 then global_tran_id end) as purchase_trips
                , sum(case when line_net_usd_amt>0 then line_net_usd_amt end) as gross_sales
                , sum(line_net_usd_amt) AS net_sales
        FROM prd_nap_vws.retail_tran_detail_fact_vw dtl
        JOIN prd_nap_usr_vws.store_dim st ON dtl.intent_store_num = st.store_num
        join prd_nap_usr_vws.day_cal dt on dtl.business_day_date=dt.day_date
        WHERE dtl.business_day_date BETWEEN {start_date} AND {end_date}
        AND st.store_type_code IN ('RK')
        AND dtl.line_item_merch_nonmerch_ind = 'MERCH'
        GROUP BY 1,2
    ) WITH DATA
UNIQUE PRIMARY INDEX(store_number, business_day_date)
ON COMMIT PRESERVE ROWS;


 --drop table rack_traffic_base_data;
CREATE MULTISET VOLATILE TABLE rack_traffic_base_data AS
(
    SELECT
          bs.store_number
        , bs.store_name
        , bs.day_date
        , bs.fiscal_year
        , bs.fiscal_month
        , bs.fiscal_week
        , bs.day_454_num
        , bs.store_open_date
        , bs.store_close_date
        , bs.region
        , bs.dma

        --COVID dates & flags
        , bs.reopen_dt
        , CASE WHEN bs.day_date BETWEEN date'2020-03-17' AND coalesce(reopen_dt-1, store_close_date) THEN 1 ELSE 0 END AS covid_store_closure_flag

        --Wifi Users
        , COALESCE(wg.wifi_count,nsg.wifi_users) AS wifi_users

        --List of holiday store closures & unplanned closures
        , case when ev.thanksgiving = 1 OR ev.christmas = 1 OR ev.easter = 1 THEN 1 ELSE 0 end as holiday_flag
        , CASE WHEN pc.closure_date IS NOT NULL THEN 1 ELSE 0 END AS unplanned_closure

        --Purchase trips & sales information
        , pt.purchase_trips as purchase_trips
        , pt.net_sales
        , pt.gross_sales

        --Traffic camera details
        , CASE WHEN business_day_date between COALESCE(bce.start_date, date'1900-01-01') and COALESCE(bce.end_date, date'2099-12-31') and imputation_flag = 0 and ctr.traffic is NOT NULL THEN 1 ELSE 0 END AS camera_flag
        , CASE WHEN business_day_date between COALESCE(bce.start_date, date'1900-01-01') and COALESCE(bce.end_date, date'2099-12-31') and imputation_flag = 0 THEN ctr.traffic END AS camera_traffic

    FROM rack_traffic_store_day_base_tmp bs
    --this join pulls wifi by store day.
    LEFT JOIN (
                  SELECT   cast(store as integer) as store_num
                        , business_date
                        , wifi_users
                     FROM {fls_traffic_model_t2_schema}.wifi_store_users
                        WHERE wifi_source='nsg_cleaned_wifi'
                        AND store NOT IN ('3lab','706t','CEC')
                        AND business_date BETWEEN date'2019-06-30' AND {end_date}
                  UNION

                SELECT store_number, day_dt, wifi_count FROM {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic
                       WHERE day_dt <= date'2019-06-29'

                ) nsg on bs.store_number = nsg.store_num AND bs.day_date = nsg.business_date
    --this join pulls information on events & holidays
    LEFT JOIN {fls_traffic_model_t2_schema}.rack_traffic_model_mlp ev ON bs.store_number=ev.store_number AND bs.day_date = ev.day_date
    --this join pulls information on Rack Purchase Trips
    LEFT JOIN rack_purchase_trips pt on bs.store_number=pt.store_number AND bs.day_date = pt.business_day_date
    --this join pulls information on store camera & source details
    LEFT JOIN {fls_traffic_model_t2_schema}.camera_store_details bce on bs.store_number = bce.store_number AND bs.day_date BETWEEN COALESCE(bce.start_date, date'1900-01-01') AND COALESCE(bce.end_date, date'2099-12-31')
    --this join pulls information on unplanned store closures
    LEFT JOIN (
                SELECT closure_date
                    , store_number
                FROM {fls_traffic_model_t2_schema}.fls_unplanned_closures
                WHERE all_store_flag <> 1
                )pc ON bs.store_number = pc.store_number AND bs.day_date = pc.closure_date
    --this join pulls traffic numbers for stores with in store traffic measurement
    LEFT JOIN (
                SELECT store_num, business_date, source_type, sum(traffic_in) as traffic
                FROM prd_nap_usr_vws.store_traffic_vw a
                WHERE measurement_location_type = 'store' -- filter on this to pull store-level numbers
                    AND business_date BETWEEN {start_date} AND {end_date}
                    AND traffic_in > 0
                GROUP BY 1,2,3
                ) ctr on bs.store_number = ctr.store_num AND bs.day_date = ctr.business_date AND ctr.source_type= bce.traffic_source
    --this join pulls imputed wifi numbers for store-days where wifi data is missing
    LEFT JOIN {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic wg on bs.store_number=wg.store_number and bs.day_date=wg.day_dt and wg.day_dt> date'2019-06-29'

)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, day_date)
ON COMMIT PRESERVE ROWS;



DELETE FROM {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily WHERE day_date BETWEEN {start_date} AND {end_date};

INSERT INTO {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily
        SELECT tr.store_number
            , store_name
            , tr.day_date
            , fiscal_week
            , fiscal_month
            , fiscal_year
            , day_454_num
            , store_open_date
            , store_close_date
            , tr.region
            , dma
            , reopen_dt as reopen_date
            , covid_store_closure_flag
            , holiday_flag
            , unplanned_closure
            , wifi_users
            , purchase_trips
            , net_sales
            , gross_sales
            , camera_flag
            , camera_traffic
            , v2.estimated_traffic
            , CURRENT_TIMESTAMP as dw_sys_load_tmstp
        FROM rack_traffic_base_data tr
        LEFT JOIN {fls_traffic_model_t2_schema}.rack_traffic_model_mlp v2 on tr.store_number=v2.store_number and tr.day_date = v2.day_date
        ;


COLLECT STATISTICS COLUMN(store_number), COLUMN(day_date) ON {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily;
COLLECT STATISTICS COLUMN (PARTITION , day_date) ON {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily;

SET QUERY_BAND = NONE FOR SESSION;