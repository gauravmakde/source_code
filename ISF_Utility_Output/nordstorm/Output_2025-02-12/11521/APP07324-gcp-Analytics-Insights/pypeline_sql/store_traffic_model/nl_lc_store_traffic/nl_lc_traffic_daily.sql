SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=nl_lc_traffic_daily_11521_ACE_ENG;
     Task_Name=nl_lc_traffic_daily;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.nl_lc_traffic_daily
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/17/2023

Notes: 
-- Supports reporting for Nordstrom Local & Last Chance camera store traffic
-- The code should be set to truncate & update data for last 2 weeks

-- Update cadence: daily

*/


/* Temp tables created
DROP TABLE os_traffic_store_day_base_tmp;
DROP TABLE positive_merch_trx;
DROP TABLE os_traffic_base_data;
*/

/* Create store-day base table*/
CREATE MULTISET VOLATILE TABLE os_traffic_store_day_base_tmp AS
(
    SELECT
        st.store_number
        , st.store_name
        , rl.day_date
        , st.store_close_date
        , st.region
        , st.dma
        , st.reopen_dt
        , st.store_type_code
    FROM
        (SELECT
            a.store_num AS store_number
            , a.store_short_name AS store_name
            -- information on when traffic measurement started in a given store. For stores where the traffic measurement started on a later date, the measurement start date is pulled from the end_date column in fls_camera_store_details table
            , COALESCE(d.end_date,a.store_open_date) AS measurement_start_date
            , a.store_close_date
            , INITCAP(a.region_desc) AS region
            , INITCAP(b.store_dma_desc) AS dma
            , a.store_type_code -- helps differentiate between the two types of stores as they are being treated differently in the downstream code
            , c.reopen_dt
        FROM prd_nap_usr_vws.STORE_DIM a
        LEFT JOIN {fls_traffic_model_t2_schema}.store_covid_reopen_dt c  ON a.store_num = c.store_number
        LEFT JOIN PRD_NAP_BASE_VWS.JWN_STORE_DIM_VW b on a.store_num = b.store_num
        --LEFT JOIN prd_nap_usr_vws.STORE b ON a.store_num = b.store_num
        --joining to camera details table to pull camera traffic measurement start date as for the older stores the traffic measurement didn't start on the same day as the store open date
        LEFT JOIN t2dl_das_fls_traffic_model.fls_camera_store_details d ON a.store_num= d.store_number  --has information on when traffic measurement started in case it doesn't align with store open date
        WHERE a.store_type_code IN ('NL', 'CC')
            AND COALESCE(a.store_close_date, DATE'2099-12-31') >= {start_date}
            AND a.store_open_date <= {end_date}
            AND a.store_num NOT IN (203, 502) -- This is a trunkclub location that is tagged as a Local and won't be removed based on the store closure day filter
        ) st
        CROSS JOIN
        -- The cross is used to create a wireframe with combination of every open fls store & date with the set date range.
        -- Helps avoid dropping any stores or days due to issues with one of the upstream data sources (especially wifi & RetailNext traffic
        (SELECT
            day_date
        FROM prd_nap_usr_vws.DAY_CAL
        WHERE day_date BETWEEN {start_date} AND {end_date}
        ) rl
     -- the two subquries are joined based on the reporting date being between the traffic measurement start data and store permanent closure date. Any store reporting dates outside of this range will be dropped
    WHERE st.measurement_start_date <= day_date AND coalesce(st.store_close_date,date'2099-12-31') >= rl.day_date
)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, day_date)
ON COMMIT PRESERVE ROWS;

/* Purchase trips calculation
 Only caculated for Last Chance stores. This metric is not relevant for Nordstrom Locals
  */
CREATE MULTISET VOLATILE TABLE positive_merch_trx as(
	 SELECT business_day_date
	        , store_number
	        , COUNT(DISTINCT global_tran_id) purchase_trips
	        , SUM(line_net_usd_amt) gross_sales
	FROM ( SELECT dtl.global_tran_id,
			      dtl.business_day_date,
			      dtl.intent_store_num as store_number,
			      CASE WHEN COALESCE(dtl.nonmerch_fee_code, '-999') <> '6666'  THEN dtl.line_net_usd_amt
						ELSE 0 END AS line_net_usd_amt
			      FROM prd_nap_vws.retail_tran_detail_fact_vw dtl
			      LEFT JOIN prd_nap_usr_vws.store_dim st ON dtl.intent_store_num = st.store_num
				  LEFT JOIN prd_nap_usr_vws.department_dim div ON dtl.merch_dept_num = div.dept_num
				    WHERE dtl.business_day_date BETWEEN {start_date} AND {end_date}
				      AND st.store_type_code IN ('CC')
				      AND dtl.line_item_merch_nonmerch_ind = 'MERCH'
				      AND division_num <> 70
			          --AND COALESCE(dtl.nonmerch_fee_code, '-999') <> '6666'
			          AND  line_net_usd_amt>0 ) z
	GROUP BY 1,2
)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, business_day_date)
ON COMMIT PRESERVE ROWS;

/*Pull together all the store, traffic & sales related metrics by store-day*/
CREATE MULTISET VOLATILE TABLE os_traffic_base_data AS
(
    SELECT
          bs.store_number
        , bs.store_name
        , bs.day_date
        , bs.region
        , bs.dma
        , bs.store_type_code
        , bs.store_close_date

        --COVID dates & flags
        , bs.reopen_dt
        , CASE WHEN day_date BETWEEN DATE'2020-03-17' AND reopen_dt-1 THEN 1 ELSE 0 END AS covid_store_closure_flag

        --List of holiday store closures & unplanned closures
        , CASE WHEN ev.thanksgiving = 1 OR ev.christmas = 1 OR ev.easter = 1 THEN 1 ELSE NULL END AS holiday_flag

        --Purchase trips & sales information
        , pt.purchase_trips
        , pt.gross_sales

        --Traffic camera details
        , CASE WHEN imputation_flag IS NOT NULL THEN 1 ELSE 0 END AS camera_flag
        , ctr.traffic


    FROM os_traffic_store_day_base_tmp bs
    LEFT JOIN T3DL_ACE_CORP.znxy_liveramp_events ev ON bs.day_date = ev.event_dt
    LEFT JOIN  t2dl_das_fls_traffic_model.fls_camera_store_details bce ON bs.store_number = bce.store_number
    LEFT JOIN ( SELECT  tr.store_num
				 	   , business_date
				 	   , source_type
				 	   , SUM(traffic_in) AS traffic
				FROM prd_nap_usr_vws.store_traffic_vw tr
				JOIN prd_nap_usr_vws.store_dim st ON tr.store_num = st.store_num
				WHERE measurement_location_type = 'store'
				--AND source_type ='RetailNext'
				AND st.store_type_code IN ('NL', 'CC')
				AND business_date BETWEEN {start_date} AND {end_date}
			    GROUP BY 1,2,3 ) ctr ON bs.store_number = ctr.store_num AND bs.day_date = ctr.business_date
    LEFT JOIN positive_merch_trx pt ON bs.store_number=pt.store_number AND bs.day_date = pt.business_day_date
)
WITH DATA
UNIQUE PRIMARY INDEX(store_number, day_date)
ON COMMIT PRESERVE ROWS;

/*
Clean up traffic tables
*/

--sets all traffic metrics to NULL where traffic,puchase trips or sales is 0.
--includes logic to exclude NLs stores as purchase trips or conversion metrics don't apply to Locals
UPDATE os_traffic_base_data
SET traffic = NULL
   , purchase_trips = NULL
WHERE ((traffic IS NULL OR purchase_trips IS NULL) AND store_type_code = 'CC') OR (gross_sales = 0 AND store_type_code = 'CC');

--sets traffic metrics to NULL on days with closure closure due to holidays, unplanned fleet-wide & individual store closures
UPDATE os_traffic_base_data
SET traffic = NULL
    , purchase_trips = NULL
WHERE covid_store_closure_flag= 1 OR holiday_flag = 1;


--Insert cleaned up traffic data into final prod table
DELETE FROM {fls_traffic_model_t2_schema}.nl_lc_traffic_daily WHERE day_date BETWEEN {start_date} AND {end_date};

INSERT INTO {fls_traffic_model_t2_schema}.nl_lc_traffic_daily
	 SELECT
	      store_number
	    , store_name
	    , region
	    , dma
	    , CASE WHEN COALESCE(store_close_date,DATE'2099-12-31')<= CURRENT_DATE THEN 1 ELSE 0 END AS store_closure_flag  -- tags permanent store closure
	    , day_date
	    --COVID Closure Details
	    , reopen_dt
	    , covid_store_closure_flag
	    --Traffic Metrics
	    , purchase_trips
	    , traffic
	    , CASE WHEN camera_flag = 1 THEN 'Camera' ELSE 'Wifi' END AS traffic_source -- all Locals & Last Chance Stores have camera traffic measurement
	    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
      FROM os_traffic_base_data;

--COLLECT STATS
COLLECT STATISTICS COLUMN(store_number), COLUMN(day_date) ON {fls_traffic_model_t2_schema}.nl_lc_traffic_daily;
COLLECT STATISTICS COLUMN (PARTITION , day_date) ON {fls_traffic_model_t2_schema}.nl_lc_traffic_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON {fls_traffic_model_t2_schema}.nl_lc_traffic_daily;

SET QUERY_BAND = NONE FOR SESSION;