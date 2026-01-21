SET QUERY_BAND = 'App_ID=APP08150; 
     DAG_ID=rn_camera_and_trips_daily_11521_ACE_ENG;
     Task_Name=rn_camera_and_trips_daily;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.rn_camera_and_trips
Team/Owner: tech_ffp_analytics/Selina Song/Matthew Bond

Notes: 
-- This job loads in RetailNext camera data from prd_nap_usr_vws.store_traffic_vw and adds in store purchase trips calculation.
-- We may need to revamp store purchase trips calculation to align with Clarity tables in the future. 
-- Update cadence: daily
-- Lookback window: 15 days

*/

-- create a wireframe with combination of every open store & date with the set date range.
CREATE MULTISET VOLATILE TABLE store_day_base AS
(
	SELECT dcal.day_date,
		   dcal.day_454_num,
		   st.store_num,
		   st.channel,
		   st.store_type_code
	FROM 
		(SELECT 		
			store_num,
			business_unit_desc as channel,
			store_type_code,
			store_open_date
		FROM T2DL_DAS_FLS_Traffic_Model.store_dim_placer_mapping
		) st
	CROSS JOIN
		(SELECT day_date,
				day_454_num
		FROM prd_nap_base_vws.DAY_CAL
		--WHERE month_num >= 202101 and day_date <= current_date - 1 -- retailnext data is up to yesterday
        WHERE day_date between date'2024-06-13' and date'2024-07-13'
		) dcal
	WHERE day_date >= st.store_open_date 
)
WITH DATA
UNIQUE PRIMARY INDEX(store_num, day_date)
ON COMMIT PRESERVE ROWS;


/* Purchase trips calculation - FLS */

--Merch store-day-customer driver table
CREATE MULTISET VOLATILE TABLE traffic_merch_qualifier_dtl AS
(
    SELECT DISTINCT
        dtl.acp_id
        , business_day_date
        , intent_store_num
    FROM prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
        LEFT JOIN prd_nap_base_vws.store_dim st ON dtl.intent_store_num = st.store_num
        LEFT JOIN prd_nap_base_vws.department_dim div on dtl.merch_dept_num = div.dept_num
    WHERE dtl.business_day_date BETWEEN date'2024-06-13' AND date'2024-07-13'
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
    FROM prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
        JOIN prd_nap_base_vws.store_dim st ON dtl.intent_store_num = st.store_num
        join prd_nap_base_vws.day_cal dt on dtl.business_day_date=dt.day_date
    WHERE dtl.business_day_date BETWEEN date'2024-06-13' AND date'2024-07-13'
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
    FROM prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
        JOIN prd_nap_base_vws.store_dim st ON dtl.intent_store_num = st.store_num
        LEFT JOIN prd_nap_base_vws.department_dim div on dtl.merch_dept_num = div.dept_num
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


/* Purchase trips calculation - Rack */

CREATE MULTISET VOLATILE TABLE rack_purchase_trips AS(
    SELECT    dtl.intent_store_num as store_number
                , dtl.business_day_date
                , count(distinct case when line_net_usd_amt>0 then global_tran_id end) as purchase_trips
                , sum(case when line_net_usd_amt>0 then line_net_usd_amt end) as gross_sales
                , sum(line_net_usd_amt) AS net_sales
        FROM prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
        JOIN prd_nap_base_vws.store_dim st ON dtl.intent_store_num = st.store_num
        join prd_nap_base_vws.day_cal dt on dtl.business_day_date=dt.day_date
        WHERE dtl.business_day_date BETWEEN date'2024-06-13' AND date'2024-07-13'
        AND st.store_type_code IN ('RK')
        AND dtl.line_item_merch_nonmerch_ind = 'MERCH'
        GROUP BY 1,2
    ) WITH DATA
UNIQUE PRIMARY INDEX(store_number, business_day_date)
ON COMMIT PRESERVE ROWS;


/* Purchase trips calculation - Last Chance stores */
-- this metric is not relevant for Nordstrom Local stores 

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
			      FROM prd_nap_usr_vws.retail_tran_detail_fact_vw dtl
			      LEFT JOIN prd_nap_base_vws.store_dim st ON dtl.intent_store_num = st.store_num
				  LEFT JOIN prd_nap_base_vws.department_dim div ON dtl.merch_dept_num = div.dept_num
				    WHERE dtl.business_day_date BETWEEN date'2024-06-13' AND date'2024-07-13'
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



/* RetailNext camera traffic & store purchase trips & net sales & gross sales */

CREATE MULTISET VOLATILE TABLE rn_camera_and_trips_base AS
(

/* Full Line stores */ 

SELECT 
	bs.day_date,
	bs.day_454_num,
	bs.store_num,
	bs.channel,
	bs.store_type_code,

    --Purchase trips & sales information
    CASE WHEN bs.day_date between CURRENT_DATE-2 AND CURRENT_DATE then est_pt.estimated_positive_merch_custs 
        ELSE pt.positive_merch_custs END as purchase_trips,
    CASE WHEN bs.day_date between CURRENT_DATE-2 AND CURRENT_DATE then est_pt.netsales 
        ELSE pt.netsales END as net_sales,
    CASE WHEN bs.day_date between CURRENT_DATE-2 AND CURRENT_DATE then est_pt.grosssales 
        ELSE pt.grosssales END as gross_sales,

	--Traffic camera details
	    -- 0: good camera;
        -- 1: impute camera traffic with lr;
        -- 2: start or end of model estimation
	CASE WHEN imputation_flag in (1,0) THEN 1 
		ELSE 0 END AS camera_flag,
    CASE WHEN imputation_flag = 1 AND bs.day_date BETWEEN bce.start_date AND coalesce(bce.end_date, date'2099-12-31')
    	THEN round(bce.intercept + bce.slope * ctr.traffic, 0)
        WHEN imputation_flag = 0 THEN ctr.traffic 
    	ELSE NULL END AS camera_traffic

FROM store_day_base as bs
LEFT JOIN (
    SELECT store_num, business_date, sum(traffic_in) as traffic
    FROM prd_nap_base_vws.store_traffic_vw a
    WHERE measurement_location_type = 'store' -- filter on this to pull store-level numbers
        AND source_type in ('RetailNext')
        AND traffic_in > 0
        AND business_date BETWEEN date'2024-06-13' AND date'2024-07-13'
    GROUP BY 1,2
    ) ctr on bs.store_num = ctr.store_num AND bs.day_date = ctr.business_date
LEFT JOIN t2dl_das_fls_traffic_model.fls_camera_store_details bce 
	on bs.store_num = bce.store_number 
		AND bs.day_date between coalesce(bce.start_date, date'1901-01-01') and coalesce(bce.end_date, date'2099-12-31')
LEFT JOIN kinds_of_trips_dtl pt on bs.store_num = pt.storenumber AND bs.day_date = pt.business_day_date
LEFT JOIN traffic_pm_tran_agg est_pt on bs.store_num = est_pt.storenumber AND bs.day_date = est_pt.business_day_date

WHERE bs.store_type_code = 'FL'

UNION ALL 

/* Rack stores */ 

SELECT 
	bs.day_date,
	bs.day_454_num,
	bs.store_num,
	bs.channel,
	bs.store_type_code,

    --Purchase trips & sales information
    pt.purchase_trips,
    pt.net_sales,
    pt.gross_sales,

	--Traffic camera details
    CASE WHEN bs.day_date between COALESCE(bce.start_date, date'1900-01-01') and COALESCE(bce.end_date, date'2099-12-31') 
    	and imputation_flag = 0 and ctr.traffic is NOT NULL THEN 1 ELSE 0 END AS camera_flag,
    CASE WHEN bs.day_date between COALESCE(bce.start_date, date'1900-01-01') and COALESCE(bce.end_date, date'2099-12-31') 
    	and imputation_flag = 0 THEN ctr.traffic END AS camera_traffic

FROM store_day_base as bs
LEFT JOIN (
    SELECT store_num, business_date, sum(traffic_in) as traffic
    FROM prd_nap_base_vws.store_traffic_vw a
    WHERE measurement_location_type = 'store' -- filter on this to pull store-level numbers
        AND source_type in ('RetailNext')
        AND traffic_in > 0
        AND business_date BETWEEN date'2024-06-13' AND date'2024-07-13'
    GROUP BY 1,2
    ) ctr on bs.store_num = ctr.store_num AND bs.day_date = ctr.business_date
LEFT JOIN t2dl_das_fls_traffic_model.camera_store_details bce 
	on bs.store_num = bce.store_number 
		AND bs.day_date between coalesce(bce.start_date, date'1901-01-01') and coalesce(bce.end_date, date'2099-12-31')
LEFT JOIN rack_purchase_trips pt on bs.store_num = pt.store_number AND bs.day_date = pt.business_day_date

WHERE bs.store_type_code = 'RK'

UNION ALL 

/* Nordstrom local and last chance stores */

SELECT 
	bs.day_date,
	bs.day_454_num,
	bs.store_num,
	bs.channel,
	bs.store_type_code,

    --Purchase trips & sales information
    pt.purchase_trips,
    NULL as net_sales,
    pt.gross_sales,

	--Traffic camera details
    CASE WHEN bs.day_date between COALESCE(bce.start_date, date'1900-01-01') and COALESCE(bce.end_date, date'2099-12-31') 
        and imputation_flag = 0 THEN 1 ELSE 0 END AS camera_flag,
    CASE WHEN bs.day_date between COALESCE(bce.start_date, date'1900-01-01') and COALESCE(bce.end_date, date'2099-12-31') 
        and imputation_flag = 0 THEN ctr.traffic END AS camera_traffic

FROM store_day_base as bs
LEFT JOIN (
    SELECT store_num, business_date, sum(traffic_in) as traffic
    FROM prd_nap_base_vws.store_traffic_vw a
    WHERE measurement_location_type = 'store' -- filter on this to pull store-level numbers
        AND source_type in ('RetailNext')
        AND traffic_in > 0
        AND business_date BETWEEN date'2024-06-13' AND date'2024-07-13'
    GROUP BY 1,2
    ) ctr on bs.store_num = ctr.store_num AND bs.day_date = ctr.business_date
LEFT JOIN t2dl_das_fls_traffic_model.fls_camera_store_details bce 
	on bs.store_num = bce.store_number 
		AND bs.day_date between coalesce(bce.start_date, date'1901-01-01') and coalesce(bce.end_date, date'2099-12-31')
LEFT JOIN positive_merch_trx pt ON bs.store_num = pt.store_number AND bs.day_date = pt.business_day_date

WHERE bs.store_type_code in ('NL','CC')

)
WITH DATA
UNIQUE PRIMARY INDEX(day_date, store_num)
ON COMMIT PRESERVE ROWS;


-- /* Pull in net sales and gross sales from Clarity mothership */

-- CREATE MULTISET VOLATILE TABLE rn_camera_and_trips_base AS
-- (
-- SELECT 
--     st.day_date,
--     st.day_454_num,
--     st.store_num,
--     st.channel,
--     st.store_type_code,
--     st.camera_flag,
--     st.camera_traffic,
--     st.purchase_trips,
--     sum(fsdf.op_gmv_usd_amt) as net_sales,
--     sum(fsdf.fulfilled_demand_usd_amt) as gross_sales
-- FROM rn_camera_and_trips_base_tmp as st 
-- LEFT JOIN PRD_NAP_JWN_METRICS_base_vws.finance_sales_demand_fact as fsdf
--     on st.store_num = fsdf.store_num and st.day_date = fsdf.tran_date
-- WHERE tran_date between date'2024-06-13' and date'2024-07-13'
-- GROUP BY 1,2,3,4,5,6,7,8
-- )
-- WITH DATA
-- UNIQUE PRIMARY INDEX(day_date, store_num)
-- ON COMMIT PRESERVE ROWS;


/* Clean up FLS traffic table */

--sets traffic metrics to NULL where conversion rate applies
update rn_camera_and_trips_base
set camera_traffic = NULL
where store_type_code = 'FL' 
	and (net_sales < 0 and (purchase_trips / NULLIF(camera_traffic, 0) < 0.05 or purchase_trips / NULLIF(camera_traffic, 0) > 0.6));

--store 520 & 73 are closed on Sundays
update rn_camera_and_trips_base
set camera_traffic = NULL
  , purchase_trips = NULL
where store_num in (520, 73)
  and day_454_num = 1;


/* Clean up Rack traffic table */

--sets traffic metrics to NULL where conversion rate applies
--force traffic to be NULL where traffic<10 or net sales <=100 
--given we might see some traffic either due to passerbys or employees on days when stores are closed

update rn_camera_and_trips_base
set camera_traffic = NULL
where store_type_code = 'RK' and 
	(((purchase_trips/NULLIF(camera_traffic, 0) < 0.05 or purchase_trips/NULLIF(camera_traffic, 0)> 1.0))
	or camera_traffic <10 or net_sales<=100)
;

update rn_camera_and_trips_base
set purchase_trips = NULL
where store_type_code = 'RK' 
    and (camera_traffic <10 or net_sales<=100)
;

-- store 539 is closed on Sundays
update rn_camera_and_trips_base
set camera_traffic = NULL
   , purchase_trips = NULL
where store_num in (539) and day_454_num = 1;


/* Clean up Nordstrom Local and last chance store traffic table */

--sets traffic metrics to NULL where traffic, puchase trips or sales is 0.
--includes logic to exclude NLs stores as purchase trips or conversion metrics don't apply to Locals
UPDATE rn_camera_and_trips_base
SET camera_traffic = NULL
   , purchase_trips = NULL
WHERE ((camera_traffic IS NULL OR purchase_trips IS NULL) AND store_type_code = 'CC') OR (gross_sales = 0 AND store_type_code = 'CC');


/* Holiday store closures */

update rn_camera_and_trips_base
set camera_traffic = NULL
  , purchase_trips = NULL
where day_date in ('2021-04-04','2022-04-17','2023-04-09','2024-03-31','2025-04-20') --easter
    or day_date in ('2021-11-25','2022-11-24','2023-11-23','2024-11-28','2025-11-27') --thanksgiving
    or day_date in ('2021-12-25','2022-12-25','2023-12-25','2024-12-25','2025-12-25') --christmas
;


--Insert cleaned up traffic data into final prod table
DELETE
FROM T2DL_DAS_FLS_TRAFFIC_MODEL.rn_camera_and_trips_daily
WHERE day_date between date'2024-06-13' and date'2024-07-13';

insert into T2DL_DAS_FLS_TRAFFIC_MODEL.rn_camera_and_trips_daily
select store_num
     , day_date
     , channel
     , store_type_code
     , camera_flag
     , round(camera_traffic,0) as camera_traffic
     , round(purchase_trips,0) as purchase_trips
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from rn_camera_and_trips_base;


--COLLECT STATS
COLLECT STATISTICS COLUMN (store_num), COLUMN (day_date) ON T2DL_DAS_FLS_TRAFFIC_MODEL.rn_camera_and_trips_daily;

SET QUERY_BAND = NONE FOR SESSION;
