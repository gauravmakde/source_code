
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08150;
DAG_ID=rn_camera_and_trips_daily_11521_ACE_ENG;
---     Task_Name=rn_camera_and_trips_daily;'*/
BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS store_day_base
AS
SELECT dcal.day_date,
 dcal.day_454_num,
 st.store_num,
 st.channel,
 st.store_type_code
FROM (SELECT store_num,
   business_unit_desc AS channel,
   store_type_code,
   store_open_date
  FROM `{{params.gcp_project_id}}`.t2dl_das_fls_traffic_model.store_dim_placer_mapping) AS st
 INNER JOIN (SELECT day_date,
   day_454_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal
  WHERE day_date BETWEEN {{params.start_date}} and {{params.end_date}}
  ) AS dcal 
  ON dcal.day_date >= CAST(st.store_open_date AS DATE);
  
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

/* Purchase trips calculation - FLS */

--Merch store-day-customer driver table

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS traffic_merch_qualifier_dtl
AS
SELECT DISTINCT dtl.acp_id,
 dtl.business_day_date,
 dtl.intent_store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st ON dtl.intent_store_num = st.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim AS div ON CAST(dtl.merch_dept_num AS FLOAT64) = div.dept_num
WHERE dtl.business_day_date BETWEEN {{params.start_date}} and {{params.end_date}}
 AND LOWER(st.store_type_code) IN (LOWER('FL'))
 AND LOWER(dtl.line_item_merch_nonmerch_ind) = LOWER('MERCH')
 AND div.division_num <> 70
 AND dtl.acp_id IS NOT NULL;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

--This table helps identify trips AND associated net & gross sales

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS traffic_trips_dtl
AS
SELECT dtl.acp_id,
 dtl.intent_store_num AS storenumber,
 dtl.business_day_date,
 dt.month_num,
 MAX(CASE
   WHEN LOWER(dtl.line_item_order_type) LIKE LOWER('CustInit%') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
       ) AND LOWER(dtl.data_source_code) = LOWER('COM')
   THEN 1
   ELSE 0
   END) AS bopus_trip,
 MAX(CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN 1
   ELSE 0
   END) AS trip_qualifier,
 SUM(dtl.line_net_usd_amt) AS netsales,
 SUM(CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS grosssales
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st ON dtl.intent_store_num = st.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS dt ON dtl.business_day_date = dt.day_date
WHERE dtl.business_day_date BETWEEN {{params.start_date}} and {{params.end_date}}
 AND LOWER(st.store_type_code) IN (LOWER('FL'))
 AND dtl.acp_id IS NOT NULL
GROUP BY dtl.acp_id,
 storenumber,
 dtl.business_day_date,
 dt.month_num;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

--Joining merch driver & trips table to identify daily positive merch customers by store

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS kinds_of_trips_dtl
AS
SELECT a.storenumber,
 a.business_day_date,
 a.month_num,
 COUNT(DISTINCT a.acp_id) AS transactors,
 COUNT(DISTINCT CASE
   WHEN a.trip_qualifier = 1
   THEN a.acp_id
   ELSE NULL
   END) AS positive_custs, --purchase _trip
 COUNT(DISTINCT CASE
   WHEN a.trip_qualifier = 1 AND LOWER(a.acp_id) = LOWER(b.acp_id) AND a.business_day_date = b.business_day_date AND a.storenumber
      = b.intent_store_num
   THEN a.acp_id
   ELSE NULL
   END) AS positive_merch_custs,
 COUNT(DISTINCT CASE
   WHEN a.trip_qualifier = 1 AND a.bopus_trip = 1 AND LOWER(a.acp_id) = LOWER(b.acp_id) AND a.business_day_date = b.business_day_date
       AND a.storenumber = b.intent_store_num
   THEN a.acp_id
   ELSE NULL
   END) AS positive_merch_custs_bopus, --purchase _trip
 SUM(a.netsales) AS netsales,
 SUM(a.grosssales) AS grosssales
FROM traffic_trips_dtl AS a
 LEFT JOIN traffic_merch_qualifier_dtl AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.business_day_date = b.business_day_date
   
GROUP BY a.storenumber,
 a.business_day_date,
 a.month_num;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

--This table helps identify positive merch transaction. This will be used for estimating purchase trips for current date-2 dates

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS traffic_pm_tran_dtl
AS
SELECT dtl.global_tran_id,
 dtl.intent_store_num AS storenumber,
 dtl.business_day_date,
  CASE
  WHEN dtl.line_net_usd_amt > 0
  THEN dtl.global_tran_id
  ELSE NULL
  END AS postive_merch_trx,
 SUM(dtl.line_net_usd_amt) AS netsales,
 SUM(CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS grosssales
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st ON dtl.intent_store_num = st.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim AS div ON CAST(dtl.merch_dept_num AS FLOAT64) = div.dept_num
WHERE dtl.business_day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 2 DAY) AND (CURRENT_DATE('PST8PDT'))
 AND LOWER(st.store_type_code) IN (LOWER('FL'))
 AND LOWER(dtl.line_item_merch_nonmerch_ind) = LOWER('MERCH')
 AND div.division_num <> 70
GROUP BY dtl.global_tran_id,
 storenumber,
 dtl.business_day_date,
 postive_merch_trx;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS traffic_pm_tran_agg
AS
SELECT a.storenumber,
 a.business_day_date,
  a.agg_merch_trx * COALESCE(b.trans_to_trips_mltplr, 1) AS estimated_positive_merch_custs,
 a.netsales,
 a.grosssales
FROM (SELECT storenumber,
   business_day_date,
   COUNT(DISTINCT postive_merch_trx) AS agg_merch_trx,
   SUM(netsales) AS netsales,
   SUM(grosssales) AS grosssales
  FROM traffic_pm_tran_dtl
  GROUP BY storenumber,
   business_day_date) AS a
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_fls_traffic_model.fls_tran_to_trip_estimate_coeff AS b ON a.storenumber = b.store_number AND LOWER(b
    .time_period_type) = LOWER('COVID');
	
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

/* Purchase trips calculation - Rack */

CREATE TEMPORARY TABLE IF NOT EXISTS rack_purchase_trips
AS
SELECT dtl.intent_store_num AS store_number,
 dtl.business_day_date,
 COUNT(DISTINCT CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.global_tran_id
   ELSE NULL
   END) AS purchase_trips,
 SUM(CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.line_net_usd_amt
   ELSE NULL
   END) AS gross_sales,
 SUM(dtl.line_net_usd_amt) AS net_sales
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st ON dtl.intent_store_num = st.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS dt ON dtl.business_day_date = dt.day_date
WHERE dtl.business_day_date BETWEEN {{params.start_date}} and {{params.end_date}}
 AND LOWER(st.store_type_code) IN (LOWER('RK'))
 AND LOWER(dtl.line_item_merch_nonmerch_ind) = LOWER('MERCH')
GROUP BY store_number,
 dtl.business_day_date;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

/* Purchase trips calculation - Last Chance stores */
-- this metric is not relevant for Nordstrom Local stores

CREATE TEMPORARY TABLE IF NOT EXISTS positive_merch_trx
AS
SELECT dtl.business_day_date,
 dtl.intent_store_num AS store_number,
 COUNT(DISTINCT dtl.global_tran_id) AS purchase_trips,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666')
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS gross_sales
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS st ON dtl.intent_store_num = st.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim AS div ON CAST(dtl.merch_dept_num AS FLOAT64) = div.dept_num
WHERE dtl.business_day_date BETWEEN {{params.start_date}} and {{params.end_date}}
 AND LOWER(st.store_type_code) IN (LOWER('CC'))
 AND LOWER(dtl.line_item_merch_nonmerch_ind) = LOWER('MERCH')
 AND div.division_num <> 70
 AND dtl.line_net_usd_amt > 0
GROUP BY dtl.business_day_date,
 store_number;
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

/* RetailNext camera traffic & store purchase trips & net sales & gross sales */

CREATE TEMPORARY TABLE IF NOT EXISTS rn_camera_and_trips_base
AS /* Full Line stores */ 
SELECT bs.day_date,
 bs.day_454_num,
 bs.store_num,
 bs.channel,
 bs.store_type_code,
 
 --Purchase trips & sales information
  CASE
  WHEN bs.day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 2 DAY) AND (CURRENT_DATE('PST8PDT'))
  THEN est_pt.estimated_positive_merch_custs
  ELSE CAST(pt.positive_merch_custs AS BIGNUMERIC)
  END AS purchase_trips,
  CASE
  WHEN bs.day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 2 DAY) AND (CURRENT_DATE('PST8PDT'))
  THEN est_pt.netsales
  ELSE pt.netsales
  END AS net_sales,
  CASE
  WHEN bs.day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 2 DAY) AND (CURRENT_DATE('PST8PDT'))
  THEN est_pt.grosssales
  ELSE pt.grosssales
  END AS gross_sales,
  
  --Traffic camera details
	    -- 0: good camera;
        -- 1: impute camera traffic with lr;
        -- 2: start or end of model estimation
  CASE
  WHEN bce.imputation_flag IN (1, 0)
  THEN 1
  ELSE 0
  END AS camera_flag,
  CASE
  WHEN bce.imputation_flag = 1 AND bs.day_date BETWEEN bce.start_date AND (COALESCE(bce.end_date, DATE '2099-12-31'))
  THEN ROUND(bce.intercept + bce.slope * ctr.traffic, 0)
  WHEN bce.imputation_flag = 0
  THEN CAST(ctr.traffic AS BIGNUMERIC)
  ELSE NULL
  END AS camera_traffic
FROM store_day_base AS bs
 LEFT JOIN (SELECT store_num,
   business_date,
   SUM(traffic_in) AS traffic
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_traffic_vw AS a
  WHERE LOWER(measurement_location_type) = LOWER('store')  -- filter on this to pull store-level numbers
   AND LOWER(source_type) IN (LOWER('RetailNext'))
   AND traffic_in > 0
   AND business_date BETWEEN {{params.start_date}} and {{params.end_date}}
  GROUP BY store_num,
   business_date) AS ctr ON bs.store_num = ctr.store_num AND bs.day_date = ctr.business_date
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_fls_traffic_model.fls_camera_store_details AS bce ON bs.store_num = bce.store_number AND bs.day_date
   BETWEEN COALESCE(bce.start_date, DATE '1901-01-01') AND (COALESCE(bce.end_date, DATE '2099-12-31'))
 LEFT JOIN kinds_of_trips_dtl AS pt ON bs.store_num = pt.storenumber AND bs.day_date = pt.business_day_date
 LEFT JOIN traffic_pm_tran_agg AS est_pt ON bs.store_num = est_pt.storenumber AND bs.day_date = est_pt.business_day_date
   
WHERE LOWER(bs.store_type_code) = LOWER('FL')
UNION ALL

/* Rack stores */ 
SELECT bs0.day_date,
 bs0.day_454_num,
 bs0.store_num,
 bs0.channel,
 bs0.store_type_code,
 
 --Purchase trips & sales information
 pt0.purchase_trips,
 pt0.net_sales,
 pt0.gross_sales,
 
 --Traffic camera details
  CASE
  WHEN bs0.day_date BETWEEN COALESCE(bce0.start_date, DATE '1900-01-01') AND (COALESCE(bce0.end_date, DATE '2099-12-31'
       )) AND bce0.imputation_flag = 0 AND ctr.traffic IS NOT NULL
  THEN 1
  ELSE 0
  END AS camera_flag,
  CASE
  WHEN bs0.day_date BETWEEN COALESCE(bce0.start_date, DATE '1900-01-01') AND (COALESCE(bce0.end_date, DATE '2099-12-31'
      )) AND bce0.imputation_flag = 0
  THEN ctr.traffic
  ELSE NULL
  END AS camera_traffic
FROM store_day_base AS bs0
 LEFT JOIN (SELECT store_num,
   business_date,
   SUM(traffic_in) AS traffic
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_traffic_vw AS a
  WHERE LOWER(measurement_location_type) = LOWER('store')  -- filter on this to pull store-level numbers
   AND LOWER(source_type) IN (LOWER('RetailNext'))
   AND traffic_in > 0
   AND business_date BETWEEN {{params.start_date}} and {{params.end_date}}
  GROUP BY store_num,
   business_date) AS ctr ON bs0.store_num = ctr.store_num AND bs0.day_date = ctr.business_date
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_fls_traffic_model.camera_store_details AS bce0 ON bs0.store_num = bce0.store_number AND bs0.day_date
   BETWEEN COALESCE(bce0.start_date, DATE '1901-01-01') AND (COALESCE(bce0.end_date, DATE '2099-12-31'))
 LEFT JOIN rack_purchase_trips AS pt0 ON bs0.store_num = pt0.store_number AND bs0.day_date = pt0.business_day_date
WHERE LOWER(bs0.store_type_code) = LOWER('RK')
UNION ALL

/* Nordstrom local and last chance stores */
SELECT bs1.day_date,
 bs1.day_454_num,
 bs1.store_num,
 bs1.channel,
 bs1.store_type_code,
 
  --Purchase trips & sales information
 pt1.purchase_trips,
 CAST(NULL AS NUMERIC) AS net_sales,
 pt1.gross_sales,
 
 --Traffic camera details
  CASE
  WHEN bs1.day_date BETWEEN COALESCE(bce1.start_date, DATE '1900-01-01') AND (COALESCE(bce1.end_date, DATE '2099-12-31'
      )) AND bce1.imputation_flag = 0
  THEN 1
  ELSE 0
  END AS camera_flag,
  CASE
  WHEN bs1.day_date BETWEEN COALESCE(bce1.start_date, DATE '1900-01-01') AND (COALESCE(bce1.end_date, DATE '2099-12-31'
      )) AND bce1.imputation_flag = 0
  THEN ctr.traffic
  ELSE NULL
  END AS camera_traffic
FROM store_day_base AS bs1
 LEFT JOIN (SELECT store_num,
   business_date,
   SUM(traffic_in) AS traffic
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_traffic_vw AS a
  WHERE LOWER(measurement_location_type) = LOWER('store') -- filter on this to pull store-level numbers
   AND LOWER(source_type) IN (LOWER('RetailNext'))
   AND traffic_in > 0
   AND business_date BETWEEN {{params.start_date}} and {{params.end_date}}
  GROUP BY store_num,
   business_date) AS ctr ON bs1.store_num = ctr.store_num AND bs1.day_date = ctr.business_date
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_fls_traffic_model.fls_camera_store_details AS bce1 ON bs1.store_num = bce1.store_number AND bs1.day_date
   BETWEEN COALESCE(bce1.start_date, DATE '1901-01-01') AND (COALESCE(bce1.end_date, DATE '2099-12-31'))
 LEFT JOIN positive_merch_trx AS pt1 ON bs1.store_num = pt1.store_number AND bs1.day_date = pt1.business_day_date
WHERE LOWER(bs1.store_type_code) IN (LOWER('NL'), LOWER('CC'));
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;
UPDATE rn_camera_and_trips_base SET
    camera_traffic = NULL
WHERE LOWER(store_type_code) = LOWER('FL') AND net_sales < 0 AND (purchase_trips / NULLIF(camera_traffic, 0) < 0.05 OR purchase_trips / NULLIF(camera_traffic, 0) > 0.6);
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

UPDATE rn_camera_and_trips_base SET
    camera_traffic = NULL,
    purchase_trips = NULL
WHERE store_num IN (520, 73) AND day_454_num = 1;

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

UPDATE rn_camera_and_trips_base SET
    camera_traffic = NULL
WHERE LOWER(store_type_code) = LOWER('RK') AND (purchase_trips / NULLIF(camera_traffic, 0) < 0.05 OR purchase_trips / NULLIF(camera_traffic, 0) > 1.0 OR camera_traffic < 10 OR net_sales <= 100);

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET ERROR_CODE  =  0;
UPDATE rn_camera_and_trips_base SET
    purchase_trips = NULL
WHERE LOWER(store_type_code) = LOWER('RK') AND (camera_traffic < 10 OR net_sales <= 100);

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

UPDATE rn_camera_and_trips_base SET
    camera_traffic = NULL,
    purchase_trips = NULL
WHERE store_num IN (539) AND day_454_num = 1;

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

UPDATE rn_camera_and_trips_base SET
    camera_traffic = NULL,
    purchase_trips = NULL
WHERE (camera_traffic IS NULL OR purchase_trips IS NULL) AND LOWER(store_type_code) = LOWER('CC') OR gross_sales = 0 AND LOWER(store_type_code) = LOWER('CC');

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

UPDATE rn_camera_and_trips_base SET
    camera_traffic = NULL,
    purchase_trips = NULL
WHERE day_date IN (DATE '2021-04-04', DATE '2022-04-17', DATE '2023-04-09', DATE '2024-03-31', DATE '2025-04-20') --easter
OR day_date IN (DATE '2021-11-25', DATE '2022-11-24', DATE '2023-11-23', DATE '2024-11-28', DATE '2025-11-27') --thanksgiving
OR day_date IN (DATE '2021-12-25', DATE '2022-12-25', DATE '2023-12-25', DATE '2024-12-25', DATE '2025-12-25');  --christmas
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

--Insert cleaned up traffic data into final prod table
DELETE FROM `{{params.gcp_project_id}}`.{{params.fls_traffic_model_t2_schema}}.rn_camera_and_trips_daily
WHERE day_date BETWEEN {{params.start_date}} and {{params.end_date}};

EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.fls_traffic_model_t2_schema}}.rn_camera_and_trips_daily
(SELECT store_num,
  day_date,
  channel,
  store_type_code,
  camera_flag,
  CAST(ROUND(camera_traffic, 0) AS NUMERIC) AS camera_traffic,
  CAST(ROUND(purchase_trips, 0) AS NUMERIC) AS purchase_trips,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM rn_camera_and_trips_base);
 
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;

END;
