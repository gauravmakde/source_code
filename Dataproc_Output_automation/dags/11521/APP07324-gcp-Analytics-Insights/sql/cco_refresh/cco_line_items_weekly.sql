/*SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_11521_ACE_ENG;
---     Task_Name=run_cco_job_1_cco_line_items;'*/
/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a line-item-level table facilitate analytics for the Aug/Sep22 "CCO Deep-dive"
 * *
 * The Steps involved are:
 * I) Create (empty) table
 *
 * II) Set the date parameters
 *
 * III) Gather the necessary transaction data
 *    a) build lookup tables to feed the transaction query
 *    b) bring all tables together in 1 place
 *
 * IV) Gather the necessary NSEAM data (for Alterations not captured at the cash register)
 *    a) build necessary lookup tables
 *    b) insert data into line-item-level table
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/



/************************************************************************************/
/************************************************************************************
 * PART II) Set the date parameters
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************/
/************************************************************************************
 * 1) Grab the current & most-recently complete fiscal month
 ************************************************************************************/
/************************************************************************************/

BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS curr_mo_lkp AS
SELECT DISTINCT month_num AS curr_mo, CASE WHEN MOD(month_num, 100) = 1 THEN month_num - 89 ELSE month_num - 1 END AS prior_mo,
 year_num AS curr_year
FROM `{{params.gcp_project_id}}`.t2dl_das_usl.usl_rolling_52wk_calendar
WHERE day_date = CURRENT_DATE('PST8PDT');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************/
/************************************************************************************
 * 2) Get the dates needed for the CCO tables
 ************************************************************************************/
/************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS date_parameter_lookup
AS
SELECT MIN(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS fy18_start_dt,
 MAX(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS fy18_end_dt,
 MIN(CASE
   WHEN year_num = 2019
   THEN day_date
   ELSE NULL
   END) AS fy19_start_dt,
 MAX(CASE
   WHEN month_num < (SELECT curr_mo
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS latest_mo_dt,
 MIN(CASE
   WHEN month_num = (SELECT curr_mo - 400
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr1_start_dt,
 MIN(CASE
   WHEN month_num = (SELECT curr_mo - 500
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr0_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT prior_mo - 400
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr0_end_dt
FROM `{{params.gcp_project_id}}`.t2dl_das_usl.usl_rolling_52wk_calendar;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/***************************************************************************************/
/********* Step II-1: create realigned fiscal calendar (going back far enough to include 4+ years of Cohorts) *********/
/***************************************************************************************/

/********* Step II-1-a: Going back 8 years from today, find all years with a "53rd week" *********/
--drop table week_53_yrs;
CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yrs
AS
SELECT year_num,
 RANK() OVER (ORDER BY year_num DESC) AS recency_rank
FROM (SELECT DISTINCT year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE week_of_fyr = 53
   AND day_date BETWEEN DATE '2009-01-01' AND (CURRENT_DATE('PST8PDT'))) AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (year_num) ON week_53_yrs;
--COLLECT STATISTICS COLUMN (recency_rank) ON week_53_yrs;
BEGIN
SET _ERROR_CODE  =  0;
/********* Step II-1-b: Count the # years with a "53rd week" *********/
--drop table week_53_yr_count;
CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yr_count
AS
SELECT COUNT(DISTINCT year_num) AS year_count
FROM week_53_yrs AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (year_count) ON week_53_yr_count;
BEGIN
SET _ERROR_CODE  =  0;

/********* Step II-1-c: Create an empty realigned calendar *********/
--drop table realigned_calendar;
CREATE TEMPORARY TABLE IF NOT EXISTS realigned_calendar (
day_date DATE,
day_num INTEGER,
day_desc STRING,
week_num INTEGER,
week_desc STRING,
month_num INTEGER,
month_short_desc STRING,
quarter_num INTEGER,
halfyear_num INTEGER,
year_num INTEGER,
month_454_num INTEGER,
year_454_num INTEGER
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/********* Step II-1-d: insert data into realigned calendar
 *  (any weeks before the latest 53rd week should get started 7 days late,
 *   any weeks before the 2nd-latest 53rd week--if there was one--should get started 14 days late)
 * *********/
INSERT INTO realigned_calendar
(SELECT CASE
   WHEN year_num > (SELECT year_num
     FROM week_53_yrs
     WHERE recency_rank = 1)
   THEN day_date
   WHEN (SELECT *
      FROM week_53_yr_count) = 1 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 1)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 2 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 2 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 21 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 4)
   THEN DATE_ADD(day_date, INTERVAL 21 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 4)
   THEN DATE_ADD(day_date, INTERVAL 28 DAY)
   ELSE NULL
   END AS day_date,
  day_num,
  day_desc,
  week_num,
  week_desc,
  month_num,
  month_short_desc,
  quarter_num,
  halfyear_num,
  year_num,
  month_454_num,
  year_num AS year_454_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
 WHERE day_date BETWEEN DATE '2009-01-01' AND (CURRENT_DATE('PST8PDT'))
  AND week_of_fyr <> 53);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (day_date) ON realigned_calendar;
--COLLECT STATISTICS COLUMN (month_num) ON realigned_calendar;
BEGIN
SET _ERROR_CODE  =  0;
/***************************************************************************************/
/********* Step II-2: Set date parameters *********/
/***************************************************************************************/

/********* Step II-2-a: Find the start & end month of the last 4 years (through latest complete fiscal month) *********/
CREATE TEMPORARY TABLE IF NOT EXISTS month_lookup
AS
SELECT DISTINCT month_num AS current_mo,
  CASE
  WHEN MOD(month_num, 100) = 1
  THEN month_num - 89
  ELSE month_num - 1
  END AS yr4_end_mo,
  month_num - 100 AS yr4_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 100 AS yr3_end_mo,
  month_num - 200 AS yr3_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 200 AS yr2_end_mo,
  month_num - 300 AS yr2_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 300 AS yr1_end_mo,
  month_num - 400 AS yr1_start_mo
FROM realigned_calendar
WHERE day_date = DATE_ADD((SELECT latest_mo_dt
    FROM date_parameter_lookup), INTERVAL 1 DAY);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (current_mo) ON month_lookup;
BEGIN
SET _ERROR_CODE  =  0;
/********* Step II-2-b: Find the start & end dates of the last 4 years (through latest complete fiscal month) *********/
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT (SELECT fy19_start_dt
  FROM date_parameter_lookup) AS yr1_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT yr1_end_mo
     FROM month_lookup)
   THEN day_date
   ELSE NULL
   END) AS yr1_end_dt,
 MIN(CASE
   WHEN month_num = (SELECT yr2_start_mo
     FROM month_lookup)
   THEN day_date
   ELSE NULL
   END) AS yr2_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT yr2_end_mo
     FROM month_lookup)
   THEN day_date
   ELSE NULL
   END) AS yr2_end_dt,
 MIN(CASE
   WHEN month_num = (SELECT yr3_start_mo
     FROM month_lookup)
   THEN day_date
   ELSE NULL
   END) AS yr3_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT yr3_end_mo
     FROM month_lookup)
   THEN day_date
   ELSE NULL
   END) AS yr3_end_dt,
 MIN(CASE
   WHEN month_num = (SELECT yr4_start_mo
     FROM month_lookup)
   THEN day_date
   ELSE NULL
   END) AS yr4_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT yr4_end_mo
     FROM month_lookup)
   THEN day_date
   ELSE NULL
   END) AS yr4_end_dt
FROM realigned_calendar
GROUP BY yr1_start_dt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (yr1_start_dt) ON date_lookup;
--COLLECT STATISTICS COLUMN (yr4_end_dt) ON date_lookup;
--COLLECT STATISTICS COLUMN (yr1_start_dt, yr4_end_dt) ON date_lookup;
BEGIN
SET _ERROR_CODE  =  0;
--dates for Pre-Prod testing
-- create multiset volatile table date_lookup as (
-- select
--    date'2024-09-10'    yr1_start_dt
--   ,date'2024-09-10'+1  yr1_end_dt
--   ,date'2024-09-10'+2  yr2_start_dt
--   ,date'2024-09-10'+3 yr2_end_dt
--   ,date'2024-09-10'+4 yr3_start_dt
--   ,date'2024-09-10'+5 yr3_end_dt
--   ,date'2024-09-10'+6 yr4_start_dt
--   ,date'2024-09-10'+7 yr4_end_dt
-- ) with data primary index(yr1_start_dt) on commit preserve rows;
CREATE TEMPORARY TABLE IF NOT EXISTS month_names
AS
SELECT MAX(CASE
   WHEN month_num = (SELECT yr1_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr1_name,
 MAX(CASE
   WHEN month_num = (SELECT yr2_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr2_name,
 MAX(CASE
   WHEN month_num = (SELECT yr3_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr3_name,
 MAX(CASE
   WHEN month_num = (SELECT yr4_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr4_name
FROM realigned_calendar
WHERE month_num IN ((SELECT yr1_end_mo
    FROM month_lookup), (SELECT yr2_end_mo
    FROM month_lookup), (SELECT yr3_end_mo
    FROM month_lookup), (SELECT yr4_end_mo
    FROM month_lookup));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (yr1_name) ON month_names;
BEGIN
SET _ERROR_CODE  =  0;
-- sel * from month_lookup;
-- sel * from date_lookup;
-- sel * from month_names;


/************************************************************************************/
/************************************************************************************
 * PART III) Gather the necessary transaction data & insert into final table
 *    a) build lookup tables to feed the transaction query
 *    b) bring all tables together in 1 place
 ************************************************************************************/
/************************************************************************************/


/************************************************************************************
 * PART III-a) create lookup table of "MERCH"-related fields unique on UPC/country
 ************************************************************************************/
--drop table upc_lookup;
CREATE TEMPORARY TABLE IF NOT EXISTS upc_lookup
AS
SELECT DISTINCT a.upc_num,
 b.div_num,
 b.div_desc,
 b.grp_num AS subdiv_num,
 b.grp_desc AS subdiv_desc,
 b.dept_num,
 b.dept_desc,
 b.brand_name,
 b.channel_country,
 b.rms_sku_num,
 COALESCE(v.npg_flag, 'N') AS npg_flag,
  CASE
  WHEN b.dept_num IN (765) AND LOWER(TRIM(b.dept_desc)) = LOWER('GIFT WRAP SERVICES')
  THEN 1
  ELSE 0
  END AS gift_wrap_service,
  CASE
  WHEN b.dept_num = 102 OR b.dept_num = 497 AND b.class_num IN (12, 14) OR b.dept_num = 497 AND (b.class_num IN (10, 11
        ) OR LOWER(b.style_desc) LIKE LOWER('%NAIL%'))
  THEN 1
  ELSE 0
  END AS beauty_service
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS a
 INNER JOIN 
 (
  SELECT div_num,
    div_desc,
    grp_num,
    grp_desc,
    dept_num,
    dept_desc,
    brand_name,
    channel_country,
    rms_sku_num,
    class_desc,
    class_num,
    style_desc,
    prmy_supp_num
   FROM 
   `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS b
   WHERE div_num IN (310, 340, 345, 351, 360, 365, 700, 800, 900)
    AND prmy_supp_num IS NOT NULL
   GROUP BY div_num,
    div_desc,
    grp_num,
    grp_desc,
    dept_num,
    dept_desc,
    brand_name,
    channel_country,
    rms_sku_num,
    class_desc,
    class_num,
    style_desc,
    prmy_supp_num
   UNION ALL
   SELECT 
   div_num,
    div_desc,
    grp_num,
    grp_desc,
    dept_num,
    dept_desc,
    brand_name,
    channel_country,
    rms_sku_num,
    class_desc,
    class_num,
    style_desc,
    prmy_supp_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS b
   WHERE div_num IN (310, 340, 345, 351, 360, 365, 700, 800, 900)
    AND prmy_supp_num IS NULL
   GROUP BY div_num,div_desc,grp_num,grp_desc,dept_num,dept_desc,brand_name,channel_country,rms_sku_num,class_desc,class_num,style_desc,prmy_supp_num) AS b 
   ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) 
   AND LOWER(a.channel_country) = LOWER(b.channel_country
    )
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v ON LOWER(b.prmy_supp_num) = LOWER(v.vendor_num);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (upc_num, channel_country) on upc_lookup;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART III-b) create lookup table of "ORDER FULFILLMENT"-related fields
 *      (order pickup & delivery method), unique on global_tran_id/line_item_seq_num
 ************************************************************************************/

 /*first create extract for efficiency*/
--drop table item_delivery_extract;
CREATE TEMPORARY TABLE IF NOT EXISTS item_delivery_extract (
business_day_date DATE,
global_tran_id BIGINT,
line_item_seq_num SMALLINT,
intent_store_num INTEGER,
item_delivery_method STRING,
curbside_ind BYTEINT,
picked_up_by_customer_node_num INTEGER,
picked_up_by_customer_tmstp_pacific DATETIME
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO item_delivery_extract
(SELECT DISTINCT business_day_date,
  global_tran_id,
  line_item_seq_num,
    CAST(TRUNC(CAST(intent_store_num AS FLOAT64)) AS INTEGER) AS intent_store_num,
  item_delivery_method,
  curbside_ind,
  picked_up_by_customer_node_num,
  picked_up_by_customer_tmstp_pacific
 FROM `{{params.gcp_project_id}}`.t2dl_das_item_delivery.item_delivery_method_funnel_daily AS a
 WHERE business_day_date BETWEEN (SELECT yr1_start_dt
    FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
  AND order_date_pacific BETWEEN (SELECT yr1_start_dt
    FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
  AND LOWER(item_delivery_method) <> LOWER('SHIP_TO_HOME')
  AND canceled_date_pacific IS NULL);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
 --drop table order_fulfillment_activity;
CREATE TEMPORARY TABLE IF NOT EXISTS order_fulfillment_activity
AS
SELECT DISTINCT a.business_day_date,
 a.global_tran_id,
 a.line_item_seq_num,
 a.item_delivery_method,
  CASE
  WHEN LOWER(SUBSTR(TRIM(a.item_delivery_method), 1, 9)) IN (LOWER('FREE_2DAY'), LOWER('PAID_EXPE')) OR LOWER(TRIM(a.item_delivery_method
      )) = LOWER('SAME_DAY_DELIVERY')
  THEN 1
  ELSE 0
  END AS svc_group_exp_delivery,
  CASE
  WHEN LOWER(a.item_delivery_method) LIKE LOWER('%BOPUS') OR LOWER(a.item_delivery_method) LIKE LOWER('%SHIP_TO_STORE')
  THEN 1
  ELSE 0
  END AS svc_group_order_pickup,
 a.curbside_ind,
 a.picked_up_by_customer_node_num AS pickup_store,
 CAST(a.picked_up_by_customer_tmstp_pacific AS DATE) AS pickup_date,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN '1) Nordstrom Stores'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
  THEN '2) Nordstrom.com'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN '3) Rack Stores'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN '4) Rack.com'
  ELSE NULL
  END AS pickup_channel,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
    LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
  THEN 'NORDSTROM'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
  THEN 'RACK'
  ELSE NULL
  END AS pickup_banner,
  CASE
  WHEN LOWER(st.business_unit_desc) LIKE LOWER('%CANADA') OR LOWER(st.business_unit_desc) LIKE LOWER('%.CA')
  THEN 'CA'
  ELSE 'US'
  END AS pickup_country,
 st.subgroup_desc AS pickup_region
FROM item_delivery_extract AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON COALESCE(a.picked_up_by_customer_node_num, - 1 * a.intent_store_num) = st
  .store_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (business_day_date), column (global_tran_id, line_item_seq_num) on order_fulfillment_activity;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART III-c) create lookup table of "RESTAURANT"-related fields (for Service reporting),
 *      unique on global_tran_id/line_item_seq_num
 ************************************************************************************/

/********* PART III-c-i) create lookup table of all Restaurant depts *********/
--drop table restaurant_service_lookup;
CREATE TEMPORARY TABLE IF NOT EXISTS restaurant_service_lookup
AS
SELECT DISTINCT SUBSTR(CAST(dept_num AS STRING), 1, 8) AS dept_num,
 dept_name AS dept_desc,
 subdivision_num AS subdiv_num,
 subdivision_name AS subdiv_desc,
 division_num AS div_num,
 division_name AS div_desc,
  CASE
  WHEN dept_num IN (113, 188, 571, 698)
  THEN 'G02) Coffee'
  WHEN dept_num IN (568, 692, 715)
  THEN 'G03) Bar'
  ELSE 'G01) Food'
  END AS restaurant_service
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim
WHERE division_num = 70;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
--COLLECT STATISTICS COLUMN (DEPT_NUM) ON restaurant_service_lookup;

/********* PART III-c-ii) create lookup table of all Restaurant transaction line-items *********/
--drop table restaurant_tran_lines;
CREATE TEMPORARY TABLE IF NOT EXISTS restaurant_extract (
business_day_date DATE,
global_tran_id BIGINT,
line_item_seq_num SMALLINT,
acp_id STRING,
intent_store_num INTEGER,
merch_dept_num STRING
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO restaurant_extract
(SELECT DISTINCT business_day_date,
  global_tran_id,
  line_item_seq_num,
  acp_id,
  intent_store_num,
  merch_dept_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 WHERE business_day_date BETWEEN (SELECT yr1_start_dt
    FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
  AND COALESCE(order_date, tran_date) BETWEEN (SELECT yr1_start_dt
    FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
  AND line_net_amt > 0
  AND LOWER(data_source_code) = LOWER('rpos')
  AND acp_id IS NOT NULL);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (INTENT_STORE_NUM) ON restaurant_extract;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS restaurant_tran_lines
AS
SELECT DISTINCT dtl.business_day_date,
 dtl.global_tran_id,
 dtl.line_item_seq_num,
 r.dept_num,
 r.dept_desc,
 r.subdiv_num,
 r.subdiv_desc,
 r.div_num,
 r.div_desc,
 r.restaurant_service
FROM restaurant_extract AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON dtl.intent_store_num = st.store_num AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'
     ), LOWER('FULL LINE CANADA'))
 INNER JOIN restaurant_service_lookup AS r ON LOWER(COALESCE(dtl.merch_dept_num, FORMAT('%11d', - 1 * dtl.intent_store_num
      ))) = LOWER(r.dept_num);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (business_day_date), column (global_tran_id, line_item_seq_num) on restaurant_tran_lines;
BEGIN
SET _ERROR_CODE  =  0;
--sel top 1000 * from restaurant_tran_lines


/************************************************************************************
 * PART III-d) create lookup table of "REMOTE-SELLING"-related fields (for Service reporting),
 *      unique on global_tran_id/UPC
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS boards
AS
SELECT DISTINCT business_day_date,
 remote_sell_swimlane,
 SUBSTR(LPAD(TRIM(upc_num), 15, '0'), 1, 32) AS upc_num,
 CAST(CASE
   WHEN global_tran_id = ''
   THEN '0'
   ELSE global_tran_id
   END AS BIGINT) AS global_tran_id
FROM `{{params.gcp_project_id}}`.t2dl_das_remote_selling.remote_sell_transactions
WHERE LOWER(remote_sell_swimlane) IN (LOWER('STYLEBOARD_ATTRIBUTED'), LOWER('STYLEBOARD_PRIVATE_ATTRIBUTED'), LOWER('PERSONAL_REQUEST_A_LOOK_ATTRIB'
    ), LOWER('PERSONAL_REQUEST_A_LOOK_ATTRIBUTED'), LOWER('REQUEST_A_LOOK_ATTRIBUTED'), LOWER('CHAT_BOARD_ATTRIBUTED'),
   LOWER('STYLELINK_ATTRIBUTED'), LOWER('STYLEBOARD_PUBLIC_ATTRIBUTED'), LOWER('PRIVATE_STYLING_ATTRIBUTED'), LOWER('ECF_ATTRIBUTED'
    ), LOWER('TRUNK_CLUB'))
 AND business_day_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
 AND upc_num IS NOT NULL
 AND global_tran_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (business_day_date), column (upc_num, global_tran_id) on boards;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART III-e) create lookup table of "TENDER"-related indicators, unique on global_tran_id
 ************************************************************************************/
CREATE TEMPORARY TABLE IF NOT EXISTS transaction_tenders
AS
SELECT business_day_date,
 global_tran_id,
 MAX(CASE
   WHEN LOWER(TRIM(card_type_code)) IN (LOWER('NC'), LOWER('NV'))
   THEN 1
   ELSE 0
   END) AS tender_nordstrom,
 MAX(CASE
   WHEN LOWER(TRIM(tender_type_code)) = LOWER('NORDSTROM_NOTE')
   THEN 1
   ELSE 0
   END) AS tender_nordstrom_note,
 MAX(CASE
   WHEN LOWER(TRIM(tender_type_code)) = LOWER('CREDIT_CARD')
   THEN 1
   ELSE 0
   END) AS tender_3rd_party_credit,
 MAX(CASE
   WHEN LOWER(TRIM(tender_type_code)) = LOWER('DEBIT_CARD')
   THEN 1
   ELSE 0
   END) AS tender_debit_card,
 MAX(CASE
   WHEN LOWER(TRIM(tender_type_code)) = LOWER('GIFT_CARD')
   THEN 1
   ELSE 0
   END) AS tender_gift_card,
 MAX(CASE
   WHEN LOWER(TRIM(tender_type_code)) = LOWER('CASH')
   THEN 1
   ELSE 0
   END) AS tender_cash,
 MAX(CASE
   WHEN LOWER(TRIM(tender_type_code)) = LOWER('PAYPAL')
   THEN 1
   ELSE 0
   END) AS tender_paypal,
 MAX(CASE
   WHEN LOWER(TRIM(tender_type_code)) = LOWER('CHECK')
   THEN 1
   ELSE 0
   END) AS tender_check
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_tender_fact AS a
WHERE business_day_date BETWEEN (SELECT DATE_SUB(yr1_start_dt, INTERVAL 14 DAY)
   FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
 AND tender_item_usd_amt > 0
 AND tender_type_code IS NOT NULL
GROUP BY business_day_date,
 global_tran_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (business_day_date), column (global_tran_id) on transaction_tenders;
BEGIN
SET _ERROR_CODE  =  0;

/************************************************************************************
 * PART III-f) create lookup table of "EVENT DATES", unique on day
 ************************************************************************************/

--drop table event_dates;
CREATE TEMPORARY TABLE IF NOT EXISTS event_dates
AS
SELECT day_date,
  CASE
  WHEN day_date BETWEEN DATE '2018-07-11' AND DATE '2018-08-05' OR day_date BETWEEN DATE '2019-07-09' AND
         DATE '2019-08-04' OR day_date BETWEEN DATE '2020-08-04' AND DATE '2020-08-29' OR day_date BETWEEN
       DATE '2021-07-12' AND DATE '2021-08-08' OR day_date BETWEEN DATE '2022-07-06' AND DATE '2022-07-31' OR day_date
     BETWEEN DATE '2023-07-11' AND DATE '2023-08-06' OR day_date BETWEEN DATE '2024-07-09' AND DATE '2024-08-04'
  THEN 1
  ELSE 0
  END AS anniv_dates_us,
  CASE
  WHEN day_date BETWEEN DATE '2018-07-11' AND DATE '2018-08-05' OR day_date BETWEEN DATE '2019-07-16' AND
       DATE '2019-08-04' OR day_date BETWEEN DATE '2020-08-17' AND DATE '2020-08-29' OR day_date BETWEEN
     DATE '2021-07-25' AND DATE '2021-08-08' OR day_date BETWEEN DATE '2022-07-14' AND DATE '2022-07-31'
  THEN 1
  ELSE 0
  END AS anniv_dates_ca,
  CASE
  WHEN month_454_num BETWEEN 9 AND 11
  THEN 1
  ELSE 0
  END AS holiday_dates,
  CASE
  WHEN month_num BETWEEN (SELECT yr1_start_mo
    FROM month_lookup) AND (SELECT yr1_end_mo
    FROM month_lookup)
  THEN (SELECT yr1_name
   FROM month_names)
  WHEN month_num BETWEEN (SELECT yr2_start_mo
    FROM month_lookup) AND (SELECT yr2_end_mo
    FROM month_lookup)
  THEN (SELECT yr2_name
   FROM month_names)
  WHEN month_num BETWEEN (SELECT yr3_start_mo
    FROM month_lookup) AND (SELECT yr3_end_mo
    FROM month_lookup)
  THEN (SELECT yr3_name
   FROM month_names)
  WHEN month_num BETWEEN (SELECT yr4_start_mo
    FROM month_lookup) AND (SELECT yr4_end_mo
    FROM month_lookup)
  THEN (SELECT yr4_name
   FROM month_names)
  ELSE NULL
  END AS reporting_year,
 SUBSTR('FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2), 1, 8) AS fiscal_year
FROM realigned_calendar AS a
WHERE day_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (CURRENT_DATE('PST8PDT'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (day_date) on event_dates;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART III-h) Insert Transaction data into final table
 ************************************************************************************/


/********* PART III-h-ii) "Monster query" to insert everything into final table *********/

/* for efficiency, extract just the sales_and_returns_fact data needed for this process*/
 --drop table sarf_extract
CREATE TEMPORARY TABLE IF NOT EXISTS sarf_extract
AS
SELECT dtl.business_day_date,
 dtl.global_tran_id,
 dtl.line_item_seq_num,
 dtl.order_num,
 dtl.acp_id,
 st.business_unit_desc,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
    LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
  THEN 'NORDSTROM'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
  THEN 'RACK'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN '1) Nordstrom Stores'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
  THEN '2) Nordstrom.com'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN '3) Rack Stores'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN '4) Rack.com'
  ELSE NULL
  END AS channel,
 st.store_num AS intent_store_num,
  CASE
  WHEN LOWER(st.business_unit_desc) LIKE LOWER('%CANADA') OR LOWER(st.business_unit_desc) LIKE LOWER('%.CA')
  THEN 'CA'
  ELSE 'US'
  END AS channel_country,
  CASE
  WHEN LOWER(st.business_unit_desc) LIKE LOWER('%CANADA') OR LOWER(st.business_unit_desc) LIKE LOWER('%.CA')
  THEN 'CA'
  ELSE 'US'
  END AS store_country,
 st.subgroup_desc,
 dtl.price_type,
 dtl.payroll_dept,
 dtl.hr_es_ind,
 dtl.order_date,
 COALESCE(dtl.order_date, dtl.tran_date) AS tran_date,
 dtl.nonmerch_fee_code,
 COALESCE(dtl.shipped_usd_sales, 0) AS shipped_usd_sales,
 COALESCE(dtl.return_usd_amt, 0) AS return_usd_amt,
 COALESCE(dtl.shipped_qty, 0) AS shipped_qty,
 COALESCE(dtl.return_qty, 0) AS return_qty,
 dtl.return_date,
 dtl.days_to_return,
 dtl.return_ringing_store_num,
 dtl.source_platform_code,
 dtl.employee_discount_usd_amt,
 dtl.upc_num,
 dtl.item_source
FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS ist ON dtl.intent_store_num = ist.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CASE
   WHEN LOWER(dtl.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND LOWER(ist.store_type_code
      ) = LOWER('RK')
   THEN 828
   WHEN LOWER(dtl.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND (LOWER(dtl.business_unit_desc
        ) LIKE LOWER('%CANADA') OR LOWER(dtl.business_unit_desc) LIKE LOWER('%.CA'))
   THEN 867
   WHEN LOWER(dtl.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) OR dtl.intent_store_num
      = 5405
   THEN 808
   ELSE dtl.intent_store_num
   END = st.store_num
WHERE dtl.business_day_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
 AND COALESCE(dtl.order_date, dtl.tran_date) BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
 AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'), LOWER('MARKETPLACE'))
 AND dtl.shipped_usd_sales >= 0
 AND COALESCE(dtl.return_usd_amt, 0) >= 0
 AND dtl.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (partition) ON sarf_extract;
--COLLECT STATISTICS COLUMN (business_day_date) ON sarf_extract;
--COLLECT STATISTICS COLUMN (tran_date) ON sarf_extract;
--COLLECT STATISTICS COLUMN (global_tran_id, line_item_seq_num) ON sarf_extract;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ntn_extract
AS
SELECT aare_global_tran_id,
 acp_id,
 aare_chnl_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
WHERE aare_status_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (CURRENT_DATE('PST8PDT'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (aare_global_tran_id) on NTN_EXTRACT;
BEGIN
SET _ERROR_CODE  =  0;
/* for efficiency, process data derivations into a NOPI
  staging table, so the final load into prod table can be
  a simple copy*/


CREATE TEMPORARY TABLE IF NOT EXISTS cco_line_items_stg (
business_day_date DATE,
global_tran_id BIGINT,
line_item_seq_num SMALLINT,
data_source STRING,
order_num STRING,
acp_id STRING,
banner STRING,
channel STRING,
channel_country STRING,
store_num INTEGER,
store_region STRING,
div_num INTEGER,
div_desc STRING,
subdiv_num INTEGER,
subdiv_desc STRING,
dept_num INTEGER,
dept_desc STRING,
brand_name STRING,
npg_flag STRING,
price_type STRING,
store_country STRING,
ntn_tran INTEGER,
date_shopped DATE,
fiscal_month_num INTEGER,
fiscal_qtr_num INTEGER,
fiscal_yr_num INTEGER,
fiscal_year_shopped STRING,
reporting_year_shopped STRING,
item_price_band STRING,
gross_sales NUMERIC(12,2),
gross_incl_gc NUMERIC(12,2),
return_amt NUMERIC(12,2),
net_sales NUMERIC(13,2),
gross_items INTEGER,
return_items INTEGER,
net_items INTEGER,
return_date DATE,
days_to_return INTEGER,
return_store INTEGER,
return_banner STRING,
return_channel STRING,
item_delivery_method STRING,
pickup_store INTEGER,
pickup_date DATE,
pickup_channel STRING,
pickup_banner STRING,
platform STRING,
employee_flag INTEGER,
tender_nordstrom INTEGER,
tender_nordstrom_note INTEGER,
tender_3rd_party_credit INTEGER,
tender_debit_card INTEGER,
tender_gift_card INTEGER,
tender_cash INTEGER,
tender_paypal INTEGER,
tender_check INTEGER,
event_holiday INTEGER,
event_anniversary INTEGER,
svc_group_exp_delivery INTEGER,
svc_group_order_pickup INTEGER,
svc_group_selling_relation INTEGER,
svc_group_remote_selling INTEGER,
svc_group_alterations INTEGER,
svc_group_in_store INTEGER,
svc_group_restaurant INTEGER,
service_free_exp_delivery INTEGER,
service_next_day_pickup INTEGER,
service_same_day_bopus INTEGER,
service_curbside_pickup INTEGER,
service_style_boards INTEGER,
service_gift_wrapping INTEGER,
service_pop_in INTEGER,
marketplace_flag INTEGER,
dw_sys_load_tmstp DATETIME,
dw_sys_updt_tmstp DATETIME
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (MONTH_NUM ,QUARTER_NUM ,YEAR_NUM) ON realigned_calendar;
--COLLECT STATISTICS COLUMN (ANNIV_DATES_CA ,HOLIDAY_DATES) ON event_dates;
--COLLECT STATISTICS COLUMN (RETURN_RINGING_STORE_NUM) ON sarf_extract;
--COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,UPC_NUM) ON sarf_extract;
--COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM) ON sarf_extract;
--COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM) ON order_fulfillment_activity;
--COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE ,GLOBAL_TRAN_ID) ON sarf_extract;
--COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE ,UPC_NUM ,GLOBAL_TRAN_ID) ON boards;
--COLLECT STATISTICS COLUMN (TRAN_DATE) ON sarf_extract;
--COLLECT STATISTICS COLUMN (DAY_DATE) ON realigned_calendar;
--COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE) ON order_fulfillment_activity;
--COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON restaurant_tran_lines;
--COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON boards;
--COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON order_fulfillment_activity;
--COLLECT STATISTICS COLUMN (PARTITION) ON order_fulfillment_activity;
--COLLECT STATISTICS COLUMN (PARTITION) ON boards;
--COLLECT STATISTICS COLUMN (PARTITION) ON restaurant_tran_lines;
--COLLECT STATISTICS COLUMN (PARTITION) ON transaction_tenders;
--COLLECT STATISTICS COLUMN (PARTITION) ON sarf_extract;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cco_line_items_stg
(SELECT DISTINCT dtl.business_day_date,
  dtl.global_tran_id,
  dtl.line_item_seq_num,
  '1) TRANSACTION' AS data_source,
  dtl.order_num,
  dtl.acp_id,
  dtl.banner,
  dtl.channel,
  dtl.channel_country,
  dtl.intent_store_num AS store_num,
  dtl.subgroup_desc AS store_region,
  COALESCE(upc.div_num, r.div_num) AS div_num,
  COALESCE(upc.div_desc, r.div_desc) AS div_desc,
  COALESCE(upc.subdiv_num, r.subdiv_num) AS subdiv_num,
  COALESCE(upc.subdiv_desc, r.subdiv_desc) AS subdiv_desc,
    CAST(TRUNC(CAST(COALESCE(FORMAT('%11d', upc.dept_num), r.dept_num) AS FLOAT64)) AS INTEGER)  AS dept_num,
  COALESCE(upc.dept_desc, r.dept_desc) AS dept_desc,
  upc.brand_name,
  upc.npg_flag,
  COALESCE(dtl.price_type, 'R') AS price_type,
  dtl.store_country,
   CASE
   WHEN LOWER(SUBSTR(dtl.channel, 1, 1)) = LOWER('1') AND LOWER(TRIM(nts.aare_chnl_code)) = LOWER('FLS')
   THEN 1
   WHEN LOWER(SUBSTR(dtl.channel, 1, 1)) = LOWER('2') AND LOWER(TRIM(nts.aare_chnl_code)) = LOWER('NCOM')
   THEN 1
   WHEN LOWER(SUBSTR(dtl.channel, 1, 1)) = LOWER('3') AND LOWER(TRIM(nts.aare_chnl_code)) = LOWER('RACK')
   THEN 1
   WHEN LOWER(SUBSTR(dtl.channel, 1, 1)) = LOWER('4') AND LOWER(TRIM(nts.aare_chnl_code)) = LOWER('NRHL')
   THEN 1
   ELSE 0
   END AS ntn_tran,
  dtl.tran_date AS date_shopped,
  cal.month_num AS fiscal_month_num,
  cal.quarter_num AS fiscal_qtr_num,
  cal.year_num AS fiscal_yr_num,
  SUBSTR('FY-' || SUBSTR(CAST(MOD(cal.year_num, 2000) AS STRING), 1, 2), 1, 8) AS fiscal_year_shopped,
   CASE
   WHEN cal.month_num BETWEEN (SELECT yr1_start_mo
     FROM month_lookup) AND (SELECT yr1_end_mo
     FROM month_lookup)
   THEN (SELECT yr1_name
    FROM month_names)
   WHEN cal.month_num BETWEEN (SELECT yr2_start_mo
     FROM month_lookup) AND (SELECT yr2_end_mo
     FROM month_lookup)
   THEN (SELECT yr2_name
    FROM month_names)
   WHEN cal.month_num BETWEEN (SELECT yr3_start_mo
     FROM month_lookup) AND (SELECT yr3_end_mo
     FROM month_lookup)
   THEN (SELECT yr3_name
    FROM month_names)
   WHEN cal.month_num BETWEEN (SELECT yr4_start_mo
     FROM month_lookup) AND (SELECT yr4_end_mo
     FROM month_lookup)
   THEN (SELECT yr4_name
    FROM month_names)
   ELSE NULL
   END AS reporting_year_shopped,
   CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666')
   THEN CASE
    WHEN dtl.shipped_usd_sales = 0
    THEN '0) $0'
    WHEN dtl.shipped_usd_sales > 0 AND dtl.shipped_usd_sales <= 25
    THEN '1) $0-25'
    WHEN dtl.shipped_usd_sales > 25 AND dtl.shipped_usd_sales <= 50
    THEN '2) $25-50'
    WHEN dtl.shipped_usd_sales > 50 AND dtl.shipped_usd_sales <= 75
    THEN '3) $50-75'
    WHEN dtl.shipped_usd_sales > 75 AND dtl.shipped_usd_sales <= 100
    THEN '4) $75-100'
    WHEN dtl.shipped_usd_sales > 100 AND dtl.shipped_usd_sales <= 150
    THEN '5) $100-150'
    WHEN dtl.shipped_usd_sales > 150 AND dtl.shipped_usd_sales <= 200
    THEN '6) $150-200'
    WHEN dtl.shipped_usd_sales > 200
    THEN '7) $200+'
    ELSE NULL
    END
   ELSE NULL
   END AS item_price_band,
  CAST(CASE
    WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666')
    THEN dtl.shipped_usd_sales
    ELSE 0
    END AS NUMERIC) AS gross_sales,
  dtl.shipped_usd_sales AS gross_incl_gc,
  CAST(CASE
    WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.return_usd_amt > 0
    THEN dtl.shipped_usd_sales
    ELSE 0
    END AS NUMERIC) AS return_amt,
  CAST(CASE
    WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.return_usd_amt > 0
    THEN 0
    WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666')
    THEN dtl.shipped_usd_sales
    ELSE 0
    END AS NUMERIC) AS net_sales,
   CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.shipped_usd_sales > 0
   THEN dtl.shipped_qty
   ELSE 0
   END AS gross_items,
   CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.return_usd_amt > 0 AND dtl.shipped_usd_sales
      > 0
   THEN dtl.shipped_qty
   ELSE 0
   END AS return_items,
   CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.return_usd_amt > 0 AND dtl.shipped_usd_sales
      > 0
   THEN 0
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.shipped_usd_sales > 0
   THEN dtl.shipped_qty
   ELSE 0
   END AS net_items,
  dtl.return_date,
  dtl.days_to_return,
  dtl.return_ringing_store_num AS return_store,
   CASE
   WHEN LOWER(rst.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
     LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
   THEN 'NORDSTROM'
   WHEN LOWER(rst.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
   THEN 'RACK'
   ELSE NULL
   END AS return_banner,
   CASE
   WHEN LOWER(rst.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
   THEN '1) Nordstrom Stores'
   WHEN LOWER(rst.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
   THEN '2) Nordstrom.com'
   WHEN LOWER(rst.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
   THEN '3) Rack Stores'
   WHEN LOWER(rst.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
   THEN '4) Rack.com'
   ELSE NULL
   END AS return_channel,
  ofa.item_delivery_method,
  ofa.pickup_store,
  ofa.pickup_date,
  ofa.pickup_channel,
  ofa.pickup_banner,
  dtl.source_platform_code AS platform,
   CASE
   WHEN COALESCE(dtl.employee_discount_usd_amt, 0) <> 0
   THEN 1
   ELSE 0
   END AS employee_flag,
  COALESCE(t.tender_nordstrom, 0) AS tender_nordstrom,
  COALESCE(t.tender_nordstrom_note, 0) AS tender_nordstrom_note,
  COALESCE(t.tender_3rd_party_credit, 0) AS tender_3rd_party_credit,
  COALESCE(t.tender_debit_card, 0) AS tender_debit_card,
  COALESCE(t.tender_gift_card, 0) AS tender_gift_card,
  COALESCE(t.tender_cash, 0) AS tender_cash,
  COALESCE(t.tender_paypal, 0) AS tender_paypal,
  COALESCE(t.tender_check, 0) AS tender_check,
  COALESCE(ed.holiday_dates, 0) AS event_holiday,
   CASE
   WHEN LOWER(dtl.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'))
   THEN COALESCE(ed.anniv_dates_us, 0)
   WHEN LOWER(dtl.business_unit_desc) IN (LOWER('FULL LINE CANADA'), LOWER('N.CA'))
   THEN COALESCE(ed.anniv_dates_ca, 0)
   ELSE 0
   END AS event_anniversary,
  COALESCE(ofa.svc_group_exp_delivery, 0) AS svc_group_exp_delivery,
  COALESCE(ofa.svc_group_order_pickup, 0) AS svc_group_order_pickup,
   CASE
   WHEN LOWER(dtl.payroll_dept) IN (LOWER('1196'), LOWER('1205'), LOWER('1404')) OR LOWER(dtl.hr_es_ind) = LOWER('Y')
   THEN 1
   ELSE 0
   END AS svc_group_selling_relation,
   CASE
   WHEN LOWER(COALESCE(b.remote_sell_swimlane, 'X')) NOT IN (LOWER('X'), LOWER('TRUNK_CLUB')) OR LOWER(TRIM(COALESCE(dtl
         .item_source, 'XYZ'))) = LOWER('SB_SALESEMPINIT') OR LOWER(dtl.upc_num) IN (LOWER('439027332977'), LOWER('439027332984'
       ))
   THEN 1
   ELSE 0
   END AS svc_group_remote_selling,
   CASE
   WHEN LOWER(TRIM(dtl.nonmerch_fee_code)) IN (LOWER('1803'), LOWER('1926'), LOWER('108'), LOWER('809'), LOWER('116'),
     LOWER('272'), LOWER('817'), LOWER('663\'\'3855'), LOWER('3786'), LOWER('647'))
   THEN 1
   ELSE 0
   END AS svc_group_alterations,
   CASE
   WHEN COALESCE(upc.beauty_service, 0) = 1 OR LOWER(TRIM(dtl.nonmerch_fee_code)) IN (LOWER('8571'), LOWER('8463'))
   THEN 1
   ELSE 0
   END AS svc_group_in_store,
   CASE
   WHEN r.global_tran_id IS NOT NULL AND dtl.shipped_usd_sales > 0
   THEN 1
   ELSE 0
   END AS svc_group_restaurant,
   CASE
   WHEN LOWER(SUBSTR(TRIM(ofa.item_delivery_method), 1, 9)) = LOWER('FREE_2DAY')
   THEN 1
   ELSE 0
   END AS service_free_exp_delivery,
   CASE
   WHEN LOWER(SUBSTR(TRIM(ofa.item_delivery_method), 1, 8)) IN (LOWER('NEXT_DAY'), LOWER('FC_BOPUS'))
   THEN 1
   ELSE 0
   END AS service_next_day_pickup,
   CASE
   WHEN LOWER(TRIM(ofa.item_delivery_method)) = LOWER('SAME_DAY_BOPUS')
   THEN 1
   ELSE 0
   END AS service_same_day_bopus,
  COALESCE(ofa.curbside_ind, 0) AS service_curbside_pickup,
   CASE
   WHEN LOWER(TRIM(b.remote_sell_swimlane)) IN (LOWER('STYLEBOARD_ATTRIBUTED'), LOWER('STYLEBOARD_PRIVATE_ATTRIBUTED'),
      LOWER('PERSONAL_REQUEST_A_LOOK_ATTRIB'), LOWER('PERSONAL_REQUEST_A_LOOK_ATTRIBUTED'), LOWER('REQUEST_A_LOOK_ATTRIBUTED'
       ), LOWER('CHAT_BOARD_ATTRIBUTED')) OR LOWER(TRIM(COALESCE(dtl.item_source, 'XYZ'))) = LOWER('SB_SALESEMPINIT')
   THEN 1
   ELSE 0
   END AS service_style_boards,
  COALESCE(upc.gift_wrap_service, 0) AS service_gift_wrapping,
   CASE
   WHEN upc.dept_num = 587
   THEN 1
   ELSE 0
   END AS service_pop_in,
   CASE
   WHEN LOWER(TRIM(dtl.business_unit_desc)) = LOWER('MARKETPLACE')
   THEN 1
   ELSE 0
   END AS marketplace_flag,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM sarf_extract AS dtl
  INNER JOIN realigned_calendar AS cal ON dtl.tran_date = cal.day_date
  LEFT JOIN event_dates AS ed ON dtl.tran_date = ed.day_date
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS rst ON dtl.return_ringing_store_num = rst.store_num
  LEFT JOIN ntn_extract AS nts ON dtl.global_tran_id = nts.aare_global_tran_id
  LEFT JOIN transaction_tenders AS t ON dtl.business_day_date = t.business_day_date AND dtl.global_tran_id = t.global_tran_id
    
  LEFT JOIN upc_lookup AS upc ON LOWER(COALESCE(dtl.upc_num, 'X' || SUBSTR(dtl.acp_id, 12, 11))) = LOWER(upc.upc_num)
   AND LOWER(dtl.channel_country) = LOWER(upc.channel_country)
  LEFT JOIN restaurant_tran_lines AS r ON dtl.business_day_date = r.business_day_date AND dtl.global_tran_id = r.global_tran_id
      AND dtl.line_item_seq_num = r.line_item_seq_num
  LEFT JOIN boards AS b ON dtl.business_day_date = b.business_day_date AND dtl.global_tran_id = b.global_tran_id AND
    LOWER(COALESCE(LPAD(TRIM(dtl.upc_num), 15, '0'), 'X' || SUBSTR(dtl.acp_id, 12, 11))) = LOWER(b.upc_num)
  LEFT JOIN order_fulfillment_activity AS ofa ON dtl.business_day_date = ofa.business_day_date AND dtl.global_tran_id =
     ofa.global_tran_id AND dtl.line_item_seq_num = ofa.line_item_seq_num);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (business_day_date) ON cco_line_items_stg;
--COLLECT STATISTICS COLUMN (global_tran_id, line_item_seq_num) ON cco_line_items_stg;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items
(SELECT *
 FROM cco_line_items_stg);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
--WHERE date_shopped BETWEEN (select start_dt from date_lookup) and  (select end_dt from date_lookup);


/************************************************************************************/
/************************************************************************************
 * PART IV) Gather the necessary NSEAM data (for Alterations not captured at the cash register)
 *    a) build necessary lookup tables
 *    b) insert data into line-item-level table
 ************************************************************************************/
/************************************************************************************/


/************************************************************************************
 * PART IV-a) create lookup table of "NSEAM-ONLY ALTERAIONS TRIPS"
 *  (customer/store/day combinations found in NSEAM, but not in our Alterations transaction data)
 ************************************************************************************/

/********* PART IV-a-i) find Alteration trips in `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_item_table *********/
--drop table alteration_trips_POS;
CREATE TEMPORARY TABLE IF NOT EXISTS alteration_trips_pos
AS
SELECT DISTINCT acp_id,
 store_num,
 date_shopped
FROM `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items
WHERE svc_group_alterations = 1
 AND LOWER(data_source) = LOWER('1) TRANSACTION');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (acp_id,store_num,date_shopped) on alteration_trips_POS;
BEGIN
SET _ERROR_CODE  =  0;
/********* PART IV-a-ii) find Alterations trips in NSEAM *********/
--drop table nseam_trips;
CREATE TEMPORARY TABLE IF NOT EXISTS nseam_trips
AS
SELECT DISTINCT xr.acp_id,
 a.store_origin AS store_num,
 a.ticket_date AS date_shopped
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.alteration_ticket_fact AS a
 INNER JOIN (SELECT DISTINCT ticket_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.alteration_ticket_garment_fact
  WHERE LOWER(LOWER(garment_status)) NOT LIKE LOWER('%delete%')) AS ntg ON a.ticket_id = ntg.ticket_id
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON a.store_origin = st.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_cust_xref AS xr ON LOWER(a.customer_id) = LOWER(xr.cust_id)
WHERE LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'), LOWER('MARKETPLACE'))
 AND a.ticket_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (CURRENT_DATE('PST8PDT'))
 AND LOWER(LOWER(a.ticket_status)) NOT LIKE LOWER('%delete%')
 AND a.store_origin IS NOT NULL
 AND xr.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (acp_id,store_num,date_shopped) on alteration_trips_POS;
BEGIN
SET _ERROR_CODE  =  0;
/********* PART IV-a-iii) combine Alteration trips, flagging which are from tran data & NSEAM *********/
--drop table alterations_trips_combined;
CREATE TEMPORARY TABLE IF NOT EXISTS alterations_trips_combined
AS
SELECT acp_id,
 store_num,
 date_shopped,
 MAX(pos_ind) AS pos_ind,
 MAX(nseam_ind) AS nseam_ind
FROM (SELECT acp_id,
    store_num,
    date_shopped,
    1 AS pos_ind,
    0 AS nseam_ind
   FROM alteration_trips_pos
   UNION ALL
   SELECT acp_id,
    store_num,
    date_shopped,
    0 AS pos_ind,
    1 AS nseam_ind
   FROM nseam_trips) AS x
GROUP BY acp_id,
 store_num,
 date_shopped;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (acp_id,store_num,date_shopped) on alterations_trips_combined;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nseam_only_alteration_trips
AS
SELECT DISTINCT acp_id,
 store_num,
 date_shopped
FROM alterations_trips_combined
WHERE pos_ind = 0
 AND nseam_ind = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (acp_id,store_num,date_shopped) on nseam_only_alteration_trips;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS nseam_only_alterations_ranked
AS
SELECT acp_id,
 store_num,
 date_shopped,
  - 1 * (ROW_NUMBER() OVER (ORDER BY acp_id, store_num, date_shopped)) AS global_tran_id
FROM nseam_only_alteration_trips;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition), column (acp_id,store_num,date_shopped) on nseam_only_alterations_ranked;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART IV-b) Insert Transaction data into final table
 ************************************************************************************/

/********* PART IV-b-i) Collect all the table statistics Teradata recommends *********/
/*COLLECT STATISTICS COLUMN (STORE_NUM) ON     nseam_only_alterations_ranked;
COLLECT STATISTICS COLUMN (DATE_SHOPPED) ON     nseam_only_alterations_ranked;
COLLECT STATISTICS COLUMN (ACP_ID) ON     nseam_only_alterations_ranked;*/


/********* PART III-B-ii) "Monster query" to insert everything into final table *********/

-- CALL SYS_MGMT.drop_if_exists_sp('T3DL_ACE_CORP', 'cco_line_item_table', OUT_RETURN_MSG);
-- create multiset table `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_item_table as (
--create multiset volatile table cco_line_item_table as (

--delete records for same run dates
--DELETE FROM table
--WHERE date_shopped BETWEEN (select start_dt from date_lookup) and  (select end_dt from date_lookup);
INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items
--explain
(SELECT DISTINCT dtl.date_shopped,
  dtl.global_tran_id,
  1 AS line_item_seq_num,
  '2) NSEAM' AS data_source,
  CAST(NULL AS STRING) AS order_num,
  dtl.acp_id,
   CASE
   WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
     LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
   THEN 'NORDSTROM'
   WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
   THEN 'RACK'
   ELSE NULL
   END AS banner,
   CASE
   WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
   THEN '1) Nordstrom Stores'
   WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'), LOWER('MARKETPLACE'))
   THEN '2) Nordstrom.com'
   WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
   THEN '3) Rack Stores'
   WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
   THEN '4) Rack.com'
   ELSE NULL
   END AS channel,
   CASE
   WHEN LOWER(st.business_unit_desc) LIKE LOWER('%CANADA') OR LOWER(st.business_unit_desc) LIKE LOWER('%.CA')
   THEN 'CA'
   ELSE 'US'
   END AS channel_country,
  dtl.store_num,
  st.subgroup_desc AS store_region,
  CAST(TRUNC(CAST(NULL AS FLOAT64)) AS INTEGER)  AS div_num,
  CAST(NULL AS STRING) AS div_desc,
  CAST(TRUNC(CAST(NULL AS FLOAT64)) AS INTEGER)  AS subdiv_num,
  CAST(NULL AS STRING) AS subdiv_desc,
  CAST(TRUNC(CAST(NULL AS FLOAT64)) AS INTEGER)  AS dept_num,
  CAST(NULL AS STRING) AS dept_desc,
  CAST(NULL AS STRING) AS brand_name,
  CAST(NULL AS STRING) AS npg_flag,
  CAST(NULL AS STRING) AS price_type,
   CASE
   WHEN LOWER(st.business_unit_desc) LIKE LOWER('%CANADA') OR LOWER(st.business_unit_desc) LIKE LOWER('%.CA')
   THEN 'CA'
   ELSE 'US'
   END AS store_country,
  0 AS ntn_tran,
  dtl.date_shopped AS date_shopped22,
  cal.month_num AS fiscal_month_num,
  cal.quarter_num AS fiscal_qtr_num,
  cal.year_num AS fiscal_yr_num,
  SUBSTR('FY-' || SUBSTR(CAST(MOD(cal.year_num, 2000) AS STRING), 1, 2), 1, 8) AS fiscal_year_shopped,
   CASE
   WHEN cal.month_num BETWEEN (SELECT yr1_start_mo
     FROM month_lookup) AND (SELECT yr1_end_mo
     FROM month_lookup)
   THEN (SELECT yr1_name
    FROM month_names)
   WHEN cal.month_num BETWEEN (SELECT yr2_start_mo
     FROM month_lookup) AND (SELECT yr2_end_mo
     FROM month_lookup)
   THEN (SELECT yr2_name
    FROM month_names)
   WHEN cal.month_num BETWEEN (SELECT yr3_start_mo
     FROM month_lookup) AND (SELECT yr3_end_mo
     FROM month_lookup)
   THEN (SELECT yr3_name
    FROM month_names)
   WHEN cal.month_num BETWEEN (SELECT yr4_start_mo
     FROM month_lookup) AND (SELECT yr4_end_mo
     FROM month_lookup)
   THEN (SELECT yr4_name
    FROM month_names)
   ELSE NULL
   END AS reporting_year_shopped,
  '0) $0' AS item_price_band,
  0 AS gross_sales,
  0 AS gross_incl_gc,
  0 AS return_amt,
  0 AS net_sales,
  0 AS gross_items,
  0 AS return_items,
  0 AS net_items,
  CAST(NULL AS DATE) AS return_date,
   CAST(TRUNC(CAST(NULL AS FLOAT64)) AS INTEGER) AS days_to_return,
   CAST(TRUNC(CAST(NULL AS FLOAT64)) AS INTEGER) AS return_store,
  CAST(NULL AS STRING) AS return_banner,
  CAST(NULL AS STRING) AS return_channel,
  CAST(NULL AS STRING) AS item_delivery_method,
   CAST(TRUNC(CAST(NULL AS FLOAT64)) AS INTEGER) AS pickup_store,
  CAST(NULL AS DATE) AS pickup_date,
  CAST(NULL AS STRING) AS pickup_channel,
  CAST(NULL AS STRING) AS pickup_banner,
  CAST(NULL AS STRING) AS platform,
  0 AS employee_flag,
  0 AS tender_nordstrom,
  0 AS tender_nordstrom_note,
  0 AS tender_3rd_party_credit,
  0 AS tender_debit_card,
  0 AS tender_gift_card,
  0 AS tender_cash,
  0 AS tender_paypal,
  0 AS tender_check,
  COALESCE(ed.holiday_dates, 0) AS event_holiday,
   CASE
   WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'))
   THEN COALESCE(ed.anniv_dates_us, 0)
   WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE CANADA'), LOWER('N.CA'))
   THEN COALESCE(ed.anniv_dates_ca, 0)
   ELSE 0
   END AS event_anniversary,
  0 AS svc_group_exp_delivery,
  0 AS svc_group_order_pickup,
  0 AS svc_group_selling_relation,
  0 AS svc_group_remote_selling,
  1 AS svc_group_alterations,
  0 AS svc_group_in_store,
  0 AS svc_group_restaurant,
  0 AS service_free_exp_delivery,
  0 AS service_next_day_pickup,
  0 AS service_same_day_bopus,
  0 AS service_curbside_pickup,
  0 AS service_style_boards,
  0 AS service_gift_wrapping,
  0 AS service_pop_in,
  0 AS marketplace_flag,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM nseam_only_alterations_ranked AS dtl
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON dtl.store_num = st.store_num
  INNER JOIN realigned_calendar AS cal ON dtl.date_shopped = cal.day_date
  LEFT JOIN event_dates AS ed ON dtl.date_shopped = ed.day_date
 WHERE dtl.date_shopped BETWEEN (SELECT yr1_start_dt
    FROM date_lookup) AND (SELECT yr4_end_dt
    FROM date_lookup)
  AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
    LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'), LOWER('MARKETPLACE'))
  AND dtl.acp_id IS NOT NULL);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
