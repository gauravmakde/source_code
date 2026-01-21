/*SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_11521_ACE_ENG;
---Task_Name=run_cco_job_3_cco_buyer_flow_fy;'*/


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Year/Channel-level table for AARE & Buyer Flow 
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


/************************************************************************************/
/************************************************************************************
 * PART I) Build reference tables:
 *  A) Realigned fiscal calendar to replace DAY_CAL
 *  B) Date lookup tables: start & end months of past 5 years, start & end dates of past 5 years, names of past 5 years
 *  C) Customer Driver tables
 ************************************************************************************/
/************************************************************************************/

/***************************************************************************************/
/********* Part I-A: create realigned fiscal calendar (going back far enough to include 4+ years of Cohorts) *********/
/***************************************************************************************/

/********* Step I-A-i: Going back 8 years from today, find all years with a "53rd week" *********/

BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;
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
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yr_count
AS
SELECT COUNT(DISTINCT year_num) AS year_count
FROM week_53_yrs AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/********* Step I-A-iv: insert data into realigned calendar 
 *  (any weeks before the latest 53rd week should get started 7 days late,
 *   any weeks before the 2nd-latest 53rd week--if there was one--should get started 14 days late)
 * *********/
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
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART I-B) Date lookup tables
 ************************************************************************************/

/********* Step I-B-i: Determine the current & most-recently complete fiscal month *********/
CREATE TEMPORARY TABLE IF NOT EXISTS curr_mo_lkp
AS
SELECT DISTINCT month_num AS curr_mo,
  CASE
  WHEN MOD(month_num, 100) = 1
  THEN month_num - 89
  ELSE month_num - 1
  END AS prior_mo,
 year_num AS curr_year
FROM `{{params.gcp_project_id}}`.t2dl_das_usl.usl_rolling_52wk_calendar
WHERE day_date = CURRENT_DATE('PST8PDT');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT MIN(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS yr0_start_dt,
 MAX(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS yr0_end_dt,
 MIN(CASE
   WHEN year_num = 2019
   THEN day_date
   ELSE NULL
   END) AS yr1_start_dt,
 MAX(CASE
   WHEN month_num < (SELECT curr_mo
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS yrn_end_dt
FROM `{{params.gcp_project_id}}`.t2dl_das_usl.usl_rolling_52wk_calendar;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
--dates for Pre-Prod testing
-- create multiset volatile table date_lookup as (
-- select date'2024-09-10'-2 yr0_start_dt
--       ,date'2024-09-10'-1 yr0_end_dt
--       ,date'2024-09-10'    yr1_start_dt
--       ,date'2024-09-10'+7 yrN_end_dt
-- ) with data primary index(yr1_start_dt) on commit preserve rows;


/********* Step I-B-iii: Generate names for the earliest rolling-12-mo period *********/
CREATE TEMPORARY TABLE IF NOT EXISTS month_names
AS
SELECT SUBSTR('FY-' || SUBSTR(CAST(MOD(MIN(year_num), 2000) AS STRING), 1, 2), 1, 8) AS earliest_fy
FROM realigned_calendar
WHERE day_date BETWEEN (SELECT yr0_start_dt
   FROM date_lookup) AND (SELECT yrn_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART I-C) Create "driver" tables (dates + customers in-scope)
 ************************************************************************************/


/********* Step I-C-i: customer/year-level driver table *********/
CREATE TEMPORARY TABLE IF NOT EXISTS cust_year_driver
AS
SELECT DISTINCT acp_id,
 fiscal_year_shopped
FROM `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items AS a
WHERE LOWER(data_source) = LOWER('1) TRANSACTION')
 AND date_shopped BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (SELECT yrn_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_year_driver
AS
SELECT DISTINCT acp_id,
 channel,
 fiscal_year_shopped
FROM `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items AS a
WHERE LOWER(data_source) = LOWER('1) TRANSACTION')
 AND date_shopped BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (SELECT yrn_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

/************************************************************************************/
/************************************************************************************
 * PART II) Build 5 lookup tables:
 *  A) Acquisition date/channel
 *  B) Activation date/Channel
 *  C) New-to-Channel/Banner lookup
 *  D) Table containing customer/channel from 1 prior year (Sep17-Aug18)
 *  E) Create customer/channel/year-level table from the past 5 years (4 years from line-item table + 1 year from "I-D")
 *  F) Create customer/channel/year-level table from the past 4 years (flagging whether they shopped 1 year before)
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART II-A) Acquisition date/channel
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition_prep
AS
SELECT DISTINCT a.acp_id,
 SUBSTR('FY-' || SUBSTR(CAST(MOD(b.year_num, 2000) AS STRING), 1, 2), 1, 8) AS fiscal_year_shopped,
 a.aare_status_date AS acquisition_date,
  CASE
  WHEN LOWER(a.aare_chnl_code) = LOWER('FLS')
  THEN '1) Nordstrom Stores'
  WHEN LOWER(a.aare_chnl_code) = LOWER('NCOM')
  THEN '2) Nordstrom.com'
  WHEN LOWER(a.aare_chnl_code) = LOWER('RACK')
  THEN '3) Rack Stores'
  WHEN LOWER(a.aare_chnl_code) = LOWER('NRHL')
  THEN '4) Rack.com'
  ELSE NULL
  END AS acquisition_channel,
  CASE
  WHEN LOWER(a.aare_chnl_code) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN '5) Nordstrom Banner'
  WHEN LOWER(a.aare_chnl_code) IN (LOWER('RACK'), LOWER('NRHL'))
  THEN '6) Rack Banner'
  ELSE NULL
  END AS acquisition_banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS a
 INNER JOIN realigned_calendar AS b ON a.aare_status_date = b.day_date
 INNER JOIN cust_year_driver AS dr ON LOWER(a.acp_id) = LOWER(dr.acp_id)
WHERE a.aare_status_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (SELECT yrn_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition (
acp_id STRING,
fiscal_year_shopped STRING,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_acquisition
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  acquisition_channel AS channel
 FROM customer_acquisition_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_acquisition
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  acquisition_banner AS channel
 FROM customer_acquisition_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_acquisition
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  '7) JWN' AS channel
 FROM customer_acquisition_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition_jwn
AS
SELECT DISTINCT acp_id,
 fiscal_year_shopped
FROM customer_acquisition;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART II-B) Activation date/Channel
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS customer_activation_prep
AS
SELECT DISTINCT a.acp_id,
  -- ,case when b.month_num between (select yr1_start_mo from month_lookup) and (select yr1_end_mo from month_lookup) 
  --         then (select yr1_name from month_names)
  --       when b.month_num between (select yr2_start_mo from month_lookup) and (select yr2_end_mo from month_lookup) 
  --         then (select yr2_name from month_names)
  --       when b.month_num between (select yr3_start_mo from month_lookup) and (select yr3_end_mo from month_lookup) 
  --         then (select yr3_name from month_names)
  --       when b.month_num between (select yr4_start_mo from month_lookup) and (select yr4_end_mo from month_lookup) 
  --         then (select yr4_name from month_names)
  --       else null end fiscal_year_shopped
 SUBSTR('FY-' || SUBSTR(CAST(MOD(b.year_num, 2000) AS STRING), 1, 2), 1, 8) AS fiscal_year_shopped,
 a.activated_date AS activation_date,
  CASE
  WHEN LOWER(a.activated_chnl_code) = LOWER('FLS')
  THEN '1) Nordstrom Stores'
  WHEN LOWER(a.activated_chnl_code) = LOWER('NCOM')
  THEN '2) Nordstrom.com'
  WHEN LOWER(a.activated_chnl_code) = LOWER('RACK')
  THEN '3) Rack Stores'
  WHEN LOWER(a.activated_chnl_code) = LOWER('NRHL')
  THEN '4) Rack.com'
  ELSE NULL
  END AS activation_channel,
  CASE
  WHEN LOWER(a.activated_chnl_code) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN '5) Nordstrom Banner'
  WHEN LOWER(a.activated_chnl_code) IN (LOWER('RACK'), LOWER('NRHL'))
  THEN '6) Rack Banner'
  ELSE NULL
  END AS activation_banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_activated_fact AS a
 INNER JOIN realigned_calendar AS b ON a.activated_date = b.day_date
 INNER JOIN cust_year_driver AS dr ON LOWER(a.acp_id) = LOWER(dr.acp_id)
WHERE a.activated_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (SELECT yrn_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_activation (
acp_id STRING,
fiscal_year_shopped STRING,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_activation
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  activation_channel AS channel
 FROM customer_activation_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_activation
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  activation_banner AS channel
 FROM customer_activation_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_activation
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  '7) JWN' AS channel
 FROM customer_activation_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART II-C) New-to-Channel/Banner lookup:
 ************************************************************************************/
CREATE TEMPORARY TABLE IF NOT EXISTS cco_new_2_channel_lkup
AS
SELECT acp_id,
 channel,
 MAX(acquired_date) AS ntx_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_channel_ntn_status_fact
GROUP BY acp_id,
 channel
UNION ALL
SELECT acp_id,
 banner,
 MAX(acquired_date) AS ntx_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_banner_ntn_status_fact
GROUP BY acp_id,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(acp_id  ) ON cco_new_2_channel_lkup;
--COLLECT STATISTICS COLUMN(channel ) ON cco_new_2_channel_lkup;
--COLLECT STATISTICS COLUMN(ntx_date) ON cco_new_2_channel_lkup;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_new2channel_prep
AS
SELECT DISTINCT a.acp_id,
 SUBSTR('FY-' || SUBSTR(CAST(MOD(b.year_num, 2000) AS STRING), 1, 2), 1, 8) AS fiscal_year_shopped,
 a.ntx_date AS new_to_channel_date,
  CASE
  WHEN LOWER(TRIM(a.channel)) = LOWER('N_STORE')
  THEN '1) Nordstrom Stores'
  WHEN LOWER(TRIM(a.channel)) = LOWER('N_COM')
  THEN '2) Nordstrom.com'
  WHEN LOWER(TRIM(a.channel)) = LOWER('R_STORE')
  THEN '3) Rack Stores'
  WHEN LOWER(TRIM(a.channel)) = LOWER('R_COM')
  THEN '4) Rack.com'
  WHEN LOWER(TRIM(a.channel)) = LOWER('NORDSTROM')
  THEN '5) Nordstrom Banner'
  WHEN LOWER(TRIM(a.channel)) = LOWER('RACK')
  THEN '6) Rack Banner'
  ELSE NULL
  END AS channel
FROM cco_new_2_channel_lkup AS a
 INNER JOIN realigned_calendar AS b ON a.ntx_date = b.day_date
 INNER JOIN cust_year_driver AS dr ON LOWER(a.acp_id) = LOWER(dr.acp_id)
WHERE a.ntx_date BETWEEN (SELECT yr1_start_dt
   FROM date_lookup) AND (SELECT yrn_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_new2channel (
acp_id STRING,
fiscal_year_shopped STRING,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_new2channel
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  channel
 FROM customer_new2channel_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_new2channel
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  '7) JWN' AS channel
 FROM customer_acquisition_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * II-D) Prior Year data (used for flagging "Retained" customers, folks who shopped last year):
 *
 *  (this part queries 1 year prior to earliest year in t2dl_das_strategy.cco_line_items)
 *
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS cust_pre_cco_line_data
AS
SELECT DISTINCT dtl.acp_id,
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
 SUBSTR('FY-' || SUBSTR(CAST(MOD(cal.year_num, 2000) AS STRING), 1, 2), 1, 8) AS fiscal_year_shopped
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS ist ON dtl.intent_store_num = ist.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CASE
   WHEN LOWER(dtl.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND LOWER(ist.store_type_code
      ) = LOWER('RK')
   THEN 828
   WHEN LOWER(dtl.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND LOWER(dtl.line_item_net_amt_currency_code
      ) = LOWER('CAD')
   THEN 867
   WHEN LOWER(dtl.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) OR dtl.intent_store_num
      = 5405
   THEN 808
   ELSE dtl.intent_store_num
   END = st.store_num
 INNER JOIN realigned_calendar AS cal ON COALESCE(dtl.order_date, dtl.tran_date) = cal.day_date
WHERE COALESCE(dtl.order_date, dtl.tran_date) BETWEEN (SELECT yr0_start_dt
   FROM date_lookup) AND (SELECT yr0_end_dt
   FROM date_lookup)
 AND dtl.business_day_date BETWEEN (SELECT yr0_start_dt
   FROM date_lookup) AND (SELECT DATE_ADD(yr0_end_dt, INTERVAL 14 DAY)
   FROM date_lookup)
 AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'), LOWER('MARKETPLACE'))
 AND dtl.line_net_usd_amt >= 0
 AND dtl.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART II-E) Create cust/channel/year-level table for all reported years (+ 1 earlier year)
 ************************************************************************************/

/********* PART II-E-i) Create empty table *********/
CREATE TEMPORARY TABLE IF NOT EXISTS cust_channel_yr (
acp_id STRING,
fiscal_year_shopped STRING,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_channel_yr
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  channel
 FROM cust_chan_year_driver);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_channel_yr
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  channel
 FROM cust_pre_cco_line_data);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_channel_yr
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
   CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
   THEN '5) Nordstrom Banner'
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
   THEN '6) Rack Banner'
   ELSE NULL
   END AS channel
 FROM cust_chan_year_driver);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_channel_yr
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
   CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
   THEN '5) Nordstrom Banner'
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
   THEN '6) Rack Banner'
   ELSE NULL
   END AS channel
 FROM cust_pre_cco_line_data);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_channel_yr
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  '7) JWN' AS channel
 FROM cust_year_driver);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_channel_yr
(SELECT DISTINCT acp_id,
  fiscal_year_shopped,
  '7) JWN' AS channel
 FROM cust_pre_cco_line_data);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART II-F) Create customer/channel/year-level table from the past 4 years (flagging whether they shopped 1 year before)
 ************************************************************************************/

/********* PART I-F-i) Create a "LY" column = the year before the current year *********/
CREATE TEMPORARY TABLE IF NOT EXISTS cust_year_chan_ly
AS
SELECT acp_id,
 fiscal_year_shopped,
 channel,
 SUBSTR('FY-' || SUBSTR(CAST(CAST(TRUNC(CAST(CASE
        WHEN SUBSTR(fiscal_year_shopped, 4, 5) = ''
        THEN '0'
        ELSE SUBSTR(fiscal_year_shopped, 4, 5)
        END  AS FLOAT64)) AS INTEGER) - 1 AS STRING), 1, 2), 1, 8) AS ly
FROM cust_channel_yr;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_year_chan
AS
SELECT a.acp_id,
 a.fiscal_year_shopped,
 a.channel,
  CASE
  WHEN LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.ly) = LOWER(b.fiscal_year_shopped
     )
  THEN 1
  ELSE 0
  END AS chan_ret_flag,
  CASE
  WHEN LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.ly) = LOWER(c.fiscal_year_shopped)
  THEN 1
  ELSE 0
  END AS jwn_ret_flag
FROM cust_year_chan_ly AS a
 LEFT JOIN cust_year_chan_ly AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND LOWER(a
    .ly) = LOWER(b.fiscal_year_shopped)
 LEFT JOIN (SELECT DISTINCT acp_id,
   fiscal_year_shopped
  FROM cust_year_chan_ly) AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.ly) = LOWER(c.fiscal_year_shopped)
WHERE LOWER(a.fiscal_year_shopped) <> LOWER((SELECT *
    FROM month_names));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON customer_new2channel;
--COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped)              ON cust_year_chan;
--COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped)              ON customer_acquisition_jwn;
--COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON customer_acquisition;
--COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON cust_year_chan;
--COLLECT STATISTICS COLUMN (ACP_ID ,fiscal_year_shopped     ,CHANNEL) ON customer_activation;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************/
/************************************************************************************
 * PART III) Join all tables together to create Buyer-Flow & AARE Indicators
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART III-A) Join all tables together to create indicators
 * **********************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS aare_buyer_flow_prep
AS
SELECT DISTINCT a.acp_id,
 a.fiscal_year_shopped,
 a.channel,
 a.chan_ret_flag,
 a.jwn_ret_flag,
  CASE
  WHEN LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(c.fiscal_year_shopped) AND a.jwn_ret_flag
     = 0
  THEN 1
  ELSE 0
  END AS ntn_jwn_flag,
  CASE
  WHEN LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(b.fiscal_year_shopped) AND LOWER(a.channel
       ) = LOWER(b.channel) AND a.jwn_ret_flag = 0 AND CASE
     WHEN LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(c.fiscal_year_shopped) AND a.jwn_ret_flag
        = 0
     THEN 1
     ELSE 0
     END = 1
  THEN 1
  ELSE 0
  END AS ntn_chan_flag,
  CASE
  WHEN LOWER(a.acp_id) = LOWER(d.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(d.fiscal_year_shopped) AND LOWER(a.channel
      ) = LOWER(d.channel) AND (a.jwn_ret_flag = 1 OR CASE
       WHEN LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(c.fiscal_year_shopped) AND a.jwn_ret_flag
          = 0
       THEN 1
       ELSE 0
       END = 1)
  THEN 1
  ELSE 0
  END AS act_flag,
  CASE
  WHEN LOWER(a.acp_id) = LOWER(e.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(e.fiscal_year_shopped) AND LOWER(a.channel
      ) = LOWER(e.channel) AND a.chan_ret_flag = 0
  THEN 1
  WHEN CASE
    WHEN LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(c.fiscal_year_shopped) AND a.jwn_ret_flag
       = 0
    THEN 1
    ELSE 0
    END = 1
  THEN 1
  ELSE 0
  END AS ntc_flag
FROM cust_year_chan AS a
 LEFT JOIN customer_acquisition AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(b.fiscal_year_shopped
     ) AND LOWER(a.channel) = LOWER(b.channel)
 LEFT JOIN customer_acquisition_jwn AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(c
    .fiscal_year_shopped)
 LEFT JOIN customer_activation AS d ON LOWER(a.acp_id) = LOWER(d.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(d.fiscal_year_shopped
     ) AND LOWER(a.channel) = LOWER(d.channel)
 LEFT JOIN customer_new2channel AS e ON LOWER(a.acp_id) = LOWER(e.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(e.fiscal_year_shopped
     ) AND LOWER(a.channel) = LOWER(e.channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS buyer_flow_all_views
AS
SELECT DISTINCT acp_id,
 fiscal_year_shopped,
 channel,
 ntn_chan_flag,
 ntn_jwn_flag,
 act_flag,
 ntc_flag,
 chan_ret_flag,
 jwn_ret_flag,
  CASE
  WHEN chan_ret_flag = 1
  THEN '3) Retained-to-Channel'
  WHEN ntn_chan_flag = 1 AND jwn_ret_flag = 0
  THEN '1) New-to-JWN'
  WHEN ntc_flag = 1 AND ntn_chan_flag = 0
  THEN '2) New-to-Channel (not JWN)'
  ELSE '4) Reactivated-to-Channel'
  END AS buyer_flow,
  CASE
  WHEN ntn_jwn_flag = 1 AND jwn_ret_flag = 0 AND LOWER(SUBSTR(CASE
       WHEN chan_ret_flag = 1
       THEN '3) Retained-to-Channel'
       WHEN ntn_chan_flag = 1 AND jwn_ret_flag = 0
       THEN '1) New-to-JWN'
       WHEN ntc_flag = 1 AND ntn_chan_flag = 0
       THEN '2) New-to-Channel (not JWN)'
       ELSE '4) Reactivated-to-Channel'
       END, 1, 1)) NOT IN (LOWER('3'), LOWER('4'))
  THEN 1
  ELSE 0
  END AS aare_acquired,
  CASE
  WHEN act_flag = 1 AND (ntn_jwn_flag = 1 OR jwn_ret_flag = 1) AND LOWER(SUBSTR(CASE
       WHEN chan_ret_flag = 1
       THEN '3) Retained-to-Channel'
       WHEN ntn_chan_flag = 1 AND jwn_ret_flag = 0
       THEN '1) New-to-JWN'
       WHEN ntc_flag = 1 AND ntn_chan_flag = 0
       THEN '2) New-to-Channel (not JWN)'
       ELSE '4) Reactivated-to-Channel'
       END, 1, 1)) NOT IN (LOWER('4'))
  THEN 1
  ELSE 0
  END AS aare_activated,
  CASE
  WHEN jwn_ret_flag = 1 AND LOWER(SUBSTR(CASE
       WHEN chan_ret_flag = 1
       THEN '3) Retained-to-Channel'
       WHEN ntn_chan_flag = 1 AND jwn_ret_flag = 0
       THEN '1) New-to-JWN'
       WHEN ntc_flag = 1 AND ntn_chan_flag = 0
       THEN '2) New-to-Channel (not JWN)'
       ELSE '4) Reactivated-to-Channel'
       END, 1, 1)) <> LOWER('1')
  THEN 1
  ELSE 0
  END AS aare_retained,
  CASE
  WHEN ntc_flag = 1 AND ntn_chan_flag = 0 AND chan_ret_flag = 0 AND LOWER(SUBSTR(CASE
       WHEN chan_ret_flag = 1
       THEN '3) Retained-to-Channel'
       WHEN ntn_chan_flag = 1 AND jwn_ret_flag = 0
       THEN '1) New-to-JWN'
       WHEN ntc_flag = 1 AND ntn_chan_flag = 0
       THEN '2) New-to-Channel (not JWN)'
       ELSE '4) Reactivated-to-Channel'
       END, 1, 1)) = LOWER('2')
  THEN 1
  ELSE 0
  END AS aare_engaged
FROM aare_buyer_flow_prep;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS aare_banner_level
AS
SELECT DISTINCT a.acp_id,
 a.fiscal_year_shopped,
 a.channel,
 b.buyer_flow,
 a.ntn_chan_flag,
 a.ntn_jwn_flag,
 a.act_flag,
 b.ntc_flag,
 a.chan_ret_flag,
 a.jwn_ret_flag,
  CASE
  WHEN a.ntn_jwn_flag = 1 AND a.jwn_ret_flag = 0 AND LOWER(SUBSTR(b.buyer_flow, 1, 1)) NOT IN (LOWER('3'), LOWER('4'))
  THEN 1
  ELSE 0
  END AS aare_acquired,
  CASE
  WHEN a.act_flag = 1 AND (a.ntn_jwn_flag = 1 OR a.jwn_ret_flag = 1) AND LOWER(SUBSTR(b.buyer_flow, 1, 1)) NOT IN (LOWER('4'
      ))
  THEN 1
  ELSE 0
  END AS aare_activated,
  CASE
  WHEN a.jwn_ret_flag = 1 AND LOWER(SUBSTR(b.buyer_flow, 1, 1)) <> LOWER('1')
  THEN 1
  ELSE 0
  END AS aare_retained,
 a.aare_engaged
FROM (SELECT acp_id,
   fiscal_year_shopped,
    CASE
    WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
    THEN '5) Nordstrom Banner'
    WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
    THEN '6) Rack Banner'
    ELSE NULL
    END AS channel,
   MAX(ntn_chan_flag) AS ntn_chan_flag,
   MAX(ntn_jwn_flag) AS ntn_jwn_flag,
   MAX(act_flag) AS act_flag,
   MAX(chan_ret_flag) AS chan_ret_flag,
   MAX(jwn_ret_flag) AS jwn_ret_flag,
   MAX(aare_acquired) AS aare_acquired,
   MAX(aare_activated) AS aare_activated,
   MAX(aare_retained) AS aare_retained,
   MAX(aare_engaged) AS aare_engaged
  FROM buyer_flow_all_views
  WHERE LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'), LOWER('3'), LOWER('4'))
  GROUP BY acp_id,
   fiscal_year_shopped,
   channel) AS a
 INNER JOIN (SELECT DISTINCT acp_id,
   fiscal_year_shopped,
   channel,
   buyer_flow,
   ntc_flag
  FROM buyer_flow_all_views
  WHERE LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('5'), LOWER('6'))) AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a
     .fiscal_year_shopped) = LOWER(b.fiscal_year_shopped) AND LOWER(a.channel) = LOWER(b.channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS aare_jwn_level
AS
SELECT DISTINCT a.acp_id,
 a.fiscal_year_shopped,
 a.channel,
 b.buyer_flow,
 a.ntn_chan_flag,
 a.ntn_jwn_flag,
 a.act_flag,
 a.chan_ret_flag,
 a.jwn_ret_flag,
  CASE
  WHEN a.ntn_jwn_flag = 1 AND a.jwn_ret_flag = 0 AND LOWER(SUBSTR(b.buyer_flow, 1, 1)) NOT IN (LOWER('3'), LOWER('4'))
  THEN 1
  ELSE 0
  END AS aare_acquired,
  CASE
  WHEN a.act_flag = 1 AND (a.ntn_jwn_flag = 1 OR a.jwn_ret_flag = 1) AND LOWER(SUBSTR(b.buyer_flow, 1, 1)) NOT IN (LOWER('4'
      ))
  THEN 1
  ELSE 0
  END AS aare_activated,
  CASE
  WHEN a.jwn_ret_flag = 1 AND LOWER(SUBSTR(b.buyer_flow, 1, 1)) <> LOWER('1')
  THEN 1
  ELSE 0
  END AS aare_retained,
 a.aare_engaged
FROM (SELECT acp_id,
   fiscal_year_shopped,
   '7) JWN' AS channel,
   MAX(ntn_chan_flag) AS ntn_chan_flag,
   MAX(ntn_jwn_flag) AS ntn_jwn_flag,
   MAX(act_flag) AS act_flag,
   MAX(chan_ret_flag) AS chan_ret_flag,
   MAX(jwn_ret_flag) AS jwn_ret_flag,
   MAX(aare_acquired) AS aare_acquired,
   MAX(aare_activated) AS aare_activated,
   MAX(aare_retained) AS aare_retained,
   MAX(aare_engaged) AS aare_engaged
  FROM aare_banner_level
  GROUP BY acp_id,
   fiscal_year_shopped,
   channel) AS a
 INNER JOIN (SELECT DISTINCT acp_id,
   fiscal_year_shopped,
   channel,
   buyer_flow,
   ntc_flag
  FROM buyer_flow_all_views
  WHERE LOWER(SUBSTR(channel, 1, 1)) = LOWER('7')) AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.fiscal_year_shopped
     ) = LOWER(b.fiscal_year_shopped) AND LOWER(a.channel) = LOWER(b.channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy
(SELECT acp_id,
  fiscal_year_shopped,
  channel,
  buyer_flow,
  aare_acquired,
  aare_activated,
  aare_retained,
  aare_engaged,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM (SELECT DISTINCT acp_id,
     fiscal_year_shopped,
     channel,
     buyer_flow,
     aare_acquired,
     aare_activated,
     aare_retained,
     aare_engaged,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM buyer_flow_all_views
    WHERE LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'), LOWER('3'), LOWER('4'))
    UNION ALL
    SELECT DISTINCT acp_id,
     fiscal_year_shopped,
     channel,
     buyer_flow,
     aare_acquired,
     aare_activated,
     aare_retained,
     aare_engaged,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM aare_banner_level
    UNION ALL
    SELECT DISTINCT acp_id,
     fiscal_year_shopped,
     channel,
     buyer_flow,
     aare_acquired,
     aare_activated,
     aare_retained,
     aare_engaged,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM aare_jwn_level) AS t6);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id) on `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy;
--collect statistics column (fiscal_year_shopped) on `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy;
--collect statistics column (channel) on `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy;
--collect statistics column (buyer_flow) on `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy;
--collect statistics column (acp_id,fiscal_year_shopped,channel) on `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
