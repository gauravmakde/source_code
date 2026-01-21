-- SET QUERY_BAND = 'App_ID=APP08240;
-- DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
-- Task_Name=run_cco_buyer_flow_cust_channel_week;'
-- FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build a Customer/Week/Channel-level table for AARE & Buyer Flow
 *
 * weekly version Aug 2024 from modification to yearly buyer flow sql
 * note that much of the code involves first developing an acp_id, week, channel level
 * record, and then programmatically inserting a banner level record, and total JWN record
 *
 ************************************************************************************/
 /***********************************************************************************/
 /***********************************************************************************/


/************************************************************************************
 * PART I-A) Dynamic dates for CCO tables
 *
 * This code uses the current_date to
 * 1) Determine the current and most-recently complete fiscal month
 * 2) Get the following dates needed for the CCO tables
 *    a) fy18_start_dt: Start of FY18
 *    b) fy18_end_dt: End of FY18
 *    c) fy19_start_dt: Start of FY18
 *    d) latest_mo_dt: End of most-recently complete fiscal month
 *    e) r4yr1_start_dt: Start of rolling-4-year period ending most-recently complete fiscal month
 *    f) r4yr0_start_dt: Start of year prior to
 *                       rolling-4-year period ending most-recently complete fiscal month
 *    g) r4yr0_end_dt: End of year prior to
 *                     rolling-4-year period ending most-recently complete fiscal month
 ************************************************************************************/

/*********** Determine the current & most-recently complete fiscal month ************/BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
---Task_Name=run_cco_buyer_flow_cust_channel_week;'*/
BEGIN
SET _ERROR_CODE  =  0;
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
/*********** Get the dates needed for the CCO tables ********************************/
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
/*********** Create Start & End dates lookup table **********************************/
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT fy19_start_dt AS cust_year_start_date,
 latest_mo_dt AS cust_year_end_date
FROM date_parameter_lookup AS dp;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

--dates for Pre-Prod testing
-- create multiset volatile table date_lookup as (
-- select date'2024-09-10' cust_year_start_date
--      , date'2024-09-10'+7 cust_year_end_date
-- from date_parameter_lookup dp
-- ) with data primary index(cust_year_start_date) on commit preserve rows;

/************************************************************************************
 * PART I-B) Create "driver" tables based on the transactions in CCO_LINES
 * (i.e., transaction filtering has already happened when building CCO_LINES, so we don't need
 * to repeat it here - just pull the transactions from CCO_LINES as a driver for the
 * rest of this query. Some table pulls below will be by acp_id, and some by acp_id and channel,
 * so there are two drivers
 ************************************************************************************/
CREATE TEMPORARY TABLE IF NOT EXISTS cco_lines_extract
AS
SELECT DISTINCT acp_id,
 channel,
 date_shopped
FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS a
WHERE LOWER(data_source) = LOWER('1) TRANSACTION')
 AND date_shopped BETWEEN (SELECT cust_year_start_date
   FROM date_lookup) AND (SELECT cust_year_end_date
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (partition) on cco_lines_extract;
--collect statistics column (date_shopped) on cco_lines_extract;
BEGIN
SET _ERROR_CODE  =  0;
/*********** Create customer/week-level driver table ********************************/
CREATE TEMPORARY TABLE IF NOT EXISTS cust_week_driver
AS
SELECT DISTINCT a.acp_id,
 b.week_idnt
FROM cco_lines_extract AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.date_shopped = b.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on cust_week_driver;
BEGIN
SET _ERROR_CODE  =  0;
/*********** Create customer/channel/week-level driver table ************************/
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_week_driver
AS
SELECT DISTINCT a.acp_id,
 a.channel,
 b.week_idnt
FROM cco_lines_extract AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.date_shopped = b.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, channel, week_idnt) on cust_chan_week_driver;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************/
/************************************************************************************
 * PART II) Build lookup tables:
 *  A) Acquisition date/channel
 *  B) Activation date/Channel
 *  C) New-to-Channel/Banner lookup
 *  D) Table containing customer/channel from 52 weeks prior
 *  E) Create customer/channel/week-level table from the past 5 years (4 years from line-item table + 1 year from "I-D")
 *  F) Create customer/channel/week-level table from the past 4 years (flagging whether they shopped 1 year before)
 ************************************************************************************/
/************************************************************************************/

/*******************************************
  most aare (acquisition, activation, ntn) just need to pull from associated tables for those values
  however, aare dates are by day, so need to convert to week
  also, they are by acp_id, and we just need acp_id's that are in cco_lines table
*******************************************/
CREATE TEMPORARY TABLE IF NOT EXISTS day_week_idnt
AS
SELECT DISTINCT day_date,
 week_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
WHERE day_date BETWEEN (SELECT cust_year_start_date
   FROM date_lookup) AND (SELECT cust_year_end_date
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (day_date) on day_week_idnt;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS acp_id_list
AS
SELECT DISTINCT acp_id
FROM cust_week_driver AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id) on acp_id_list;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART II-A) For each week, build a table of the 52 weeks preceding it
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS extract_week_idnts
AS
SELECT DISTINCT week_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b
WHERE day_date BETWEEN (SELECT DATE_SUB(cust_year_start_date, INTERVAL 365 DAY)
   FROM date_lookup) AND (SELECT cust_year_end_date
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_52wks
AS
SELECT DISTINCT week_idnt AS curr_week_idnt,
 LAG(week_idnt, 52) OVER (ORDER BY week_idnt) AS prev_52wk_start_week_idnt,
 LAG(week_idnt, 1) OVER (ORDER BY week_idnt) AS prev_52wk_end_week_idnt
FROM extract_week_idnts AS b;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_52wks_expand_wks
AS
SELECT DISTINCT wks.curr_week_idnt,
 cal.week_idnt AS prev_52_wks_wk_idnt
FROM cust_52wks AS wks
 INNER JOIN extract_week_idnts AS cal ON cal.week_idnt BETWEEN wks.prev_52wk_start_week_idnt AND wks.prev_52wk_end_week_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (curr_week_idnt, prev_52_wks_wk_idnt) on cust_52wks_expand_wks;
--collect statistics column (curr_week_idnt) on cust_52wks_expand_wks;
--collect statistics column (prev_52_wks_wk_idnt) on cust_52wks_expand_wks;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART II-B) Acquisition date/channel
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition_prep
AS
SELECT DISTINCT a.acp_id,
 c.week_idnt,
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
 INNER JOIN acp_id_list AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
 INNER JOIN day_week_idnt AS c ON a.aare_status_date = c.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition (
acp_id STRING,
week_idnt INTEGER,
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
  week_idnt,
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
  week_idnt,
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
  week_idnt,
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
 week_idnt
FROM customer_acquisition;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

/************************************************************************************
 * PART II-C) Activation date/Channel
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS customer_activation_prep
AS
SELECT DISTINCT a.acp_id,
 c.week_idnt,
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
 INNER JOIN acp_id_list AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
 INNER JOIN day_week_idnt AS c ON a.activated_date = c.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_activation (
acp_id STRING,
week_idnt INTEGER,
channel STRING
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO customer_activation
(SELECT DISTINCT acp_id,
  week_idnt,
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
  week_idnt,
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
  week_idnt,
  '7) JWN' AS channel
 FROM customer_activation_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


/************************************************************************************
 * PART II-D) New-to-Channel/Banner lookup:
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
--collect statistics column (acp_id  ) on cco_new_2_channel_lkup;
--collect statistics column (channel ) on cco_new_2_channel_lkup;
--collect statistics column (ntx_date) on cco_new_2_channel_lkup;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_new2channel_prep
AS
SELECT DISTINCT a.acp_id,
 c.week_idnt,
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
 INNER JOIN acp_id_list AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
 INNER JOIN day_week_idnt AS c ON a.ntx_date = c.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_new2channel (
acp_id STRING,
week_idnt INTEGER,
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
  week_idnt,
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
  week_idnt,
  '7) JWN' AS channel
 FROM customer_acquisition_prep);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

/************************************************************************************
 * Part II-E) Prior data (used for flagging "Retained" customers, folks who shopped
 * in 52 wks prior to the 4 year (by week) window pulled from CCO_LINES
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS cust_prev_cco_line_data (
acp_id STRING,
channel STRING,
week_idnt INTEGER
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_prev_cco_line_data
(SELECT DISTINCT dtl.acp_id,
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
  c.week_idnt
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
  INNER JOIN acp_id_list AS b ON LOWER(dtl.acp_id) = LOWER(b.acp_id)
  INNER JOIN day_week_idnt AS c ON COALESCE(dtl.order_date, dtl.tran_date) = c.day_date
 WHERE dtl.business_day_date BETWEEN (SELECT DATE_SUB(cust_year_start_date, INTERVAL 728 DAY)
    FROM date_lookup) AND (SELECT cust_year_start_date
    FROM date_lookup)
  AND COALESCE(dtl.order_date, dtl.tran_date) BETWEEN (SELECT DATE_SUB(cust_year_start_date, INTERVAL 721 DAY)
    FROM date_lookup) AND (SELECT cust_year_start_date
    FROM date_lookup)
  AND dtl.line_net_usd_amt >= 0
  AND dtl.acp_id IS NOT NULL);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART II-F) Create cust/channel/week-level table for all report weeks (+ 1 earlier year)
 ************************************************************************************/

CREATE TEMPORARY TABLE IF NOT EXISTS cust_week_channel_base (
acp_id STRING,
week_idnt INTEGER,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_week_channel_base
(SELECT DISTINCT acp_id,
  week_idnt,
  channel
 FROM cust_chan_week_driver);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_week_channel_base
(SELECT DISTINCT acp_id,
  week_idnt,
  channel
 FROM cust_prev_cco_line_data);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt, channel) on cust_week_channel_base;
--collect statistics column (acp_id, week_idnt) on cust_week_channel_base;
--collect statistics column (acp_id, channel) on cust_week_channel_base;
--collect statistics column (week_idnt) on cust_week_channel_base;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_week_channel_channel (
acp_id STRING,
week_idnt INTEGER,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_week_channel_channel
(SELECT DISTINCT acp_id,
  week_idnt,
   CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
   THEN '5) Nordstrom Banner'
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
   THEN '6) Rack Banner'
   ELSE NULL
   END AS channel
 FROM cust_chan_week_driver);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_week_channel_channel
(SELECT DISTINCT acp_id,
  week_idnt,
   CASE
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
   THEN '5) Nordstrom Banner'
   WHEN LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
   THEN '6) Rack Banner'
   ELSE NULL
   END AS channel
 FROM cust_prev_cco_line_data);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt, channel) on cust_week_channel_channel;
--collect statistics column (acp_id, week_idnt) on cust_week_channel_channel;
--collect statistics column (acp_id, channel) on cust_week_channel_channel;
--collect statistics column (week_idnt) on cust_week_channel_channel;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_week_channel_banner (
acp_id STRING,
week_idnt INTEGER,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_week_channel_banner
(SELECT DISTINCT acp_id,
  week_idnt,
  '7) JWN' AS channel
 FROM cust_week_driver);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_week_channel_banner
(SELECT DISTINCT acp_id,
  week_idnt,
  '7) JWN' AS channel
 FROM cust_prev_cco_line_data);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt, channel) on cust_week_channel_banner;
--collect statistics column (acp_id, week_idnt) on cust_week_channel_banner;
--collect statistics column (acp_id, channel) on cust_week_channel_banner;
--collect statistics column (week_idnt) on cust_week_channel_banner;
BEGIN
SET _ERROR_CODE  =  0;
/******************************************************
 Make a table of weeks having previous 52 week purchases, so a
 successful join to this table means there was activity in
 the 52 wk period preceding this wk
 To not exceed cpu limits, it breaks into 3 pieces
 base, channel, and banner. Also exceeded cpu limits,
 so build three NOPI tables unioned into final table
 prev_52wk_table because teradata loads empty tables
 with block loads for speed.
 *****************************************************/
 CREATE TEMPORARY TABLE IF NOT EXISTS prev_52wk_table_base (
acp_id STRING,
week_idnt INTEGER,
channel STRING
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO prev_52wk_table_base
(SELECT DISTINCT drv1.acp_id,
  drv1.week_idnt,
  drv1.channel
 FROM cust_week_channel_base AS drv1
  INNER JOIN cust_week_channel_base AS drv2 ON LOWER(drv1.acp_id) = LOWER(drv2.acp_id) AND LOWER(drv1.channel) = LOWER(drv2
     .channel)
  INNER JOIN cust_52wks_expand_wks AS wks ON drv1.week_idnt = wks.curr_week_idnt AND drv2.week_idnt = wks.prev_52_wks_wk_idnt
    );
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS prev_52wk_table_channel (
acp_id STRING,
week_idnt INTEGER,
channel STRING
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO prev_52wk_table_channel
(SELECT DISTINCT drv1.acp_id,
  drv1.week_idnt,
  drv1.channel
 FROM cust_week_channel_channel AS drv1
  INNER JOIN cust_week_channel_channel AS drv2 ON LOWER(drv1.acp_id) = LOWER(drv2.acp_id) AND LOWER(drv1.channel) =
    LOWER(drv2.channel)
  INNER JOIN cust_52wks_expand_wks AS wks ON drv1.week_idnt = wks.curr_week_idnt AND drv2.week_idnt = wks.prev_52_wks_wk_idnt
    );
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS prev_52wk_table_banner (
acp_id STRING,
week_idnt INTEGER,
channel STRING
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO prev_52wk_table_banner
(SELECT DISTINCT drv1.acp_id,
  drv1.week_idnt,
  drv1.channel
 FROM cust_week_channel_banner AS drv1
  INNER JOIN cust_week_channel_banner AS drv2 ON LOWER(drv1.acp_id) = LOWER(drv2.acp_id) AND LOWER(drv1.channel) = LOWER(drv2
     .channel)
  INNER JOIN cust_52wks_expand_wks AS wks ON drv1.week_idnt = wks.curr_week_idnt AND drv2.week_idnt = wks.prev_52_wks_wk_idnt
    );
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS prev_52wk_table (
acp_id STRING,
week_idnt INTEGER,
channel STRING
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO prev_52wk_table
(SELECT *
 FROM (SELECT *
    FROM prev_52wk_table_base
    UNION ALL
    SELECT *
    FROM prev_52wk_table_channel
    UNION ALL
    SELECT *
    FROM prev_52wk_table_banner) AS t);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt, channel) on prev_52wk_table;
--collect statistics column (acp_id, week_idnt) on prev_52wk_table;
BEGIN
SET _ERROR_CODE  =  0;
/*********** Join the current week to the prior 52 week year on:
 *    a) customer/channel/week... to get a channel-level retention indicators
 *    b) customer/week ... to get JWN-level retention indicators
 ***********/
CREATE TEMPORARY TABLE IF NOT EXISTS cust_channel_wk
AS
SELECT *
FROM cust_week_channel_base
UNION ALL
SELECT *
FROM cust_week_channel_channel
UNION ALL
SELECT *
FROM cust_week_channel_banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt, channel) on cust_channel_wk;
--collect statistics column (acp_id, week_idnt) on cust_channel_wk;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_week_chan_retention
AS
SELECT DISTINCT a.acp_id,
 a.week_idnt,
 a.channel,
  CASE
  WHEN b.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS chan_ret_flag,
  CASE
  WHEN c.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS jwn_ret_flag
FROM cust_channel_wk AS a
 LEFT JOIN prev_52wk_table AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.week_idnt = b.week_idnt AND LOWER(a.channel)
   = LOWER(b.channel)
 LEFT JOIN (SELECT DISTINCT acp_id,
   week_idnt
  FROM prev_52wk_table) AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND a.week_idnt = c.week_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

/************************************************************************************/
/************************************************************************************
 * PART III) Join all tables together to create Buyer-Flow & AARE Indicators
 ************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART III-A) Join all tables together to create indicators
 ************************************************************************************/

--collect statistics column (acp_id, week_idnt, channel) on cust_week_chan_retention;
--collect statistics column (acp_id, week_idnt) on cust_week_chan_retention;
--collect statistics column (acp_id, week_idnt) on customer_acquisition_jwn;
--collect statistics column (acp_id, week_idnt, channel) on customer_acquisition;
--collect statistics column (acp_id, week_idnt, channel) on customer_activation;
--collect statistics column (acp_id, week_idnt, channel) on customer_new2channel;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS aare_buyer_flow_prep
AS
SELECT DISTINCT retain.acp_id,
 retain.week_idnt,
 retain.channel,
 retain.chan_ret_flag,
 retain.jwn_ret_flag,
  CASE
  WHEN acqjwn.acp_id IS NOT NULL AND retain.jwn_ret_flag = 0
  THEN 1
  ELSE 0
  END AS ntn_jwn_flag,
  CASE
  WHEN retain.jwn_ret_flag = 0 AND CASE
      WHEN acqjwn.acp_id IS NOT NULL AND retain.jwn_ret_flag = 0
      THEN 1
      ELSE 0
      END = 1 AND acq.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS ntn_chan_flag,
  CASE
  WHEN act.acp_id IS NOT NULL AND (retain.jwn_ret_flag = 1 OR CASE
       WHEN acqjwn.acp_id IS NOT NULL AND retain.jwn_ret_flag = 0
       THEN 1
       ELSE 0
       END = 1)
  THEN 1
  ELSE 0
  END AS act_flag,
  CASE
  WHEN retain.chan_ret_flag = 0 AND ntn.acp_id IS NOT NULL OR CASE
     WHEN acqjwn.acp_id IS NOT NULL AND retain.jwn_ret_flag = 0
     THEN 1
     ELSE 0
     END = 1
  THEN 1
  ELSE 0
  END AS ntc_flag
FROM cust_week_chan_retention AS retain
 LEFT JOIN customer_acquisition AS acq ON LOWER(retain.acp_id) = LOWER(acq.acp_id) AND retain.week_idnt = acq.week_idnt
  AND LOWER(retain.channel) = LOWER(acq.channel)
 LEFT JOIN customer_acquisition_jwn AS acqjwn ON LOWER(retain.acp_id) = LOWER(acqjwn.acp_id) AND retain.week_idnt = acq
   .week_idnt
 LEFT JOIN customer_activation AS act ON LOWER(retain.acp_id) = LOWER(act.acp_id) AND retain.week_idnt = acq.week_idnt
  AND LOWER(retain.channel) = LOWER(act.channel)
 LEFT JOIN customer_new2channel AS ntn ON LOWER(retain.acp_id) = LOWER(ntn.acp_id) AND retain.week_idnt = acq.week_idnt
  AND LOWER(retain.channel) = LOWER(ntn.channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART III-B) Create Buyer-Flow & AARE columns
 ************************************************************************************/

/*********** PART III-B-i) Create the Buyer-Flow segments + Channel-level AARE indicators
 * 1) Create the Buyer-Flow segments first,
 * 2) then create the AARE columns in a way that Buyer-Flow & AARE don't conflict
 ***********/
CREATE TEMPORARY TABLE IF NOT EXISTS buyer_flow_all_views
AS
SELECT DISTINCT acp_id,
 week_idnt,
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
--collect statistics column (acp_id, week_idnt) on buyer_flow_all_views;
--collect statistics column (channel) on buyer_flow_all_views;
BEGIN
SET _ERROR_CODE  =  0;

/*********** PART III-B-ii) Create the Banner-level AARE indicators
 * Ideally, these are the "max" of the channel-level indicators...
 * but must be changed if they conflict with Buyer-Flow values
 ***********/
CREATE TEMPORARY TABLE IF NOT EXISTS aare_banner_level
AS
SELECT DISTINCT a.acp_id,
 a.week_idnt,
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
   week_idnt,
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
   week_idnt,
   channel) AS a
 INNER JOIN (SELECT DISTINCT acp_id,
   week_idnt,
   channel,
   buyer_flow,
   ntc_flag
  FROM buyer_flow_all_views
  WHERE LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('5'), LOWER('6'))) AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.week_idnt
     = b.week_idnt AND LOWER(a.channel) = LOWER(b.channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, week_idnt) on aare_banner_level;
BEGIN
SET _ERROR_CODE  =  0;
/*********** PART III-B-iii) Create the JWN-level AARE indicators
 * Ideally, these are the "max" of the banner-level indicators...
 * but must be changed if they conflict with Buyer-Flow values
 ***********/
CREATE TEMPORARY TABLE IF NOT EXISTS aare_jwn_level
AS
SELECT DISTINCT a.acp_id,
 a.week_idnt,
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
   week_idnt,
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
   week_idnt,
   channel) AS a
 INNER JOIN (SELECT DISTINCT acp_id,
   week_idnt,
   channel,
   buyer_flow,
   ntc_flag
  FROM buyer_flow_all_views
  WHERE LOWER(SUBSTR(channel, 1, 1)) = LOWER('7')) AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.week_idnt = b.week_idnt
     AND LOWER(a.channel) = LOWER(b.channel);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************
 * PART III-C) Insert Buyer-Flow & AARE values into final table
 ************************************************************************************/

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_cust_channel_week;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_cust_channel_week
(SELECT *
 FROM (SELECT DISTINCT acp_id,
     week_idnt,
     channel,
     buyer_flow,
     aare_acquired,
     aare_activated,
     aare_retained,
     aare_engaged,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM buyer_flow_all_views
    WHERE LOWER(SUBSTR(channel, 1, 1)) IN (LOWER('1'), LOWER('2'), LOWER('3'), LOWER('4'))
    UNION ALL
    SELECT DISTINCT acp_id,
     week_idnt,
     channel,
     buyer_flow,
     aare_acquired,
     aare_activated,
     aare_retained,
     aare_engaged,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM aare_banner_level
    UNION ALL
    SELECT DISTINCT acp_id,
     week_idnt,
     channel,
     buyer_flow,
     aare_acquired,
     aare_activated,
     aare_retained,
     aare_engaged,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM aare_jwn_level) AS t6);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
