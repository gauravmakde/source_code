/* SET QUERY_BAND = 'App_ID=APP08159;
     DAG_ID=cust_service_11521_ACE_ENG;
     Task_Name=cust_service_6mo;'
     FOR SESSION VOLATILE;*/

/*
T2/Table Name: T2DL_DAS_SERVICE_ENG.cust_service_6mo
Team：Customer Analytics - Styling & Strategy
Date Created: Mar. 12th 2023 

Note:
-- Update Cadence: Monthly (after end of each fiscal month)

*/


/***************************************************************************************/
/***************************************************************************************/
/********* Step 1: create realigned fiscal calendar                            *********/
/***************************************************************************************/
/***************************************************************************************/

/***************************************************************************************/
/********* Step 1-a: Going back to start of 2009, find all years with a "53rd week" *********/
/***************************************************************************************/
--drop table week_53_yrs;



CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yrs
AS
SELECT year_num,
 RANK() OVER (ORDER BY year_num DESC) AS recency_rank
FROM (SELECT DISTINCT year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE week_of_fyr = 53
   AND day_date >= DATE '2009-01-01'
   AND day_date <= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 365 DAY)) AS x;


CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yr_count
CLUSTER BY year_count
AS
SELECT COUNT(DISTINCT year_num) AS year_count
FROM week_53_yrs AS x;


CREATE TEMPORARY TABLE IF NOT EXISTS realigned_fiscal_calendar (
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
) CLUSTER BY day_date;


INSERT INTO realigned_fiscal_calendar
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
 WHERE day_date >= DATE '2009-01-01'
  AND day_date <= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 365 DAY)
  AND week_of_fyr <> 53);


--COLLECT   STATISTICS COLUMN (MONTH_NUM) ON     realigned_fiscal_calendar


--COLLECT   STATISTICS COLUMN (DAY_DATE) ON     realigned_fiscal_calendar


CREATE TEMPORARY TABLE IF NOT EXISTS get_months
AS
SELECT DISTINCT earliest_minus_3,
 earliest_act_mo,
  last_complete_mo - 200 AS part1_end_mo,
  CASE
  WHEN MOD(last_complete_mo, 100) = 12
  THEN last_complete_mo - 111
  ELSE last_complete_mo - 199
  END AS part2_start_mo,
  last_complete_mo - 100 AS part2_end_mo,
  CASE
  WHEN MOD(last_complete_mo, 100) = 12
  THEN last_complete_mo - 11
  ELSE last_complete_mo - 99
  END AS part3_start_mo,
 last_complete_mo
FROM (SELECT DISTINCT CURRENT_DATE('PST8PDT') AS todays_date,
   month_num AS todays_month,
    CASE
    WHEN MOD(month_num, 100) = 1
    THEN month_num - 89
    ELSE month_num - 1
    END AS last_complete_mo,
    CASE
    WHEN MOD(month_num, 100) = 12
    THEN month_num - 211
    ELSE month_num - 299
    END AS earliest_act_mo,
    CASE
    WHEN MOD(month_num, 100) IN (1, 2)
    THEN month_num - 390
    ELSE month_num - 302
    END AS earliest_minus_3
  FROM realigned_fiscal_calendar AS a
  WHERE day_date = CURRENT_DATE('PST8PDT')) AS x;


CREATE TEMPORARY TABLE IF NOT EXISTS fiscal_months
AS
SELECT DISTINCT CASE
  WHEN MOD(CAST(TRUNC(floor(month_num)) AS INTEGER), 100) IN (1, 2, 3, 4, 5)
  THEN month_num - 93
  ELSE month_num - 5
  END AS start_6mo,
 month_num AS year_ending
FROM realigned_fiscal_calendar
WHERE month_num >= (SELECT part2_start_mo
   FROM get_months)
 AND month_num <= (SELECT last_complete_mo
   FROM get_months);


CREATE TEMPORARY TABLE IF NOT EXISTS acp_id_service_6mo_temp
AS
SELECT a.acp_id,
 b.year_ending AS six_mo_ending,
 a.service_name,
 MAX(a.customer_qualifier) AS customer_qualifier,
 SUM(a.gross_usd_amt_whole) AS gross_usd_amt_whole,
 SUM(a.net_usd_amt_whole) AS net_usd_amt_whole,
 SUM(a.gross_usd_amt_split) AS gross_usd_amt_split,
 SUM(a.net_usd_amt_split) AS net_usd_amt_split,
 MAX(a.private_style) AS private_style
FROM `{{params.gcp_project_id}}`.{{params.service_eng_t2_schema}}.cust_service_month AS a
 INNER JOIN fiscal_months AS b ON a.month_num >= b.start_6mo AND a.month_num <= b.year_ending
GROUP BY a.acp_id,
 a.service_name,
 b.year_ending;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.service_eng_t2_schema}}.cust_service_6mo;


INSERT INTO `{{params.gcp_project_id}}`.{{params.service_eng_t2_schema}}.cust_service_6mo
(SELECT DISTINCT a.acp_id,
  CAST(a.six_mo_ending AS NUMERIC) AS six_mo_ending,
  a.service_name,
  a.customer_qualifier,
  a.gross_usd_amt_whole,
  a.net_usd_amt_whole,
  a.gross_usd_amt_split,
  a.net_usd_amt_split,
  a.private_style,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM acp_id_service_6mo_temp AS a
  INNER JOIN (SELECT DISTINCT acp_id,
    six_mo_ending
   FROM acp_id_service_6mo_temp
   WHERE LOWER(service_name) = LOWER('Z_) Nordstrom')) AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.six_mo_ending = b
    .six_mo_ending);


--COLLECT   STATISTICS COLUMN(acp_id),                    COLUMN(six_mo_ending),                    COLUMN(service_name) ON T2DL_DAS_SERVICE_ENG.cust_service_6mo