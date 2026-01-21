
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP02602;
DAG_ID=crr_wbr_dashboard_11521_ACE_ENG;
---     Task_Name=crr_wbr_dashboard;'*/
---     FOR SESSION VOLATILE;

/*
T2/Table Name: {params.ccr_t2_schema}.crr_wbr_dashboard
Team/Owner: Deboleena Ganguly (deboleena.ganguly@nordstrom.com)
Date Created/Modified: 02/09/2023

Note:
-- What is the the purpose of the table

-- What is the update cadence/lookback window

*/

/*
Temp table notes here if applicable
*/
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS tmpinsults
AS
SELECT DISTINCT nefi_npr_id,
 insult_flag
FROM `{{params.gcp_project_id}}`.t2dl_das_crr.crr_model_output AS o
WHERE insult_flag = 1
 AND nefi_npr_id IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS tmpclaims
AS
SELECT DISTINCT p.nefi_npr_id,
 p.which_table,
 p.order_num,
 p.recommended_action,
 p.claim_date,
 p.npr_amount,
  CASE
  WHEN LOWER(p.recommended_action) = LOWER('approve')
  THEN 'approved'
  WHEN LOWER(p.recommended_action) = LOWER('deny') AND LOWER(p.actual_action) = LOWER('approve')
  THEN 'approved'
  WHEN LOWER(p.recommended_action) = LOWER('deny')
  THEN 'denied'
  WHEN LOWER(p.actual_action) = LOWER('approve') AND p.is_returned = 1 AND LOWER(p.recommended_action) NOT IN (LOWER('approve'
      ), LOWER('deny'))
  THEN 'approved'
  WHEN LOWER(p.actual_action) = LOWER('deny') AND p.is_returned = 1 AND LOWER(p.recommended_action) NOT IN (LOWER('approve'
      ), LOWER('deny'))
  THEN 'denied'
  WHEN p.actual_action IS NULL AND p.is_returned = 1 AND LOWER(p.recommended_action) NOT IN (LOWER('approve'), LOWER('deny'
      ))
  THEN 'approved'
  ELSE NULL
  END AS status,
  CASE
  WHEN LOWER(oldf.source_channel_code) = LOWER('FULL_LINE')
  THEN 'NORDSTROM'
  WHEN LOWER(oldf.source_channel_code) = LOWER('RACK')
  THEN 'NORDSTROM_RACK'
  ELSE 'UNKNOWN'
  END AS channel_brand
FROM `{{params.gcp_project_id}}`.t2dl_das_crr.crr_nefi_input AS p
 LEFT JOIN tmpinsults AS s ON LOWER(p.nefi_npr_id) = LOWER(s.nefi_npr_id)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_hdr_fact AS rthf ON CAST(p.order_num AS FLOAT64) = CAST(TRUNC(CAST(rthf.order_num AS FLOAT64)) AS INTEGER)                          
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf ON LOWER(rthf.order_num) = LOWER(oldf.order_num)
WHERE p.is_returned = 1
 AND LOWER(rthf.tran_type_code) = LOWER('{{params.tran_type_code_sale}}')
 AND p.claim_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 183 DAY), WEEK)
 AND LOWER(p.which_table) = LOWER('{{params.which_table}}');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS intermediate
AS
SELECT CAST(f.day_date AS DATE) AS cal_date,
 f.year_num AS fiscal_year,
 f.quarter_454_num AS quarter_num,
 f.month_desc AS fiscal_month,
 f.month_454_num AS fiscal_month_nbr,
 f.week_of_fyr AS fiscal_week,
 o.channel_brand,
 o.which_table,
 SUM(CASE
   WHEN LOWER(o.status) = LOWER('denied')
   THEN 1
   ELSE 0
   END) AS denied_claims_cnt,
 SUM(CASE
   WHEN LOWER(o.status) = LOWER('approved')
   THEN 1
   ELSE 0
   END) AS approved_claims_cnt,
 SUM(CASE
   WHEN LOWER(o.status) = LOWER('denied')
   THEN o.npr_amount
   ELSE NULL
   END) AS denied_amt,
 SUM(CASE
   WHEN LOWER(o.status) = LOWER('approved')
   THEN o.npr_amount
   ELSE NULL
   END) AS approved_amt,
 SUM(CASE
   WHEN i.insult_flag = 1
   THEN o.npr_amount
   ELSE 0
   END) AS insult_amt,
 SUM(CASE
   WHEN LOWER(o.status) IN (LOWER('approved'), LOWER('denied'))
   THEN o.npr_amount
   ELSE NULL
   END) AS total_claim_amt,
 SUM(CASE
   WHEN LOWER(o.status) IN (LOWER('approved'), LOWER('denied'))
   THEN 1
   ELSE 0
   END) AS total_claims_cnt,
 SUM(CASE
   WHEN LOWER(o.recommended_action) NOT IN (LOWER('approve'), LOWER('deny'))
   THEN 1
   ELSE 0
   END) AS reviewed_claims_cnt,
 SUM(CASE
   WHEN LOWER(o.recommended_action) NOT IN (LOWER('approve'), LOWER('deny'))
   THEN o.npr_amount
   ELSE NULL
   END) AS reviewed_claims_amt,
 SUM(CASE
   WHEN i.insult_flag = 1
   THEN 1
   ELSE 0
   END) AS insults_cnt
FROM tmpclaims AS o
 LEFT JOIN tmpinsults AS i ON LOWER(o.nefi_npr_id) = LOWER(i.nefi_npr_id)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS f ON CAST(o.claim_date AS DATE) = f.day_date
GROUP BY cal_date,
 fiscal_year,
 quarter_num,
 fiscal_month,
 fiscal_month_nbr,
 fiscal_week,
 o.channel_brand,
 o.which_table;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS pre_insert
AS
SELECT cal_date,
 fiscal_week,
 fiscal_month,
 fiscal_year,
 denied_claims_cnt AS denied_claims,
 approved_claims_cnt AS approved_claims,
 denied_amt,
 approved_amt,
 insults_cnt AS insults,
 total_claim_amt,
 total_claims_cnt AS total_claims,
 reviewed_claims_cnt AS reviewed_claims_count,
 reviewed_claims_amt AS reviewed_claims_amount,
 insult_amt AS insult_dollars,
 denied_amt AS net_savings,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'NORDSTROM_RACK'
  ELSE 'NORDSTROM'
  END AS channel_brand,
 which_table,
 denied_claims_cnt,
 approved_claims_cnt,
 reviewed_claims_cnt
FROM intermediate
GROUP BY cal_date,
 fiscal_week,
 fiscal_month,
 fiscal_year,
 denied_claims,
 approved_claims,
 denied_amt,
 approved_amt,
 insults,
 total_claim_amt,
 total_claims,
 reviewed_claims_count,
 reviewed_claims_amount,
 insult_dollars,
 net_savings,
 channel_brand,
 which_table;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.ccr_t2_schema}}.crr_wbr_dashboard
WHERE cal_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 183 DAY), WEEK);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.ccr_t2_schema}}.crr_wbr_dashboard
(SELECT cal_date,
  fiscal_week,
  fiscal_month,
  fiscal_year,
  denied_claims,
  approved_claims,
  denied_amt,
  approved_amt,
  insults,
  total_claim_amt,
  total_claims,
  reviewed_claims_count,
  reviewed_claims_amount,
  insult_dollars,
  net_savings,
  channel_brand,
  which_table,
  denied_claims_cnt,
  approved_claims_cnt,
  reviewed_claims_cnt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM pre_insert);
 
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (cal_date), COLUMN (fiscal_week), COLUMN (fiscal_month), COLUMN (fiscal_year), COLUMN (channel_brand), COLUMN (which_table) on t2dl_das_crr.crr_wbr_dashboard;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
