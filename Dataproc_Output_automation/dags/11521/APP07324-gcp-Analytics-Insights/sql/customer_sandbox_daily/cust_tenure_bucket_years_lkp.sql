-- SET QUERY_BAND = 'App_ID=APP08737;
-- DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
-- Task_Name=cust_tenure_bucket_years_lkp_build;'
-- FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Customer Tenure Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_tenure_bucket_years_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_tenure_bucket_years_lkp
(SELECT cust_tenure_bucket_years_desc,
  ROW_NUMBER() OVER (ORDER BY cust_tenure_bucket_years_desc) AS cust_tenure_bucket_years_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(cust_tenure_bucket_years, 'Unknown') AS cust_tenure_bucket_years_desc
   FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_cust_chan_yr_attributes) AS a);

