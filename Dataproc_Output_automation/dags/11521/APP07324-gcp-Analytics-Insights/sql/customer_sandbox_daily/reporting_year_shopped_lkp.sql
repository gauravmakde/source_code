-- SET QUERY_BAND = 'App_ID=APP08737;
-- DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
-- Task_Name=reporting_year_shopped_lkp_build;'
-- FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Reporting Year Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.reporting_year_shopped_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.reporting_year_shopped_lkp
(SELECT reporting_year_shopped_desc,
  ROW_NUMBER() OVER (ORDER BY reporting_year_shopped_desc) AS reporting_year_shopped_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(reporting_year_shopped, 'Missing') AS reporting_year_shopped_desc
   FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS cli) AS a);


-- SET QUERY_BAND = NONE FOR SESSION;
