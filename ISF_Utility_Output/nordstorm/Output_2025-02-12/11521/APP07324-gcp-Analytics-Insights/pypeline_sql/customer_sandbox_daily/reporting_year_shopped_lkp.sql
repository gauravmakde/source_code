SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=reporting_year_shopped_lkp_build;'
FOR SESSION VOLATILE;


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
DELETE
FROM {str_t2_schema}.reporting_year_shopped_lkp
;

INSERT INTO {str_t2_schema}.reporting_year_shopped_lkp
SELECT DISTINCT reporting_year_shopped_desc
     , ROW_NUMBER() OVER (ORDER BY reporting_year_shopped_desc ASC) AS reporting_year_shopped_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(reporting_year_shopped, 'Missing') AS reporting_year_shopped_desc
      FROM T2DL_DAS_STRATEGY.cco_line_items cli) a
;


SET QUERY_BAND = NONE FOR SESSION;
