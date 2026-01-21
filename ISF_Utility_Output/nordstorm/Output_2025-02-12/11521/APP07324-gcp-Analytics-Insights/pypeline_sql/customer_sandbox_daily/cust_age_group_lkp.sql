SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=cust_age_group_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Customer Age Group Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.cust_age_group_lkp
;

INSERT INTO {str_t2_schema}.cust_age_group_lkp
SELECT DISTINCT cust_age_group_desc
     , ROW_NUMBER() OVER (ORDER BY cust_age_group_desc ASC) AS cust_age_group_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(cust_age_group, 'Unknown') AS cust_age_group_desc
      FROM T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes) a
;


SET QUERY_BAND = NONE FOR SESSION;
