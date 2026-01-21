SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=cust_loyalty_level_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Loyalty Level Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM T2DL_DAS_STRATEGY.cust_loyalty_level_lkp
;

INSERT INTO T2DL_DAS_STRATEGY.cust_loyalty_level_lkp
SELECT DISTINCT cust_loyalty_level_desc
     , ROW_NUMBER() OVER (ORDER BY cust_loyalty_level_desc ASC) AS cust_loyalty_level_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(cust_loyalty_level, 'Unknown') AS cust_loyalty_level_desc
      FROM T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes) a
;


SET QUERY_BAND = NONE FOR SESSION;
