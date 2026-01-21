SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=cust_loyalty_type_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Loyalty Type Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.cust_loyalty_type_lkp
;

INSERT INTO {str_t2_schema}.cust_loyalty_type_lkp
SELECT DISTINCT cust_loyalty_type_desc
     , ROW_NUMBER() OVER (ORDER BY cust_loyalty_type_desc ASC) AS cust_loyalty_type_num
     , CURRENT_TIMESTAMP(6) as dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(cust_loyalty_type, 'Unknown') AS cust_loyalty_type_desc
      FROM T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes) a
;


SET QUERY_BAND = NONE FOR SESSION;
