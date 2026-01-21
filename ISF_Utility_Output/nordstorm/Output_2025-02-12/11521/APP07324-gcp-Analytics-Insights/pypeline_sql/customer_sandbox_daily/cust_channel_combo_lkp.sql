SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=cust_channel_combo_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Channel Combo Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.cust_channel_combo_lkp
;

INSERT INTO {str_t2_schema}.cust_channel_combo_lkp
SELECT DISTINCT cust_channel_combo_desc
     , ROW_NUMBER() OVER (ORDER BY cust_channel_combo_desc ASC) AS cust_channel_combo_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT cust_channel_combo AS cust_channel_combo_desc
      FROM T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes) a
;


SET QUERY_BAND = NONE FOR SESSION;
