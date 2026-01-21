SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=cust_chan_buyer_flow_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Customer Buyer Flow Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.cust_chan_buyer_flow_lkp
;

INSERT INTO {str_t2_schema}.cust_chan_buyer_flow_lkp
SELECT DISTINCT cust_chan_buyer_flow_desc
     , ROW_NUMBER() OVER (ORDER BY cust_chan_buyer_flow_desc ASC) AS cust_chan_buyer_flow_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(cust_chan_buyer_flow, 'NA') AS cust_chan_buyer_flow_desc
      FROM T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes cli) a
;

COLLECT STATISTICS COLUMN (cust_chan_buyer_flow_num),
                   COLUMN (cust_chan_buyer_flow_desc)
ON {str_t2_schema}.cust_chan_buyer_flow_lkp
;


SET QUERY_BAND = NONE FOR SESSION;
