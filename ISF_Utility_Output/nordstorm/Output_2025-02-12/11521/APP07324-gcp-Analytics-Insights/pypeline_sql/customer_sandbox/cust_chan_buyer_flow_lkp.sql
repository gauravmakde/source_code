SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_chan_buyer_flow_lkp_11521_ACE_ENG;
     Task_Name=cust_chan_buyer_flow_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_chan_buyer_flow_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Notes:
-- Customer Buyerflow Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_chan_buyer_flow_lkp
;

INSERT INTO {str_t2_schema}.cust_chan_buyer_flow_lkp
    SELECT DISTINCT cust_chan_buyer_flow_desc
                   ,ROW_NUMBER() OVER (ORDER BY cust_chan_buyer_flow_desc  ASC ) AS cust_chan_buyer_flow_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT COALESCE(cust_chan_buyer_flow, 'NA') as cust_chan_buyer_flow_desc  
            from T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes cli)a ;

COLLECT STATISTICS COLUMN (cust_chan_buyer_flow_num),
                   COLUMN (cust_chan_buyer_flow_desc)
ON {str_t2_schema}.cust_chan_buyer_flow_lkp ;

SET QUERY_BAND = NONE FOR SESSION;