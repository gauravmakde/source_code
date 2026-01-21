SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_chan_buyer_flow_lkp_11521_ACE_ENG;
     Task_Name=cust_nord_buyer_flow_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_nord_buyer_flow_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 05/22/2023

Notes:
-- Customer Nordstrom Banner Buyerflow Look-up table for microstrategy customer sandbox
-- Rebuilding a copy of cust_nord_buyer_flow_lkp table as Microstrategy requires seperate look-up for each prompt
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_nord_buyer_flow_lkp
;

INSERT INTO {str_t2_schema}.cust_nord_buyer_flow_lkp
    SELECT DISTINCT cust_chan_buyer_flow_desc AS cust_nord_buyer_flow_desc
                   , cust_chan_buyer_flow_num AS cust_nord_buyer_flow_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM {str_t2_schema}.cust_chan_buyer_flow_lkp;

COLLECT STATISTICS COLUMN (cust_nord_buyer_flow_num),
                   COLUMN (cust_nord_buyer_flow_desc)
ON {str_t2_schema}.cust_nord_buyer_flow_lkp ;



SET QUERY_BAND = NONE FOR SESSION;