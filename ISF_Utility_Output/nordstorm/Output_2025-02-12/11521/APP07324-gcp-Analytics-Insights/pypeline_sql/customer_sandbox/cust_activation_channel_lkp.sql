SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_activation_channel_lkp_11521_ACE_ENG;
     Task_Name=cust_activation_channel_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_activation_channel_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/05/2023

Notes:
-- Customer activation channel lookup table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_activation_channel_lkp
;

INSERT INTO {str_t2_schema}.cust_activation_channel_lkp
    SELECT DISTINCT cust_activation_channel_desc
                   ,ROW_NUMBER() OVER (ORDER BY cust_activation_channel_desc  ASC ) AS cust_activation_channel_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT COALESCE(cust_activation_channel, 'Unknown') as cust_activation_channel_desc  
            from T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes)a ;

COLLECT STATISTICS COLUMN (cust_activation_channel_num),
                   COLUMN (cust_activation_channel_desc)
ON {str_t2_schema}.cust_activation_channel_lkp;

SET QUERY_BAND = NONE FOR SESSION;