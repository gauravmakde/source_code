SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_channel_combo_lkp_11521_ACE_ENG;
     Task_Name=cust_channel_combo_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_channel_combo_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 03/24/2023

Notes:
-- Channel Combo Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_channel_combo_lkp
;

INSERT INTO {str_t2_schema}.cust_channel_combo_lkp
    SELECT DISTINCT cust_channel_combo_desc
                   ,ROW_NUMBER() OVER (ORDER BY cust_channel_combo_desc  ASC ) AS cust_channel_combo_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT cust_channel_combo as cust_channel_combo_desc  
            from T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes)a ;

SET QUERY_BAND = NONE FOR SESSION;