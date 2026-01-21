SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_loyalty_level_lkp_11521_ACE_ENG;
     Task_Name=cust_loyalty_level_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_loyalty_level_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/07/2023

Notes:
-- Loyalty Level Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_loyalty_level_lkp
;

INSERT INTO {str_t2_schema}.cust_loyalty_level_lkp
    SELECT DISTINCT cust_loyalty_level_desc
                   ,ROW_NUMBER() OVER (ORDER BY cust_loyalty_level_desc  ASC ) AS cust_loyalty_level_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT COALESCE(cust_loyalty_level, 'Unknown') as cust_loyalty_level_desc  
            from T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes)a ;

SET QUERY_BAND = NONE FOR SESSION;