SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=cust_age_group_lkp_11521_ACE_ENG;
     Task_Name=cust_age_group_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.cust_age_group_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/07/2023

Notes:
-- Customer Age Group Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.cust_age_group_lkp
;

INSERT INTO {str_t2_schema}.cust_age_group_lkp
    SELECT DISTINCT cust_age_group_desc
                   ,ROW_NUMBER() OVER (ORDER BY cust_age_group_desc  ASC ) AS cust_age_group_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT COALESCE(cust_age_group, 'Unknown') as cust_age_group_desc  
            from T2DL_DAS_STRATEGY.cco_cust_chan_yr_attributes)a ;

SET QUERY_BAND = NONE FOR SESSION;