SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=reporting_year_shopped_lkp_11521_ACE_ENG;
     Task_Name=reporting_year_shopped_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.reporting_year_shopped_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/07/2023

Notes:
-- Reporting Year Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.reporting_year_shopped_lkp
;

INSERT INTO {str_t2_schema}.reporting_year_shopped_lkp
    SELECT DISTINCT reporting_year_shopped_desc
                   ,ROW_NUMBER() OVER (ORDER BY reporting_year_shopped_desc  ASC ) AS reporting_year_shopped_num
                   , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    FROM
         (SELECT DISTINCT COALESCE(reporting_year_shopped, 'Missing') as reporting_year_shopped_desc  
            from T2DL_DAS_STRATEGY.cco_line_items cli)a ;

SET QUERY_BAND = NONE FOR SESSION;