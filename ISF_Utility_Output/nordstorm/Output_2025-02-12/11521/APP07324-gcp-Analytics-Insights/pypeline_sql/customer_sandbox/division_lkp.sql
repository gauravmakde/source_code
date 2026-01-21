SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=division_lkp_11521_ACE_ENG;
     Task_Name=division_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.division_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Notes:
-- Audience Engagement Cohort Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.division_lkp
;

INSERT INTO {str_t2_schema}.division_lkp
    SELECT DISTINCT COALESCE(div_num,-1) as div_num
            ,COALESCE(div_desc,'Others') as div_desc 
            ,COALESCE(subdiv_num,-1) as subdiv_num 
            ,COALESCE(subdiv_desc, 'Others') as subdiv_desc 
            ,COALESCE(dept_num,-1) as dept_num
            ,COALESCE(dept_desc,'Others')  as dept_desc
            , CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM T2DL_DAS_STRATEGY.cco_line_items cli  ;

SET QUERY_BAND = NONE FOR SESSION;