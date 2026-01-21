SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=audience_engagement_cohort_lkp_11521_ACE_ENG;
     Task_Name=audience_engagement_cohort_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.audience_engagement_cohort_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/14/2023

Notes:
-- Audience Engagement Cohort Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.audience_engagement_cohort_lkp
;

INSERT INTO {str_t2_schema}.audience_engagement_cohort_lkp
    SELECT DISTINCT audience_engagement_cohort_desc
		,ROW_NUMBER() OVER (ORDER BY audience_engagement_cohort_desc  ASC ) AS audience_engagement_cohort_num
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
	from (SELECT DISTINCT coalesce(engagement_cohort, 'missing') as audience_engagement_cohort_desc  
                FROM t2dl_das_aec.audience_engagement_cohorts a)a ;

SET QUERY_BAND = NONE FOR SESSION;