SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=audience_engagement_cohort_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Audience Engagement Cohort Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.audience_engagement_cohort_lkp
;

INSERT INTO {str_t2_schema}.audience_engagement_cohort_lkp
SELECT DISTINCT audience_engagement_cohort_desc
     , ROW_NUMBER() OVER (ORDER BY audience_engagement_cohort_desc ASC) AS audience_engagement_cohort_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(engagement_cohort, 'missing') AS audience_engagement_cohort_desc
      FROM t2dl_das_aec.audience_engagement_cohorts) a
;


SET QUERY_BAND = NONE FOR SESSION;
