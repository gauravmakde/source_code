/* SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=audience_engagement_cohort_lkp_build;'
FOR SESSION VOLATILE;
 */

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
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.audience_engagement_cohort_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.audience_engagement_cohort_lkp
(SELECT audience_engagement_cohort_desc,
  ROW_NUMBER() OVER (ORDER BY audience_engagement_cohort_desc) AS audience_engagement_cohort_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(engagement_cohort, 'missing') AS audience_engagement_cohort_desc
   FROM t2dl_das_aec.audience_engagement_cohorts) AS a);

-- SET QUERY_BAND = NONE FOR SESSION;
