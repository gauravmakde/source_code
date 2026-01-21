SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_aec_audience_engagement_cohorts_11521_ACE_ENG;
     Task_Name=ddl_audience_engagement_cohorts;'
     FOR SESSION VOLATILE;

/*
Table definition for T2DL_DAS_AEC.audience_engagement_cohorts
*/

create MULTISET table  {audience_engagement_t2_schema}.audience_engagement_cohorts ,
    FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1

(    
    
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,defining_year_ending_qtr INTEGER NOT NULL
    ,defining_year_start_dt DATE
    ,defining_year_end_dt DATE
    ,execution_qtr INTEGER NOT NULL
    ,execution_qtr_start_dt DATE
    ,execution_qtr_end_dt DATE
    ,acquired_ind  INTEGER COMPRESS
    ,nonrestaurant_trips DECIMAL(22,2) DEFAULT 0.00 COMPRESS 0.00
    ,engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL

) primary index (acp_id) 
partition by execution_qtr
;


-- table comment
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts IS 'Audience Engagement Cohort Customer Mapping by 12 months ending in Fiscal Quarters';
-- Column comments
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.acp_id IS 'analytical customer profile identifier';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.defining_year_ending_qtr IS '12-month measurement window ending this quarter based on which the cohorts are defined, with the exception of Acquired Mid-Qtr';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.defining_year_start_dt IS 'start date for the 12 month measurement window';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.defining_year_end_dt IS 'end date for the 12 month measurement window';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.execution_qtr IS 'fiscal quarter to which the cohort applies';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.execution_qtr_start_dt IS 'start date for the fiscal quarter to which the cohort applies';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.execution_qtr_end_dt IS 'end date for the fiscal quarter to which the cohort applies';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.acquired_ind IS 'indicator for whether a customer was acquired in a given measurement window with 1 equals yes and 0 equals no';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.nonrestaurant_trips IS 'trip count excluding division 70';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.engagement_cohort IS 'the engagement cohort a customer was mapped to based on non-restaurant trips & acquired indicator in the given measurement window';
COMMENT ON  {audience_engagement_t2_schema}.audience_engagement_cohorts.dw_sys_load_tmstp IS 'timestamp for last table update';

SET QUERY_BAND = NONE FOR SESSION;