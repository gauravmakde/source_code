/* 
SQL script must begin with autocommit_on and QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=ddl_customer_cohort_11521_ACE_ENG;
     Task_Name=ddl_customer_cohort;' 
     FOR SESSION VOLATILE;
 

/*
T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window
  
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'customer_cohort', OUT_RETURN_MSG);

create multiset table {deg_t2_schema}.customer_cohort
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      defining_year_ending_qtr INTEGER NOT NULL,
      defining_year_start_dt DATE FORMAT 'YYYY-MM-DD',
      defining_year_end_dt DATE FORMAT 'YYYY-MM-DD',
      execution_qtr INTEGER NOT NULL,
      execution_qtr_start_dt DATE FORMAT 'YYYY-MM-DD',
      execution_qtr_end_dt DATE FORMAT 'YYYY-MM-DD',
      acquired_ind INTEGER  ,
      nonrestaurant_trips DECIMAL(22,2) ,
      engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      dw_sys_load_tmstp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      execution_qtr_partition INTEGER
      )
primary index(acp_id, defining_year_ending_qtr,acquired_ind,nonrestaurant_trips,engagement_cohort)
partition by execution_qtr_partition
;

-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.customer_cohort IS 'Audience Engagement Cohort Customer Mapping by 12 months ending in Fiscal Quarters';

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
