/* 
SQL script must begin with autocommit_on and QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=ddl_customer_cohort_trips_11521_ACE_ENG;
     Task_Name=ddl_customer_cohort_trips;' 
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
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'customer_cohort_trips', OUT_RETURN_MSG);

create multiset table {deg_t2_schema}.customer_cohort_trips
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    source_platform_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	SOURCE_channel_CODE VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	business_unit_desc VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	tran_type_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    trips_stores INTEGER,
    trips_online INTEGER,  
    trips_stores_gross_usd_amt DECIMAL(38,2),
    trips_online_gross_usd_amt DECIMAL(38,2)
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ,tran_date DATE FORMAT 'YYYY-MM-DD'
    )
primary index(acp_id, tran_date,source_platform_code,SOURCE_channel_CODE,business_unit_desc,tran_type_code,engagement_cohort)
partition by RANGE_N(tran_date BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.customer_cohort_trips IS 'the initial table that helps to get customer level cohort trips by date with cohort definition';

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
