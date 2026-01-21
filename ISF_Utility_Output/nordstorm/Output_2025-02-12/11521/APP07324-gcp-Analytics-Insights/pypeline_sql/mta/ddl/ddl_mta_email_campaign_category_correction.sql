/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08300;
     DAG_ID=ddl_mta_email_campaign_category_correction_11521_ACE_ENG;
     Task_Name=ddl_mta_email_campaign_category_correction;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_MTA.T2DL_DAS_MTA.MTA_EMAIL_CAMPAIGN_CATEGORY_CORRECTION
Team/Owner: MOA/MTA
Date Created/Modified: 6/2023

Note:
-- this table is used to correct campaign name and category for td2l_das_mta.mta_email_performance

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

create multiset table {mta_t2_schema}.mta_email_campaign_category_correction
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    campaign_name                       varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,campaign_category_corrected        varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(campaign_name)
;

-- Table Comment (STANDARD)
COMMENT ON  {mta_t2_schema}.mta_email_campaign_category_correction IS 'Email Campaign category corrections from the business';


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


