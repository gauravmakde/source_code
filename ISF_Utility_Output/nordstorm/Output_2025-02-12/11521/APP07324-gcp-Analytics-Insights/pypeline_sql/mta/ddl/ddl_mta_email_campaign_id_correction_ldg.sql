/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08300;
     DAG_ID=mta_email_campaign_correction_11521_ACE_ENG;
     Task_Name=mta_email_campaign_id_correction_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_MTA.T2DL_DAS_MTA.MTA_EMAIL_CAMPAIGN_id_CORRECTION
Team/Owner: MOA/MTA
Date Created/Modified: 6/2023

Note:
-- this table is used to correct campaign name and category for td2l_das_mta.mta_email_performance

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mta_t2_schema}', 'mta_email_campaign_id_correction_ldg', OUT_RETURN_MSG);

create multiset table {mta_t2_schema}.mta_email_campaign_id_correction_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    campaign_id_version           varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,campaign_category_new        varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC 
    )
primary index(campaign_id_version)
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


