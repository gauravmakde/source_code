/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08805;
     DAG_ID=ddl_mktg_dm_customer_audience_11521_ACE_ENG;
     Task_Name=ddl_mktg_dm_customer_audience_s3_ldg;'
     FOR SESSION VOLATILE;



/*
T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.mktg_dm_customer_audience
Team/Owner: Nicole Miao
Date Created/Modified: Dec 5, 2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.
*/



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mktg_audience_t2_schema}', 'mktg_dm_customer_audience_ldg', OUT_RETURN_MSG);

create multiset table {mktg_audience_t2_schema}.mktg_dm_customer_audience_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     acp_id                     varchar(50)
    )
primary index(acp_id)
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
