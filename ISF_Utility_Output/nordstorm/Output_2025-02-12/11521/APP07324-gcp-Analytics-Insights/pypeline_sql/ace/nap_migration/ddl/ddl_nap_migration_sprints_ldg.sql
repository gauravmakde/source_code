/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_sprints_11521_ACE_ENG;
     Task_Name=ddl_nap_migration_sprints;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'nap_migration_sprints_ldg', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.nap_migration_sprints_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
        databasename            char(60) CHARACTER SET UNICODE NOT CASESPECIFIC  
        , tablename             char(60) CHARACTER SET UNICODE NOT CASESPECIFIC 
        , sprint            char(5) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    )
primary index(databasename, tablename)
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;