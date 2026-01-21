/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_move_groups_11521_ACE_ENG;
     Task_Name=ddl_nap_migration_move_groups;'
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
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'nap_migration_move_groups', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.nap_migration_move_groups
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio 
    (
        databasename            char(60) CHARACTER SET UNICODE NOT CASESPECIFIC  
        , tablename             char(60) CHARACTER SET UNICODE NOT CASESPECIFIC 
        , move_group            char(5) CHARACTER SET UNICODE NOT CASESPECIFIC compress
        , dw_sys_load_tmstp     TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
unique primary index(databasename, tablename)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.nap_migration_move_groups IS 'NAP migration move groups';

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;