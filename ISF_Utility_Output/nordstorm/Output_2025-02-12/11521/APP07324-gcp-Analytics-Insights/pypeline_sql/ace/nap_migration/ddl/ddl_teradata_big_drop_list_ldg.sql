
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_teradata_big_drop_list_11521_ACE_ENG;
     Task_Name=teradata_big_drop_list_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.teradata_big_drop_list_ldg
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/17

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/teradata_big_drop_list/
Sharepoint Source:  

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'teradata_big_drop_list_ldg', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.teradata_big_drop_list_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      databasename       VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tablename          VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_type        VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      update_date        DATE FORMAT 'yyyy-mm-dd',
      comments           VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
     )
primary index(databasename,tablename)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.teradata_big_drop_list_ldg IS 'NAP Migration Big Drop List';



COLLECT STATISTICS  COLUMN (databasename,tablename),
                    COLUMN (databasename),
                    COLUMN (tablename) 
on {techex_t2_schema}.teradata_big_drop_list_ldg;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;