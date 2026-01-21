
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_big_drop_scope_metadata_11521_ACE_ENG;
     Task_Name=big_drop_scope_metadata;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.big_drop_scope_metadata
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/17

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'big_drop_scope_metadata', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.big_drop_scope_metadata
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      DataBaseName VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TableName VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TableKind CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      big_drop_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      out_of_scope VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      bigdrop_comment VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      bigdrop_update_date DATE FORMAT 'yyyy-mm-dd',
      key_columns VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merge_key_columns VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      is_full_load VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      is_staging VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pelican_audit_columns VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pelican_requirements VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      refresh_schedule VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      comments VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dag_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      migration_phase VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      app_id VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      rowcount_snapshot FLOAT,
      rowcount_current FLOAT,
      tablesize_gb DECIMAL(12,2),
      LastCollectTimeStamp TIMESTAMP(0),
      teradata_index VARCHAR(10000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      timestamp_count INTEGER,
      timestamp_columns VARCHAR(10000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      platform VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC,
      site_name VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      workbook_name VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      group_manager VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      group_director VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      group_vp VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      
      )
primary index(databasename,tablename)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.big_drop_scope_metadata IS 'Metadata for Big Drop objects.  Source of tableau reporting.';



COLLECT STATISTICS  COLUMN (databasename,tablename),
                    COLUMN (databasename),
                    COLUMN (tablename)
on {techex_t2_schema}.big_drop_scope_metadata;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;