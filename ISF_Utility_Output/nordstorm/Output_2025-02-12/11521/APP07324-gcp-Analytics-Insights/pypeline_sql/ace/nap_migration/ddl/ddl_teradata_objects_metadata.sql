
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_teradata_objects_metadata_11521_ACE_ENG;
     Task_Name=teradata_objects_metadata;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.teradata_objects_metadata
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/17

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/teradata_objects_metadata/
Sharepoint Source:  

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'teradata_objects_metadata', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.teradata_objects_metadata
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      DataBaseName VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TableName VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_type VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CommentString VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CreatorName VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CreateTimeStamp TIMESTAMP(0),
      RowCount FLOAT,
      LastCollectTimeStamp TIMESTAMP(0),
      tablesize_gb DECIMAL(38,3),
      LastAccessTimeStamp TIMESTAMP(0),
      query_count_60_days INTEGER,
      DISTINCT_users_60_days INTEGER,
      best_app_id VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      primary_indextype VARCHAR(28) CHARACTER SET LATIN NOT CASESPECIFIC,
      primary_index VARCHAR(10000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      additional_index_count INTEGER,
      additional_index_array SYSUDTLIB.INDEX_ARRAY,
      timestamp_count INTEGER,
      timestamp_columns VARCHAR(10000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_snapshot_date DATE FORMAT 'YY/MM/DD',
      exclude_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      exclude_type VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )
primary index(databasename,tablename)
;



-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.teradata_objects_metadata IS 'NAP Migration metadata from EBA';



COLLECT STATISTICS  COLUMN (databasename,tablename),
                    COLUMN (databasename),
                    COLUMN (tablename) 
on {techex_t2_schema}.teradata_objects_metadata;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;