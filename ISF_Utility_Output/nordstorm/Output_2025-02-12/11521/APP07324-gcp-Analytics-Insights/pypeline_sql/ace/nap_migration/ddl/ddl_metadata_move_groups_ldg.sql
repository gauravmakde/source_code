
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_metadata_move_groups_11521_ACE_ENG;
     Task_Name=metadata_move_groups_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.metadata_move_groups_ldg
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/15

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/metadata_move_groups/
Sharepoint Source:  

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'metadata_move_groups_ldg', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.metadata_move_groups_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      object_source VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_schema VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      schema_object_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      needs_data_transfer CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
      coe_engineer VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      td_mirroring_status VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pelican_testing_result VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pelican_errors VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      table_merge_keys VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      timestamp_column VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      is_full_load VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      refresh_cadence VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      is_staging VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      appid VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sprint VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      updated_timestamp TIMESTAMP(6)
      )
primary index(object_schema,object_name)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.metadata_move_groups_ldg IS 'NAP Migration move groups';



COLLECT STATISTICS  COLUMN (object_schema,object_name),
                    COLUMN (object_schema) ,
                    COLUMN (object_name)
on {techex_t2_schema}.metadata_move_groups_ldg;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;