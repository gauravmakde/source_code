SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_manual_object_metadata_capture_11521_ACE_ENG;
     Task_Name=manual_object_metadata_capture;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.manual_object_metadata_capture
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/15

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/manual_object_metadata_capture/
Sharepoint Source:  https://nordstrom.sharepoint.com/:x:/r/sites/EnterpriseInsights/_layouts/15/Doc.aspx?sourcedoc=%7B7CEBD302-CC74-4AB5-8B13-A97F666ECB61%7D&file=manual_object_metadata_capture.csv&action=default&mobileredirect=true
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'manual_object_metadata_capture', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.manual_object_metadata_capture
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      schema_name             VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_name             VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      schema_object_name      VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_type             VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      terameta_appid          VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      nerds_appid             VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      terameta_owner_team_name VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      terameta_appid_owner    VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      owner_em                VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      key_columns             VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merge_key_columns       VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      is_full_load            VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      is_staging              VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      refresh_schedule        VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pelican_audit_columns   VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pelican_requirements    VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      metadata_complete_status VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      comments                VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      last_updated            TIMESTAMP(6),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )
primary index(schema_name, object_name)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.manual_object_metadata_capture IS 'Object metadata captured by product teams';



COLLECT STATISTICS  COLUMN (schema_name, object_name),
                    COLUMN (schema_name) ,
                    COLUMN (object_name)
on {techex_t2_schema}.manual_object_metadata_capture;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;