
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_manual_dag_object_mapping_11521_ACE_ENG;
     Task_Name=manual_dag_object_mapping;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.manual_dag_object_mapping
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/15

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/manual_dag_object_mapping/
Sharepoint Source:  https://nordstrom.sharepoint.com/:x:/r/sites/EnterpriseInsights/_layouts/15/Doc.aspx?sourcedoc=%7B2883BB8A-0E36-4FEF-9B8D-E8D7E5CD97C3%7D&file=manual_dag_object_mapping.csv&action=default&mobileredirect=true

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'manual_dag_object_mapping', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.manual_dag_object_mapping
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      dag_id            VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      git_repo_url      VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      last_dagrun       TIMESTAMP(6),
      app_id            VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      team_id           VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dag_status        VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      manager           VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      director          VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      vp                VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      application_tier  VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      schema_object_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      source_sys        VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      source_target     VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      h2_commit         VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      last_updated      TIMESTAMP(6),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      
      )
primary index(schema_object_name)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.manual_dag_object_mapping IS 'Source and Target objects by Dag_ID';



COLLECT STATISTICS  COLUMN (schema_object_name),
                    COLUMN (dag_id) 
on {techex_t2_schema}.manual_dag_object_mapping;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;