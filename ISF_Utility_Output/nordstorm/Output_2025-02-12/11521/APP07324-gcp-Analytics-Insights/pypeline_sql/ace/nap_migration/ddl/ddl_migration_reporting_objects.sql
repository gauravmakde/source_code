SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_migration_reporting_objects_11521_ACE_ENG;
     Task_Name=migration_reporting_objects;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.migration_reporting_objects
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/15

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/migration_reporting_objects/
Sharepoint Source:  https://nordstrom.sharepoint.com/:x:/r/sites/EnterpriseInsights/_layouts/15/Doc.aspx?sourcedoc=%7BC75AEAA7-F56F-4DFD-81BD-B47808539B0A%7D&file=migration_reporting_objects.csv&action=default&mobileredirect=true
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'migration_reporting_objects', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.migration_reporting_objects
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      site_name VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      workbook_name VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      schema_object_name VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      schema_name VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      table_name VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      bi_platform VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )
primary index(schema_object_name, schema_name)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.migration_reporting_objects IS 'Direct reporting objects for tableau dashboards';



COLLECT STATISTICS  COLUMN (schema_name, schema_object_name),
                    COLUMN (schema_name) ,
                    COLUMN (schema_object_name)
on {techex_t2_schema}.migration_reporting_objects;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;