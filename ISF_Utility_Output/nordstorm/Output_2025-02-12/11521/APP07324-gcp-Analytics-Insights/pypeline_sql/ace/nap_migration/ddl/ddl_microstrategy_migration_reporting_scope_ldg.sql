SET QUERY_BAND = 'App_ID=APP08905;
DAG_ID=ddl_nap_migration_microstrategy_migration_reporting_scope_11521_ACE_ENG;
     Task_Name=microstrategy_migration_reporting_scope_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.microstrategy_migration_reporting_scope_ldg
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/15

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/microstrategy_migration_reporting_scope/
Sharepoint Source:  https://nordstrom.sharepoint.com/:x:/r/sites/EnterpriseInsights/_layouts/15/Doc.aspx?sourcedoc=%7B73219936-C1D6-4112-AF1E-2CE98F265A14%7D&file=microstrategy_migration_reporting_scope.csv&action=default&mobileredirect=true
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'microstrategy_migration_reporting_scope_ldg', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.microstrategy_migration_reporting_scope_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      project VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      schema_object_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      schema_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
      )
primary index(schema_object_name, schema_name)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.microstrategy_migration_reporting_scope_ldg IS 'Direct reporting objects for tableau dashboards';



COLLECT STATISTICS  COLUMN (schema_name, schema_object_name),
                    COLUMN (schema_name) ,
                    COLUMN (schema_object_name)
on {techex_t2_schema}.microstrategy_migration_reporting_scope_ldg;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

