
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_terameta_objects_11521_ACE_ENG;
     Task_Name=terameta_objects_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.terameta_objects_ldg
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/17

Note:
S3 Location: s3://analytics-insights-triggers/nap_migration/terameta_objects/
Sharepoint Source:  

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'terameta_objects_ldg', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.terameta_objects_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      object_database VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_database_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_kind VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      object_bus_desc VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      business_process VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      terameta_appid VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
      
      )
primary index(object_database,object_name)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.terameta_objects_ldg IS 'Metadata info provided to terameta';



COLLECT STATISTICS  COLUMN (object_database,object_name),
                    COLUMN (object_database),
                    COLUMN (object_name)
on {techex_t2_schema}.terameta_objects_ldg;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;