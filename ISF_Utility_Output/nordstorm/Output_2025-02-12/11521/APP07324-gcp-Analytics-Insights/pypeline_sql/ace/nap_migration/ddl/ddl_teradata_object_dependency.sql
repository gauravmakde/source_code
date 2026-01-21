
SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_teradata_object_dependency_11521_ACE_ENG;
     Task_Name=teradata_object_dependency;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_TECHEX.teradata_object_dependency
Team/Owner: AE/Tamara Tangen
Date Created/Modified: 2024/10/17

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'teradata_object_dependency', OUT_RETURN_MSG);


CREATE MULTISET TABLE {techex_t2_schema}.teradata_object_dependency ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SOURCE_DB VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SOURCE_OBJ VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SOURCE_OBJ_KIND CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DEPENDENT_DB VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DEPENDENT_OBJ VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DEPENDENT_OBJ_KIND VARCHAR(5) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC,
      DEPENDENCY_LEVEL SMALLINT)
PRIMARY INDEX ( SOURCE_DB ,SOURCE_OBJ );

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.teradata_object_dependency IS 'Source objects for views';



COLLECT STATISTICS  COLUMN (DEPENDENT_DB,DEPENDENT_OBJ),
                    COLUMN (DEPENDENT_DB),
                    COLUMN (DEPENDENT_OBJ)
on {techex_t2_schema}.teradata_object_dependency;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;