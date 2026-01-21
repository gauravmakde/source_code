SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_dashboard_catalog_11521_ACE_ENG;
     Task_Name=ddl_dashboard_catalog;'
     FOR SESSION VOLATILE;

/*
AS Tableau Dashboard Catalog
T2DL_DAS_BIE_DEV.DASHBOARD_CATALOG
*/

CREATE MULTISET TABLE {t2_schema}.dashboard_catalog,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(
    is_featured	char(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , has_rms	char(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , analyst	varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , asset_type	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , business_area	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subject_area	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dashboard_name	varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "description"	varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "data_source"	varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "database" varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , update_frequency	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dashboard_url	varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(dashboard_url)
; 

-- table comment
COMMENT ON  {t2_schema}.dashboard_catalog IS 'Tableau dashboard catalog for Analytical Sciences site AS';

SET QUERY_BAND = NONE FOR SESSION;