SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_fetch_info_11521_ACE_ENG;
     Task_Name=ddl_techex_fetch_info;'
     FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'techex_fetch_info_ldg', OUT_RETURN_MSG);	

create multiset table {techex_t2_schema}.techex_fetch_info_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio 
     (
          first_name                varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , last_name                 varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , lan_id                    varchar(4) CHARACTER SET UNICODE NOT CASESPECIFIC
        , "title"                   varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , manager                   varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC
        , email                     varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
        , created_time              timestamp
        , active                    varchar(1) CHARACTER SET UNICODE NOT CASESPECIFIC
        , office                    varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , direct_reports_lan_id     varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
        , direct_reports_name       varchar(600) CHARACTER SET UNICODE NOT CASESPECIFIC
        , manager_hierarchy_name    varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
        , manager_hierarchy_lan_id  varchar(60) CHARACTER SET UNICODE NOT CASESPECIFIC
)
PRIMARY INDEX(lan_id)
;

SET QUERY_BAND = NONE FOR SESSION;