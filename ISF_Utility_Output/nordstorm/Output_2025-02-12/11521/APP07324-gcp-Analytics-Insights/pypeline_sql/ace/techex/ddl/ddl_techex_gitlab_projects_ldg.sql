SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_gitlab_projects_ldg_11521_ACE_ENG;
     Task_Name=ddl_gitlab_projects_ldg;'
     FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'gitlab_projects_ldg', OUT_RETURN_MSG);


create multiset table {techex_t2_schema}.gitlab_projects_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio 
     (    app_id                varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , team_id               varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , archived              varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , fullPath              varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
        , httpUrlToRepo         varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
)
PRIMARY INDEX(app_id)
;

SET QUERY_BAND = NONE FOR SESSION;