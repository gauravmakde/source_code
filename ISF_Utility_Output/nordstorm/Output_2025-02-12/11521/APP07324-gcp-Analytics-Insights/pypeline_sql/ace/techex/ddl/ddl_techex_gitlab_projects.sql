SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_gitlab_projects_11521_ACE_ENG;
     Task_Name=ddl_gitlab_projects;'
     FOR SESSION VOLATILE;

create multiset table {techex_t2_schema}.gitlab_projects
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
        app_id                varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , team_id               varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , archived              varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , fullPath              varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
        , httpUrlToRepo         varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
        , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(app_id)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.gitlab_projects IS 'Contains gitlab projects.';

SET QUERY_BAND = NONE FOR SESSION;