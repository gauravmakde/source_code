SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_nap_migration_teradata_usage_11521_ACE_ENG;
     Task_Name=ddl_nap_migration_teradata_usage;'
     FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_TECHEX', 'nap_migration_teradata_usage', OUT_RETURN_MSG);

create multiset table T2DL_DAS_TECHEX.nap_migration_teradata_usage
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
        day_date                  DATE FORMAT 'YYYY-MM-DD'
        , databasename              varchar (64) CHARACTER SET UNICODE NOT CASESPECIFIC
        , tablename                 varchar (80) CHARACTER SET UNICODE NOT CASESPECIFIC
        , tablekind                 varchar (1) CHARACTER SET UNICODE NOT CASESPECIFIC
        , active					integer
        , creatorname               varchar (40) CHARACTER SET UNICODE NOT CASESPECIFIC
        , createtimestamp           timestamp
        , tablesize                 integer
        , tableskew                 integer
        , query_count               integer
        , ampcputime                integer
        --, objecttype                varchar (5) CHARACTER SET UNICODE NOT CASESPECIFIC
        , statementtype             varchar (24) CHARACTER SET UNICODE NOT CASESPECIFIC
        , username                  varchar (40) CHARACTER SET UNICODE NOT CASESPECIFIC
        , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX(day_date, databasename, tablename)
partition by range_n(day_date between date '2021-01-01' and date '2025-12-31' each interval '1' day);

SET QUERY_BAND = NONE FOR SESSION;
