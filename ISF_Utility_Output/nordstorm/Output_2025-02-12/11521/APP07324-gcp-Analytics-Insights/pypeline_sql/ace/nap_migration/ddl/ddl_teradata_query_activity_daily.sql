SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_teradata_query_activity_daily_11521_ACE_ENG;
     Task_Name=ddl_teradata_query_activity_daily;'
     FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'teradata_query_activity_daily', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.teradata_query_activity_daily
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio 
(
        logdate                  	DATE FORMAT 'YYYY-MM-DD'
        , objectdatabasename        varchar (64) CHARACTER SET UNICODE NOT CASESPECIFIC
        , objecttablename           varchar (80) CHARACTER SET UNICODE NOT CASESPECIFIC
        , objecttype                varchar (5) CHARACTER SET UNICODE NOT CASESPECIFIC
        , statementtype             varchar (24) CHARACTER SET UNICODE NOT CASESPECIFIC
        , username                  varchar (40) CHARACTER SET UNICODE NOT CASESPECIFIC
        , query_count               integer
        , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ) 
PRIMARY INDEX(logdate, objectdatabasename, objecttablename)
partition by range_n(logdate between date '2021-01-01' and date '2025-12-31' each interval '1' day);

SET QUERY_BAND = NONE FOR SESSION;
