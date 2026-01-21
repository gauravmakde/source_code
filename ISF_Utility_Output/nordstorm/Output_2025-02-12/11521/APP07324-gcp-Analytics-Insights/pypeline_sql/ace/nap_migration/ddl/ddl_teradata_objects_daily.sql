SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_teradata_objects_daily_11521_ACE_ENG;
     Task_Name=ddl_teradata_objects_daily;'
     FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{techex_t2_schema}', 'teradata_objects_daily', OUT_RETURN_MSG);

create multiset table {techex_t2_schema}.teradata_objects_daily
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
        , creatorname               varchar (40) CHARACTER SET UNICODE NOT CASESPECIFIC
        , createtimestamp           timestamp
        , tablesize_gb              DECIMAL(12,2)
        , tableskew                 DECIMAL(12,2)        
        , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    ) 
PRIMARY INDEX(day_date, databasename, tablename)
partition by range_n(day_date between date '2021-01-01' and date '2025-12-31' each interval '1' day);

SET QUERY_BAND = NONE FOR SESSION;
