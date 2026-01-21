SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_dag_ldg_11521_ACE_ENG;
     Task_Name=ddl_techex_dag_ldg;'
     FOR SESSION VOLATILE;

create multiset table {techex_t2_schema}.techex_dag_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio 
     (
           dag_id                      varchar (600) CHARACTER SET UNICODE NOT CASESPECIFIC
         , is_paused                   varchar(5) compress ('TRUE', 'FALSE')
         , is_subdag                   varchar(5) compress ('TRUE', 'FALSE')
         , is_active                   varchar(5) compress ('TRUE', 'FALSE')
         , last_scheduler_run          timestamp
         , last_expired                timestamp
         , fileloc                     varchar (1000) CHARACTER SET UNICODE NOT CASESPECIFIC
         , owners                      varchar (300)
         , description                 VARCHAR(500)
         , default_view                VARCHAR(500)
         , schedule_interval           VARCHAR(100)
)
PRIMARY INDEX(dag_id)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.techex_dag_ldg IS 'Contains all DAG ID information for Production Airflow DAGs';

SET QUERY_BAND = NONE FOR SESSION;