SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_dag_run_11521_ACE_ENG;
     Task_Name=ddl_techex_dag_run;'
     FOR SESSION VOLATILE;

create multiset table {techex_t2_schema}.techex_dag_run
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
         id                         integer
         , dag_id                   varchar(300) CHARACTER SET UNICODE NOT CASESPECIFIC
         , state                    varchar(60) CHARACTER SET UNICODE NOT CASESPECIFIC
         , run_id                   varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
         , external_trigger         varchar(5) compress ('TRUE', 'FALSE')
         , start_date               timestamp
         , end_date                 timestamp
         , execution_date           date
         , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(dag_id)
PARTITION BY RANGE_N(execution_date BETWEEN DATE '2018-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {techex_t2_schema}.techex_dag_run IS 'Contains all DAG RUN information for Production Airflow DAGs';

SET QUERY_BAND = NONE FOR SESSION;