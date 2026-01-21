SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_dag_run_11521_ACE_ENG;
     Task_Name=ddl_techex_dag_run_ldg;'
     FOR SESSION VOLATILE;

create multiset table {techex_t2_schema}.techex_dag_run_ldg
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
)
PRIMARY INDEX(dag_id)
PARTITION BY RANGE_N(execution_date BETWEEN DATE '2018-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY)
;

SET QUERY_BAND = NONE FOR SESSION;