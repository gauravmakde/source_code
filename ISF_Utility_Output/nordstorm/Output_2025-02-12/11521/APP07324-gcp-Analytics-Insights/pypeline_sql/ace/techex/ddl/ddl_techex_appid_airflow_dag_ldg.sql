SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_appid_airflow_dag_11521_ACE_ENG;
     Task_Name=ddl_techex_appid_airflow_dag_ldg;'
     FOR SESSION VOLATILE;


create multiset table {techex_t2_schema}.techex_appid_airflow_dag_ldg,
fallback,
no before journal,
no
after
    journal,
    checksum = default,
    default mergeblockratio (
        job_type varchar(3) CHARACTER SET UNICODE NOT CASESPECIFIC,
        gitlab_repo varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        file_name varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        dag_name varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        app_id varchar(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
        dag_id varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    ) PRIMARY INDEX(dag_id);

SET
    QUERY_BAND = NONE FOR SESSION;
