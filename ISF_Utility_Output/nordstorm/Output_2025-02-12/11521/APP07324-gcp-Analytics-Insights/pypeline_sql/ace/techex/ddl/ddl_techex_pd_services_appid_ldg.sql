 SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_pd_services_appid_11521_ACE_ENG;
     Task_Name=ddl_techex_pd_services_appid_ldg;' 
     FOR SESSION VOLATILE;


create multiset table {techex_t2_schema}.techex_pd_services_appid_ldg,
fallback,
no before journal,
no
after
    journal,
    checksum = default,
    default mergeblockratio (
        app_id varchar(13) CHARACTER SET UNICODE NOT CASESPECIFIC,
        service_name varchar(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
        service_id varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC
    ) PRIMARY INDEX(app_id);

SET
    QUERY_BAND = NONE FOR SESSION;