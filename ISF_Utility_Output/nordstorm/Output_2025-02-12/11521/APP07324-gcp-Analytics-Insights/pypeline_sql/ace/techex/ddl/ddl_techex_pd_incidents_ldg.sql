SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_pd_incidents_11521_ACE_ENG;
     Task_Name=ddl_techex_pd_incidents_ldg;'
     FOR SESSION VOLATILE;


create multiset table {techex_t2_schema}.techex_pd_incidents_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
          incident_number   integer
        , incident_title    varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC
        , incident_summary  varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC
        , created_at        timestamp
        , updated_at        timestamp
        , urgency           varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC
        , "status"          varchar(20) CHARACTER SET UNICODE NOT CASESPECIFIC
        , incident_id       varchar(14) CHARACTER SET UNICODE NOT CASESPECIFIC
        , app_id            varchar(13) CHARACTER SET UNICODE NOT CASESPECIFIC
        , service_name      varchar(300) CHARACTER SET UNICODE NOT CASESPECIFIC
        , service_id        varchar(7) CHARACTER SET UNICODE NOT CASESPECIFIC
)
PRIMARY INDEX(incident_id)
;

SET QUERY_BAND = NONE FOR SESSION;