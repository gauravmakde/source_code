SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=techex_pd_incidents_11521_ACE_ENG;
     Task_Name=techex_pd_services_appid;' FOR SESSION VOLATILE;

COLLECT STATISTICS COLUMN (app_id) ON {techex_t2_schema}.techex_pd_services_appid_ldg;

delete from
    {techex_t2_schema}.techex_pd_services_appid;

insert into
    {techex_t2_schema}.techex_pd_services_appid
SELECT
    app_id,
    service_name,
    service_id,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    {techex_t2_schema}.techex_pd_services_appid_ldg;

COLLECT STATISTICS COLUMN (app_id) ON {techex_t2_schema}.techex_pd_services_appid;

SET
    QUERY_BAND = NONE FOR SESSION;
