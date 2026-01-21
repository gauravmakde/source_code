SET QUERY_BAND = 'App_ID=APP08905;
     DAG_ID=ddl_techex_fetch_info_11521_ACE_ENG;
     Task_Name=ddl_techex_fetch_info;'
     FOR SESSION VOLATILE;

delete
from    {techex_t2_schema}.techex_fetch_info
;

insert into {techex_t2_schema}.techex_fetch_info
SELECT
      first_name
    , last_name 
    , lan_id
    , "title" 
    , manager
    , email
    , created_time
    , active
    , office
    , direct_reports_lan_id
    , direct_reports_name
    , manager_hierarchy_name 
    , manager_hierarchy_lan_id
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp           
from   {techex_t2_schema}.techex_fetch_info_ldg
;

COLLECT STATISTICS COLUMN (lan_id) ON {techex_t2_schema}.techex_fetch_info;

SET QUERY_BAND = NONE FOR SESSION;
