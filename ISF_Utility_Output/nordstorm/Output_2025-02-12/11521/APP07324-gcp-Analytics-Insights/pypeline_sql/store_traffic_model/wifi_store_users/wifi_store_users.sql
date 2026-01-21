SET QUERY_BAND = 'App_ID=APP08158;
     DAG_ID=wifi_store_users_11521_ACE_ENG;
     Task_Name=wifi_store_users;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_FLS_TRAFFIC_MODEL.WIFI_STORE_USERS
Team/Owner: Selina Song 
Date Created/Modified: 2023-02-02

Note:
-- Use wifi users to derive store traffic analysis
-- SLA of upstream wifi data is 7:45am

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

delete
from    {fls_traffic_model_t2_schema}.wifi_store_users
where   business_date between {start_date} and {end_date}
;

insert into {fls_traffic_model_t2_schema}.wifi_store_users
select  store
    , business_date
    , wifi_users
    , wifi_source
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {fls_traffic_model_t2_schema}.wifi_store_users_ldg
where   business_date between {start_date} and {end_date}
;

collect statistics column (store)
                   ,column (business_date)
on {fls_traffic_model_t2_schema}.wifi_store_users
;

-- drop staging table
drop table {fls_traffic_model_t2_schema}.wifi_store_users_ldg
; 

SET QUERY_BAND = NONE FOR SESSION;