SET QUERY_BAND = 'App_ID=APP08823;
     DAG_ID=affiliates_cost_11521_ACE_ENG;
     Task_Name=affiliates_return_rate;'
     FOR SESSION VOLATILE;

/*


T2/Table Name:
Team/Owner: Nathan Eyre
Date Created/Modified: 12/22/22

*/

delete
from    {funnel_io_t2_schema}.affiliates_return_rate
;

insert into {funnel_io_t2_schema}.affiliates_return_rate
select    banner
        , country
        , start_day_date
        , end_day_date
        , return_rate
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {funnel_io_t2_schema}.affiliates_return_rate_ldg
;

collect statistics column (start_day_date)
on {funnel_io_t2_schema}.affiliates_return_rate
;

-- drop staging table
drop table {funnel_io_t2_schema}.affiliates_return_rate_ldg
;

SET QUERY_BAND = NONE FOR SESSION;