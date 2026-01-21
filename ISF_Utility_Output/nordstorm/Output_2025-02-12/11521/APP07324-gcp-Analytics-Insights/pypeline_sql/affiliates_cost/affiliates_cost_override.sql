SET QUERY_BAND = 'App_ID=APP08823;
     DAG_ID=affiliates_cost_11521_ACE_ENG;
     Task_Name=affiliates_cost_override;'
     FOR SESSION VOLATILE;

/*


T2/Table Name:
Team/Owner: Nathan Eyre
Date Created/Modified: 12/22/22

*/

delete
from    {funnel_io_t2_schema}.affiliates_cost_override
;

insert into {funnel_io_t2_schema}.affiliates_cost_override
select    banner
        , country
        , day_date
        , lf_cost
        , total_cost
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {funnel_io_t2_schema}.affiliates_cost_override_ldg
;

collect statistics column (day_date)
on {funnel_io_t2_schema}.affiliates_cost_override
;

-- drop staging table
drop table {funnel_io_t2_schema}.affiliates_cost_override_ldg
;

SET QUERY_BAND = NONE FOR SESSION;