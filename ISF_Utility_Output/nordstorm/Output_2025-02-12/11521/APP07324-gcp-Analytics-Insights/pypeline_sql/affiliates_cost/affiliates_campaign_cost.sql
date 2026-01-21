SET QUERY_BAND = 'App_ID=APP08823;
     DAG_ID=affiliates_cost_11521_ACE_ENG;
     Task_Name=affiliates_campaign_cost;'
     FOR SESSION VOLATILE;

/*


T2/Table Name:
Team/Owner: Nathan Eyre
Date Created/Modified: 12/20/22

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete
from    {funnel_io_t2_schema}.affiliates_campaign_cost
;

insert into {funnel_io_t2_schema}.affiliates_campaign_cost
select  banner
        , country
        , encrypted_id
        , publisher
        , start_day_date
        , end_day_date
        , mf_total_spend
        , lf_commission
        , lf_paid_placement
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {funnel_io_t2_schema}.affiliates_campaign_cost_ldg
;

collect statistics column (publisher)
                   ,column (start_day_date)
on {funnel_io_t2_schema}.affiliates_campaign_cost
;

-- drop staging table
drop table {funnel_io_t2_schema}.affiliates_campaign_cost_ldg
;


SET QUERY_BAND = NONE FOR SESSION;