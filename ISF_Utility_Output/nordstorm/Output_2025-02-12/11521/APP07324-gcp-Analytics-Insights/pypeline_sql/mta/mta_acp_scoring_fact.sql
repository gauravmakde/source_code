SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_11521_ACE_ENG;
     Task_Name=mta_acp_scoring_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_MTA.mta_scp_scoring_fact
Team/Owner: Analytics Engineering
Date Created/Modified: 11/17/22

Note:
-- Daily refresh looking back 5 days

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

delete
from    {mta_t2_schema}.mta_acp_scoring_fact
where   order_date_pacific between {start_date} and {end_date}
;

-- stats on ldg
collect statistics COLUMN(order_date_pacific)
                 , COLUMN(partition)
on {mta_t2_schema}.mta_acp_scoring_fact_ldg;

-- insert to main
insert into {mta_t2_schema}.mta_acp_scoring_fact
select  m.order_date_pacific
        , m.order_number
        , m.sku_id
        , m.acp_id
        , m.order_channel
        , m.order_channelcountry
        , m.arrived_channel
        , m.arrived_platform
        , m.arrived_channelcountry
        , m.mktg_type
        , m.marketing_type
        , m.finance_rollup
        , m.finance_detail
        , m.utm_channel
        , coalesce(m.utm_source, m.sp_source) as utm_source
        , coalesce(m.utm_campaign, m.sp_campaign) as utm_campaign
        , m.utm_term
        , m.utm_content
        , m.attributed_demand as attributed_demand
        , m.attributed_pred_net as attributed_pred_net
        , m.attributed_units as attributed_units
        , m.attributed_order as attributed_orders
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
        , m.is_marketplace as is_marketplace
        , m.partner_relationship_id as partner_relationship_id
        , m.partner_relationship_type as partner_relationship_type
from {mta_t2_schema}.mta_acp_scoring_fact_ldg m
where   order_date_pacific between {start_date} and {end_date}
;
collect statistics COLUMN(order_date_pacific)
                 , COLUMN(finance_detail)
                 , COLUMN(acp_id)
                 , COLUMN(order_date_pacific ,finance_detail ,attributed_demand, acp_id)
                 , COLUMN(partition)
on {mta_t2_schema}.mta_acp_scoring_fact
;
-- drop landing table
drop table {mta_t2_schema}.mta_acp_scoring_fact_ldg
;

SET QUERY_BAND = NONE FOR SESSION;