SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=dior_sessions_11521_ACE_ENG;
     Task_Name=dior_session_fact_daily;'
     FOR SESSION VOLATILE;

/*


T2/Table Name: T2DL_DAS_SESSIONS.DIOR_SESSION_FACT_DAILY
Team/Owner: Data Foundations, Nate Eyre  
Date Created/Modified: 1/31/2023      

Note:
DIOR SESSION FACT does common transformations and outputs basic metrics and dimensions at the session_id level
this table rolls them up to a daily grain

Included in first release:
- digital interactions and purchasing
- marketing infromation
- authentication information
- customer identifiers

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    T2DL_DAS_SESSIONS.dior_session_fact_daily
where   activity_date_pacific between '2024-07-01' and '2024-07-19'
;

insert into T2DL_DAS_SESSIONS.dior_session_fact_daily
select distinct  
	activity_date_pacific
    , sessions
    , channelcountry
    , channel
    , experience
    , mrkt_type
    , finance_rollup
    , finance_detail
    , recognized_flag
    , guest_flag
    , authenticated_flag
    , shopper_ids
    , acp_ids
    , session_duration_seconds
    , bounce_flag
    , product_views
    , cart_adds
    , web_orders
    , web_ordered_units
    , web_demand_usd 
    , web_demand 
    , web_demand_currency_code 
    , oms_orders 
    , oms_ordered_units
    , oms_demand_usd
    , oms_demand
    , oms_demand_currency_code
    , bopus_orders
    , bopus_ordered_units
    , bopus_demand_usd
    , bopus_demand
    , bopus_demand_currency_code
    , product_view_session
    , cart_add_session
    , web_order_session
    , oms_order_session
    , bopus_order_session
    , visited_homepage_session
    , visited_checkout_session
    , searched_session
    , browsed_session
    , first_page_type
    , last_page_type
    , CURRENT_DATE as dw_batch_date
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
	, active_session_flag
    , deterministic_bot_flag
    , sus_bot_flag
    , bot_demand_flag
from    T2DL_DAS_SESSIONS.dior_session_fact_daily_ldg
where   activity_date_pacific between '2024-07-01' and '2024-07-19'
;


collect statistics column (activity_date_pacific, channelcountry, channel, experience, mrkt_type, finance_rollup, finance_detail)
on T2DL_DAS_SESSIONS.dior_session_fact_daily
;

SET QUERY_BAND = NONE FOR SESSION;
