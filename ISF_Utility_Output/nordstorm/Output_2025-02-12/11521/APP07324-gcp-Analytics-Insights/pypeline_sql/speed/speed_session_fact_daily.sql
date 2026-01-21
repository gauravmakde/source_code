SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=speed_session_fact_daily_11521_ACE_ENG;
     Task_Name=speed_session_fact_daily;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_SESSIONS.SPEED_SESSION_FACT_DAILY
Team/Owner: Digital Optimization/Sheeba Philip
Date Created/Modified: 04/19/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/
delete 
from    {mwp_t2_schema}.speed_session_fact_daily
where   activity_date_pacific between {start_date} and {end_date}
;
insert into {mwp_t2_schema}.speed_session_fact_daily
select distinct 
 activity_date_pacific ,
   channel ,
   experience ,
   channelcountry ,
   landing_page ,
   exit_page ,
   mrkt_type ,
   finance_rollup ,
   finance_detail ,
   bounce_ind ,
   acp_id_populated ,
   loyalty_id_populated ,
   total_pages_visited ,
   total_unique_pages_visited ,
   session_count ,
   session_duration_seconds ,
   hp_sessions ,
   hp_engaged_sessions ,
   hp_views ,
   pdp_sessions,
   pdp_engaged_sessions,
   pdp_views,
   browse_sessions,
   browse_engaged_sessions,
   browse_views,
   search_sessions,
   search_engaged_sessions,
   search_views,
   sbn_sessions,
   sbn_engaged_sessions,
   sbn_views,
   wishlist_sessions,
   wishlist_engaged_sessions,
   wishlist_views,
   checkout_sessions,
   checkout_engaged_sessions,
   checkout_views,
   shopb_sessions,
   shopb_engaged_sessions,
   shopb_views,
   atb_sessions,
   atb_views,
   atb_events,
   ordered_sessions,
   sales_qty,
   web_orders,
   web_demand_usd,
   dw_batch_date,
   dw_sys_load_tmstp  
   from    {mwp_t2_schema}.speed_session_fact_daily_ldg
where   activity_date_pacific between {start_date} and {end_date}
;
collect statistics column (activity_date_pacific, channelcountry, channel, experience, mrkt_type, finance_rollup, finance_detail)
on {mwp_t2_schema}.speed_session_fact_daily
;

-- drop staging table
drop table {mwp_t2_schema}.speed_session_fact_daily_ldg
;
SET QUERY_BAND = NONE FOR SESSION;