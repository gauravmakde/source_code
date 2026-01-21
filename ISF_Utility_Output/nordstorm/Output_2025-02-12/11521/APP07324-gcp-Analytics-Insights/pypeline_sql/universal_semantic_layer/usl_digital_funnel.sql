/*

T2/Table Name: T2DL_DAS_DIGENG.usl_digital_funnel
Team/Owner: Customer Analytics/ Rathin Deshpande
Date Created/Modified: 06/23/2023

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/



SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=usl_digital_funnel_11521_ACE_ENG;
     Task_Name=usl_digital_funnel;'
     FOR SESSION VOLATILE;



delete 
from    {usl_t2_schema}.usl_digital_funnel 
where   activity_date_partition between {start_date} and {end_date} ;

insert into {usl_t2_schema}.usl_digital_funnel

select distinct 
channel  
,experience 
,channelcountry 
,acp_id 
,inception_session_id 
,new_recognized 
,session_start 
,landing_page 
,exit_page 
,mrkt_type 
,finance_rollup 
,finance_detail 
,session_count 
,acp_count 
,unique_cust_count 
,bounced_sessions 
,total_pages_visited     
,total_unique_pages_visited 
--,hp_sessions
,hp_engaged_sessions     
,hp_views 
,pdp_sessions 
,pdp_engaged_sessions 
,pdp_views 
,browse_sessions    
,browse_engaged_sessions 
,browse_views 
,search_sessions    
,search_engaged_sessions 
,search_views 
,sbn_sessions 
,sbn_engaged_sessions 
,sbn_views 
,wishlist_sessions 
,wishlist_engaged_sessions 
,wishlist_views 
,checkout_sessions 
,checkout_engaged_sessions 
,checkout_views 
,shopb_sessions 
,shopb_engaged_sessions 
,shopb_views   
,atb_sessions 
,atb_views 
,atb_events 
,ordered_sessions 
,sales_qty 
,web_orders 
,web_demand_usd 
,removed_from_sb 
,atw 
,remove_from_wl 
,atb_from_wl 
,filters_used 
,reviews_eng 
,content_eng 
,reccommendations 
,saved_for_later 
,sfl_to_atb 
,sfl_removed 
,dw_batch_date 
,dw_sys_load_tmstp
,activity_date_partition 
from {usl_t2_schema}.usl_digital_funnel_ldg
where activity_date_partition between {start_date} and {end_date}
;
collect statistics column (activity_date_partition, channel, experience)
on {usl_t2_schema}.usl_digital_funnel
;

SET QUERY_BAND = NONE FOR SESSION;
