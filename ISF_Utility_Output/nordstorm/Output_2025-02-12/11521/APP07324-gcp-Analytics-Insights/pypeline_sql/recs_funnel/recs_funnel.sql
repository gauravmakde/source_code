SET QUERY_BAND = 'App_ID=APP08142;
     DAG_ID=recs_funnel_11521_ACE_ENG;
     Task_Name=recs_funnel;'
     FOR SESSION VOLATILE; 

/*

T2/Table Name: T2DL_DAS_DSA_RECS_REPORTING.recs_funnel_daily
Team/Owner: Sachin Goyal
Date Created/Modified: 02/02/2023

Note:
-- What is the the purpose of the table 
-- What is the update cadence/lookback window

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    {recommendations_t2_schema}.recs_funnel_daily
where   pst_date between {start_date} and {end_date}
;


insert into {recommendations_t2_schema}.recs_funnel_daily
select
    country,
    channel,
    platform,
    context_page_type,
    placement,
    strategy,
    user_id_type,
    currencycode,
    etl_timestamp,
    pst_date,
    recs_clicked_users,
    adding_users,
    ordering_users,
    product_summary_selected_views,
    add_to_bag_items,
    order_items,
    orders,
    demand,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from {recommendations_t2_schema}.recs_funnel_daily_ldg
where pst_date between {start_date} and {end_date}
;

collect statistics column (pst_date)
on {recommendations_t2_schema}.recs_funnel_daily
;

drop table {recommendations_t2_schema}.recs_funnel_daily_ldg
;

SET QUERY_BAND = NONE FOR SESSION;