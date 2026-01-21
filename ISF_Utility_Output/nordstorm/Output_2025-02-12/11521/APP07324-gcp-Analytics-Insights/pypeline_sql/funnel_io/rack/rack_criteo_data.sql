SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_funnel_cost_fact_11521_ACE_ENG;
     Task_Name=rack_criteo_data;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_FUNNEL_IO.rack_criteo_data
Owner: Analytics Engineering
Modified: 2024-04-02

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/


DELETE
FROM  {funnel_io_t2_schema}.rack_funnel_cost_fact
WHERE  stats_date between {start_date} and {end_date}
AND     file_name = 'rack_criteo'
;

INSERT INTO {funnel_io_t2_schema}.rack_funnel_cost_fact
SELECT DISTINCT
     stats_date
    , 'rack_criteo' AS file_name     
    , sourcetype
    , currency
    , sourcename
    , NULL media_type
    , NULL device_type
    , campaign_name
    , campaign_id
    , campaign_type
    , NULL adgroup_name
    , NULL adgroup_id
    , NULL ad_name
    , NULL ad_id
    , NULL account_name
    , NULL advertising_channel
    , NULL order_number
    , NULL platform
    , NULL platform_id
    , NULL estimated_net_total_cost
    , NULL gross_commissions
    , NULL gross_sales
    , NULL sales
    , cost
    , impressions
    , clicks
    , conversions
    , conversion_value
    , Null video100
    , NULL video75
    , NULL video_views
    , NULL video_views_15s
    , NULL likes
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {funnel_io_t2_schema}.rack_criteo_data_ldg
WHERE 1=1
AND stats_date BETWEEN {start_date} AND {end_date}
;


SET QUERY_BAND = NONE FOR SESSION;

