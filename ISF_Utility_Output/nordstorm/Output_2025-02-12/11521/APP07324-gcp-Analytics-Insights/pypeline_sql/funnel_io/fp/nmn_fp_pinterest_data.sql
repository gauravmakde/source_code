SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_funnel_cost_fact_11521_ACE_ENG;
     Task_Name=nmn_fp_pinterest_data;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_FUNNEL_IO.nmn_fp_pinterest_data
Owner: Analytics Engineering
Modified: 2022-12-08

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {funnel_io_t2_schema}.fp_funnel_cost_fact
WHERE  stats_date between {start_date} and {end_date}
AND     file_name = 'nmn_fp_pinterest'
;

INSERT INTO {funnel_io_t2_schema}.fp_funnel_cost_fact
SELECT DISTINCT
    day_date as stats_date
    , 'nmn_fp_pinterest' AS file_name    
    , data_source_type as sourcetype
    , NULL currency
    , data_source_name as sourcename
    , NULL media_type
    , NULL device_type
    , campaign_name
    , campaign_id
    , NULL campaign_type
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
    , spend as cost
    , total_impressions as impressions
    , clicks
    , conversions
    , total_conversions_value as conversion_value
    , total_video_played_at_100 as video100
    , total_video_played_at_75 as video75
    , video_views
    , NULL video_views_15s
    , NULL likes
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM  {funnel_io_t2_schema}.nmn_fp_pinterest_data_ldg

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;
SET QUERY_BAND = NONE FOR SESSION;
