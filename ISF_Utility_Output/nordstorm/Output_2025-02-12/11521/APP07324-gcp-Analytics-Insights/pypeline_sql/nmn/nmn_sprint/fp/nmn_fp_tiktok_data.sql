SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_nmn_cost_data_11521_ACE_ENG;
     Task_Name=nmn_fp_tiktok_data;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_FUNNEL_IO.fp_tiktok_data
Owner: Analytics Engineering
Modified: 2022-12-09

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {nmn_t2_schema}.fp_nmn_cost_data
WHERE  stats_date between {start_date} and {end_date}
AND     file_name = 'nmn_fp_tiktok'
;

INSERT INTO {nmn_t2_schema}.fp_nmn_cost_data
SELECT DISTINCT
    stats_date
    , 'nmn_fp_tiktok' AS file_name    
    , sourcetype
    , currency
    , sourcename
    , media_type
    , NULL device_type
    , campaign_name
    , campaign_id
    , campaign_type
    , adgroup_name
    , adgroup_id
    , ad_name
    , ad_id
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
    , NULL link_clicks
    , conversions
    , conversion_value
    , engagements
    , video100
    , video75
    , video_views
    , NULL video_views_15s
    , likes
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM  {nmn_t2_schema}.nmn_fp_tiktok_data_ldg

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;



SET QUERY_BAND = NONE FOR SESSION;
