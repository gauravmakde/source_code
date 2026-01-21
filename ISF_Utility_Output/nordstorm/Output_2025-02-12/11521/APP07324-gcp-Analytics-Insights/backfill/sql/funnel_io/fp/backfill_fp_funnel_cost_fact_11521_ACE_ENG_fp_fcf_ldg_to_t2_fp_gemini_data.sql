SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_funnel_cost_fact_11521_ACE_ENG;
     Task_Name=fp_gemini_data;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_FUNNEL_IO.fp_gemini_data
Owner: Analytics Engineering
Modified: 2022-12-07

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  T2DL_DAS_FUNNEL_IO.fp_funnel_cost_fact
WHERE  stats_date between '2024-01-01' and '2024-07-01'
AND     file_name = 'fp_gemini'
;

INSERT INTO T2DL_DAS_FUNNEL_IO.fp_funnel_cost_fact
SELECT DISTINCT
    stats_date
    , 'fp_gemini' AS file_name    
    , sourcetype
    , currency
    , sourcename
    , media_type
    , device_type
    , campaign_name
    , campaign_id
    , NULL campaign_type
    , adgroup_name
    , adgroup_id
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
    , video100
    , video75
    , video_views
    , NULL video_views_15s
    , NULL likes
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM  T2DL_DAS_FUNNEL_IO.fp_gemini_data_ldg

WHERE 1=1
    AND stats_date BETWEEN '2024-01-01' AND '2024-07-01'
;


SET QUERY_BAND = NONE FOR SESSION;