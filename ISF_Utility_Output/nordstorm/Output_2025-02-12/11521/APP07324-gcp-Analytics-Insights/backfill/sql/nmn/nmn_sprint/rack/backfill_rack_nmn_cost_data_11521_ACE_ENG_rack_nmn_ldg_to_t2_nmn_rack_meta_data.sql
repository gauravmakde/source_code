SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_nmn_cost_data_11521_ACE_ENG;
     Task_Name=nmn_rack_meta_data;'
     FOR SESSION VOLATILE;


/*
Table: T2DL_DAS_FUNNEL_IO.rack_facegram_data
Owner: Analytics Engineering
Modified: 2022-12-13

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  T2DL_DAS_NMN.rack_nmn_cost_data
WHERE  stats_date between date'2022-01-01' and date'2024-10-15'
AND     file_name = 'nmn_rack_meta'
;

INSERT INTO T2DL_DAS_NMN.rack_nmn_cost_data
SELECT DISTINCT
    stats_date
    , 'nmn_rack_meta' AS file_name
    , sourcetype
    , currency
    , sourcename
    , media_type
    , device_type
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
    , link_clicks
    , clicks
    , conversions
    , conversion_value
    , engagements
    , video100
    , video75
    , video_views
    , NULL video_views_15s
    , NULL likes
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM  T2DL_DAS_NMN.nmn_rack_meta_data_ldg

WHERE 1=1
    AND stats_date BETWEEN date'2022-01-01' AND date'2024-10-15'
;




SET QUERY_BAND = NONE FOR SESSION;
