SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_nmn_cost_data_11521_ACE_ENG;
     Task_Name=nmn_rack_criteo_data;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_FUNNEL_IO.rack_criteo_data
Owner: Analytics Engineering
Modified: 2024-04-02

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/


DELETE
FROM  T2DL_DAS_NMN.rack_nmn_cost_data
WHERE  stats_date between date'2022-01-01' and date'2024-10-15'
AND     file_name = 'nmn_rack_criteo'
;

INSERT INTO T2DL_DAS_NMN.rack_nmn_cost_data
SELECT DISTINCT
     stats_date
    , 'nmn_rack_criteo' AS file_name     
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
    , NULL link_clicks
    , clicks
    , conversions
    , conversion_value
    , engagements
    , Null video100
    , NULL video75
    , NULL video_views
    , NULL video_views_15s
    , NULL likes
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  T2DL_DAS_NMN.nmn_rack_criteo_data_ldg
WHERE 1=1
AND stats_date BETWEEN date'2022-01-01' AND date'2024-10-15'
;


SET QUERY_BAND = NONE FOR SESSION;
