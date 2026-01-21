SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_nmn_cost_data_11521_ACE_ENG;
     Task_Name=nmn_rack_dv_three_sixty_data;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_FUNNEL_IO.rack_dv_three_sixty_data_ldg
Owner: Analytics Engineering
Modified: 2022-12-06

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {nmn_t2_schema}.rack_nmn_cost_data
WHERE  stats_date between {start_date} and {end_date}
AND     file_name = 'rack_dv360'
;

INSERT INTO {nmn_t2_schema}.rack_nmn_cost_data
SELECT DISTINCT
     stats_date
    , 'rack_dv360' AS file_name     
    , sourcetype
    , currency
    , sourcename
    , Null media_type
    , NULL device_type
    , campaign_name
    , campaign_id
    , NULL campaign_type
    , adgroup_name
    , NULL adgroup_id
    , NULL ad_name
    , NULL ad_id
    , account_name
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
    , NULL Engagements
    , video100
    , video75
    , video_views
    , NULL video_views_15s
    , NULL likes
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM  {nmn_t2_schema}.nmn_rack_dv_three_sixty_data_ldg

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;



SET QUERY_BAND = NONE FOR SESSION;

