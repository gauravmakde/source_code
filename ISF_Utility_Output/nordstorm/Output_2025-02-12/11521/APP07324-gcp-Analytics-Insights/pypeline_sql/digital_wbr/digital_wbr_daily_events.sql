SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=digital_wbr_daily_events_11521_ACE_ENG;
     Task_Name=digital_wbr_daily_events;'
     FOR SESSION VOLATILE;

----- delete from
DELETE
FROM    {site_merch_t2_schema}.digital_wbr_daily_events
WHERE   day_dt_pacific BETWEEN {start_date} AND {end_date};

----- insert into
INSERT INTO {site_merch_t2_schema}.digital_wbr_daily_events
SELECT  cust_id_type
        , channel
        , channel_country
        , platform
        , day_dt_pacific
        , referrer
        , referral_event
        , referral_detail_header_1
        , referral_detail_header_2
        , referral_detail
        , url_path
        , next_event
        , next_event_detail_header_1
        , next_event_detail_header_2
        , next_event_detail
        , page_interaction_ind
        , sort_ind
        , filter_ind
        , pagination_ind
        , last_event_ind
        , visitors
        , cust_order_demand
        , cust_order_quantity
        , cust_orders
        , ordering_visitors
        , page_views
        , page_clicks
        , sort_clicks
        , filter_clicks
        , pagination_clicks
        , pss_clicks
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM    {site_merch_t2_schema}.digital_wbr_daily_events_ldg

WHERE   day_dt_pacific BETWEEN {start_date} AND {end_date};

-- Collect statistics after insert/delete
COLLECT STATISTICS  column (day_dt_pacific),
                    column (url_path)
ON {site_merch_t2_schema}.digital_wbr_daily_events;

-- Drop staging table
-- DROP TABLE {site_merch_t2_schema}.digital_wbr_daily_events_ldg;

SET QUERY_BAND = NONE FOR SESSION;