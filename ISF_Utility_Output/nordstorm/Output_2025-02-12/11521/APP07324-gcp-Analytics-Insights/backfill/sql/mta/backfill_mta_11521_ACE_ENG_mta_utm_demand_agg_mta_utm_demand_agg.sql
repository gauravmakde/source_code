SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_11521_ACE_ENG;
     Task_Name=mta_utm_demand_agg;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_MTA.mta_utm_demand_agg
Team/Owner: Analytics Engineering
Date Created/Modified: 5/25/22

Note:
-- Daily refresh looking back 5 days
-- tuning notes: https://git.jwn.app/TM01007/ae-general/-/blob/main/tuning/tuning_mta_utm_demand_agg.sql
*/

COLLECT SUMMARY STATISTICS T2DL_DAS_MTA.MTA_ACP_SCORING_FACT;

CREATE MULTISET VOLATILE TABLE utm_demand_load as (
-- load data without aggregation prior to insert
SELECT  a.order_date_pacific,
        a.order_channel,
        a.order_channelcountry,
        a.arrived_channel,
        a.arrived_platform,
        a.arrived_channelcountry,
        a.mktg_type,
        COALESCE(CASE WHEN a.mktg_type = 'BASE' THEN a.mktg_type ELSE a.marketing_type END, 'UNATTRIBUTED') AS marketing_type,
        COALESCE(CASE WHEN a.mktg_type = 'BASE' THEN a.mktg_type ELSE a.finance_rollup END, 'UNATTRIBUTED') AS finance_rollup,
        COALESCE(CASE WHEN a.mktg_type = 'BASE' THEN a.mktg_type ELSE a.finance_detail END, 'UNATTRIBUTED') AS finance_detail,
        CASE WHEN UPPER(SUBSTR(a.utm_content, 1, 3)) = 'ICF' THEN 1 ELSE 0 END AS is_nmn,
        a.utm_channel,
        a.utm_source,
        a.utm_campaign,
        a.utm_term,
        a.utm_content,
        case when a.order_channelcountry = 'CA' then 'CAD' else 'USD' end as currency,
        a.attributed_demand,
        CAST(CASE WHEN CAST(a.attributed_pred_net AS varchar(22)) ='**********************' THEN NULL
                      ELSE CAST(a.attributed_pred_net AS varchar(22))
                 END AS FLOAT) AS attributed_pred_net,
        a.attributed_units,
        a.attributed_orders
FROM    t2dl_das_mta.mta_acp_scoring_fact a
WHERE   a.order_date_pacific BETWEEN date'2024-05-16' and date'2024-05-22'
) with data
    primary index(order_date_pacific, finance_detail, utm_content)
    on commit preserve rows
;


collect statistics
    COLUMN(
        order_date_pacific,
        order_channel,
        order_channelcountry,
        arrived_channel,
        arrived_platform,
        arrived_channelcountry,
        mktg_type,
        marketing_type,
        finance_rollup,
        finance_detail,
        is_nmn,
        utm_channel,
        utm_source,
        utm_campaign,
        utm_term,
        utm_content,
        currency
    )
    , COLUMN(partition)
on utm_demand_load
;

DELETE FROM T2DL_DAS_MTA.mta_utm_demand_agg
WHERE   order_date_pacific BETWEEN date'2024-05-16' AND date'2024-05-22'
;

INSERT INTO T2DL_DAS_MTA.mta_utm_demand_agg
   SELECT
        a.order_date_pacific,
        a.order_channel,
        a.order_channelcountry,
        a.arrived_channel,
        a.arrived_platform,
        a.arrived_channelcountry,
        a.mktg_type,
        a.marketing_type,
        a.finance_rollup,
        a.finance_detail,
        a.is_nmn,
        a.utm_channel,
        a.utm_source,
        a.utm_campaign,
        a.utm_term,
        a.utm_content,
        NULL AS loyalty_type,
        NULL AS loyalty_level,
        a.currency,
        SUM(a.attributed_demand) AS attributed_demand,
        SUM(a.attributed_pred_net) AS attributed_pred_net,
        SUM(a.attributed_units) AS attributed_units,
        SUM(a.attributed_orders) AS attributed_orders,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM    utm_demand_load a
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (order_date_pacific ,finance_detail, utm_content), -- column names used for primary index
                    COLUMN (order_date_pacific)  -- column names used for partition
on T2DL_DAS_MTA.mta_utm_demand_agg
;

SET QUERY_BAND = NONE FOR SESSION;