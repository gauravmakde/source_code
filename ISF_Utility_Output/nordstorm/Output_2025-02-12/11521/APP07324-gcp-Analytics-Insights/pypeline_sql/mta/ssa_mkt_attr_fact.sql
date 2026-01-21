SET QUERY_BAND = 'App_ID=APP08134;
     DAG_ID=analytical_session_fact_11521_ACE_ENG;
     Task_Name=ssa_mkt_attr_fact;'
     FOR SESSION VOLATILE;

--DESCRIPTION: T2DL_DAS_MTA.SSA_MKT_ATTR_FACT
--In the script, we are doing marketing attribution of arrived sessions with orders and demand by mapping the utm parameter parsed from the url
--to different granularity level(utm_term, utm_channel, utm_campaign, mrkt_type, finance_rollup, finance_detail)
--by using utm_channel_lookup and marketing_channel_hierarchy tables. And also binning non-arrived sessions with orders and demand as "DIRECT LOAD"/BASE

--Categorized null value on mrkt_touch,mrkt_type,finance_rollup,finance_detail to be UNATTR


DELETE
FROM {mta_t2_schema}.ssa_mkt_attr_fact
WHERE 1=1
    AND activity_date_pacific BETWEEN {start_date} and {end_date}
;

INSERT INTO {mta_t2_schema}.ssa_mkt_attr_fact
SELECT DISTINCT
    activity_date_pacific,
    channelcountry,
    channel,
    experience,
    session_id,
    referrer,
    siteid,
    utm_source,
    utm_medium,
    sp_source,
    utm_channel,
    utm_campaign,
    sp_campaign,
    utm_term,
    utm_content,
    gclid,
    bounce_ind,
    funnel_type,
    web_orders,
    web_demand_usd,
    session_type,
    mrkt_touch,
    mrkt_type,
    finance_rollup,
    finance_detail,
    CURRENT_DATE dw_batch_date,
    CURRENT_TIMESTAMP as dw_sys_updt_tmstp
FROM {mta_t2_schema}.ssa_mkt_attr_fact_ldg
;

COLLECT STATISTICS COLUMN (partition),
COLUMN (session_id) ON {mta_t2_schema}.ssa_mkt_attr_fact;

SET QUERY_BAND = NONE FOR SESSION;
