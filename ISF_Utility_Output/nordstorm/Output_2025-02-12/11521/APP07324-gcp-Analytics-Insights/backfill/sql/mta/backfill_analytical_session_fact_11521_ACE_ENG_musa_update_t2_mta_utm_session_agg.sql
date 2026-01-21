SET QUERY_BAND = 'App_ID=APP08134;
     DAG_ID=analytical_session_fact_11521_ACE_ENG;
     Task_Name=mta_utm_session_agg;'
     FOR SESSION VOLATILE;

/*
-------------------------------------------------------------------------------------------------------------------------------
DESCRIPTION: T2DL_DAS_MTA.MTA_UTM_SESSION_AGG
In the script, we are aggregating marketing attribution of sessions for reporting
Getting total session count, orders, demand and bounced session count. For bounced sessions count we aggregate based off bounce_ind an indicator cloumn categorized into 'Y'-Yes,'N'-NO,'U'-Unknown by aggregating to different granularity level
-------------------------------------------------------------------------------------------------------------------------------
*/

--Table Comments
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg IS 'UTM level session data aggregated for reporting';
--Column Comments
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.activity_date_pacific IS 'Date that the session was triggered';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.channelcountry IS 'Country from where the sessions were triggered, i.e, US or CA';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.channel IS 'Business line/unit to which the sessions were triggered, i.e, TRUNK_CLUB,NORDSTROM_RACK,NORDSTROM,HAUTELOOK';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.experience IS 'Platform through which the sessions were triggered (ex: DESKTOP_WEB, IOS_APP, MOBILE_WEB)';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.siteid IS 'ID for source site';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.utm_term IS 'channel or platform. Contain keywords like, IOS';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.utm_channel IS 'UTM parameters defined by marketing campaigns. utm_channel is in the form funnel_fundingtype_channel (ex:low_nd_affiliates)';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.utm_campaign IS 'Identifies a specific product promotion or strategic campaign';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.utm_source IS 'The individual site within that channel.For example, Facebook would be one of the sources within your Social medium for any unpaid links that you post to Facebook.';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.utm_medium IS 'The individual site within that channel.For example,Social medium for any unpaid links that you post to Facebook.';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.utm_content IS 'This is an optional field. If you have multiple links in the same campaign, like two links in the same email, you can fill in this value so you can differentiate them. For most marketers, this data is more detailed than they really need.';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.sp_campaign IS 'Single-player campaign';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.sp_source IS 'Single-player source';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.gclid IS 'A parameter passed in the URL with ad clicks, to identify the campaign and other attributes of the click associated with the ad for ad tracking and campaign attribution';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.funnel_type IS 'Up, Low, or Mid of the Funnel';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.mrkt_touch IS 'A flag to identify whether the sessions were through marketing or were from base';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.mrkt_type IS 'High level marketing type information. 3 potential values: PAID, UNPAID, BASE';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.session_type IS 'Type of sessions, if those sessions have an arrived event or not';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.finance_rollup IS 'Finance defined granularity one step down from marketing type';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.finance_detail IS 'Finance defined granularity one step down from finance rollup';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.web_orders IS 'Orders count per session';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.web_demand_usd IS 'Value of the order purchased';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.bounced_sessions IS 'Total amount of sessions that were bounced';
COMMENT ON  T2DL_DAS_MTA.mta_utm_session_agg.total_sessions IS 'Total amount of sessions';


DELETE FROM T2DL_DAS_MTA.mta_utm_session_agg
WHERE activity_date_pacific BETWEEN '2024-08-18' and '2024-08-18'
;

INSERT INTO T2DL_DAS_MTA.mta_utm_session_agg

SELECT
activity_date_pacific
, channelcountry
, channel
, experience
, siteid
, utm_source
, utm_medium
, sp_source
, utm_channel
, utm_campaign
, sp_campaign
, utm_term
, utm_content
, gclid
, case when funnel_type is null and mrkt_type = 'UNATTR/TIEOUT' then 'UNATTRIBUTED'
       when funnel_type is null and mrkt_type = 'BASE' then 'BASE'
  else funnel_type end as funnel_type
, mrkt_touch
, session_type
, mrkt_type
, finance_rollup
, finance_detail
, sum(web_orders) AS web_orders
, sum(web_demand_usd) AS web_demand_usd
--Binning ‘U’ bounce inidicated sessions into ‘Y’,since discrepency between GA bounce rate and NAP bounce rate is much lower this way when performed trend analysis.
--Based on the logic, ‘U’ generally means we have no strong signal they saw >1 page, it makes more sense to categorize ‘U’ bounce inidicated sessions into ‘Y’
, sum(CASE WHEN bounce_ind IN ('Y','U') THEN 1 ELSE 0 END) AS bounced_sessions
, count(DISTINCT session_id) AS total_sessions
, CURRENT_DATE dw_batch_date
, CURRENT_TIMESTAMP dw_sys_updt_tmstp

FROM T2DL_DAS_MTA.ssa_mkt_attr_fact

WHERE 1=1
    AND activity_date_pacific BETWEEN '2024-08-18' and '2024-08-18'

GROUP BY activity_date_pacific
, channelcountry
, channel
, experience
, siteid
, utm_source
, utm_medium
, sp_source
, utm_channel
, utm_campaign
, sp_campaign
, utm_term
, utm_content
, gclid
, funnel_type
, mrkt_touch
, session_type
, mrkt_type
, finance_rollup
, finance_detail
, dw_batch_date
, dw_sys_updt_tmstp
;

COLLECT STATISTICS 
COLUMN (partition),
COLUMN (activity_date_pacific),
COLUMN (activity_date_pacific, channel, channelcountry, experience, utm_campaign, mrkt_type, finance_rollup, finance_detail, web_orders,bounced_sessions, total_sessions) ON T2DL_DAS_MTA.mta_utm_session_agg
;

SET QUERY_BAND = NONE FOR SESSION;