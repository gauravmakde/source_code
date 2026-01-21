SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mkt_utm_agg_11521_ACE_ENG;
     Task_Name=mkt_utm_agg;'
     FOR SESSION VOLATILE;

/*
--------------------------------------------
--t2dl_das_mta.mkt_utm_agg
Migrated AS-IS from NAPBI - will not adhere to standards
--------------------------------------------
*/
-- drop table mta_data;
-- drop table ssa_data;

-- Summarize MTA data by channel, finance rollup, UTM parameters.
CREATE MULTISET VOLATILE TABLE mta_data AS (
SELECT
order_date_pacific,
utm_source,
utm_channel,
utm_campaign,
utm_term,
utm_content,
order_channel AS channel,
order_channelcountry as channelcountry,
CASE arrived_channel
     WHEN 'FULL_LINE' THEN 'N.COM'
     WHEN 'RACK' THEN 'R.COM'
ELSE 'NULL' END AS arrived_channel,
CASE WHEN arrived_platform = '' OR arrived_platform IS NULL THEN 'UNKNOWN'
      ELSE arrived_platform END AS device_type,
COALESCE(CASE WHEN mktg_type = 'BASE' THEN mktg_type ELSE marketing_type END, 'UNATTRIBUTED') AS marketing_type,
COALESCE(CASE WHEN mktg_type = 'BASE' THEN mktg_type ELSE finance_rollup END, 'UNATTRIBUTED') AS finance_rollup,
COALESCE(CASE WHEN mktg_type = 'BASE' THEN mktg_type ELSE finance_detail END, 'UNATTRIBUTED') AS finance_detail,
SUM(attributed_demand) AS gross,
SUM(attributed_pred_net) AS net_sales,
SUM(attributed_units) AS units,
SUM(attributed_orders) AS orders
FROM  T2DL_DAS_MTA.mta_acp_scoring_fact
WHERE order_date_pacific BETWEEN {start_date} AND {end_date}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
PRIMARY INDEX(order_date_pacific, channel, channelcountry, device_type, utm_campaign, finance_detail)
PARTITION BY RANGE_N(order_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;

-- Summarize SSA data by channel, finance rollup, UTM parameters.
CREATE MULTISET VOLATILE TABLE ssa_data AS (
SELECT
activity_date_pacific,
utm_source,
utm_medium,
funnel_type,
utm_channel,
utm_campaign,
utm_term,
utm_content,
CASE WHEN channel = 'NORDSTROM' AND channelcountry = 'US' THEN 'N.COM'
     WHEN channel = 'NORDSTROM_RACK' AND channelcountry = 'US' THEN 'RACK.COM'
     WHEN channel = 'NORDSTROM_RACK' AND channelcountry = 'CA' THEN 'RACK.CA'
     WHEN channel = 'NORDSTROM' AND channelcountry = 'CA' THEN 'N.CA'
ELSE channel END AS channel,
UPPER(channelcountry) as channelcountry,
'NULL' AS arrived_channel,
UPPER(CASE WHEN experience IN ('DESKTOP_WEB','CARE_PHONE','POINT_OF_SALE','VENDOR') THEN 'WEB'
           WHEN experience = 'MOBILE_WEB' THEN 'MOW'
           WHEN experience = 'ANDROID_APP' THEN 'ANDROID'
           WHEN experience = 'IOS_APP' THEN 'IOS'
      ELSE experience END) AS device_type,
CASE WHEN mrkt_type  = 'UNATTR/TIEOUT' THEN 'UNATTRIBUTED' ELSE mrkt_type END AS marketing_type,
CASE WHEN finance_rollup = 'UNATTR/TIEOUT' THEN 'UNATTRIBUTED' ELSE finance_rollup END AS finance_rollup,
CASE WHEN (utm_source LIKE '%planned' OR utm_source LIKE '%promotional' OR utm_source IS NULL OR utm_source IN ('SALESFORCE','SFMC','LEANPLUM')) AND finance_detail = 'APP_PUSH' THEN  'APP_PUSH_PLANNED'
	WHEN utm_source LIKE '%triggered' AND finance_detail = 'APP_PUSH' THEN 'APP_PUSH_TRIGGERED'
     WHEN utm_source LIKE '%transactional' AND finance_detail = 'APP_PUSH' THEN 'APP_PUSH_TRANSACTIONAL'
ELSE finance_detail
END AS finance_detail,
SUM(total_sessions) as total_sessions,
SUM(web_orders) as web_orders,
SUM(web_demand_usd) as web_demand_usd,
SUM(bounced_sessions) as bounced_sessions
FROM T2DL_DAS_MTA.mta_utm_session_agg
WHERE activity_date_pacific BETWEEN {start_date} AND {end_date}
AND channelcountry in ('US','CA')
AND channel in ('NORDSTROM','NORDSTROM_RACK')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX(activity_date_pacific, channel, channelcountry, utm_campaign, finance_rollup, finance_detail)
PARTITION BY RANGE_N(activity_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;

/*
--------------------------------------------
DELETE any overlapping records from destination
table prior to INSERT of new data
--------------------------------------------
*/
DELETE
FROM    {mta_t2_schema}.mkt_utm_agg
WHERE   activity_date_pacific >= {start_date}
AND     activity_date_pacific <= {end_date}
;


INSERT INTO {mta_t2_schema}.mkt_utm_agg
SELECT
    COALESCE(mta.order_date_pacific,ssa.activity_date_pacific) AS activity_date_pacific,
    COALESCE(mta.channel,ssa.channel) AS channel,
    COALESCE(mta.channelcountry,ssa.channelcountry) AS channelcountry,
    COALESCE(mta.arrived_channel,ssa.arrived_channel) AS arrived_channel,
    COALESCE(mta.device_type,ssa.device_type) AS device_type,
    COALESCE(mta.marketing_type,ssa.marketing_type) AS marketing_type,
    COALESCE(mta.finance_rollup,ssa.finance_rollup) AS finance_rollup,
    COALESCE(mta.finance_detail,ssa.finance_detail) AS finance_detail,
    COALESCE(mta.utm_channel,ssa.utm_channel) AS utm_channel,
    COALESCE(mta.utm_source,ssa.utm_source) AS utm_source,
    ssa.utm_medium AS utm_medium,
    ssa.funnel_type AS funnel_type,
    COALESCE(mta.utm_campaign,ssa.utm_campaign) AS utm_campaign,
    COALESCE(mta.utm_term,ssa.utm_term) AS utm_term,
    COALESCE(mta.utm_content,ssa.utm_content) AS utm_content,
    CASE WHEN UPPER(SUBSTR(COALESCE(mta.utm_content,ssa.utm_content), 1, 3)) = 'ICF' THEN 'Yes' ELSE 'No' END AS NMN,
    --MTA Metrics
    ZEROIFNULL(SUM(mta.gross)) AS gross,
    ZEROIFNULL(SUM(mta.net_sales)) AS net_sales,
    ZEROIFNULL(SUM(mta.units)) AS units,
    ZEROIFNULL(SUM(mta.orders)) AS orders,
    --SSA Metrics
    ZEROIFNULL(SUM(ssa.web_orders)) AS session_orders,
    ZEROIFNULL(SUM(ssa.web_demand_usd)) AS session_demand,
    ZEROIFNULL(SUM(ssa.total_sessions)) AS sessions,
    ZEROIFNULL(SUM(ssa.bounced_sessions)) AS bounced_sessions
FROM ssa_data ssa
FULL OUTER JOIN mta_data mta
    ON ssa.utm_campaign = mta.utm_campaign
    AND ssa.utm_channel = mta.utm_channel
    AND ssa.activity_date_pacific = mta.order_date_pacific
    AND ssa.utm_term =mta.utm_term
    AND ssa.arrived_channel = mta.arrived_channel
    AND ssa.utm_source =mta.utm_source
    AND ssa.utm_content =mta.utm_content
    AND ssa.channel = mta.channel
    AND ssa.channelcountry = mta.channelcountry
    AND ssa.device_type = mta.device_type
    AND ssa.marketing_type = mta.marketing_type
    AND ssa.finance_rollup = mta.finance_rollup
    AND ssa.finance_detail = mta.finance_detail
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;


COLLECT STATISTICS  COLUMN (activity_date_pacific),
                    COLUMN (finance_detail)
ON {mta_t2_schema}.mkt_utm_agg;

SET QUERY_BAND = NONE FOR SESSION;



