/*
--------------------------------------------
--t2dl_das_mta.mkt_utm_agg
Migrated AS-IS from NAPBI - will not adhere to standards
--------------------------------------------
*/
-- drop table mta_data;
-- drop table ssa_data;

-- Summarize MTA data by channel, finance rollup, UTM parameters.


CREATE TEMPORARY TABLE IF NOT EXISTS mta_data
AS
SELECT order_date_pacific,
 utm_source,
 utm_channel,
 utm_campaign,
 utm_term,
 utm_content,
 order_channel AS channel,
 order_channelcountry AS channelcountry,
  CASE
  WHEN LOWER(arrived_channel) = LOWER('FULL_LINE')
  THEN 'N.COM'
  WHEN LOWER(arrived_channel) = LOWER('RACK')
  THEN 'R.COM'
  ELSE 'NULL'
  END AS arrived_channel,
  CASE
  WHEN LOWER(arrived_platform) = LOWER('') OR arrived_platform IS NULL
  THEN 'UNKNOWN'
  ELSE arrived_platform
  END AS device_type,
 COALESCE(CASE
   WHEN LOWER(mktg_type) = LOWER('BASE')
   THEN mktg_type
   ELSE marketing_type
   END, 'UNATTRIBUTED') AS marketing_type,
 COALESCE(CASE
   WHEN LOWER(mktg_type) = LOWER('BASE')
   THEN mktg_type
   ELSE finance_rollup
   END, 'UNATTRIBUTED') AS finance_rollup,
 COALESCE(CASE
   WHEN LOWER(mktg_type) = LOWER('BASE')
   THEN mktg_type
   ELSE finance_detail
   END, 'UNATTRIBUTED') AS finance_detail,
 SUM(attributed_demand) AS gross,
 SUM(attributed_pred_net) AS net_sales,
 SUM(attributed_units) AS units,
 SUM(attributed_orders) AS orders
FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mta_acp_scoring_fact
WHERE order_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}                               
GROUP BY order_date_pacific,
 utm_source,
 utm_channel,
 utm_campaign,
 utm_term,
 utm_content,
 channel,
 channelcountry,
 arrived_channel,
 device_type,
 marketing_type,
 finance_rollup,
 finance_detail;

 -- Summarize SSA data by channel, finance rollup, UTM parameters.
CREATE TEMPORARY TABLE IF NOT EXISTS ssa_data AS (
SELECT
activity_date_pacific,
utm_source,
utm_medium,
funnel_type,
utm_channel,
utm_campaign,
utm_term,
utm_content,
CASE WHEN LOWER(channel) = LOWER('NORDSTROM') AND LOWER(channelcountry) = LOWER('US') THEN 'N.COM'
     WHEN LOWER(channel) = LOWER('NORDSTROM_RACK') AND LOWER(channelcountry) = LOWER('US') THEN 'RACK.COM'
     WHEN LOWER(channel) = LOWER('NORDSTROM_RACK') AND LOWER(channelcountry) = LOWER('CA') THEN 'RACK.CA'
     WHEN LOWER(channel) = LOWER('NORDSTROM') AND LOWER(channelcountry) = LOWER('CA') THEN 'N.CA'
ELSE channel END AS channel,
UPPER(channelcountry) as channelcountry,
'NULL' AS arrived_channel,
UPPER(CASE WHEN LOWER(experience) IN (LOWER('DESKTOP_WEB'),LOWER('CARE_PHONE'),LOWER('POINT_OF_SALE'),LOWER('VENDOR')) THEN 'WEB'
           WHEN LOWER(experience) = LOWER('MOBILE_WEB') THEN 'MOW'
           WHEN LOWER(experience) = LOWER('ANDROID_APP') THEN 'ANDROID'
           WHEN LOWER(experience) = LOWER('IOS_APP') THEN 'IOS'
      ELSE experience END) AS device_type,
CASE WHEN LOWER(mrkt_type)  = LOWER('UNATTR/TIEOUT') THEN 'UNATTRIBUTED' ELSE mrkt_type END AS marketing_type,
CASE WHEN LOWER(finance_rollup) = LOWER('UNATTR/TIEOUT') THEN 'UNATTRIBUTED' ELSE finance_rollup END AS finance_rollup,
CASE WHEN (utm_source LIKE '%planned' OR utm_source LIKE '%promotional' OR utm_source IS NULL OR LOWER(utm_source) IN (LOWER('SALESFORCE'),LOWER('SFMC'),LOWER('LEANPLUM'))) AND LOWER(finance_detail) = LOWER('APP_PUSH') THEN  'APP_PUSH_PLANNED'
	WHEN utm_source LIKE '%triggered' AND LOWER(finance_detail) = LOWER('APP_PUSH') THEN 'APP_PUSH_TRIGGERED'
     WHEN utm_source LIKE '%transactional' AND LOWER(finance_detail) = LOWER('APP_PUSH') THEN 'APP_PUSH_TRANSACTIONAL'
ELSE finance_detail
END AS finance_detail,
SUM(total_sessions) as total_sessions,
SUM(web_orders) as web_orders,
SUM(web_demand_usd) as web_demand_usd,
SUM(bounced_sessions) as bounced_sessions
FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mta_utm_session_agg
WHERE activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}                         
AND LOWER(channelcountry) in (LOWER('US'),LOWER('CA'))
AND LOWER(channel) in (LOWER('NORDSTROM'),LOWER('NORDSTROM_RACK'))
GROUP BY activity_date_pacific,utm_source,utm_medium,funnel_type,utm_channel,utm_campaign,utm_term,utm_content,channel,channelcountry,arrived_channel,device_type,marketing_type,finance_rollup,finance_detail
)
 ;

/*
--------------------------------------------
DELETE any overlapping records from destination
table prior to INSERT of new data
--------------------------------------------
*/

DELETE
FROM    `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mkt_utm_agg
WHERE   activity_date_pacific >= {{params.start_date}}                                  
AND     activity_date_pacific <= {{params.end_date}}                                   
;


INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mkt_utm_agg
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
    CASE WHEN UPPER(SUBSTR(COALESCE(mta.utm_content,ssa.utm_content), 1, 3)) = 'ICF' THEN 'Yes' ELSE 'No' END AS nmn,
    --MTA Metrics
    IFNULL(SUM(mta.gross),0) AS gross,
    IFNULL(SUM(mta.net_sales),0) AS net_sales,
    IFNULL(SUM(mta.units),0) AS units,
    IFNULL(SUM(mta.orders),0) AS orders,
    --SSA Metrics
    IFNULL(SUM(ssa.web_orders),0) AS session_orders,
    IFNULL(SUM(ssa.web_demand_usd),0) AS session_demand,
    IFNULL(SUM(ssa.total_sessions),0) AS sessions,
    IFNULL(SUM(ssa.bounced_sessions),0) AS bounced_sessions
FROM ssa_data AS ssa
FULL OUTER JOIN mta_data AS mta
    ON LOWER(ssa.utm_campaign) = LOWER(mta.utm_campaign)
    AND LOWER(ssa.utm_channel) = LOWER(mta.utm_channel)
    AND ssa.activity_date_pacific = mta.order_date_pacific
    AND LOWER(ssa.utm_term) = LOWER(mta.utm_term)
    AND LOWER(ssa.arrived_channel) = LOWER(mta.arrived_channel)
    AND LOWER(ssa.utm_source) = LOWER(mta.utm_source)
    AND LOWER(ssa.utm_content) = LOWER(mta.utm_content)
    AND LOWER(ssa.channel) = LOWER(mta.channel)
    AND LOWER(ssa.channelcountry) = LOWER(mta.channelcountry)
    AND LOWER(ssa.device_type) = LOWER(mta.device_type)
    AND LOWER(ssa.marketing_type) = LOWER(mta.marketing_type)
    AND LOWER(ssa.finance_rollup) = LOWER(mta.finance_rollup)
    AND LOWER(ssa.finance_detail) = LOWER(mta.finance_detail)
GROUP BY activity_date_pacific,channel,channelcountry,arrived_channel,device_type,marketing_type,finance_rollup,finance_detail,utm_channel,utm_source,utm_medium,funnel_type,utm_campaign,utm_term,utm_content,nmn
;