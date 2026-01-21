/*SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_paid_campaign_daily_11521_ACE_ENG;
     Task_Name=mta_paid_campaign_daily;'
     FOR SESSION VOLATILE;

T2/Table Name: t2dl_das_mta.mta_paid_campaign_daily
Team/Owner: MTA
Date Created/Modified: 05/2023
Note:
-- This table is is the source for the MTA 4 Box and DAD dashboards.
-- Lookback window is 90 days to match the upstream funnel_cost_fact lookback window.
*/
/*

V2 Questions/Enhancements
-- I don't think finance_detail is needed as a join criteria for any channels.  Something to check in the future for additional streamlining of logic.
-- add collect stats on upstream/downstream temp table to assist platform joins.
-- look into keeping original .csv sourcefile names for each platform instead of renaming, specifically for verizon>oath and yahoo>gemini.
        -- This would require creating duplicate banner specific definitions in the mta/downstream temp table.
-- Suggest combining the platforms that have similar joins to reduce the number of steps.
-- Suggest using the same improvements logic for all platforms and doing it once at the end instead of for each individual platform.
		-- When doing this, make sure ALL dimensions get updated appropriately to new field including the misc flags so they match the new campaign names, etc.
*/
-- CREATING UPSTREAM TABLE
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS funnel_cost_fact
AS
SELECT stats_date,
 file_name,
 REGEXP_EXTRACT_ALL(file_name, '[^_]+')[SAFE_OFFSET(1)] AS platform,
  CASE
  WHEN LOWER(REGEXP_EXTRACT_ALL(file_name, '[^_]+')[SAFE_OFFSET(0)]) = LOWER('fp')
  THEN 'N.COM'
  WHEN LOWER(REGEXP_EXTRACT_ALL(file_name, '[^_]+')[SAFE_OFFSET(0)]) = LOWER('rack')
  THEN 'R.COM'
  ELSE NULL
  END AS banner,
 sourcename,
  CASE
  WHEN LOWER(REGEXP_EXTRACT_ALL(file_name, '[^_]+')[SAFE_OFFSET(1)]) LIKE LOWER('%bing')
  THEN CASE
   WHEN LOWER(campaign_name) LIKE LOWER('%@_PSU@_%') OR LOWER(campaign_name) LIKE LOWER('%DB%') OR LOWER(campaign_name)
      LIKE LOWER('%NB%') OR LOWER(campaign_name) LIKE LOWER('%competitor%')
   THEN 'PAID_SEARCH_UNBRANDED'
   WHEN LOWER(campaign_name) LIKE LOWER('%@_PSB@_%')
   THEN 'PAID_SEARCH_BRANDED'
   WHEN LOWER(sourcename) LIKE LOWER('%producT%') OR LOWER(media_type) LIKE LOWER('%Price comparison%') OR LOWER(sourcename
       ) LIKE LOWER('%Shopping%') OR LOWER(campaign_name) LIKE LOWER('MSAN%')
   THEN 'PAID_SHOPPING'
   ELSE NULL
   END
  WHEN LOWER(sourcetype) = LOWER('yahoo')
  THEN CASE
   WHEN LOWER(sourcename) IN (LOWER('Nordstrom 2017 DPA Retention'), LOWER('Nordstrom Retention Native'))
   THEN 'DISPLAY'
   WHEN LOWER(sourcename) = LOWER('Nordstrom Terms')
   THEN 'PAID_SEARCH_BRANDED'
   WHEN LOWER(sourcename) = LOWER('Nordstrom')
   THEN 'PAID_SEARCH_UNBRANDED'
   ELSE NULL
   END
  WHEN LOWER(advertising_channel) IN (LOWER('SEARCH'), LOWER('SHOPPING'))
  THEN CASE
   WHEN LOWER(sourcename) IN (LOWER('Nordstrom Acquisition - 4967171468'), LOWER('Nordstrom Main - 5417253570')) OR
        LOWER(campaign_name) LIKE LOWER('%@_PSU@_%') OR LOWER(campaign_name) LIKE LOWER('%@_DB@_%') OR LOWER(campaign_name
       ) LIKE LOWER('%@_NB@_%') OR LOWER(campaign_name) LIKE LOWER('%competitor%')
   THEN 'PAID_SEARCH_UNBRANDED'
   WHEN LOWER(sourcename) IN (LOWER('Nordstrom Terms - 5601420631')) OR LOWER(campaign_name) LIKE LOWER('%@_PSB@_%')
   THEN 'PAID_SEARCH_BRANDED'
   WHEN LOWER(sourcename) IN (LOWER('Nordstrom - Product Listing Ads - 5035034225'), LOWER('Nordstrom Shopping Coop - 9385258156'
       )) OR LOWER(media_type) LIKE LOWER('%Price comparison%')
   THEN 'PAID_SHOPPING'
   ELSE NULL
   END
  WHEN LOWER(media_type) LIKE LOWER('%display%') OR LOWER(campaign_name) LIKE LOWER('%display%')
  THEN CASE
   WHEN LOWER(sourcename) LIKE LOWER('%Nordstrom US Video - 5038267247%')
   THEN 'VIDEO'
   ELSE 'DISPLAY'
   END
  WHEN LOWER(file_name) LIKE LOWER('%adwords') AND (LOWER(campaign_name) LIKE LOWER('PMAX%') OR LOWER(campaign_name)
      LIKE LOWER('%- PMax_%'))
  THEN 'PAID_SHOPPING'
  WHEN LOWER(media_type) = LOWER('video')
  THEN 'VIDEO'
  ELSE 'Unknown'
  END AS finance_detail,
 TRIM(campaign_name) AS campaign_name,
 campaign_id,
 TRIM(adgroup_name) AS adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
  CASE
  WHEN LOWER(device_type) LIKE LOWER('phone') OR LOWER(device_type) LIKE LOWER('smartphone') OR LOWER(device_type) LIKE
         LOWER('%mobile%') OR LOWER(device_type) LIKE LOWER('%android_smartphone%') OR LOWER(device_type) LIKE LOWER('%iphone%'
        ) OR LOWER(device_type) LIKE LOWER('%ipod%') OR LOWER(device_type) LIKE LOWER('%table%') OR LOWER(device_type)
    LIKE LOWER('%ipad%')
  THEN 'MOBILE'
  WHEN LOWER(device_type) LIKE LOWER('%tv%') OR LOWER(device_type) LIKE LOWER('%computer%') OR LOWER(device_type) LIKE
    LOWER('%desktop%')
  THEN 'COMPUTER'
  ELSE 'Unknown'
  END AS device_type,
  CASE
  WHEN LOWER(campaign_name) LIKE LOWER('%low_nd%') OR LOWER(adgroup_name) LIKE LOWER('%low_nd%')
  THEN 'LOW'
  WHEN LOWER(campaign_name) LIKE LOWER('%mid_nd%') OR LOWER(campaign_name) LIKE LOWER('%@_mid@_%') OR LOWER(campaign_name
      ) LIKE LOWER('%midfunnel%') OR LOWER(adgroup_name) LIKE LOWER('%mid_nd%')
  THEN 'MID'
  WHEN LOWER(campaign_name) LIKE LOWER('%up_nd%') OR LOWER(adgroup_name) LIKE LOWER('%up_nd%')
  THEN 'UP'
  WHEN LOWER(campaign_name) LIKE LOWER('MSAN%')
  THEN 'LOW'
  ELSE 'Unknown'
  END AS funnel_type,
  CASE
  WHEN LOWER(campaign_name) LIKE LOWER('%@_nd@_%') OR LOWER(adgroup_name) LIKE LOWER('%@_nd@_%')
  THEN 'ND'
  WHEN LOWER(campaign_name) LIKE LOWER('%@_ex@_%') OR LOWER(adgroup_name) LIKE LOWER('%@_ex@_%')
  THEN 'EX'
  WHEN LOWER(adgroup_name) LIKE LOWER('%@_P@_%') OR LOWER(campaign_name) LIKE LOWER('%@_P@_%') OR LOWER(campaign_name)
       LIKE LOWER('%@_Persistent@_%') OR LOWER(sourcename) IN (LOWER('Nordstrom Terms - 5601420631'), LOWER('Nordstrom Main - 5417253570'
        )) OR LOWER(sourcename) IN (LOWER('DPA'), LOWER('DABA'), LOWER('Local Dynamic Ads'), LOWER('Social - 2018 Canada'
       )) OR LOWER(sourcename) IN (LOWER('Nordstrom 2017 DPA Retention'))
  THEN 'Persistent'
  WHEN LOWER(adgroup_name) LIKE LOWER('%vendor%') OR LOWER(adgroup_name) LIKE LOWER('%@_VF@_%') OR LOWER(campaign_name)
      LIKE LOWER('%@_VF@_%') OR LOWER(campaign_name) LIKE LOWER('%icf%') OR LOWER(sourcename) IN (LOWER('Nordstrom Acquisition - 4967171468'
      ), LOWER('Nordstrom Canada Search - 1893217362'), LOWER('Nordstrom Shopping Coop - 9385258156'))
  THEN 'Coop'
  WHEN LOWER(campaign_name) LIKE LOWER('MSAN%')
  THEN 'ND'
  ELSE 'Unknown'
  END AS funding,
  CASE
  WHEN LOWER(campaign_name) LIKE LOWER('%_2928_%') OR LOWER(campaign_name) LIKE LOWER('%_2929_%') OR LOWER(campaign_name
      ) LIKE LOWER('%_2932_%') OR LOWER(campaign_name) LIKE LOWER('%_2922_%')
  THEN 'delete'
  WHEN LOWER(adgroup_name) LIKE LOWER('%@_RTG@_%') OR LOWER(adgroup_name) LIKE LOWER('%retargeting%') OR LOWER(adgroup_name
     ) LIKE LOWER('ALL_PRODUCTS|DRT_60DAY')
  THEN 'Retargeting'
  WHEN LOWER(campaign_name) LIKE LOWER('%ACQ%') OR LOWER(adgroup_name) LIKE LOWER('%@_Acq@_%') OR LOWER(adgroup_name)
       LIKE LOWER('%|ACQ|%') OR LOWER(adgroup_name) LIKE LOWER('%INTEREST_KW%') OR LOWER(sourcename) IN (LOWER('Nordstrom Acquisition - 4967171468'
       ), LOWER('Nordstrom Canada Search - 1893217362')) OR LOWER(sourcename) IN (LOWER('Nordstrom Kenshoo #2'), LOWER('Social 2018 - Facebook'
      ), LOWER('Social 2018 - Instagram'))
  THEN 'Acquisition'
  WHEN LOWER(campaign_name) LIKE LOWER('%RET%') OR LOWER(adgroup_name) LIKE LOWER('%@_Ret@_%') OR LOWER(adgroup_name)
     LIKE LOWER('%|Ret|%') OR LOWER(sourcename) IN (LOWER('Nordstrom 2017 DPA Retention'), LOWER('Nordstrom Retention Native'
      ))
  THEN 'Retention'
  WHEN LOWER(adgroup_name) LIKE LOWER('%Conquesting%')
  THEN 'Conquesting'
  WHEN LOWER(campaign_name) LIKE LOWER('%@_PSB@_%') OR LOWER(campaign_name) LIKE LOWER('%@_Brand@_%') OR LOWER(adgroup_name
      ) LIKE LOWER('%@_Brand@_%') OR LOWER(campaign_name) LIKE LOWER('%TERMS%')
  THEN 'Brand'
  ELSE 'Others'
  END AS ar_flag,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(campaign_name) LIKE LOWER('%@_nd@_%') OR LOWER(adgroup_name) LIKE LOWER('%@_nd@_%')
     THEN 'ND'
     WHEN LOWER(campaign_name) LIKE LOWER('%@_ex@_%') OR LOWER(adgroup_name) LIKE LOWER('%@_ex@_%')
     THEN 'EX'
     WHEN LOWER(adgroup_name) LIKE LOWER('%@_P@_%') OR LOWER(campaign_name) LIKE LOWER('%@_P@_%') OR LOWER(campaign_name
           ) LIKE LOWER('%@_Persistent@_%') OR LOWER(sourcename) IN (LOWER('Nordstrom Terms - 5601420631'), LOWER('Nordstrom Main - 5417253570'
           )) OR LOWER(sourcename) IN (LOWER('DPA'), LOWER('DABA'), LOWER('Local Dynamic Ads'), LOWER('Social - 2018 Canada'
          )) OR LOWER(sourcename) IN (LOWER('Nordstrom 2017 DPA Retention'))
     THEN 'Persistent'
     WHEN LOWER(adgroup_name) LIKE LOWER('%vendor%') OR LOWER(adgroup_name) LIKE LOWER('%@_VF@_%') OR LOWER(campaign_name
          ) LIKE LOWER('%@_VF@_%') OR LOWER(campaign_name) LIKE LOWER('%icf%') OR LOWER(sourcename) IN (LOWER('Nordstrom Acquisition - 4967171468'
         ), LOWER('Nordstrom Canada Search - 1893217362'), LOWER('Nordstrom Shopping Coop - 9385258156'))
     THEN 'Coop'
     WHEN LOWER(campaign_name) LIKE LOWER('MSAN%')
     THEN 'ND'
     ELSE 'Unknown'
     END) = LOWER('EX')
  THEN 'Yes'
  ELSE 'No'
  END AS nmn_flag,
  CASE
  WHEN LOWER(currency) = LOWER('CAD') OR LOWER(campaign_name) LIKE LOWER('Canada %')
  THEN 'CA'
  ELSE 'US'
  END AS country,
 SUM(cost) AS cost,
 SUM(impressions) AS impressions,
 SUM(clicks) AS clicks,
 SUM(conversions) AS conversions,
 SUM(conversion_value) AS conversion_value,
 SUM(video_views) AS video_views,
 SUM(video100) AS video100,
 SUM(video75) AS video75
FROM `{{params.gcp_project_id}}`.t2dl_das_funnel_io.funnel_cost_fact_vw
WHERE stats_date BETWEEN  {{params.start_date}}  AND {{params.end_date}} 
GROUP BY stats_date,
 file_name,
 platform,
 banner,
 sourcename,
 finance_detail,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
 device_type,
 funnel_type,
 funding,
 ar_flag,
 nmn_flag,
 country;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS      COLUMN (finance_detail), COLUMN (platform), COLUMN (stats_date,banner,device_type,adgroup_id), COLUMN (stats_date,banner,device_type,ad_id), COLUMN (stats_date,banner,device_type,finance_detail,adgroup_id), COLUMN (stats_date,banner,device_type,finance_detail,adgroup_id,ad_name), COLUMN (banner,finance_detail,adgroup_id) ON funnel_cost_fact;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS mkt_utm
AS
SELECT CASE
  WHEN LOWER(arrived_channel) = LOWER('NULL') AND LOWER(channel) = LOWER('N.CA')
  THEN 'N.COM'
  WHEN LOWER(arrived_channel) = LOWER('NULL') AND LOWER(channel) = LOWER('N.COM')
  THEN 'N.COM'
  WHEN LOWER(arrived_channel) = LOWER('NULL') AND LOWER(channel) = LOWER('RACK.COM')
  THEN 'R.COM'
  ELSE arrived_channel
  END AS arrived_channel,
 utm_source,
 utm_channel,
 TRIM(utm_campaign) AS utm_campaign,
 utm_term,
 utm_content,
 channel AS order_channel,
  CASE
  WHEN LOWER(finance_detail) = LOWER('SHOPPING')
  THEN 'PAID_SHOPPING'
  ELSE finance_detail
  END AS finance_detail,
  CASE
  WHEN LOWER(utm_source) LIKE LOWER('%bing%')
  THEN 'bing'
  WHEN LOWER(utm_source) LIKE LOWER('%SNAPCHAT%')
  THEN 'snapchat'
  WHEN LOWER(utm_source) LIKE LOWER('%pinterest%') AND LOWER(finance_detail) LIKE LOWER('SOCIAL_PAID') AND LOWER(utm_campaign
     ) NOT LIKE LOWER('PRODUCTSHARE')
  THEN 'pinterest'
  WHEN LOWER(utm_source) LIKE LOWER('%TIKTOK%')
  THEN 'tiktok'
  WHEN LOWER(utm_source) LIKE LOWER('%gemini%')
  THEN 'gemini'
  WHEN LOWER(utm_source) LIKE LOWER('%google%') OR LOWER(utm_source) LIKE LOWER('%googl%') OR LOWER(utm_source) LIKE
     LOWER('%gdn%') OR LOWER(utm_source) LIKE LOWER('%youtube%')
  THEN 'adwords'
  WHEN (LOWER(utm_source) LIKE LOWER('%facebook%') OR LOWER(utm_source) LIKE LOWER('%insta%') OR LOWER(utm_source) LIKE
       LOWER('%displa%fb%') OR LOWER(utm_source) LIKE LOWER('%kenshoo%')) AND LOWER(finance_detail) NOT LIKE LOWER('%organic%'
     )
  THEN 'facebook'
  WHEN LOWER(utm_source) LIKE LOWER('%aol%') OR LOWER(utm_source) LIKE LOWER('%vzn%') OR LOWER(utm_source) LIKE LOWER('%oath%'
       ) OR LOWER(utm_source) LIKE LOWER('%yahoo%') OR LOWER(utm_source) LIKE LOWER('%yahoo@_dpa%')
  THEN 'oath'
  WHEN (LOWER(utm_source) NOT LIKE LOWER('%AFFILIATE%') AND LOWER(utm_source) NOT LIKE LOWER('%bing%') AND LOWER(utm_source
                        ) NOT LIKE LOWER('%google%') AND LOWER(utm_source) NOT LIKE LOWER('%googl%') AND LOWER(utm_source
                      ) NOT LIKE LOWER('%gdn%') AND LOWER(utm_source) NOT LIKE LOWER('%youtube%') AND LOWER(utm_source)
                   NOT LIKE LOWER('%gemini%') AND LOWER(utm_source) NOT LIKE LOWER('%yahoo@_dpa%') AND LOWER(utm_source
                  ) NOT LIKE LOWER('%aol%') AND LOWER(utm_source) NOT LIKE LOWER('%vzn%') AND LOWER(utm_source) NOT LIKE
               LOWER('%oath%') AND LOWER(utm_source) NOT LIKE LOWER('%yahoo%') AND LOWER(utm_source) NOT LIKE LOWER('%pinterest%'
              ) AND LOWER(utm_source) NOT LIKE LOWER('%TIKTOK%') AND LOWER(utm_source) NOT LIKE LOWER('%facebook%') AND
          LOWER(utm_source) NOT LIKE LOWER('%insta%') AND LOWER(utm_source) NOT LIKE LOWER('%displa%fb%') AND LOWER(utm_source
         ) NOT LIKE LOWER('%kenshoo%') AND LOWER(utm_source) NOT LIKE LOWER('%SNAPCHAT%') OR utm_source IS NULL) AND
    LOWER(finance_detail) IN (LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED'), LOWER('DISPLAY'), LOWER('VIDEO'
      ), LOWER('SOCIAL_PAID'), LOWER('PAID_SHOPPING'), LOWER('SHOPPING'))
  THEN 'Unknown'
  ELSE NULL
  END AS platform,
  CASE
  WHEN LOWER(device_type) IN (LOWER('ANDROID'), LOWER('IOS'), LOWER('MOW'))
  THEN 'MOBILE'
  WHEN LOWER(device_type) IN (LOWER('TABLET'), LOWER('PAD'))
  THEN 'TABLET'
  WHEN LOWER(device_type) IN (LOWER('WEB'), LOWER('COMPUTER'))
  THEN 'COMPUTER'
  WHEN device_type IS NULL OR LOWER(device_type) = LOWER('') OR LOWER(device_type) LIKE LOWER('Unknown')
  THEN 'Unknown'
  ELSE device_type
  END AS device_type,
 activity_date_pacific,
 SUM(gross) AS attributed_demand,
 SUM(orders) AS attributed_orders,
 SUM(net_sales) AS attributed_pred_net,
 SUM(units) AS attributed_units,
 SUM(session_orders) AS session_orders,
 SUM(sessions) AS sessions,
 SUM(bounced_sessions) AS bounced_sessions
FROM `{{params.gcp_project_id}}`.t2dl_das_mta.mkt_utm_agg AS mua
WHERE activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}} 
 AND (LOWER(utm_source) <> LOWER('') AND LOWER(utm_source) NOT LIKE LOWER('%email%') AND LOWER(utm_source) NOT LIKE
     LOWER('%leanplum%') OR utm_source IS NULL)
 AND arrived_channel IS NOT NULL
GROUP BY arrived_channel,
 utm_source,
 utm_channel,
 utm_campaign,
 utm_term,
 utm_content,
 order_channel,
 finance_detail,
 platform,
 device_type,
 activity_date_pacific;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;



CREATE TEMPORARY TABLE IF NOT EXISTS adwords_display_dad_4box AS (
SELECT  a.activity_date_pacific
, a.platform
, a.arrived_channel
, a.order_channel
, a.finance_detail
, a.sourcename
, a.campaign_name
, a.campaign_id
, a.adgroup_name
, a.adgroup_id
, a.ad_name
, a.ad_id
, a.funding
, a.ar_flag
, a.country
, a.funnel_type
, a.nmn_flag
, a.device_type
, SUM(CASE WHEN fcf_row_num = 1 THEN a.COST ELSE NULL END) AS cost
, SUM(CASE WHEN fcf_row_num = 1 THEN a.impressions ELSE NULL END) AS impressions
, SUM(CASE WHEN fcf_row_num = 1 THEN a.clicks ELSE NULL END) AS clicks
, SUM(CASE WHEN fcf_row_num = 1 THEN a.conversions ELSE NULL END) AS conversions
, SUM(CASE WHEN fcf_row_num = 1 THEN a.conversion_value ELSE NULL END) AS conversion_value
, SUM(CASE WHEN fcf_row_num = 1 THEN a.video_views ELSE NULL END) AS video_views
, SUM(CASE WHEN fcf_row_num = 1 THEN a.video100 ELSE NULL END) AS video100
, SUM(CASE WHEN fcf_row_num = 1 THEN a.video75 ELSE NULL END) AS video75

, SUM(a.attributed_demand) AS attributed_demand
, SUM(a.attributed_orders) AS attributed_orders
, SUM(a.attributed_units) AS attributed_units
, SUM(a.attributed_pred_net) AS attributed_pred_net
, SUM(a.sessions) AS sessions
, SUM(a.bounced_sessions) AS bounced_sessions
, SUM(a.session_orders) AS session_orders
FROM (	SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
, COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
, COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
, COALESCE(mta.order_channel,'Unknown') AS order_channel
, CASE WHEN fcf.sourcename LIKE '%Nordstrom US Video - 5038267247%'
OR fcf.sourcename LIKE 'Nordstrom Rack - YouTube - 5266227484'
THEN 'VIDEO'
ELSE 'DISPLAY'
END AS finance_detail
, COALESCE(fcf.sourcename,'Unknown') AS sourcename
, COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
, COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
, COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
, COALESCE(fcf.adgroup_id,mta.utm_term,'Unknown') AS adgroup_id
, 'Unknown' AS ad_name
, 'Unknown' AS ad_id
, COALESCE(fcf.funding,'Unknown') AS funding
, COALESCE(fcf.ar_flag,'Others') AS ar_flag
, COALESCE(fcf.country,'US' ) AS country
, COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
, COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
, COALESCE(fcf.device_type,mta.device_type) AS device_type
, fcf.stats_date
, fcf.banner
, fcf.device_type AS fcf_device_type
, fcf.adgroup_id AS fcf_adgroupid
, mta.order_channel AS mta_order_channel
, ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf.device_type, fcf.adgroup_id order by mta.order_channel ASC) AS fcf_row_num

, SUM(fcf.COST) AS cost
, SUM(fcf.impressions) AS impressions
, SUM(fcf.clicks) AS clicks
, SUM(fcf.conversions) AS conversions
, SUM(fcf.conversion_value) AS conversion_value
, SUM(fcf.video_views) AS video_views
, SUM(fcf.video100) AS video100
, SUM(fcf.video75) AS video75

, SUM(mta.attributed_demand) AS attributed_demand
, SUM(mta.attributed_orders) AS attributed_orders
, SUM(mta.attributed_units) AS attributed_units
, SUM(mta.attributed_pred_net) AS attributed_pred_net
, SUM(mta.sessions) AS sessions
, SUM(mta.bounced_sessions) AS bounced_sessions
, SUM(mta.session_orders) AS session_orders
FROM (  SELECT  stats_date
, file_name
, platform
, banner
, sourcename
, finance_detail
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, device_type
, funnel_type
, funding
, ar_flag
, nmn_flag
, country
, SUM(cost) AS cost
, SUM(impressions) AS impressions
, SUM(clicks) AS clicks
, SUM(conversions) AS conversions
, SUM(conversion_value) AS conversion_value
, SUM(video_views) AS video_views
, SUM(video100) AS video100
, SUM(video75) AS video75
FROM funnel_cost_fact
WHERE LOWER(platform) = LOWER('adwords')
AND LOWER(finance_detail) IN (LOWER('display'), LOWER('video'))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) fcf
FULL JOIN (SELECT arrived_channel
, CASE WHEN utm_source LIKE '%youtube%' AND arrived_channel = 'N.com'
then
CASE WHEN utm_term LIKE '%15072977038%'
THEN '128617871973'
WHEN utm_campaign LIKE '%4229_j012594_17325%'
THEN '131947704380'
ELSE SUBSTRING(utm_term, 5, 12)
end
ELSE utm_term
END AS utm_term
, platform
, device_type
, order_channel
, finance_detail
, activity_date_pacific
, SUM(attributed_demand) AS attributed_demand
, SUM(attributed_orders) AS attributed_orders
, SUM(attributed_pred_net) AS attributed_pred_net
, SUM(attributed_units) AS attributed_units
, SUM(session_orders) AS session_orders
, SUM(sessions) AS sessions
, SUM(bounced_sessions) AS bounced_sessions
FROM mkt_utm
WHERE LOWER(platform) = LOWER('adwords')
AND (LOWER(finance_detail) IN (LOWER('display'), LOWER('video'))
     OR LOWER(utm_campaign) LIKE LOWER('%4230_j012315_16912%'))
GROUP BY 1,2,3,4,5,6,7
) mta
ON fcf.stats_date = mta.activity_date_pacific
AND fcf.banner = mta.arrived_channel
AND fcf.device_type = mta.device_type

AND fcf.adgroup_id = mta.utm_term
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23) a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) 
;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS adwords_search_dad_4box
AS
SELECT t5.activity_date_pacific,
 t5.platform,
 t5.arrived_channel,
 t5.order_channel,
 t5.finance_detail,
 t5.sourcename,
 t5.campaign_name,
 t5.campaign_id,
 t5.adgroup_name,
 t5.adgroup_id,
 t5.ad_name,
 t5.ad_id,
 t5.funding,
 t5.ar_flag,
 t5.country,
 t5.funnel_type,
 t5.nmn_flag,
 t5.device_type,
 t5.stats_date,
 t5.banner,
 t5.fcf_device_type,
 t5.adgroup_id AS fcf_adgroupid,
 t5.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t5.stats_date, t5.banner, t5.fcf_device_type, t5.adgroup_id ORDER BY t5.order_channel)
 AS fcf_row_num,
 t5.cost_0 AS cost,
 t5.impressions_0 AS impressions,
 t5.clicks_0 AS clicks,
 t5.conversions_0 AS conversions,
 t5.conversion_value_0 AS conversion_value,
 t5.video_views_0 AS video_views,
 t5.video100_0 AS video100,
 t5.video75_0 AS video75,
 t5.attributed_demand_0 AS attributed_demand,
 t5.attributed_orders_0 AS attributed_orders,
 t5.attributed_units_0 AS attributed_units,
 t5.attributed_pred_net_0 AS attributed_pred_net,
 t5.sessions_0 AS sessions,
 t5.bounced_sessions_0 AS bounced_sessions,
 t5.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, mta.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, mta.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, mta.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(mta.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, mta.finance_detail, 'Unknown') AS finance_detail,
   COALESCE(fcf.sourcename, 'Unknown') AS sourcename,
   COALESCE(fcf.campaign_name, 'Unknown') AS campaign_name,
   COALESCE(fcf.campaign_id, 'Unknown') AS campaign_id,
   COALESCE(fcf.adgroup_name, 'Unknown') AS adgroup_name,
   COALESCE(fcf.adgroup_id, mta.utm_content, 'Unknown') AS adgroup_id,
   'Unknown' AS ad_name,
   COALESCE(fcf.funding, 'Unknown') AS funding,
   COALESCE(fcf.ar_flag, 'Others') AS ar_flag,
   COALESCE(fcf.country, 'US') AS country,
   COALESCE(fcf.funnel_type, 'Unknown') AS funnel_type,
   COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
   COALESCE(fcf.device_type, mta.device_type) AS device_type,
   fcf.stats_date,
   fcf.banner,
   fcf.device_type AS fcf_device_type,
   'Unknown' AS ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(mta.attributed_demand) AS attributed_demand_0,
   SUM(mta.attributed_orders) AS attributed_orders_0,
   SUM(mta.attributed_units) AS attributed_units_0,
   SUM(mta.attributed_pred_net) AS attributed_pred_net_0,
   SUM(mta.sessions) AS sessions_0,
   SUM(mta.bounced_sessions) AS bounced_sessions_0,
   SUM(mta.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) = LOWER('adwords')
     AND LOWER(finance_detail) IN (LOWER('PAID_SHOPPING'), LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED')
       )
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     TRIM(REGEXP_EXTRACT_ALL(NULLIF(utm_content, ''), '[^_]+')[SAFE_OFFSET(0)]) AS utm_content,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) = LOWER('adwords')
     AND LOWER(finance_detail) IN (LOWER('PAID_SHOPPING'), LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED')
       )
     AND utm_content IS NOT NULL
    GROUP BY arrived_channel,
     utm_content,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific) AS mta ON fcf.stats_date = mta.activity_date_pacific AND LOWER(fcf.banner) = LOWER(mta.arrived_channel
         ) AND LOWER(fcf.device_type) = LOWER(mta.device_type) AND LOWER(fcf.adgroup_id) = LOWER(mta.utm_content) AND
     LOWER(fcf.finance_detail) = LOWER(mta.finance_detail)
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   sourcename,
   campaign_name,
   campaign_id,
   adgroup_name,
   adgroup_id,
   ad_name,
   funding,
   ar_flag,
   country,
   funnel_type,
   nmn_flag,
   device_type,
   fcf.stats_date,
   fcf.banner,
   fcf_device_type,
   ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders) AS t5;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (ARRIVED_CHANNEL ,FINANCE_DETAIL, ADGROUP_ID) ON adwords_search_dad_4box;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS adwords_search_improvements
AS
SELECT stats_date,
 banner,
 sourcename,
 finance_detail,
 adgroup_id,
 adgroup_name,
 funnel_type,
 campaign_name,
 campaign_id,
 funding,
 ROW_NUMBER() OVER (PARTITION BY banner, adgroup_id, finance_detail ORDER BY stats_date DESC) AS row_count
FROM (SELECT DISTINCT stats_date,
   banner,
   sourcename,
   finance_detail,
   adgroup_id,
   adgroup_name,
   funnel_type,
   campaign_name,
   campaign_id,
   funding
  FROM funnel_cost_fact
  WHERE LOWER(platform) = LOWER('adwords')
   AND LOWER(finance_detail) IN (LOWER('PAID_SHOPPING'), LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED')))
 AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (ROW_COUNT) ON adwords_search_improvements;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS adwords_search_dad_4box_improvements
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 t1.finance_detail,
  CASE
  WHEN LOWER(t1.sourcename) = LOWER('Unknown') AND t2.sourcename IS NOT NULL
  THEN t2.sourcename
  ELSE t1.sourcename
  END AS sourcename,
  CASE
  WHEN LOWER(t1.campaign_name) = LOWER('Unknown') AND t2.campaign_name IS NOT NULL
  THEN t2.campaign_name
  ELSE t1.campaign_name
  END AS campaign_name,
  CASE
  WHEN LOWER(t1.campaign_id) = LOWER('Unknown') AND t2.campaign_id IS NOT NULL
  THEN t2.campaign_id
  ELSE t1.campaign_id
  END AS campaign_id,
  CASE
  WHEN LOWER(t1.adgroup_name) = LOWER('Unknown') AND t2.adgroup_name IS NOT NULL
  THEN t2.adgroup_name
  ELSE t1.adgroup_name
  END AS adgroup_name,
  CASE
  WHEN LOWER(t1.adgroup_id) = LOWER('Unknown') AND t2.adgroup_id IS NOT NULL
  THEN t2.adgroup_id
  ELSE t1.adgroup_id
  END AS adgroup_id,
 'Unknown' AS ad_name,
 'Unknown' AS ad_id,
  CASE
  WHEN LOWER(t1.funding) = LOWER('Unknown') AND t2.funding IS NOT NULL
  THEN t2.funding
  ELSE t1.funding
  END AS funding,
 t1.ar_flag,
 t1.country,
  CASE
  WHEN LOWER(t1.funnel_type) = LOWER('Unknown') AND t2.funnel_type IS NOT NULL
  THEN t2.funnel_type
  ELSE t1.funnel_type
  END AS funnel_type,
 t1.nmn_flag,
 t1.device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM adwords_search_dad_4box AS t1
 LEFT JOIN (SELECT *
  FROM adwords_search_improvements
  WHERE ROW_COUNT = 1) AS t2 ON LOWER(t1.adgroup_id) = LOWER(t2.adgroup_id) AND LOWER(t1.finance_detail) = LOWER(t2
     .finance_detail) AND LOWER(t1.arrived_channel) = LOWER(t2.banner)
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 t1.finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 funding,
 t1.ar_flag,
 t1.country,
 funnel_type,
 t1.nmn_flag,
 t1.device_type,
 ad_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS adwords_shopping_dad_4box_pmax
AS
SELECT t5.activity_date_pacific,
 t5.platform,
 t5.arrived_channel,
 t5.order_channel,
 t5.finance_detail,
 t5.sourcename,
 t5.campaign_name,
 t5.campaign_id,
 t5.adgroup_name,
 t5.adgroup_id,
 t5.ad_name,
 t5.ad_id,
 t5.funding,
 t5.ar_flag,
 t5.country,
 t5.funnel_type,
 t5.nmn_flag,
 t5.device_type,
 t5.stats_date,
 t5.banner,
 t5.device_type AS fcf_device_type,
 t5.campaign_id AS fcf_campaign_id,
 t5.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t5.stats_date, t5.banner, t5.device_type, t5.campaign_id ORDER BY t5.order_channel) AS
 fcf_row_num,
 t5.cost_0 AS cost,
 t5.impressions_0 AS impressions,
 t5.clicks_0 AS clicks,
 t5.conversions_0 AS conversions,
 t5.conversion_value_0 AS conversion_value,
 t5.video_views_0 AS video_views,
 t5.video100_0 AS video100,
 t5.video75_0 AS video75,
 t5.attributed_demand_0 AS attributed_demand,
 t5.attributed_orders_0 AS attributed_orders,
 t5.attributed_units_0 AS attributed_units,
 t5.attributed_pred_net_0 AS attributed_pred_net,
 t5.sessions_0 AS sessions,
 t5.bounced_sessions_0 AS bounced_sessions,
 t5.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, mta.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, mta.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, mta.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(mta.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, mta.finance_detail, 'Unknown') AS finance_detail,
   COALESCE(fcf.sourcename, 'Unknown') AS sourcename,
   COALESCE(fcf.campaign_name, 'Unknown') AS campaign_name,
   COALESCE(fcf.campaign_id, mta.utm_campaign, 'Unknown') AS campaign_id,
   'Unknown' AS adgroup_name,
   COALESCE(fcf.funding, 'ND') AS funding,
   COALESCE(fcf.ar_flag, 'Others') AS ar_flag,
   COALESCE(fcf.country, 'US') AS country,
   COALESCE(fcf.funnel_type, 'LOW') AS funnel_type,
   COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
   COALESCE(fcf.device_type, mta.device_type) AS device_type,
   fcf.stats_date,
   fcf.banner,
   'Unknown' AS adgroup_id,
   'Unknown' AS ad_name,
   'Unknown' AS ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(mta.attributed_demand) AS attributed_demand_0,
   SUM(mta.attributed_orders) AS attributed_orders_0,
   SUM(mta.attributed_units) AS attributed_units_0,
   SUM(mta.attributed_pred_net) AS attributed_pred_net_0,
   SUM(mta.sessions) AS sessions_0,
   SUM(mta.bounced_sessions) AS bounced_sessions_0,
   SUM(mta.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) = LOWER('adwords')
     AND LOWER(finance_detail) = LOWER('PAID_SHOPPING')
     AND (LOWER(campaign_name) LIKE LOWER('PMAX%') OR LOWER(campaign_name) LIKE LOWER('%- PMax_%'))
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     TRIM(REGEXP_EXTRACT_ALL(NULLIF(utm_campaign, ''), '[^_]+')[SAFE_OFFSET(0)]) AS utm_campaign,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) = LOWER('adwords')
     AND LOWER(finance_detail) = LOWER('PAID_SHOPPING')
     AND utm_content IS NULL
     AND utm_term IS NULL
     AND LOWER(utm_channel) LIKE LOWER('%PMAX')
     AND utm_campaign IS NOT NULL
    GROUP BY arrived_channel,
     utm_campaign,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific) AS mta ON fcf.stats_date = mta.activity_date_pacific AND LOWER(fcf.banner) = LOWER(mta.arrived_channel
         ) AND LOWER(fcf.device_type) = LOWER(mta.device_type) AND LOWER(fcf.campaign_id) = LOWER(mta.utm_campaign) AND
     LOWER(fcf.finance_detail) = LOWER(mta.finance_detail)
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   sourcename,
   campaign_name,
   campaign_id,
   adgroup_name,
   funding,
   ar_flag,
   country,
   funnel_type,
   nmn_flag,
   device_type,
   fcf.stats_date,
   fcf.banner,
   adgroup_id,
   ad_name,
   ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders) AS t5;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS adwords_shopping_improvements_pmax
AS
SELECT stats_date,
 banner,
 sourcename,
 finance_detail,
 adgroup_id,
 adgroup_name,
 funnel_type,
 campaign_name,
 campaign_id,
 funding,
 ROW_NUMBER() OVER (PARTITION BY banner, campaign_id, finance_detail ORDER BY stats_date DESC) AS row_count
FROM (SELECT DISTINCT stats_date,
   banner,
   sourcename,
   finance_detail,
   adgroup_id,
   adgroup_name,
   funnel_type,
   campaign_name,
   campaign_id,
   funding
  FROM funnel_cost_fact
  WHERE LOWER(platform) = LOWER('adwords')
   AND LOWER(finance_detail) = LOWER('PAID_SHOPPING')
   AND (LOWER(campaign_name) LIKE LOWER('PMAX%') OR LOWER(campaign_name) LIKE LOWER('%- PMax_%'))) AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS adwords_shopping_dad_4box_improvements_pmax
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 t1.finance_detail,
  CASE
  WHEN LOWER(t1.sourcename) = LOWER('Unknown') AND t2.sourcename IS NOT NULL
  THEN t2.sourcename
  ELSE t1.sourcename
  END AS sourcename,
  CASE
  WHEN LOWER(t1.campaign_name) = LOWER('Unknown') AND t2.campaign_name IS NOT NULL
  THEN t2.campaign_name
  ELSE t1.campaign_name
  END AS campaign_name,
  CASE
  WHEN LOWER(t1.campaign_id) = LOWER('Unknown') AND t2.campaign_id IS NOT NULL
  THEN t2.campaign_id
  ELSE t1.campaign_id
  END AS campaign_id,
  CASE
  WHEN LOWER(t1.adgroup_name) = LOWER('Unknown') AND t2.adgroup_name IS NOT NULL
  THEN t2.adgroup_name
  ELSE t1.adgroup_name
  END AS adgroup_name,
  CASE
  WHEN LOWER(t1.adgroup_id) = LOWER('Unknown') AND t2.adgroup_id IS NOT NULL
  THEN t2.adgroup_id
  ELSE t1.adgroup_id
  END AS adgroup_id,
 'Unknown' AS ad_name,
 'Unknown' AS ad_id,
  CASE
  WHEN LOWER(t1.funding) = LOWER('Unknown') AND t2.funding IS NOT NULL
  THEN t2.funding
  ELSE t1.funding
  END AS funding,
 t1.ar_flag,
 t1.country,
  CASE
  WHEN LOWER(t1.funnel_type) = LOWER('Unknown') AND t2.funnel_type IS NOT NULL
  THEN t2.funnel_type
  ELSE t1.funnel_type
  END AS funnel_type,
 t1.nmn_flag,
 t1.device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM adwords_shopping_dad_4box_pmax AS t1
 LEFT JOIN (SELECT *
  FROM adwords_shopping_improvements_pmax
  WHERE ROW_COUNT = 1) AS t2 ON LOWER(t1.campaign_id) = LOWER(t2.campaign_id) AND LOWER(t1.finance_detail) = LOWER(t2
     .finance_detail) AND LOWER(t1.arrived_channel) = LOWER(t2.banner)
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 t1.finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 funding,
 t1.ar_flag,
 t1.country,
 funnel_type,
 t1.nmn_flag,
 t1.device_type,
 ad_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CREATE TEMPORARY TABLE IF NOT EXISTS oath_display_dad_4box AS (
SELECT  a.activity_date_pacific
, a.platform
, a.arrived_channel
, a.order_channel
, a.finance_detail
, a.sourcename
, a.campaign_name
, a.campaign_id
, a.adgroup_name
, a.adgroup_id
, a.ad_name
, a.ad_id
, a.funding
, a.ar_flag
, a.country
, a.funnel_type
, a.nmn_flag
, a.device_type
, SUM(CASE WHEN fcf_row_num = 1 THEN a.COST ELSE NULL END) AS cost
, SUM(CASE WHEN fcf_row_num = 1 THEN a.impressions ELSE NULL END) AS impressions
, SUM(CASE WHEN fcf_row_num = 1 THEN a.clicks ELSE NULL END) AS clicks
, SUM(CASE WHEN fcf_row_num = 1 THEN a.conversions ELSE NULL END) AS conversions
, SUM(CASE WHEN fcf_row_num = 1 THEN a.conversion_value ELSE NULL END) AS conversion_value
, SUM(CASE WHEN fcf_row_num = 1 THEN a.video_views ELSE NULL END) AS video_views
, SUM(CASE WHEN fcf_row_num = 1 THEN a.video100 ELSE NULL END) AS video100
, SUM(CASE WHEN fcf_row_num = 1 THEN a.video75 ELSE NULL END) AS video75

, SUM(a.attributed_demand) AS attributed_demand
, SUM(a.attributed_orders) AS attributed_orders
, SUM(a.attributed_units) AS attributed_units
, SUM(a.attributed_pred_net) AS attributed_pred_net
, SUM(a.sessions) AS sessions
, SUM(a.bounced_sessions) AS bounced_sessions
, SUM(a.session_orders) AS session_orders
FROM (	SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
, COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
, COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
, COALESCE(mta.order_channel,'Unknown') AS order_channel
, CASE WHEN fcf.finance_detail='display'
THEN 'DISPLAY'
WHEN  fcf.finance_detail='video'
OR fcf.video75>0
OR fcf.video100>0
OR fcf.video_views>0
THEN 'VIDEO'
ELSE 'DISPLAY'
END AS finance_detail
, COALESCE(fcf.sourcename,'Unknown') AS sourcename
, COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
, COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
, COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
, COALESCE(fcf.adgroup_id,mta.utm_term,'Unknown') AS adgroup_id
, 'Unknown' AS ad_name
, 'Unknown' AS ad_id
, COALESCE(fcf.funding,'Unknown') AS funding
, COALESCE(fcf.ar_flag,'Others') AS ar_flag
, COALESCE(fcf.country,'US' ) AS country
, COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
, COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
, COALESCE(fcf.device_type,mta.device_type) AS device_type
, fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.adgroup_id AS fcf_adgroupid, mta.order_channel AS mta_order_channel
, ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf.device_type, fcf.adgroup_id order by mta.order_channel ASC) AS fcf_row_num

, SUM(fcf.COST) AS cost
, SUM(fcf.impressions) AS impressions
, SUM(fcf.clicks) AS clicks
, SUM(fcf.conversions) AS conversions
, SUM(fcf.conversion_value) AS conversion_value
, SUM(fcf.video_views) AS video_views
, SUM(fcf.video100) AS video100
, SUM(fcf.video75) AS video75

, SUM(mta.attributed_demand) AS attributed_demand
, SUM(mta.attributed_orders) AS attributed_orders
, SUM(mta.attributed_units) AS attributed_units
, SUM(mta.attributed_pred_net) AS attributed_pred_net
, SUM(mta.sessions) AS sessions
, SUM(mta.bounced_sessions) AS bounced_sessions
, SUM(mta.session_orders) AS session_orders
FROM (SELECT stats_date
, file_name
, 'oath' AS platform
, banner
, sourcename
, finance_detail
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, device_type
, funnel_type
, funding
, ar_flag
, nmn_flag
, country
, SUM(cost) AS cost
, SUM(impressions) AS impressions
, SUM(clicks) AS clicks
, SUM(conversions) AS conversions
, SUM(conversion_value) AS conversion_value
, SUM(video_views) AS video_views
, SUM(video100) AS video100
, SUM(video75) AS video75
FROM funnel_cost_fact
WHERE LOWER(platform) IN (LOWER('oath'), LOWER('verizon'))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) fcf
FULL JOIN (SELECT arrived_channel
, utm_term
, platform
, device_type
, order_channel
, finance_detail
, activity_date_pacific
, SUM(attributed_demand) AS attributed_demand
, SUM(attributed_orders) AS attributed_orders
, SUM(attributed_pred_net) AS attributed_pred_net
, SUM(attributed_units) AS attributed_units
, SUM(session_orders) AS session_orders
, SUM(sessions) AS sessions
, SUM(bounced_sessions) AS bounced_sessions
FROM mkt_utm
WHERE lower(platform) = lower('oath')
GROUP BY 1,2,3,4,5,6,7
) mta
ON fcf.stats_date = mta.activity_date_pacific
AND fcf.banner = mta.arrived_channel
AND fcf.device_type = mta.device_type

AND fcf.adgroup_id = mta.utm_term
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 ) a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) 
;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS bing_search_dad_4box
AS
SELECT t5.activity_date_pacific,
 t5.platform,
 t5.arrived_channel,
 t5.order_channel,
 t5.finance_detail,
 t5.sourcename,
 t5.campaign_name,
 t5.campaign_id,
 t5.adgroup_name,
 t5.adgroup_id,
 t5.ad_name,
 t5.ad_id,
 t5.funding,
 t5.ar_flag,
 t5.country,
 t5.funnel_type,
 t5.nmn_flag,
 t5.device_type,
 t5.stats_date,
 t5.banner,
 t5.device_type AS fcf_device_type,
 t5.adgroup_id AS fcf_adgroupid,
 t5.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t5.stats_date, t5.banner, t5.device_type, t5.adgroup_id ORDER BY t5.order_channel) AS
 fcf_row_num,
 t5.cost_0 AS cost,
 t5.impressions_0 AS impressions,
 t5.clicks_0 AS clicks,
 t5.conversions_0 AS conversions,
 t5.conversion_value_0 AS conversion_value,
 t5.video_views_0 AS video_views,
 t5.video100_0 AS video100,
 t5.video75_0 AS video75,
 t5.attributed_demand_0 AS attributed_demand,
 t5.attributed_orders_0 AS attributed_orders,
 t5.attributed_units_0 AS attributed_units,
 t5.attributed_pred_net_0 AS attributed_pred_net,
 t5.sessions_0 AS sessions,
 t5.bounced_sessions_0 AS bounced_sessions,
 t5.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, t3.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, t3.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, t3.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(t3.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, t3.finance_detail, 'Unknown') AS finance_detail,
   COALESCE(fcf.sourcename, 'Unknown') AS sourcename,
   COALESCE(fcf.campaign_name, 'Unknown') AS campaign_name,
   COALESCE(fcf.campaign_id, 'Unknown') AS campaign_id,
   COALESCE(fcf.adgroup_name, 'Unknown') AS adgroup_name,
   COALESCE(fcf.adgroup_id, t3.utm_content, 'Unknown') AS adgroup_id,
   'Unknown' AS ad_name,
   COALESCE(fcf.funding, 'Unknown') AS funding,
   COALESCE(fcf.ar_flag, 'Others') AS ar_flag,
   COALESCE(fcf.country, 'Unknown') AS country,
   COALESCE(fcf.funnel_type, 'Unknown') AS funnel_type,
   COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
   COALESCE(fcf.device_type, t3.device_type) AS device_type,
   fcf.stats_date,
   fcf.banner,
   'Unknown' AS ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   t3.attributed_demand,
   t3.attributed_orders,
   t3.attributed_units,
   t3.attributed_pred_net,
   t3.sessions,
   t3.bounced_sessions,
   t3.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(t3.attributed_demand) AS attributed_demand_0,
   SUM(t3.attributed_orders) AS attributed_orders_0,
   SUM(t3.attributed_units) AS attributed_units_0,
   SUM(t3.attributed_pred_net) AS attributed_pred_net_0,
   SUM(t3.sessions) AS sessions_0,
   SUM(t3.bounced_sessions) AS bounced_sessions_0,
   SUM(t3.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) = LOWER('bing')
     AND LOWER(finance_detail) IN (LOWER('PAID_SHOPPING'), LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED')
       )
     AND LOWER(sourcename) <> LOWER('%canada%')
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     utm_content,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) = LOWER('bing')
     AND LOWER(finance_detail) IN (LOWER('PAID_SHOPPING'), LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED')
       )
    GROUP BY arrived_channel,
     utm_content,
     order_channel,
     finance_detail,
     platform,
     device_type,
     activity_date_pacific) AS t3 ON fcf.stats_date = t3.activity_date_pacific AND LOWER(fcf.banner) = LOWER(t3.arrived_channel
         ) AND LOWER(fcf.device_type) = LOWER(t3.device_type) AND LOWER(fcf.adgroup_id) = LOWER(t3.utm_content) AND
     LOWER(fcf.finance_detail) = LOWER(t3.finance_detail)
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   sourcename,
   campaign_name,
   campaign_id,
   adgroup_name,
   adgroup_id,
   ad_name,
   funding,
   ar_flag,
   country,
   funnel_type,
   nmn_flag,
   device_type,
   fcf.stats_date,
   fcf.banner,
   ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   t3.attributed_demand,
   t3.attributed_orders,
   t3.attributed_units,
   t3.attributed_pred_net,
   t3.sessions,
   t3.bounced_sessions,
   t3.session_orders) AS t5;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS bing_search_improvements
AS
SELECT banner,
 sourcename,
 finance_detail,
 adgroup_id,
 adgroup_name,
 campaign_name,
 campaign_id,
 funding,
 ar_flag,
 funnel_type,
 nmn_flag,
 country,
 stats_date,
 ROW_NUMBER() OVER (PARTITION BY banner, adgroup_id, campaign_id ORDER BY stats_date DESC) AS row_count
FROM (SELECT DISTINCT banner,
   sourcename,
   finance_detail,
   adgroup_id,
   adgroup_name,
   campaign_name,
   campaign_id,
   funding,
   ar_flag,
   funnel_type,
   nmn_flag,
   country,
   stats_date
  FROM funnel_cost_fact
  WHERE LOWER(platform) = LOWER('bing')
   AND LOWER(finance_detail) IN (LOWER('PAID_SHOPPING'), LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED'))
   AND LOWER(sourcename) <> LOWER('%canada%')) AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS bing_search_dad_4box_improvements
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
  CASE
  WHEN LOWER(t1.finance_detail) = LOWER('Unknown') AND t2.finance_detail IS NOT NULL
  THEN t2.finance_detail
  ELSE t1.finance_detail
  END AS finance_detail,
  CASE
  WHEN LOWER(t1.sourcename) = LOWER('Unknown') AND t2.sourcename IS NOT NULL
  THEN t2.sourcename
  ELSE t1.sourcename
  END AS sourcename,
  CASE
  WHEN LOWER(t1.campaign_name) = LOWER('Unknown') AND t2.campaign_name IS NOT NULL
  THEN t2.campaign_name
  ELSE t1.campaign_name
  END AS campaign_name,
  CASE
  WHEN LOWER(t1.campaign_id) = LOWER('Unknown') AND t2.campaign_id IS NOT NULL
  THEN t2.campaign_id
  ELSE t1.campaign_id
  END AS campaign_id,
  CASE
  WHEN LOWER(t1.adgroup_name) = LOWER('Unknown') AND t2.adgroup_name IS NOT NULL
  THEN t2.adgroup_name
  ELSE t1.adgroup_name
  END AS adgroup_name,
  CASE
  WHEN LOWER(t1.adgroup_id) = LOWER('Unknown') AND t2.adgroup_id IS NOT NULL
  THEN t2.adgroup_id
  ELSE t1.adgroup_id
  END AS adgroup_id,
 'Unknown' AS ad_name,
 'Unknown' AS ad_id,
  CASE
  WHEN LOWER(t1.funding) = LOWER('Unknown') AND t2.funding IS NOT NULL
  THEN t2.funding
  ELSE t1.funding
  END AS funding,
  CASE
  WHEN LOWER(t1.ar_flag) = LOWER('Others') AND t2.ar_flag IS NOT NULL
  THEN t2.ar_flag
  ELSE t1.ar_flag
  END AS ar_flag,
  CASE
  WHEN LOWER(t1.country) = LOWER('Unknown') AND t2.country IS NOT NULL
  THEN t2.country
  ELSE t1.country
  END AS country,
  CASE
  WHEN LOWER(t1.funnel_type) = LOWER('Unknown') AND t2.funnel_type IS NOT NULL
  THEN t2.funnel_type
  ELSE t1.funnel_type
  END AS funnel_type,
 t1.nmn_flag,
 t1.device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM bing_search_dad_4box AS t1
 LEFT JOIN (SELECT *
  FROM bing_search_improvements
  WHERE ROW_COUNT = 1) AS t2 ON LOWER(t1.adgroup_id) = LOWER(t2.adgroup_id) AND LOWER(t1.arrived_channel) = LOWER(t2
    .banner)
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 funding,
 ar_flag,
 country,
 funnel_type,
 t1.nmn_flag,
 t1.device_type,
 ad_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS tiktok_dad_4box
AS
SELECT t6.activity_date_pacific,
 t6.platform,
 t6.arrived_channel,
 t6.order_channel,
 t6.finance_detail,
 t6.sourcename,
 t6.campaign_name,
 t6.campaign_id,
 t6.adgroup_name,
 t6.adgroup_id,
 t6.ad_name,
 t6.ad_id,
 t6.funding,
 t6.ar_flag,
 t6.country,
 t6.funnel_type,
 t6.nmn_flag,
 t6.device_type,
 t6.stats_date,
 t6.banner,
 t6.fcf_device_type,
 t6.ad_name AS fcf_ad_name,
 t6.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.ad_name ORDER BY t6.order_channel) AS
 fcf_row_num,
 t6.cost_0 AS cost,
 t6.impressions_0 AS impressions,
 t6.clicks_0 AS clicks,
 t6.conversions_0 AS conversions,
 t6.conversion_value_0 AS conversion_value,
 t6.video_views_0 AS video_views,
 t6.video100_0 AS video100,
 t6.video75_0 AS video75,
 t6.attributed_demand_0 AS attributed_demand,
 t6.attributed_orders_0 AS attributed_orders,
 t6.attributed_units_0 AS attributed_units,
 t6.attributed_pred_net_0 AS attributed_pred_net,
 t6.sessions_0 AS sessions,
 t6.bounced_sessions_0 AS bounced_sessions,
 t6.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, mta.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, mta.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, mta.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(mta.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, mta.finance_detail, 'Unknown') AS finance_detail,
   fcf.sourcename,
   COALESCE(fcf.campaign_name, mta.utm_campaign, 'Unknown') AS campaign_name,
   fcf.campaign_id,
   fcf.adgroup_name,
   COALESCE(fcf.adgroup_id, mta.utm_term, 'Unknown') AS adgroup_id,
   COALESCE(fcf.ad_name, mta.utm_content, 'Unknown') AS ad_name,
   fcf.ad_id,
   fcf.funding,
   fcf.ar_flag,
   fcf.country,
   fcf.funnel_type,
   COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
   COALESCE(fcf.device_type, mta.device_type) AS device_type,
   fcf.stats_date,
   fcf.banner,
   fcf.device_type AS fcf_device_type,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(mta.attributed_demand) AS attributed_demand_0,
   SUM(mta.attributed_orders) AS attributed_orders_0,
   SUM(mta.attributed_units) AS attributed_units_0,
   SUM(mta.attributed_pred_net) AS attributed_pred_net_0,
   SUM(mta.sessions) AS sessions_0,
   SUM(mta.bounced_sessions) AS bounced_sessions_0,
   SUM(mta.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     'SOCIAL_PAID' AS finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_name,
     ad_id,
      CASE
      WHEN LOWER(banner) = LOWER('N.COM')
      THEN 'Unknown'
      WHEN LOWER(banner) = LOWER('R.COM')
      THEN 'MOBILE'
      ELSE NULL
      END AS device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) = LOWER('tiktok')
     AND LOWER(sourcename) NOT LIKE LOWER('%canada%')
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_name,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     SUBSTR(utm_term, 0, 16) AS utm_term,
     utm_campaign,
      CASE
      WHEN LOWER(utm_content) LIKE LOWER('17%')
      THEN SUBSTR(utm_content, 18)
      ELSE utm_content
      END AS utm_content,
     platform,
      CASE
      WHEN LOWER(arrived_channel) = LOWER('N.COM')
      THEN 'Unknown'
      WHEN LOWER(arrived_channel) = LOWER('R.COM')
      THEN 'MOBILE'
      ELSE NULL
      END AS device_type,
     order_channel,
     'SOCIAL_PAID' AS finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) = LOWER('tiktok')
     AND LOWER(finance_detail) LIKE LOWER('SOCIAL_PAID')
    GROUP BY arrived_channel,
     utm_term,
     utm_campaign,
     utm_content,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific) AS mta ON fcf.stats_date = mta.activity_date_pacific AND LOWER(fcf.banner) = LOWER(mta.arrived_channel
           ) AND LOWER(fcf.device_type) = LOWER(mta.device_type) AND LOWER(fcf.adgroup_id) = LOWER(SUBSTR(mta.utm_term,
          0, 16)) AND LOWER(fcf.campaign_name) = LOWER(mta.utm_campaign) AND LOWER(fcf.ad_name) = LOWER(mta.utm_content
       ) AND LOWER(fcf.finance_detail) = LOWER(mta.finance_detail)
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   fcf.sourcename,
   campaign_name,
   fcf.campaign_id,
   fcf.adgroup_name,
   adgroup_id,
   ad_name,
   fcf.ad_id,
   fcf.funding,
   fcf.ar_flag,
   fcf.country,
   fcf.funnel_type,
   nmn_flag,
   device_type,
   fcf.stats_date,
   fcf.banner,
   fcf_device_type,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders) AS t6;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS tiktok_improvements
AS
SELECT sourcename,
 banner,
 finance_detail,
 ad_id,
 ad_name,
 adgroup_id,
 adgroup_name,
 campaign_name,
 campaign_id,
 funding,
 ar_flag,
 funnel_type,
 nmn_flag,
 country,
 cost,
 ROW_NUMBER() OVER (PARTITION BY banner, ad_name, finance_detail ORDER BY cost DESC) AS row_count
FROM (SELECT sourcename,
   banner,
   finance_detail,
   ad_id,
   ad_name,
   adgroup_id,
   adgroup_name,
   campaign_name,
   campaign_id,
   funding,
   ar_flag,
   funnel_type,
   nmn_flag,
   country,
   SUM(cost) AS cost
  FROM funnel_cost_fact
  WHERE LOWER(platform) = LOWER('tiktok')
   AND LOWER(sourcename) NOT LIKE LOWER('%canada%')
  GROUP BY banner,
   sourcename,
   finance_detail,
   campaign_name,
   campaign_id,
   adgroup_name,
   adgroup_id,
   ad_name,
   ad_id,
   funnel_type,
   funding,
   ar_flag,
   nmn_flag,
   country) AS t1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS tiktok_dad_4box_improvements
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 COALESCE(t1.finance_detail, t2.finance_detail, 'Unknown') AS finance_detail,
 COALESCE(t1.sourcename, t2.sourcename, 'Unknown') AS sourcename,
 COALESCE(t1.campaign_name, t2.campaign_name, 'Unknown') AS campaign_name,
 COALESCE(t1.campaign_id, t2.campaign_id, 'Unknown') AS campaign_id,
 COALESCE(t1.adgroup_name, t2.adgroup_name, 'Unknown') AS adgroup_name,
 COALESCE(t1.adgroup_id, t2.adgroup_id, 'Unknown') AS adgroup_id,
 COALESCE(t1.ad_name, t2.ad_name, 'Unknown') AS ad_name,
 COALESCE(t1.ad_id, t2.ad_id, 'Unknown') AS ad_id,
 COALESCE(t1.funding, t2.funding, 'Unknown') AS funding,
 COALESCE(t1.ar_flag, t2.ar_flag, 'Others') AS ar_flag,
 COALESCE(t1.country, t2.country, 'Unknown') AS country,
 COALESCE(t1.funnel_type, t2.funnel_type, 'Unknown') AS funnel_type,
 t1.nmn_flag,
 t1.device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM tiktok_dad_4box AS t1
 LEFT JOIN (SELECT *
  FROM tiktok_improvements
  WHERE ROW_COUNT = 1) AS t2 ON LOWER(t1.adgroup_id) = LOWER(t2.adgroup_id) AND LOWER(t1.ad_name) = LOWER(t2.ad_name
     ) AND LOWER(t1.arrived_channel) = LOWER(t2.banner)
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
 funding,
 ar_flag,
 country,
 funnel_type,
 t1.nmn_flag,
 t1.device_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS snapchat_dad_4box
AS
SELECT activity_date_pacific,
 platform,
 arrived_channel,
 order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
 funding,
 ar_flag,
 country,
 funnel_type,
 nmn_flag,
 device_type,
 SUM(`A121839`) AS cost,
 SUM(`A121840`) AS impressions,
 SUM(`A121841`) AS clicks,
 SUM(`A121842`) AS conversions,
 SUM(`A121843`) AS conversion_value,
 SUM(`A121844`) AS video_views,
 SUM(`A121845`) AS video100,
 SUM(`A121846`) AS video75,
 SUM(attributed_demand) AS attributed_demand,
 SUM(attributed_orders) AS attributed_orders,
 SUM(attributed_units) AS attributed_units,
 SUM(attributed_pred_net) AS attributed_pred_net,
 SUM(sessions) AS sessions,
 SUM(bounced_sessions) AS bounced_sessions,
 SUM(session_orders) AS session_orders
FROM (SELECT t6.activity_date_pacific,
   t6.platform,
   t6.arrived_channel,
   t6.order_channel,
   t6.finance_detail,
   t6.sourcename,
   t6.campaign_name,
   t6.campaign_id,
   t6.adgroup_name,
   t6.adgroup_id,
   t6.ad_name,
   t6.ad_id,
   t6.funding,
   t6.ar_flag,
   t6.country,
   t6.funnel_type,
   t6.nmn_flag,
   t6.device_type,
   t6.attributed_demand_0 AS attributed_demand,
   t6.attributed_orders_0 AS attributed_orders,
   t6.attributed_units_0 AS attributed_units,
   t6.attributed_pred_net_0 AS attributed_pred_net,
   t6.sessions_0 AS sessions,
   t6.bounced_sessions_0 AS bounced_sessions,
   t6.session_orders_0 AS session_orders,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.cost_0
    ELSE NULL
    END AS `A121839`,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.impressions_0
    ELSE NULL
    END AS `A121840`,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.clicks_0
    ELSE NULL
    END AS `A121841`,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.conversions_0
    ELSE NULL
    END AS `A121842`,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.conversion_value_0
    ELSE NULL
    END AS `A121843`,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.video_views_0
    ELSE NULL
    END AS `A121844`,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.video100_0
    ELSE NULL
    END AS `A121845`,
    CASE
    WHEN (ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel
         )) = 1
    THEN t6.video75_0
    ELSE NULL
    END AS `A121846`
  FROM (SELECT COALESCE(fcf.stats_date, mta.activity_date_pacific) AS activity_date_pacific,
     COALESCE(fcf.platform, mta.platform, 'Unknown') AS platform,
     COALESCE(fcf.banner, mta.arrived_channel, 'Unknown') AS arrived_channel,
     COALESCE(mta.order_channel, 'Unknown') AS order_channel,
     COALESCE(fcf.finance_detail, mta.finance_detail, 'Unknown') AS finance_detail,
     COALESCE(fcf.sourcename, 'Unknown') AS sourcename,
     COALESCE(fcf.campaign_name, 'Unknown') AS campaign_name,
     COALESCE(fcf.campaign_id, 'Unknown') AS campaign_id,
     COALESCE(fcf.adgroup_name, 'Unknown') AS adgroup_name,
     COALESCE(fcf.adgroup_id, mta.utm_term, 'Unknown') AS adgroup_id,
     'Unknown' AS ad_name,
     COALESCE(fcf.funding, 'Unknown') AS funding,
     COALESCE(fcf.ar_flag, 'Others') AS ar_flag,
     COALESCE(fcf.country, 'Unknown') AS country,
     COALESCE(fcf.funnel_type, 'Unknown') AS funnel_type,
     COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
     COALESCE(fcf.device_type, mta.device_type) AS device_type,
     fcf.stats_date,
     fcf.banner,
     fcf.device_type AS fcf_device_type,
     'Unknown' AS ad_id,
     fcf.cost,
     fcf.impressions,
     fcf.clicks,
     fcf.conversions,
     fcf.conversion_value,
     fcf.video_views,
     fcf.video100,
     fcf.video75,
     mta.attributed_demand,
     mta.attributed_orders,
     mta.attributed_units,
     mta.attributed_pred_net,
     mta.sessions,
     mta.bounced_sessions,
     mta.session_orders,
     SUM(fcf.cost) AS cost_0,
     SUM(fcf.impressions) AS impressions_0,
     SUM(fcf.clicks) AS clicks_0,
     SUM(fcf.conversions) AS conversions_0,
     SUM(fcf.conversion_value) AS conversion_value_0,
     SUM(fcf.video_views) AS video_views_0,
     SUM(fcf.video100) AS video100_0,
     SUM(fcf.video75) AS video75_0,
     SUM(mta.attributed_demand) AS attributed_demand_0,
     SUM(mta.attributed_orders) AS attributed_orders_0,
     SUM(mta.attributed_units) AS attributed_units_0,
     SUM(mta.attributed_pred_net) AS attributed_pred_net_0,
     SUM(mta.sessions) AS sessions_0,
     SUM(mta.bounced_sessions) AS bounced_sessions_0,
     SUM(mta.session_orders) AS session_orders_0
    FROM (SELECT stats_date,
       file_name,
       platform,
       banner,
       sourcename,
       'SOCIAL_PAID' AS finance_detail,
       campaign_name,
       campaign_id,
       adgroup_name,
       adgroup_id,
       'MOBILE' AS device_type,
       funnel_type,
       funding,
       ar_flag,
       nmn_flag,
       country,
       SUM(cost) AS cost,
       SUM(impressions) AS impressions,
       SUM(clicks) AS clicks,
       SUM(conversions) AS conversions,
       SUM(conversion_value) AS conversion_value,
       SUM(video_views) AS video_views,
       SUM(video100) AS video100,
       SUM(video75) AS video75
      FROM funnel_cost_fact
      WHERE LOWER(platform) = LOWER('snapchat')
       AND LOWER(sourcename) NOT LIKE LOWER('%canada%')
      GROUP BY stats_date,
       file_name,
       platform,
       banner,
       sourcename,
       finance_detail,
       campaign_name,
       campaign_id,
       adgroup_name,
       adgroup_id,
       device_type,
       funnel_type,
       funding,
       ar_flag,
       nmn_flag,
       country) AS fcf
     FULL JOIN (SELECT arrived_channel,
       SUBSTR(utm_term, 0, 36) AS utm_term,
       platform,
       'MOBILE' AS device_type,
       order_channel,
       'SOCIAL_PAID' AS finance_detail,
       activity_date_pacific,
       SUM(attributed_demand) AS attributed_demand,
       SUM(attributed_orders) AS attributed_orders,
       SUM(attributed_pred_net) AS attributed_pred_net,
       SUM(attributed_units) AS attributed_units,
       SUM(session_orders) AS session_orders,
       SUM(sessions) AS sessions,
       SUM(bounced_sessions) AS bounced_sessions
      FROM mkt_utm
      WHERE LOWER(platform) = LOWER('snapchat')
      GROUP BY arrived_channel,
       utm_term,
       platform,
       device_type,
       order_channel,
       finance_detail,
       activity_date_pacific) AS mta ON fcf.stats_date = mta.activity_date_pacific AND LOWER(fcf.banner) = LOWER(mta.arrived_channel
           ) AND LOWER(fcf.device_type) = LOWER(mta.device_type) AND LOWER(fcf.adgroup_id) = LOWER(mta.utm_term) AND
       LOWER(fcf.finance_detail) = LOWER(mta.finance_detail)
    GROUP BY activity_date_pacific,
     platform,
     arrived_channel,
     order_channel,
     finance_detail,
     sourcename,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_name,
     funding,
     ar_flag,
     country,
     funnel_type,
     nmn_flag,
     device_type,
     fcf.stats_date,
     fcf.banner,
     fcf_device_type,
     ad_id,
     fcf.cost,
     fcf.impressions,
     fcf.clicks,
     fcf.conversions,
     fcf.conversion_value,
     fcf.video_views,
     fcf.video100,
     fcf.video75,
     mta.attributed_demand,
     mta.attributed_orders,
     mta.attributed_units,
     mta.attributed_pred_net,
     mta.sessions,
     mta.bounced_sessions,
     mta.session_orders) AS t6) AS t7
GROUP BY activity_date_pacific,
 platform,
 arrived_channel,
 order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
 funding,
 ar_flag,
 country,
 funnel_type,
 nmn_flag,
 device_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pinterest_dad_4box
AS
SELECT t6.activity_date_pacific,
 t6.platform,
 t6.arrived_channel,
 t6.order_channel,
 t6.finance_detail,
 t6.sourcename,
 t6.campaign_name,
 t6.campaign_id,
 t6.adgroup_name,
 t6.adgroup_id,
 t6.ad_name,
 t6.ad_id,
 t6.funding,
 t6.ar_flag,
 t6.country,
 t6.funnel_type,
 t6.nmn_flag,
 t6.device_type,
 t6.stats_date,
 t6.banner,
 t6.fcf_device_type,
 t6.adgroup_id AS fcf_adgroupid,
 t6.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.fcf_device_type, t6.adgroup_id ORDER BY t6.order_channel)
 AS fcf_row_num,
 t6.cost_0 AS cost,
 t6.impressions_0 AS impressions,
 t6.clicks_0 AS clicks,
 t6.conversions_0 AS conversions,
 t6.conversion_value_0 AS conversion_value,
 t6.video_views_0 AS video_views,
 t6.video100_0 AS video100,
 t6.video75_0 AS video75,
 t6.attributed_demand_0 AS attributed_demand,
 t6.attributed_orders_0 AS attributed_orders,
 t6.attributed_units_0 AS attributed_units,
 t6.attributed_pred_net_0 AS attributed_pred_net,
 t6.sessions_0 AS sessions,
 t6.bounced_sessions_0 AS bounced_sessions,
 t6.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, mta.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, mta.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, mta.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(mta.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, mta.finance_detail, 'Unknown') AS finance_detail,
   COALESCE(fcf.sourcename, 'Unknown') AS sourcename,
   COALESCE(fcf.campaign_name, 'Unknown') AS campaign_name,
   COALESCE(fcf.campaign_id, 'Unknown') AS campaign_id,
   COALESCE(fcf.adgroup_name, 'Unknown') AS adgroup_name,
   COALESCE(fcf.adgroup_id, mta.utm_term, 'Unknown') AS adgroup_id,
   COALESCE(fcf.adgroup_id, 'Unknown') AS ad_name,
   COALESCE(fcf.ad_id, mta.utm_content, 'Unknown') AS ad_id,
   COALESCE(fcf.funding, 'Unknown') AS funding,
   COALESCE(fcf.ar_flag, 'Others') AS ar_flag,
   COALESCE(fcf.country, 'Unknown') AS country,
   COALESCE(fcf.funnel_type, 'Unknown') AS funnel_type,
   COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
   COALESCE(fcf.device_type, mta.device_type) AS device_type,
   fcf.stats_date,
   fcf.banner,
   fcf.device_type AS fcf_device_type,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(mta.attributed_demand) AS attributed_demand_0,
   SUM(mta.attributed_orders) AS attributed_orders_0,
   SUM(mta.attributed_units) AS attributed_units_0,
   SUM(mta.attributed_pred_net) AS attributed_pred_net_0,
   SUM(mta.sessions) AS sessions_0,
   SUM(mta.bounced_sessions) AS bounced_sessions_0,
   SUM(mta.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     'SOCIAL_PAID' AS finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_name,
     ad_id,
     'Unknown' AS device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) = LOWER('pinterest')
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_name,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     SUBSTR(utm_term, 0, 13) AS utm_term,
     SUBSTR(utm_campaign, 0, 13) AS utm_campaign,
     SUBSTR(utm_content, 0, 13) AS utm_content,
     platform,
     'Unknown' AS device_type,
     order_channel,
     'SOCIAL_PAID' AS finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) = LOWER('pinterest')
    GROUP BY arrived_channel,
     utm_term,
     utm_campaign,
     utm_content,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific) AS mta ON fcf.stats_date = mta.activity_date_pacific AND LOWER(fcf.banner) = LOWER(mta.arrived_channel
         ) AND LOWER(fcf.device_type) = LOWER(mta.device_type) AND LOWER(fcf.adgroup_id) = LOWER(mta.utm_term) AND LOWER(fcf
      .finance_detail) = LOWER(mta.finance_detail)
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   sourcename,
   campaign_name,
   campaign_id,
   adgroup_name,
   adgroup_id,
   ad_name,
   ad_id,
   funding,
   ar_flag,
   country,
   funnel_type,
   nmn_flag,
   device_type,
   fcf.stats_date,
   fcf.banner,
   fcf_device_type,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders) AS t6;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pinterest_improvements
AS
SELECT DISTINCT banner,
 sourcename,
 'SOCIAL_PAID' AS finance_detail,
 adgroup_id,
 adgroup_name,
 campaign_name,
 campaign_id,
 ad_name,
 ad_id,
 funding,
 ar_flag,
 nmn_flag,
 funnel_type,
 country
FROM funnel_cost_fact
WHERE LOWER(platform) = LOWER('pinterest');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pinterest_dad_4box_improvements
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
  CASE
  WHEN LOWER(t1.finance_detail) = LOWER('Unknown') AND pinterest_improvements.finance_detail IS NOT NULL
  THEN pinterest_improvements.finance_detail
  ELSE t1.finance_detail
  END AS finance_detail,
  CASE
  WHEN LOWER(t1.sourcename) = LOWER('Unknown') AND pinterest_improvements.sourcename IS NOT NULL
  THEN pinterest_improvements.sourcename
  ELSE t1.sourcename
  END AS sourcename,
  CASE
  WHEN LOWER(t1.campaign_name) = LOWER('Unknown') AND pinterest_improvements.campaign_name IS NOT NULL
  THEN pinterest_improvements.campaign_name
  ELSE t1.campaign_name
  END AS campaign_name,
  CASE
  WHEN LOWER(t1.campaign_id) = LOWER('Unknown') AND pinterest_improvements.campaign_id IS NOT NULL
  THEN pinterest_improvements.campaign_id
  ELSE t1.campaign_id
  END AS campaign_id,
  CASE
  WHEN LOWER(t1.adgroup_name) = LOWER('Unknown') AND pinterest_improvements.adgroup_name IS NOT NULL
  THEN pinterest_improvements.adgroup_name
  ELSE t1.adgroup_name
  END AS adgroup_name,
  CASE
  WHEN LOWER(t1.adgroup_id) = LOWER('Unknown') AND pinterest_improvements.adgroup_id IS NOT NULL
  THEN pinterest_improvements.adgroup_id
  ELSE t1.adgroup_id
  END AS adgroup_id,
  CASE
  WHEN LOWER(t1.ad_name) = LOWER('Unknown') AND pinterest_improvements.ad_name IS NOT NULL
  THEN pinterest_improvements.ad_name
  ELSE t1.ad_name
  END AS ad_name,
  CASE
  WHEN LOWER(t1.ad_id) = LOWER('Unknown') AND pinterest_improvements.ad_id IS NOT NULL
  THEN pinterest_improvements.ad_id
  ELSE t1.ad_id
  END AS ad_id,
  CASE
  WHEN LOWER(t1.funding) = LOWER('Unknown') AND pinterest_improvements.funding IS NOT NULL
  THEN pinterest_improvements.funding
  ELSE t1.funding
  END AS funding,
  CASE
  WHEN LOWER(t1.ar_flag) = LOWER('Others') AND pinterest_improvements.ar_flag IS NOT NULL
  THEN pinterest_improvements.ar_flag
  ELSE t1.ar_flag
  END AS ar_flag,
  CASE
  WHEN LOWER(t1.country) = LOWER('Unknown') AND pinterest_improvements.country IS NOT NULL
  THEN pinterest_improvements.country
  ELSE t1.country
  END AS country,
  CASE
  WHEN LOWER(t1.funnel_type) = LOWER('Unknown') AND pinterest_improvements.funnel_type IS NOT NULL
  THEN pinterest_improvements.funnel_type
  ELSE t1.funnel_type
  END AS funnel_type,
 t1.nmn_flag,
 t1.device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM pinterest_dad_4box AS t1
 LEFT JOIN pinterest_improvements ON LOWER(t1.adgroup_id) = LOWER(pinterest_improvements.adgroup_id) AND LOWER(t1.arrived_channel
    ) = LOWER(pinterest_improvements.banner)
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
 funding,
 ar_flag,
 country,
 funnel_type,
 t1.nmn_flag,
 t1.device_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS facebook_search_dad_4box
AS
SELECT t10.activity_date_pacific,
 t10.platform,
 t10.arrived_channel,
 t10.order_channel,
 t10.finance_detail,
 t10.sourcename,
 t10.campaign_name,
 t10.campaign_id,
 t10.adgroup_name,
 t10.adgroup_id,
 t10.ad_name,
 t10.ad_id,
 t10.funding,
 t10.ar_flag,
 t10.country,
 t10.funnel_type,
 t10.nmn_flag,
 t10.device_type,
 t10.stats_date,
 t10.banner,
 t10.fcf_device_type,
 t10.fcf_ad_id,
 t10.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t10.stats_date, t10.banner, t10.fcf_device_type, t10.fcf_ad_id ORDER BY t10.order_channel
    ) AS fcf_row_num,
 t10.cost_0 AS cost,
 t10.impressions_0 AS impressions,
 t10.clicks_0 AS clicks,
 t10.conversions_0 AS conversions,
 t10.conversion_value_0 AS conversion_value,
 t10.video_views_0 AS video_views,
 t10.video100_0 AS video100,
 t10.video75_0 AS video75,
 t10.attributed_demand_0 AS attributed_demand,
 t10.attributed_orders_0 AS attributed_orders,
 t10.attributed_units_0 AS attributed_units,
 t10.attributed_pred_net_0 AS attributed_pred_net,
 t10.sessions_0 AS sessions,
 t10.bounced_sessions_0 AS bounced_sessions,
 t10.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, mta.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, mta.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, mta.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(mta.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, mta.finance_detail) AS finance_detail,
   fcf.sourcename,
   fcf.campaign_name,
   fcf.campaign_id,
   fcf.adgroup_name,
   fcf.adgroup_id,
   fcf.ad_name,
   COALESCE(fcf.ad_id, mta.utm_content) AS ad_id,
   COALESCE(fcf.funding, CASE
     WHEN LOWER(mta.utm_term) LIKE LOWER('%@_ND@_%')
     THEN 'ND'
     WHEN LOWER(mta.utm_term) LIKE LOWER('%@_EX@_%') OR LOWER(mta.utm_term) LIKE LOWER('%icf%')
     THEN 'EX'
     WHEN LOWER(mta.utm_term) LIKE LOWER('%@_P@_%') OR LOWER(mta.utm_term) LIKE LOWER('%dpa%') OR LOWER(mta.utm_term)
       LIKE LOWER('%daba%')
     THEN 'Persistent'
     WHEN LOWER(mta.utm_term) LIKE LOWER('%@_VF@_%')
     THEN 'Coop'
     ELSE NULL
     END) AS funding,
   COALESCE(fcf.ar_flag, CASE
     WHEN LOWER(mta.utm_term) LIKE LOWER('%@_Brand@_%')
     THEN 'Brand'
     WHEN LOWER(mta.utm_term) LIKE LOWER('%@_acq@_%') OR LOWER(mta.utm_term) LIKE LOWER('%dpa%acq%')
     THEN 'Acquisition'
     WHEN LOWER(mta.utm_term) LIKE LOWER('%@_ret@_%')
     THEN 'Retention'
     ELSE 'Others'
     END) AS ar_flag,
   fcf.country,
   fcf.funnel_type,
   COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
   COALESCE(fcf.device_type, mta.device_type) AS device_type,
   fcf.stats_date,
   fcf.banner,
   fcf.device_type AS fcf_device_type,
   fcf.ad_id AS fcf_ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(mta.attributed_demand) AS attributed_demand_0,
   SUM(mta.attributed_orders) AS attributed_orders_0,
   SUM(mta.attributed_units) AS attributed_units_0,
   SUM(mta.attributed_pred_net) AS attributed_pred_net_0,
   SUM(mta.sessions) AS sessions_0,
   SUM(mta.bounced_sessions) AS bounced_sessions_0,
   SUM(mta.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     'facebook' AS platform,
     banner,
     sourcename,
     NULL AS finance_detail,
     campaign_name,
     campaign_id,
     REPLACE(adgroup_name, ' ', '') AS adgroup_name,
     adgroup_id,
     ad_name,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
      CASE
      WHEN LOWER(banner) = LOWER('R.com')
      THEN 'US'
      ELSE country
      END AS country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) = LOWER('facegram')
     AND ad_id NOT IN (SELECT ad_id
       FROM funnel_cost_fact
       WHERE LOWER(platform) = LOWER('facegram')
       GROUP BY ad_id
       HAVING SUM(cost) <= 0 AND SUM(impressions) > 0)
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_name,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     SUBSTR(utm_content, 0, 13) AS utm_content,
     SUBSTR(utm_term, 0, 13) AS utm_term,
     platform,
     device_type,
     order_channel,
     NULL AS finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) = LOWER('facebook')
     AND utm_term IS NOT NULL
    GROUP BY arrived_channel,
     utm_content,
     utm_term,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific) AS mta ON fcf.stats_date = mta.activity_date_pacific AND LOWER(fcf.banner) = LOWER(mta.arrived_channel
        ) AND LOWER(fcf.device_type) = LOWER(mta.device_type) AND LOWER(SUBSTR(fcf.ad_id, 0, 13)) = LOWER(SUBSTR(mta.utm_content
       , 0, 13))
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   fcf.sourcename,
   fcf.campaign_name,
   fcf.campaign_id,
   fcf.adgroup_name,
   fcf.adgroup_id,
   fcf.ad_name,
   ad_id,
   funding,
   ar_flag,
   fcf.country,
   fcf.funnel_type,
   nmn_flag,
   device_type,
   fcf.stats_date,
   fcf.banner,
   fcf_device_type,
   fcf_ad_id,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   mta.attributed_demand,
   mta.attributed_orders,
   mta.attributed_units,
   mta.attributed_pred_net,
   mta.sessions,
   mta.bounced_sessions,
   mta.session_orders) AS t10;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS facebook_improvements
AS
SELECT finance_detail,
 banner,
 sourcename,
 adgroup_id,
 adgroup_name,
 ad_id,
 ad_name,
 campaign_id,
 campaign_name,
 funnel_type,
 funding,
 ar_flag,
 nmn_flag,
 country,
 cost,
 ROW_NUMBER() OVER (PARTITION BY banner, campaign_id, ad_id ORDER BY cost DESC) AS row_num
FROM (SELECT 'SOCIAL_PAID' AS finance_detail,
   banner,
   sourcename,
   adgroup_id,
   adgroup_name,
   ad_id,
   ad_name,
   campaign_id,
   campaign_name,
   funnel_type,
   funding,
   ar_flag,
   nmn_flag,
   country,
   SUM(cost) AS cost
  FROM funnel_cost_fact
  WHERE LOWER(platform) = LOWER('facegram')
   AND ad_id NOT IN (SELECT ad_id
     FROM funnel_cost_fact
     WHERE LOWER(platform) = LOWER('facegram')
     GROUP BY ad_id
     HAVING SUM(cost) <= 0 AND SUM(impressions) > 0)
  GROUP BY finance_detail,
   banner,
   sourcename,
   adgroup_id,
   adgroup_name,
   ad_id,
   ad_name,
   campaign_id,
   campaign_name,
   funnel_type,
   funding,
   ar_flag,
   nmn_flag,
   country) AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS facebook_dad_4box_improvements
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 COALESCE(cast(t1.finance_detail as string), 'SOCIAL_PAID') AS finance_detail,
 COALESCE(t1.sourcename, t2.sourcename, 'Unknown') AS sourcename,
 COALESCE(t1.campaign_name, t2.campaign_name, 'Unknown') AS campaign_name,
 COALESCE(t1.campaign_id, t2.campaign_id, 'Unknown') AS campaign_id,
 COALESCE(t1.adgroup_name, t2.adgroup_name, 'Unknown') AS adgroup_name,
 COALESCE(t1.adgroup_id, t2.adgroup_id, 'Unknown') AS adgroup_id,
 COALESCE(t1.ad_name, t2.ad_name, 'Unknown') AS ad_name,
 COALESCE(t1.ad_id, t2.ad_id, 'Unknown') AS ad_id,
 COALESCE(t1.funding, t2.funding, 'Unknown') AS funding,
 COALESCE(t1.ar_flag, t2.ar_flag, 'Others') AS ar_flag,
 COALESCE(t1.country, t2.country, 'Unknown') AS country,
 COALESCE(t1.funnel_type, t2.funnel_type, 'Unknown') AS funnel_type,
 t1.nmn_flag,
 t1.device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM facebook_search_dad_4box AS t1
 LEFT JOIN (SELECT *
  FROM facebook_improvements
  WHERE row_num = 1) AS t2 ON LOWER(t1.ad_id) = LOWER(t2.ad_id) AND LOWER(t1.arrived_channel) = LOWER(t2.banner)
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
 funding,
 ar_flag,
 country,
 funnel_type,
 t1.nmn_flag,
 t1.device_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS gemini_dad_4box
AS
SELECT t5.activity_date_pacific,
 t5.platform,
 t5.arrived_channel,
 t5.order_channel,
 t5.finance_detail,
 t5.sourcename,
 t5.campaign_name,
 t5.campaign_id,
 t5.adgroup_name,
 t5.adgroup_id,
 t5.ad_name,
 t5.ad_id,
 t5.funding,
 t5.ar_flag,
 t5.country,
 t5.funnel_type,
 t5.nmn_flag,
 t5.device_type,
 t5.stats_date,
 t5.banner,
 t5.fcf_device_type,
 t5.adgroup_id AS fcf_adgroupid,
 t5.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t5.stats_date, t5.banner, t5.fcf_device_type, t5.adgroup_id ORDER BY t5.order_channel)
 AS fcf_row_num,
 t5.cost_0 AS cost,
 t5.impressions_0 AS impressions,
 t5.clicks_0 AS clicks,
 t5.conversions_0 AS conversions,
 t5.conversion_value_0 AS conversion_value,
 t5.video_views_0 AS video_views,
 t5.video100_0 AS video100,
 t5.video75_0 AS video75,
 t5.attributed_demand_0 AS attributed_demand,
 t5.attributed_orders_0 AS attributed_orders,
 t5.attributed_units_0 AS attributed_units,
 t5.attributed_pred_net_0 AS attributed_pred_net,
 t5.sessions_0 AS sessions,
 t5.bounced_sessions_0 AS bounced_sessions,
 t5.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, t3.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, t3.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, t3.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(t3.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, t3.finance_detail) AS finance_detail,
   COALESCE(fcf.sourcename, 'Unknown') AS sourcename,
   COALESCE(fcf.campaign_name, 'Unknown') AS campaign_name,
   COALESCE(fcf.campaign_id, 'Unknown') AS campaign_id,
   COALESCE(fcf.adgroup_name, 'Unknown') AS adgroup_name,
   COALESCE(fcf.adgroup_id, t3.utm_term, 'Unknown') AS adgroup_id,
   'Unknown' AS ad_name,
   COALESCE(fcf.ad_id, 'Unknown') AS ad_id,
   COALESCE(fcf.funding, CASE
     WHEN LOWER(t3.utm_term) LIKE LOWER('%@_ND@_%')
     THEN 'ND'
     WHEN LOWER(t3.utm_term) LIKE LOWER('%@_EX@_%') OR LOWER(t3.utm_term) LIKE LOWER('%icf%')
     THEN 'EX'
     WHEN LOWER(t3.utm_term) LIKE LOWER('%@_P@_%') OR LOWER(t3.utm_term) LIKE LOWER('%dpa%') OR LOWER(t3.utm_term) LIKE
       LOWER('%daba%')
     THEN 'Persistent'
     WHEN LOWER(t3.utm_term) LIKE LOWER('%@_VF@_%')
     THEN 'Coop'
     ELSE NULL
     END) AS funding,
   COALESCE(fcf.ar_flag, CASE
     WHEN LOWER(t3.utm_term) LIKE LOWER('%@_Brand@_%')
     THEN 'Brand'
     WHEN LOWER(t3.utm_term) LIKE LOWER('%@_acq@_%') OR LOWER(t3.utm_term) LIKE LOWER('%dpa%acq%')
     THEN 'Acquisition'
     WHEN LOWER(t3.utm_term) LIKE LOWER('%@_ret@_%')
     THEN 'Retention'
     ELSE 'Others'
     END) AS ar_flag,
   COALESCE(fcf.country, 'US') AS country,
   COALESCE(fcf.funnel_type, 'Unknown') AS funnel_type,
   COALESCE(fcf.nmn_flag, 'Unknown') AS nmn_flag,
   COALESCE(fcf.device_type, t3.device_type) AS device_type,
   fcf.stats_date,
   fcf.banner,
   fcf.device_type AS fcf_device_type,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   t3.attributed_demand,
   t3.attributed_orders,
   t3.attributed_units,
   t3.attributed_pred_net,
   t3.sessions,
   t3.bounced_sessions,
   t3.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(t3.attributed_demand) AS attributed_demand_0,
   SUM(t3.attributed_orders) AS attributed_orders_0,
   SUM(t3.attributed_units) AS attributed_units_0,
   SUM(t3.attributed_pred_net) AS attributed_pred_net_0,
   SUM(t3.sessions) AS sessions_0,
   SUM(t3.bounced_sessions) AS bounced_sessions_0,
   SUM(t3.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) IN (LOWER('yahoo'), LOWER('gemini'))
     AND LOWER(finance_detail) = LOWER('DISPLAY')
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     utm_content,
     utm_term,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) IN (LOWER('gemini'))
     AND LOWER(finance_detail) = LOWER('DISPLAY')
    GROUP BY arrived_channel,
     utm_term,
     utm_content,
     order_channel,
     finance_detail,
     platform,
     device_type,
     activity_date_pacific) AS t3 ON fcf.stats_date = t3.activity_date_pacific AND LOWER(fcf.banner) = LOWER(t3.arrived_channel
        ) AND LOWER(fcf.device_type) = LOWER(t3.device_type) AND LOWER(fcf.adgroup_id) = LOWER(t3.utm_term)
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   sourcename,
   campaign_name,
   campaign_id,
   adgroup_name,
   adgroup_id,
   ad_name,
   ad_id,
   funding,
   ar_flag,
   country,
   funnel_type,
   nmn_flag,
   device_type,
   fcf.stats_date,
   fcf.banner,
   fcf_device_type,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   t3.attributed_demand,
   t3.attributed_orders,
   t3.attributed_units,
   t3.attributed_pred_net,
   t3.sessions,
   t3.bounced_sessions,
   t3.session_orders) AS t5;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS gemini_improvements
AS
SELECT stats_date,
 banner,
 sourcename,
 campaign_id,
 campaign_name,
 adgroup_id,
 adgroup_name,
 funnel_type,
 ROW_NUMBER() OVER (PARTITION BY banner, adgroup_id ORDER BY stats_date DESC) AS row_num
FROM (SELECT DISTINCT stats_date,
   banner,
   sourcename,
   campaign_id,
   campaign_name,
   adgroup_id,
   adgroup_name,
   funnel_type
  FROM funnel_cost_fact
  WHERE LOWER(platform) IN (LOWER('yahoo'), LOWER('gemini'))) AS a;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS gemini_dad_4box_improvements
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 COALESCE(t1.finance_detail, 'DISPLAY') AS finance_detail,
  CASE
  WHEN LOWER(t1.sourcename) = LOWER('Unknown') AND t2.sourcename IS NOT NULL
  THEN t2.sourcename
  ELSE t1.sourcename
  END AS sourcename,
  CASE
  WHEN LOWER(t1.campaign_name) = LOWER('Unknown') AND t2.campaign_name IS NOT NULL
  THEN t2.campaign_name
  ELSE t1.campaign_name
  END AS campaign_name,
  CASE
  WHEN LOWER(t1.campaign_id) = LOWER('Unknown') AND t2.campaign_id IS NOT NULL
  THEN t2.campaign_id
  ELSE t1.campaign_id
  END AS campaign_id,
  CASE
  WHEN LOWER(t1.adgroup_name) = LOWER('Unknown') AND t2.adgroup_name IS NOT NULL
  THEN t2.adgroup_name
  ELSE t1.adgroup_name
  END AS adgroup_name,
  CASE
  WHEN LOWER(t1.adgroup_id) = LOWER('Unknown') AND t2.adgroup_id IS NOT NULL
  THEN t2.adgroup_id
  ELSE t1.adgroup_id
  END AS adgroup_id,
 t1.ad_name,
 t1.ad_id,
 t1.funding,
 t1.ar_flag,
 t1.country,
  CASE
  WHEN LOWER(t1.funnel_type) = LOWER('Unknown') AND t2.funnel_type IS NOT NULL
  THEN t2.funnel_type
  ELSE t1.funnel_type
  END AS funnel_type,
 t1.nmn_flag,
 t1.device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM gemini_dad_4box AS t1
 LEFT JOIN (SELECT *
  FROM gemini_improvements
  WHERE row_num = 1) AS t2 ON LOWER(t1.adgroup_id) = LOWER(t2.adgroup_id) AND LOWER(t1.arrived_channel) = LOWER(t2.banner
    )
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 adgroup_id,
 t1.ad_name,
 t1.ad_id,
 t1.funding,
 t1.ar_flag,
 t1.country,
 funnel_type,
 t1.nmn_flag,
 t1.device_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS gemini_search_dad_4box
AS
SELECT t6.activity_date_pacific,
 t6.platform,
 t6.arrived_channel,
 t6.order_channel,
 t6.finance_detail,
 t6.sourcename,
 t6.campaign_name,
 t6.campaign_id,
 t6.adgroup_name,
 t6.adgroup_id,
 t6.ad_name,
 t6.ad_id,
 t6.funding,
 t6.ar_flag,
 t6.country,
 t6.funnel_type,
 t6.device_type,
 t6.nmn_flag,
 t6.stats_date,
 t6.banner,
 t6.device_type AS fcf_device_type,
 t6.adgroup_id AS fcf_adgroupid,
 t6.order_channel AS mta_order_channel,
 ROW_NUMBER() OVER (PARTITION BY t6.stats_date, t6.banner, t6.device_type, t6.adgroup_id ORDER BY t6.order_channel) AS
 fcf_row_num,
 t6.cost_0 AS cost,
 t6.impressions_0 AS impressions,
 t6.clicks_0 AS clicks,
 t6.conversions_0 AS conversions,
 t6.conversion_value_0 AS conversion_value,
 t6.video_views_0 AS video_views,
 t6.video100_0 AS video100,
 t6.video75_0 AS video75,
 t6.attributed_demand_0 AS attributed_demand,
 t6.attributed_orders_0 AS attributed_orders,
 t6.attributed_units_0 AS attributed_units,
 t6.attributed_pred_net_0 AS attributed_pred_net,
 t6.sessions_0 AS sessions,
 t6.bounced_sessions_0 AS bounced_sessions,
 t6.session_orders_0 AS session_orders
FROM (SELECT COALESCE(fcf.stats_date, t4.activity_date_pacific) AS activity_date_pacific,
   COALESCE(fcf.platform, t4.platform, 'Unknown') AS platform,
   COALESCE(fcf.banner, t4.arrived_channel, 'Unknown') AS arrived_channel,
   COALESCE(t4.order_channel, 'Unknown') AS order_channel,
   COALESCE(fcf.finance_detail, t4.finance_detail) AS finance_detail,
   COALESCE(fcf.sourcename, 'Unknown') AS sourcename,
   COALESCE(fcf.campaign_name, 'Unknown') AS campaign_name,
   COALESCE(fcf.campaign_id, 'Unknown') AS campaign_id,
   COALESCE(fcf.adgroup_name, 'Unknown') AS adgroup_name,
   COALESCE(fcf.adgroup_id, t4.utm_content, 'Unknown') AS adgroup_id,
   'Unknown' AS ad_name,
   COALESCE(fcf.ad_id, 'Unknown') AS ad_id,
   COALESCE(fcf.funding, CASE
     WHEN LOWER(t4.utm_term) LIKE LOWER('%@_ND@_%')
     THEN 'ND'
     WHEN LOWER(t4.utm_term) LIKE LOWER('%@_EX@_%') OR LOWER(t4.utm_term) LIKE LOWER('%icf%')
     THEN 'EX'
     WHEN LOWER(t4.utm_term) LIKE LOWER('%@_P@_%') OR LOWER(t4.utm_term) LIKE LOWER('%dpa%') OR LOWER(t4.utm_term) LIKE
       LOWER('%daba%')
     THEN 'Persistent'
     WHEN LOWER(t4.utm_term) LIKE LOWER('%@_VF@_%')
     THEN 'Coop'
     ELSE NULL
     END) AS funding,
   COALESCE(fcf.ar_flag, CASE
     WHEN LOWER(t4.utm_term) LIKE LOWER('%@_Brand@_%')
     THEN 'Brand'
     WHEN LOWER(t4.utm_term) LIKE LOWER('%@_acq@_%') OR LOWER(t4.utm_term) LIKE LOWER('%dpa%acq%')
     THEN 'Acquisition'
     WHEN LOWER(t4.utm_term) LIKE LOWER('%@_ret@_%')
     THEN 'Retention'
     ELSE 'Others'
     END) AS ar_flag,
   COALESCE(fcf.country, 'US') AS country,
   COALESCE(fcf.funnel_type, 'Unknown') AS funnel_type,
   COALESCE(fcf.device_type, t4.device_type) AS device_type,
   COALESCE(fcf.nmn_flag, 'No') AS nmn_flag,
   fcf.stats_date,
   fcf.banner,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   t4.attributed_demand,
   t4.attributed_orders,
   t4.attributed_units,
   t4.attributed_pred_net,
   t4.sessions,
   t4.bounced_sessions,
   t4.session_orders,
   SUM(fcf.cost) AS cost_0,
   SUM(fcf.impressions) AS impressions_0,
   SUM(fcf.clicks) AS clicks_0,
   SUM(fcf.conversions) AS conversions_0,
   SUM(fcf.conversion_value) AS conversion_value_0,
   SUM(fcf.video_views) AS video_views_0,
   SUM(fcf.video100) AS video100_0,
   SUM(fcf.video75) AS video75_0,
   SUM(t4.attributed_demand) AS attributed_demand_0,
   SUM(t4.attributed_orders) AS attributed_orders_0,
   SUM(t4.attributed_units) AS attributed_units_0,
   SUM(t4.attributed_pred_net) AS attributed_pred_net_0,
   SUM(t4.sessions) AS sessions_0,
   SUM(t4.bounced_sessions) AS bounced_sessions_0,
   SUM(t4.session_orders) AS session_orders_0
  FROM (SELECT stats_date,
     file_name,
     'gemini' AS platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country,
     SUM(cost) AS cost,
     SUM(impressions) AS impressions,
     SUM(clicks) AS clicks,
     SUM(conversions) AS conversions,
     SUM(conversion_value) AS conversion_value,
     SUM(video_views) AS video_views,
     SUM(video100) AS video100,
     SUM(video75) AS video75
    FROM funnel_cost_fact
    WHERE LOWER(platform) IN (LOWER('yahoo'), LOWER('gemini'))
     AND LOWER(finance_detail) IN (LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED'))
    GROUP BY stats_date,
     file_name,
     platform,
     banner,
     sourcename,
     finance_detail,
     campaign_name,
     campaign_id,
     adgroup_name,
     adgroup_id,
     ad_id,
     device_type,
     funnel_type,
     funding,
     ar_flag,
     nmn_flag,
     country) AS fcf
   FULL JOIN (SELECT arrived_channel,
     utm_content,
     utm_term,
     platform,
     device_type,
     order_channel,
     finance_detail,
     activity_date_pacific,
     SUM(attributed_demand) AS attributed_demand,
     SUM(attributed_orders) AS attributed_orders,
     SUM(attributed_pred_net) AS attributed_pred_net,
     SUM(attributed_units) AS attributed_units,
     SUM(session_orders) AS session_orders,
     SUM(sessions) AS sessions,
     SUM(bounced_sessions) AS bounced_sessions
    FROM mkt_utm
    WHERE LOWER(platform) IN (LOWER('gemini'))
     AND LOWER(finance_detail) IN (LOWER('PAID_SHOPPING'), LOWER('PAID_SEARCH_BRANDED'), LOWER('PAID_SEARCH_UNBRANDED')
       )
    GROUP BY arrived_channel,
     utm_term,
     utm_content,
     order_channel,
     finance_detail,
     platform,
     device_type,
     activity_date_pacific) AS t4 ON fcf.stats_date = t4.activity_date_pacific AND LOWER(fcf.banner) = LOWER(t4.arrived_channel
         ) AND LOWER(fcf.device_type) = LOWER(t4.device_type) AND LOWER(fcf.adgroup_id) = LOWER(t4.utm_content) AND
     LOWER(fcf.finance_detail) = LOWER(t4.finance_detail)
  GROUP BY activity_date_pacific,
   platform,
   arrived_channel,
   order_channel,
   finance_detail,
   sourcename,
   campaign_name,
   campaign_id,
   adgroup_name,
   adgroup_id,
   ad_name,
   ad_id,
   funding,
   ar_flag,
   country,
   funnel_type,
   device_type,
   nmn_flag,
   fcf.stats_date,
   fcf.banner,
   fcf.cost,
   fcf.impressions,
   fcf.clicks,
   fcf.conversions,
   fcf.conversion_value,
   fcf.video_views,
   fcf.video100,
   fcf.video75,
   t4.attributed_demand,
   t4.attributed_orders,
   t4.attributed_units,
   t4.attributed_pred_net,
   t4.sessions,
   t4.bounced_sessions,
   t4.session_orders) AS t6;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (adgroup_id, finance_detail, device_type, arrived_channel) on gemini_search_dad_4box;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS gemini_search_dad_4box_improvements
AS
SELECT t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 t1.finance_detail,
 t1.sourcename,
  CASE
  WHEN LOWER(t1.campaign_name) = LOWER('Unknown') AND t2.campaign_name IS NOT NULL
  THEN t2.campaign_name
  ELSE t1.campaign_name
  END AS campaign_name,
  CASE
  WHEN LOWER(t1.campaign_id) = LOWER('Unknown') AND t2.campaign_id IS NOT NULL
  THEN t2.campaign_id
  ELSE t1.campaign_id
  END AS campaign_id,
  CASE
  WHEN LOWER(t1.adgroup_name) = LOWER('Unknown') AND t2.adgroup_name IS NOT NULL
  THEN t2.adgroup_name
  ELSE t1.adgroup_name
  END AS adgroup_name,
 t1.adgroup_id,
 t1.ad_name,
 t1.ad_id,
  CASE
  WHEN LOWER(t1.funding) = LOWER('Unknown') AND t2.funding IS NOT NULL
  THEN t2.funding
  ELSE t1.funding
  END AS funding,
  CASE
  WHEN LOWER(t1.ar_flag) = LOWER('Others') AND t2.ar_flag IS NOT NULL
  THEN t2.ar_flag
  ELSE t1.ar_flag
  END AS ar_flag,
 t1.country,
  CASE
  WHEN LOWER(t1.funnel_type) = LOWER('Unknown') AND t2.funnel_type IS NOT NULL
  THEN t2.funnel_type
  ELSE t1.funnel_type
  END AS funnel_type,
 t1.nmn_flag,
  CASE
  WHEN LOWER(t1.device_type) = LOWER('Unknown') AND t2.device_type IS NOT NULL
  THEN t2.device_type
  ELSE t1.device_type
  END AS device_type,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.cost
   ELSE NULL
   END) AS cost,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.impressions
   ELSE NULL
   END) AS impressions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.clicks
   ELSE NULL
   END) AS clicks,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversions
   ELSE NULL
   END) AS conversions,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.conversion_value
   ELSE NULL
   END) AS conversion_value,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video_views
   ELSE NULL
   END) AS video_views,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video100
   ELSE NULL
   END) AS video100,
 SUM(CASE
   WHEN t1.fcf_row_num = 1
   THEN t1.video75
   ELSE NULL
   END) AS video75,
 SUM(t1.attributed_demand) AS attributed_demand,
 SUM(t1.attributed_orders) AS attributed_orders,
 SUM(t1.attributed_units) AS attributed_units,
 SUM(t1.attributed_pred_net) AS attributed_pred_net,
 SUM(t1.sessions) AS sessions,
 SUM(t1.bounced_sessions) AS bounced_sessions,
 SUM(t1.session_orders) AS session_orders
FROM gemini_search_dad_4box AS t1
 LEFT JOIN (SELECT DISTINCT finance_detail,
   arrived_channel,
   campaign_name,
   campaign_id,
   adgroup_name,
   adgroup_id,
   device_type,
   funnel_type,
   funding,
   ar_flag
  FROM gemini_search_dad_4box
  WHERE LOWER(campaign_name) <> LOWER('Unknown')) AS t2 ON LOWER(t1.adgroup_id) = LOWER(t2.adgroup_id) AND LOWER(t1.finance_detail
      ) = LOWER(t2.finance_detail) AND LOWER(t1.device_type) = LOWER(t2.device_type) AND LOWER(t1.arrived_channel)
  IS NOT NULL
GROUP BY t1.activity_date_pacific,
 t1.platform,
 t1.arrived_channel,
 t1.order_channel,
 t1.finance_detail,
 t1.sourcename,
 campaign_name,
 campaign_id,
 adgroup_name,
 t1.adgroup_id,
 t1.ad_name,
 t1.ad_id,
 funding,
 ar_flag,
 t1.country,
 funnel_type,
 t1.nmn_flag,
 device_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS Unknown_dad_4box
AS
SELECT activity_date_pacific,
 COALESCE(platform, 'Unknown') AS platform,
 COALESCE(arrived_channel, 'Unknown') AS arrived_channel,
 COALESCE(order_channel, 'Unknown') AS order_channel,
 finance_detail,
 'Unknown' AS sourcename,
 NULL AS campaign_name,
 NULL AS campaign_id,
 NULL AS adgroup_name,
 NULL AS adgroup_id,
 NULL AS ad_name,
 NULL AS ad_id,
 NULL AS funding,
 NULL AS ar_flag,
 NULL AS country,
 NULL AS funnel_type,
 NULL AS nmn_flag,
 COALESCE(device_type, 'Unknown') AS device_type,
 NULL AS cost,
 NULL AS impressions,
 NULL AS clicks,
 NULL AS conversions,
 NULL AS conversion_value,
 NULL AS video_views,
 NULL AS video100,
 NULL AS video75,
 SUM(attributed_demand) AS attributed_demand,
 SUM(attributed_orders) AS attributed_orders,
 SUM(attributed_units) AS attributed_units,
 SUM(attributed_pred_net) AS attributed_pred_net,
 SUM(sessions) AS sessions,
 SUM(bounced_sessions) AS bounced_sessions,
 SUM(session_orders) AS session_orders
FROM mkt_utm AS mta
WHERE LOWER(platform) IN (LOWER('Unknown'))
GROUP BY activity_date_pacific,
 platform,
 arrived_channel,
 order_channel,
 finance_detail,
 sourcename,
 campaign_name,
 device_type,
 campaign_id,
 adgroup_name,
 adgroup_id,
 ad_name,
 ad_id,
 funding,
 ar_flag,
 country,
 funnel_type,
 nmn_flag;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
WHERE activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}} ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
         , arrived_channel
         , order_channel
         , finance_detail
         , platform
         , sourcename
         , campaign_name
         , campaign_id
         , adgroup_name
         , adgroup_id
         , ad_name
         , ad_id
         , funding
         , ar_flag
         , country
         , funnel_type
         , nmn_flag
         , device_type
         , cost
         , impressions
         , clicks
         , conversions
         , conversion_value
         , video_views
         , video100
         , video75
         , attributed_demand
         , attributed_orders
         , attributed_units
         , attributed_pred_net
         , sessions
         , bounced_sessions
         , session_orders
         ,  dw_sys_load_tmstp)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	adwords_display_dad_4box
WHERE   activity_date_pacific BETWEEN  {{params.start_date}}  AND {{params.end_date}}
;




INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
( activity_date_pacific,
    arrived_channel,
    order_channel,
    finance_detail,
    platform,
    sourcename,
    campaign_name,
    campaign_id,
    adgroup_name,
    adgroup_id,
    ad_name,
    ad_id,
    funding,
    ar_flag,
    country,
    funnel_type,
    nmn_flag,
    device_type,
    cost,
    impressions,
    clicks,
    conversions,
    conversion_value,
    video_views,
    video100,
    video75,
    attributed_demand,
    attributed_orders,
    attributed_units,
    attributed_pred_net,
    sessions,
    bounced_sessions,
    session_orders,
   dw_sys_load_tmstp)
SELECT
    activity_date_pacific,
    arrived_channel,
    order_channel,
    finance_detail,
    platform,
    sourcename,
    campaign_name,
    campaign_id,
    adgroup_name,
    adgroup_id,
    ad_name,
    ad_id,
    funding,
    CASE  
    WHEN LOWER(campaign_name) LIKE LOWER('%_2928_%')
      OR LOWER(campaign_name) LIKE LOWER('%_2929_%')
      OR LOWER(campaign_name) LIKE LOWER('%_2932_%')
      OR LOWER(campaign_name) LIKE LOWER('%_2922_%') 
      THEN 'delete'
    WHEN REGEXP_CONTAINS(LOWER(adgroup_name), r'@_rtg@_')
      OR LOWER(adgroup_name) LIKE LOWER('%retargeting%')
      OR LOWER(adgroup_name) = LOWER('all_products|drt_60day')
      THEN 'Retargeting'
    WHEN LOWER(campaign_name) LIKE LOWER('%acq%')
      OR REGEXP_CONTAINS(LOWER(adgroup_name), r'@_acq@_')
      OR LOWER(adgroup_name) LIKE LOWER('%|acq|%')
      OR LOWER(adgroup_name) LIKE LOWER('%interest_kw%')
      OR LOWER(sourcename) IN (
          LOWER('Nordstrom Acquisition - 4967171468'),
          LOWER('Nordstrom Canada Search - 1893217362')
      )
      OR LOWER(sourcename) IN (
          LOWER('Nordstrom Kenshoo #2'),
          LOWER('Social 2018 - Facebook'),
          LOWER('Social 2018 - Instagram')
      )
      THEN 'Acquisition'
    WHEN LOWER(campaign_name) LIKE LOWER('%ret%')
      OR REGEXP_CONTAINS(LOWER(adgroup_name), r'@_ret@_')
      OR LOWER(adgroup_name) LIKE LOWER('%|ret|%')
      OR LOWER(sourcename) IN (
          LOWER('Nordstrom 2017 DPA Retention'),
          LOWER('Nordstrom Retention Native')
      )
      THEN 'Retention'
    WHEN LOWER(adgroup_name) LIKE LOWER('%conquesting%') 
      THEN 'Conquesting'
    WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'@_psb@_')
      OR REGEXP_CONTAINS(LOWER(campaign_name), r'@_brand@_')
      OR REGEXP_CONTAINS(LOWER(adgroup_name), r'@_brand@_')
      OR LOWER(campaign_name) LIKE LOWER('%terms%')
      THEN 'Brand'
    ELSE 'Others'
END AS ar_flag,
    country,
    funnel_type,
    nmn_flag,
    device_type,
    cost,
    impressions,
    clicks,
    conversions,
    conversion_value,
    video_views,
    video100,
    video75,
    attributed_demand,
    attributed_orders,
    attributed_units,
    attributed_pred_net,
    sessions,
    bounced_sessions,
    session_orders,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM 
    adwords_search_dad_4box_improvements
WHERE 
    activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}} 
   AND LOWER(campaign_name) NOT LIKE LOWER('%pmax%');




INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
( activity_date_pacific,
    arrived_channel,
    order_channel,
    finance_detail,
    platform,
    sourcename,
    campaign_name,
    campaign_id,
    adgroup_name,
    adgroup_id,
    ad_name,
    ad_id,
    funding,
    ar_flag,
    country,
    funnel_type,
    nmn_flag,
    device_type,
    cost,
    impressions,
    clicks,
    conversions,
    conversion_value,
    video_views,
    video100,
    video75,
    attributed_demand,
    attributed_orders,
    attributed_units,
    attributed_pred_net,
    sessions,
    bounced_sessions,
    session_orders,
     dw_sys_load_tmstp)
SELECT 
    activity_date_pacific,
    arrived_channel,
    order_channel,
    finance_detail,
    platform,
    sourcename,
    campaign_name,
    campaign_id,
    adgroup_name,
    adgroup_id,
    ad_name,
    ad_id,
    funding,
   CASE 
    WHEN LOWER(campaign_name) LIKE LOWER('%_2928_%')
      OR LOWER(campaign_name) LIKE LOWER('%_2929_%')
      OR LOWER(campaign_name) LIKE LOWER('%_2932_%')
      OR LOWER(campaign_name) LIKE LOWER('%_2922_%')
      THEN 'delete'
    WHEN REGEXP_CONTAINS(LOWER(adgroup_name), r'@_rtg@_')
      OR LOWER(adgroup_name) LIKE LOWER('%retargeting%')
      OR LOWER(adgroup_name) = LOWER('all_products|drt_60day')
      THEN 'Retargeting'
    WHEN LOWER(campaign_name) LIKE LOWER('%acq%')
      OR REGEXP_CONTAINS(LOWER(adgroup_name), r'@_acq@_')
      OR LOWER(adgroup_name) LIKE LOWER('%|acq|%')
      OR LOWER(adgroup_name) LIKE LOWER('%interest_kw%')
      OR LOWER(sourcename) IN (
          LOWER('nordstrom acquisition - 4967171468'),
          LOWER('nordstrom canada search - 1893217362')
      )
      OR LOWER(sourcename) IN (
          LOWER('nordstrom kenshoo #2'), 
          LOWER('social 2018 - facebook'), 
          LOWER('social 2018 - instagram')
      )
      THEN 'Acquisition'
    WHEN LOWER(campaign_name) LIKE LOWER('%ret%')
      OR REGEXP_CONTAINS(LOWER(adgroup_name), r'@_ret@_')
      OR LOWER(adgroup_name) LIKE LOWER('%|ret|%')
      OR LOWER(sourcename) IN (
          LOWER('nordstrom 2017 dpa retention'),
          LOWER('nordstrom retention native')
      )
      THEN 'Retention'
    WHEN LOWER(adgroup_name) LIKE LOWER('%conquesting%') 
      THEN 'Conquesting'
    WHEN REGEXP_CONTAINS(LOWER(campaign_name), r'@_psb@_')
      OR REGEXP_CONTAINS(LOWER(campaign_name), r'@_brand@_')
      OR REGEXP_CONTAINS(LOWER(adgroup_name), r'@_brand@_')
      OR LOWER(campaign_name) LIKE LOWER('%terms%')
      THEN 'Brand'
    ELSE 'Others'
END AS ar_flag,
    country,
    funnel_type,
    nmn_flag,
    device_type,
    cost,
    impressions,
    clicks,
    conversions,
    conversion_value,
    video_views,
    video100,
    video75,
    attributed_demand,
    attributed_orders,
    attributed_units,
    attributed_pred_net,
    sessions,
    bounced_sessions,
    session_orders,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM 
   adwords_shopping_dad_4box_improvements_pmax
WHERE 
    activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}} ;





INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
,dw_sys_load_tmstp)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
,CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	oath_display_dad_4box
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;






INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	bing_search_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;




INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp
)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	tiktok_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;



INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp
)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	snapchat_dad_4box
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;






INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp
)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	pinterest_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;





INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp
)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	facebook_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;






INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp
)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	gemini_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;


INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp
)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	gemini_search_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;






INSERT INTO `{{params.gcp_project_id}}`.{{params.mta_t2_schema}}.mta_paid_campaign_daily
(activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, campaign_name
, campaign_id
, adgroup_name
, adgroup_id
, ad_name
, ad_id
, funding
, ar_flag
, country
, funnel_type
, nmn_flag
, device_type
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, dw_sys_load_tmstp
)
SELECT	activity_date_pacific
, arrived_channel
, order_channel
, finance_detail
, platform
, sourcename
, cast(campaign_name as string)
, cast(campaign_id as string)
, cast(adgroup_name as string)
, cast(adgroup_id as string)
, cast(ad_name as string)
, cast(ad_id as string)
, cast(funding as string)
, cast(ar_flag as string)
, cast(country as string)
, cast(funnel_type as string)
, cast(nmn_flag as string)
, cast(device_type as string)
, cost
, impressions
, clicks
, conversions
, conversion_value
, video_views
, video100
, video75
, attributed_demand
, attributed_orders
, attributed_units
, attributed_pred_net
, sessions
, bounced_sessions
, session_orders
, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM	Unknown_dad_4box
WHERE   activity_date_pacific BETWEEN {{params.start_date}}  AND {{params.end_date}}
;
END;
