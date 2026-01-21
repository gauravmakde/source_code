SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_paid_campaign_daily_11521_ACE_ENG;
     Task_Name=mta_paid_campaign_daily;'
     FOR SESSION VOLATILE;
/*
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

CREATE MULTISET VOLATILE TABLE funnel_cost_fact AS (
SELECT
        stats_date
        , file_name
        , platform
        , banner
        , sourcename
        , finance_detail
        , campaign_name
        , campaign_id
        , adgroup_name
        , adgroup_id
        , ad_name
        , ad_id
        , device_type
        , funnel_type
        , funding
        , ar_flag
        , CASE WHEN funding = 'EX' then 'Yes' else 'No' end nmn_flag
        , country
        , SUM(cost) AS cost
        , SUM(impressions) AS impressions
        , SUM(clicks) AS clicks
        , SUM(conversions) AS conversions
        , SUM(conversion_value) AS conversion_value
        , SUM(video_views) AS video_views
        , SUM(video100) AS video100
        , SUM(video75) AS video75
FROM (
        SELECT  stats_date
                , file_name
                , STRTOK(file_name,'_',2) AS platform
                , CASE WHEN STRTOK(file_name,'_',1) = 'fp' THEN 'N.COM'
                WHEN STRTOK(file_name,'_',1) = 'rack' THEN 'R.COM'
                ELSE NULL
                END AS banner
                , sourcename
                , CASE WHEN STRTOK(file_name,'_',2) LIKE '%bing' THEN
                        CASE WHEN (campaign_name LIKE '%@_PSU@_%' ESCAPE '@'
                                        OR campaign_name LIKE '%DB%'
                                        OR campaign_name LIKE '%NB%'
                                        OR campaign_name LIKE '%competitor%')
                                THEN 'PAID_SEARCH_UNBRANDED'
                        WHEN (campaign_name LIKE '%@_PSB@_%' ESCAPE '@')
                                THEN 'PAID_SEARCH_BRANDED'
                        WHEN (sourcename LIKE '%producT%')
                                        OR (media_type LIKE '%Price comparison%')
                                        OR  (sourcename LIKE '%Shopping%')
                                        OR  (campaign_name LIKE 'MSAN%')
                                THEN 'PAID_SHOPPING'
                        END
                        WHEN sourcetype='yahoo' THEN
                                CASE WHEN sourcename IN ('Nordstrom 2017 DPA Retention','Nordstrom Retention Native')
                                        THEN 'DISPLAY'
                                WHEN sourcename = 'Nordstrom Terms'
                                        THEN 'PAID_SEARCH_BRANDED'
                                WHEN sourcename = 'Nordstrom'
                                        THEN 'PAID_SEARCH_UNBRANDED'
                                END
                        WHEN advertising_channel IN ('SEARCH','SHOPPING') then
                                CASE WHEN (sourcename IN ('Nordstrom Acquisition - 4967171468','Nordstrom Main - 5417253570')
                                                OR campaign_name LIKE '%@_PSU@_%' ESCAPE '@'
                                                OR campaign_name LIKE '%@_DB@_%' ESCAPE '@'
                                                OR campaign_name LIKE '%@_NB@_%' ESCAPE '@'
                                                OR campaign_name LIKE '%competitor%')
                                        THEN 'PAID_SEARCH_UNBRANDED'
                                WHEN (sourcename IN ('Nordstrom Terms - 5601420631')
                                                OR (campaign_name LIKE '%@_PSB@_%' ESCAPE '@'))
                                        THEN 'PAID_SEARCH_BRANDED'
                                WHEN (sourcename IN ('Nordstrom - Product Listing Ads - 5035034225','Nordstrom Shopping Coop - 9385258156')
                                                OR media_type LIKE '%Price comparison%' )
                                        THEN 'PAID_SHOPPING'
                                end
                        WHEN (media_type LIKE '%display%' OR campaign_name LIKE '%display%') THEN
                                CASE WHEN sourcename LIKE '%Nordstrom US Video - 5038267247%'
                                        THEN 'VIDEO'
                                        ELSE 'DISPLAY'
                                END
                        when  file_name like '%adwords' AND (campaign_name LIKE 'PMAX%'
                         OR  campaign_name LIKE '%- PMax_%') THEN 'PAID_SHOPPING'
                        WHEN Media_Type='video' THEN 'VIDEO'
                        ELSE 'Unknown'
                END AS finance_detail

                , trim(campaign_name) AS campaign_name
                , campaign_id
                , trim(adgroup_name) AS adgroup_name
                , adgroup_id
                , ad_name
                , ad_id
                , CASE WHEN device_type  LIKE 'phone'
                                OR device_type LIKE 'smartphone'
                                OR device_type LIKE '%mobile%'
                                OR device_type LIKE '%android_smartphone%'
                                OR device_type LIKE '%iphone%'
                                OR device_type LIKE '%ipod%'
                                        OR device_type LIKE '%table%'
                                OR device_type LIKE '%ipad%'
                        THEN 'MOBILE'
                        WHEN device_type LIKE '%tv%'
                                OR device_type LIKE '%computer%'
                                OR device_type LIKE '%desktop%'
                        THEN 'COMPUTER'
                        ELSE 'Unknown'
                END AS device_type

                -- some campaign_names are low_nd while the adgroup_name is mid_nd.  How to handle?
                -- This simplified CASE WHEN statement seems to negate the need fOR the specific campagin name call outs FROM the original sql.
                , CASE WHEN campaign_name  LIKE '%low_nd%'
                                OR adgroup_name  LIKE '%low_nd%'
                                --OR campaign_name LIKE 'LOW@_%' ESCAPE '@'
                        THEN 'LOW'
                WHEN  campaign_name LIKE '%mid_nd%'
                                OR campaign_name LIKE '%@_mid@_%' ESCAPE '@'
                                OR campaign_name LIKE '%midfunnel%'
                                OR adgroup_name LIKE '%mid_nd%'
                                --OR campaign_name LIKE 'MID@_%' ESCAPE '@'
                        THEN 'MID'
                WHEN campaign_name LIKE '%up_nd%'
                                OR adgroup_name LIKE '%up_nd%'
                                --OR campaign_name LIKE 'UP@_%' ESCAPE '@'
                        THEN 'UP'
                 WHEN campaign_name LIKE 'MSAN%' THEN 'LOW'
                        ELSE 'Unknown'
                END AS funnel_type

                , CASE
                WHEN campaign_name like '%@_nd@_%' ESCAPE '@' or adgroup_name like '%@_nd@_%' ESCAPE '@' then 'ND'
                WHEN campaign_name like '%@_ex@_%' ESCAPE '@' or adgroup_name like '%@_ex@_%' ESCAPE '@' then 'EX'
                WHEN adgroup_name LIKE '%@_P@_%' ESCAPE '@'
                                OR campaign_name LIKE '%@_P@_%' ESCAPE '@'
                                OR campaign_name LIKE '%@_Persistent@_%' ESCAPE '@'
                                OR sourcename IN ('Nordstrom Terms - 5601420631','Nordstrom Main - 5417253570')  -- adwords logic
                                OR sourcename IN ('DPA','DABA', 'Local Dynamic Ads', 'Social - 2018 Canada')      -- facebook logic
                                OR sourcename  in ('Nordstrom 2017 DPA Retention')  -- gemini logic
                                THEN 'Persistent'
                        WHEN adgroup_name LIKE '%vendor%'
                                OR adgroup_name LIKE '%@_VF@_%' ESCAPE '@'
                                OR campaign_name LIKE '%@_VF@_%' ESCAPE '@'
                                OR campaign_name LIKE '%icf%'
                                OR sourcename IN ('Nordstrom Acquisition - 4967171468','Nordstrom Canada Search - 1893217362','Nordstrom Shopping Coop - 9385258156')  -- adwords logic
                                THEN 'Coop'
                        WHEN campaign_name LIKE 'MSAN%' THEN 'ND'
                                ELSE 'Unknown'
                END AS funding

                , CASE WHEN campaign_name LIKE '%_2928_%'
                                OR campaign_name LIKE '%_2929_%'
                                OR campaign_name LIKE '%_2932_%'
                                OR campaign_name LIKE '%_2922_%'
                        THEN 'delete'                -- oath logic
                        WHEN adgroup_name like '%@_RTG@_%' ESCAPE '@'
                                        OR adgroup_name like '%retargeting%'
                                        OR adgroup_name like 'ALL_PRODUCTS|DRT_60DAY' then 'Retargeting' -- piniterest logic
                        WHEN campaign_name LIKE '%ACQ%'
                                OR adgroup_name LIKE '%@_Acq@_%' ESCAPE '@'
                                OR adgroup_name LIKE '%|ACQ|%'			-- pinterest logic
                                OR adgroup_name LIKE '%INTEREST_KW%'    -- pinterest logic
                                OR sourcename IN ('Nordstrom Acquisition - 4967171468','Nordstrom Canada Search - 1893217362')   -- adwords logic
                                OR sourcename IN ('Nordstrom Kenshoo #2', 'Social 2018 - Facebook', 'Social 2018 - Instagram')   -- facebook logic
                        THEN 'Acquisition'
                        WHEN campaign_name LIKE '%RET%'
                                OR adgroup_name LIKE '%@_Ret@_%' ESCAPE '@'
                                OR adgroup_name LIKE '%|Ret|%'						-- pinterest logic
                                OR sourcename IN ('Nordstrom 2017 DPA Retention','Nordstrom Retention Native')  -- gemini logic
                                THEN 'Retention'
                        WHEN adgroup_name LIKE '%Conquesting%' THEN 'Conquesting'
                        WHEN (campaign_name LIKE '%@_PSB@_%' ESCAPE '@')
                                OR campaign_name LIKE '%@_Brand@_%' ESCAPE '@'
                                OR adgroup_name LIKE '%@_Brand@_%' ESCAPE '@'
                                OR (campaign_name LIKE '%TERMS%')                    --- would this apply to all platforms fOR defining brand?
                                THEN 'Brand'
                                ELSE 'Others'
                END AS ar_flag

        , CASE WHEN currency ='CAD' OR campaign_name LIKE 'Canada %' THEN 'CA'
                        ELSE 'US'
                        END AS country
                , cost
                , impressions
                , clicks
                , conversions
                , conversion_value
                , video_views
                , video100
                , video75
        FROM	T2DL_DAS_FUNNEL_IO.funnel_cost_fact_vw
        WHERE 	stats_date BETWEEN {start_date} AND {end_date}
        ) a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(stats_date,banner,device_type,adgroup_id,finance_detail) ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS      COLUMN (finance_detail),
                        COLUMN (platform),
                        COLUMN (stats_date,banner,device_type,adgroup_id),
                        COLUMN (stats_date,banner,device_type,ad_id),
                        COLUMN (stats_date,banner,device_type,finance_detail,adgroup_id),
                        COLUMN (stats_date,banner,device_type,finance_detail,adgroup_id,ad_name),
                        COLUMN (banner,finance_detail,adgroup_id)
ON funnel_cost_fact;

-- CREATING DOWNSTREAM TABLE
-- arrived_channel = 'NULL' coming from mta session data.  This is expected.
CREATE MULTISET VOLATILE TABLE mkt_utm AS (
SELECT  CASE
                WHEN arrived_channel = 'NULL' AND channel = 'N.CA' THEN 'N.COM'
                WHEN arrived_channel = 'NULL' AND channel = 'N.COM' THEN 'N.COM'
                WHEN arrived_channel = 'NULL' AND channel = 'RACK.COM' THEN 'R.COM'
        ELSE arrived_channel
        END AS arrived_channel
        , utm_source
        , utm_channel
        , trim(utm_campaign) AS utm_campaign
        , utm_term
        , utm_content
        , channel AS order_channel
        , CASE WHEN finance_detail = 'SHOPPING'
                THEN 'PAID_SHOPPING'
        ELSE finance_detail
        END AS finance_detail
        , CASE WHEN utm_source LIKE '%bing%'
                THEN 'bing'
             WHEN utm_source LIKE '%SNAPCHAT%'
                THEN 'snapchat'
             WHEN utm_source LIKE '%pinterest%' AND finance_detail LIKE 'SOCIAL_PAID' AND utm_campaign not LIKE 'PRODUCTSHARE'
                THEN 'pinterest'
             WHEN utm_source LIKE '%TIKTOK%'
                THEN 'tiktok'
             WHEN  utm_source LIKE '%gemini%'
                THEN 'gemini'
             WHEN utm_source LIKE '%google%'
                        OR utm_source LIKE '%googl%'
                        OR utm_source LIKE '%gdn%'
                        OR utm_source LIKE '%youtube%'
                THEN 'adwords'
             WHEN (utm_source LIKE '%facebook%'
                        OR utm_source LIKE '%insta%'
                        OR utm_source LIKE '%displa%fb%'
                        OR utm_source LIKE '%kenshoo%')
                                AND finance_detail NOT LIKE '%organic%'
                        THEN 'facebook'
             WHEN utm_source LIKE '%aol%'
                        OR utm_source LIKE '%vzn%'
                        OR utm_source LIKE '%oath%'
                        OR utm_source LIKE '%yahoo%'
                        OR utm_source LIKE '%yahoo@_dpa%' escape '@'
                THEN 'oath'

             WHEN ((utm_source not LIKE '%AFFILIATE%'
                        and utm_source not LIKE '%bing%'
                        and utm_source not LIKE '%google%'
                        and utm_source not LIKE '%googl%'
                        and utm_source not LIKE '%gdn%'
                        and utm_source not LIKE '%youtube%'
                        and utm_source not LIKE '%gemini%'
                        and utm_source not LIKE '%yahoo@_dpa%' escape '@'
                        and utm_source not LIKE '%aol%'
                        and utm_source not LIKE '%vzn%'
                        and utm_source not LIKE '%oath%'
                        and utm_source not LIKE '%yahoo%'
                        and utm_source not LIKE '%pinterest%'
                        and utm_source not LIKE '%TIKTOK%'
                        and utm_source not LIKE '%facebook%'
                        and utm_source not LIKE '%insta%'
                        and utm_source not LIKE '%displa%fb%'
                        and utm_source not LIKE '%kenshoo%'
                        and utm_source not LIKE '%SNAPCHAT%')
                        OR utm_source IS NULL )
                        AND finance_detail IN  ('PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED', 'DISPLAY','VIDEO','SOCIAL_PAID','PAID_SHOPPING','SHOPPING')
                        THEN 'Unknown'
             ELSE NULL
        END AS platform

        , CASE WHEN device_type IN ('ANDROID','IOS','MOW')
                THEN 'MOBILE'
             WHEN device_type IN ('TABLET','PAD')
                THEN 'TABLET'
             WHEN device_type IN ('WEB','COMPUTER')
                THEN 'COMPUTER'
             WHEN device_type = ''
                    OR device_type IS NULL
                    OR device_type LIKE 'Unknown'
                THEN 'Unknown'
             ELSE device_type
        END AS device_type
        , activity_date_pacific
        , SUM(gross) AS attributed_demand
        , SUM(orders) AS attributed_orders
        , SUM(net_sales) AS attributed_pred_net
        , SUM(units) AS attributed_units
        , SUM(session_orders) AS session_orders
        , SUM(sessions) AS sessions
        , SUM(bounced_sessions) AS bounced_sessions
FROM  t2dl_das_mta.mkt_utm_agg mua
WHERE activity_date_pacific BETWEEN {start_date} AND {end_date}
AND ((utm_source <>'' AND utm_source not LIKE '%email%' AND utm_source not LIKE '%leanplum%')OR utm_source is null)
AND arrived_channel is not null
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA PRIMARY INDEX(activity_date_pacific,arrived_channel,device_type,utm_content,finance_detail) ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS      COLUMN (platform),
                        COLUMN (utm_campaign),
                        COLUMN (utm_content),
                        COLUMN (utm_term),
                        COLUMN (finance_detail),
                        COLUMN (activity_date_pacific,arrived_channel,device_type,utm_content),
                        COLUMN (activity_date_pacific,arrived_channel,device_type,utm_term),
                        COLUMN (activity_date_pacific,arrived_channel,device_type,utm_content,finance_detail),
                        COLUMN (activity_date_pacific,arrived_channel,device_type,utm_term,utm_content,finance_detail),
                        COLUMN (activity_date_pacific,arrived_channel,device_type,utm_term,finance_detail),
                        COLUMN (arrived_channel,finance_detail,device_type,activity_date_pacific),
                        COLUMN (arrived_channel,order_channel,finance_detail,platform,activity_date_pacific)
ON mkt_utm;

-- How much of the following CASE WHEN statements can be moved up to the appropriate tables above??


/******************************************
 *
 * ADWORDS DISPLAY
 *
 ******************************************/

CREATE MULTISET VOLATILE TABLE adwords_display_dad_4box AS (
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
                , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
                        WHERE platform = 'adwords'
                        AND finance_detail IN ('DISPLAY','VIDEO')
                        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
                        ) fcf
                FULL JOIN (SELECT arrived_channel
                                , CASE WHEN utm_source LIKE '%youtube%' AND arrived_channel = 'N.com'    -- different logic used for N.com/R.com.  Is this correct?  Can one logic be used for both.
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
                        WHERE platform = 'adwords'
                        AND (finance_detail IN ('DISPLAY','VIDEO')
                                OR utm_campaign LIKE '%4230_j012315_16912%')
                        GROUP BY 1,2,3,4,5,6,7
                        ) mta
                        ON fcf.stats_date = mta.activity_date_pacific
                        AND fcf.banner = mta.arrived_channel
                        AND fcf.device_type = mta.device_type
                        -- the remainder of the joins are platform dependent
                        AND fcf.adgroup_id = mta.utm_term
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23) a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;

/******************************************
 *
 * ADWORDS SEARCH
 *
 ******************************************/
-- JOINING UPSTREAM AND DOWNSTREAM FOR ADWORDS SEARCH
CREATE MULTISET VOLATILE TABLE adwords_search_dad_4box AS (
SELECT  COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
        , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
        , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
        , COALESCE(mta.order_channel,'Unknown') AS order_channel
        , COALESCE(fcf.finance_detail,mta.finance_detail,'Unknown') AS finance_detail
        , COALESCE(fcf.sourcename,'Unknown') AS sourcename
        , COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
        , COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
        , COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
        , COALESCE(fcf.adgroup_id,mta.utm_content,'Unknown') AS adgroup_id
        , 'Unknown' AS ad_name
        , 'Unknown' AS ad_id
        , COALESCE(fcf.funding,'Unknown') AS funding
        , COALESCE(fcf.ar_flag,'Others') AS ar_flag
        , COALESCE(fcf.country,'US' ) AS country
        , COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
        , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
        , COALESCE(fcf.device_type,mta.device_type) AS device_type
        , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.adgroup_id AS fcf_adgroupid, mta.order_channel AS mta_order_channel
        , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
          WHERE platform = 'adwords'
          AND finance_detail IN ('PAID_SHOPPING','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED')
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
          ) fcf
  FULL JOIN (SELECT arrived_channel
                    , trim(STRTOK(nullif(utm_content,''),'_',1)) AS utm_content
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
               WHERE platform = 'adwords'
               AND finance_detail IN ('PAID_SHOPPING','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED')
               AND utm_content is not null
               GROUP BY 1,2,3,4,5,6,7
               ) mta
                ON fcf.stats_date = mta.activity_date_pacific
                AND fcf.banner = mta.arrived_channel
                AND fcf.device_type = mta.device_type
                -- the remainder of the joins are platform dependent
                AND fcf.adgroup_id = mta.utm_content
                AND fcf.finance_detail = mta.finance_detail
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) WITH DATA PRIMARY INDEX(adgroup_id, finance_detail, arrived_channel) ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (ARRIVED_CHANNEL ,FINANCE_DETAIL, ADGROUP_ID) ON adwords_search_dad_4box;

-- An additional 'improvements' step is included to account fOR searches that happen
-- before midnight AND the order is placed after midnight.  This step ensures that the the order is correctly attributed
-- even though it occurred on a different date.
-- drop table  adwords_search_improvements;
CREATE MULTISET VOLATILE TABLE adwords_search_improvements AS (
SELECT a.*, row_number() over (partition by a.banner, a.adgroup_id,a.finance_detail order by a.stats_date desc) row_count
FROM (	SELECT distinct stats_date
                , banner
                , sourcename
                , finance_detail
                , adgroup_id
                , adgroup_name
                , funnel_type
                , campaign_name
                , campaign_id
                , funding
        FROM funnel_cost_fact
        WHERE platform = 'adwords'
        AND finance_detail IN ('PAID_SHOPPING','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED')
        ) a
) WITH DATA PRIMARY INDEX(adgroup_id, finance_detail, banner) ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (ROW_COUNT) ON adwords_search_improvements;

CREATE MULTISET VOLATILE TABLE adwords_search_dad_4box_improvements AS (
SELECT	t1.activity_date_pacific
        , t1.platform
        , t1.arrived_channel
        , t1.order_channel
        , t1.finance_detail
        , CASE WHEN t1.sourcename = 'Unknown' AND t2.sourcename IS NOT NULL THEN t2.sourcename ELSE t1.sourcename END AS sourcename
        , CASE WHEN t1.campaign_name = 'Unknown' AND t2.campaign_name IS NOT NULL THEN t2.campaign_name ELSE t1.campaign_name END AS campaign_name
        , CASE WHEN t1.campaign_id = 'Unknown' AND t2.campaign_id IS NOT NULL THEN t2.campaign_id ELSE t1.campaign_id END AS campaign_id
        , CASE WHEN t1.adgroup_name = 'Unknown' AND t2.adgroup_name IS NOT NULL THEN t2.adgroup_name ELSE t1.adgroup_name END AS adgroup_name
        , CASE WHEN t1.adgroup_id = 'Unknown' AND t2.adgroup_id IS NOT NULL THEN t2.adgroup_id ELSE t1.adgroup_id END AS adgroup_id
        , 'Unknown' AS ad_name
        , 'Unknown' AS ad_id
        , CASE WHEN t1.funding = 'Unknown' AND t2.funding IS NOT NULL THEN t2.funding ELSE t1.funding END AS funding
        , t1.ar_flag
        , t1.country
        , CASE WHEN t1.funnel_type = 'Unknown' AND t2.funnel_type IS NOT NULL THEN t2.funnel_type ELSE t1.funnel_type END AS funnel_type
        , t1.nmn_flag
        , t1.device_type

        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

        , SUM(t1.attributed_demand) AS attributed_demand
        , SUM(t1.attributed_orders) AS attributed_orders
        , SUM(t1.attributed_units) AS attributed_units
        , SUM(t1.attributed_pred_net) AS attributed_pred_net
        , SUM(t1.sessions) AS sessions
        , SUM(t1.bounced_sessions) AS bounced_sessions
        , SUM(t1.session_orders) AS session_orders

FROM    adwords_search_dad_4box t1
LEFT JOIN (     SELECT  *
                FROM    adwords_search_improvements
                WHERE   row_count = 1) t2 ON t1.adgroup_id = t2.adgroup_id
                                     AND t1.finance_detail = t2.finance_detail
                                     AND t1.arrived_channel = t2.banner
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;

/******************************************
 *
 * ADWORDS PMAX
 *
 ******************************************/
-- JOINING UPSTREAM AND DOWNSTREAM FOR ADWORDS PMAX

CREATE MULTISET VOLATILE TABLE adwords_shopping_dad_4box_pmax AS (
SELECT  COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
        , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
        , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
        , COALESCE(mta.order_channel,'Unknown') AS order_channel
        , COALESCE(fcf.finance_detail,mta.finance_detail,'Unknown') AS finance_detail
        , COALESCE(fcf.sourcename,'Unknown') AS sourcename
        , COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
        , COALESCE(fcf.campaign_id,mta.utm_campaign,'Unknown') AS campaign_id
        , 'Unknown' AS adgroup_name
        , 'Unknown' AS adgroup_id
        , 'Unknown' AS ad_name
        , 'Unknown' AS ad_id
        , COALESCE(fcf.funding,'ND') AS funding
        , COALESCE(fcf.ar_flag,'Others') AS ar_flag
        , COALESCE(fcf.country,'US' ) AS country
        , COALESCE(fcf.funnel_type,'LOW') AS funnel_type
        , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
        , COALESCE(fcf.device_type,mta.device_type) AS device_type
        , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.campaign_id AS fcf_campaign_id, mta.order_channel AS mta_order_channel
        , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_campaign_id order by mta_order_channel ASC) AS fcf_row_num

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
          WHERE platform = 'adwords'
          AND finance_detail = 'PAID_SHOPPING'
          AND (campaign_name LIKE 'PMAX%' OR  campaign_name LIKE '%- PMax_%')
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
          ) fcf
  FULL JOIN (SELECT arrived_channel
                    , trim(STRTOK(nullif(utm_campaign,''),'_',1)) AS utm_campaign
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
               WHERE platform = 'adwords'
               AND finance_detail ='PAID_SHOPPING'
               and utm_content is null
               and utm_term is null
               and utm_campaign is not null
               and utm_channel like '%PMAX'
               GROUP BY 1,2,3,4,5,6,7
               ) mta
                ON fcf.stats_date = mta.activity_date_pacific
                AND fcf.banner = mta.arrived_channel
                AND fcf.device_type = mta.device_type
                -- the remainder of the joins are platform dependent
                AND fcf.campaign_id = mta.utm_campaign
                AND fcf.finance_detail = mta.finance_detail
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) WITH DATA PRIMARY INDEX(campaign_id, finance_detail, arrived_channel) ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE adwords_shopping_improvements_pmax AS (
SELECT a.*, row_number() over (partition by a.banner, a.campaign_id,a.finance_detail order by a.stats_date desc) row_count
FROM (	SELECT distinct stats_date
                , banner
                , sourcename
                , finance_detail
                , adgroup_id
                , adgroup_name
                , funnel_type
                , campaign_name
                , campaign_id
                , funding
        FROM funnel_cost_fact
        WHERE platform = 'adwords'
        AND finance_detail = 'PAID_SHOPPING'
        AND (campaign_name LIKE 'PMAX%' OR  campaign_name LIKE '%- PMax_%')
        ) a
) WITH DATA PRIMARY INDEX(campaign_id, finance_detail, banner) ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE adwords_shopping_dad_4box_improvements_pmax AS (
SELECT	t1.activity_date_pacific
        , t1.platform
        , t1.arrived_channel
        , t1.order_channel
        , t1.finance_detail
        , CASE WHEN t1.sourcename = 'Unknown' AND t2.sourcename IS NOT NULL THEN t2.sourcename ELSE t1.sourcename END AS sourcename
        , CASE WHEN t1.campaign_name = 'Unknown' AND t2.campaign_name IS NOT NULL THEN t2.campaign_name ELSE t1.campaign_name END AS campaign_name
        , CASE WHEN t1.campaign_id = 'Unknown' AND t2.campaign_id IS NOT NULL THEN t2.campaign_id ELSE t1.campaign_id END AS campaign_id
        , CASE WHEN t1.adgroup_name = 'Unknown' AND t2.adgroup_name IS NOT NULL THEN t2.adgroup_name ELSE t1.adgroup_name END AS adgroup_name
        , CASE WHEN t1.adgroup_id = 'Unknown' AND t2.adgroup_id IS NOT NULL THEN t2.adgroup_id ELSE t1.adgroup_id END AS adgroup_id
        , 'Unknown' AS ad_name
        , 'Unknown' AS ad_id
        , CASE WHEN t1.funding = 'Unknown' AND t2.funding IS NOT NULL THEN t2.funding ELSE t1.funding END AS funding
        , t1.ar_flag
        , t1.country
        , CASE WHEN t1.funnel_type = 'Unknown' AND t2.funnel_type IS NOT NULL THEN t2.funnel_type ELSE t1.funnel_type END AS funnel_type
        , t1.nmn_flag
        , t1.device_type

        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

        , SUM(t1.attributed_demand) AS attributed_demand
        , SUM(t1.attributed_orders) AS attributed_orders
        , SUM(t1.attributed_units) AS attributed_units
        , SUM(t1.attributed_pred_net) AS attributed_pred_net
        , SUM(t1.sessions) AS sessions
        , SUM(t1.bounced_sessions) AS bounced_sessions
        , SUM(t1.session_orders) AS session_orders

FROM    adwords_shopping_dad_4box_pmax t1
LEFT JOIN (     SELECT  *
                FROM    adwords_shopping_improvements_pmax
                WHERE   row_count = 1) t2 ON t1.campaign_id = t2.campaign_id
                                     AND t1.finance_detail = t2.finance_detail
                                     AND t1.arrived_channel = t2.banner
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;

/******************************************
 *
 * OATH DISPLAY
 *
 * (FP Oath & Verizon Rack)
 *
 ******************************************/

CREATE MULTISET VOLATILE TABLE oath_display_dad_4box AS (
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
                        WHEN  fcf.finance_detail='video'                               --- <<<< platform specific logic
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
                , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
                WHERE platform IN ('oath','verizon')
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
                WHERE platform = 'oath'
                GROUP BY 1,2,3,4,5,6,7
                ) mta
                ON fcf.stats_date = mta.activity_date_pacific
                AND fcf.banner = mta.arrived_channel
                AND fcf.device_type = mta.device_type
                -- the remainder of the joins are platform dependent
                AND fcf.adgroup_id = mta.utm_term
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 ) a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;


/******************************************
 *
 * Bing SEARCH
 *
 ******************************************/

CREATE MULTISET VOLATILE TABLE bing_search_dad_4box AS (
SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
        , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
        , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
        , COALESCE(mta.order_channel,'Unknown') AS order_channel
        , COALESCE(fcf.finance_detail,mta.finance_detail,'Unknown') AS finance_detail
        , COALESCE(fcf.sourcename,'Unknown') AS sourcename
        , COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
        , COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
        , COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
        , COALESCE(fcf.adgroup_id,mta.utm_content,'Unknown') AS adgroup_id
        , 'Unknown' AS ad_name
        , 'Unknown' AS ad_id
        , COALESCE(fcf.funding,'Unknown') AS funding
        , COALESCE(fcf.ar_flag,'Others') AS ar_flag
        , COALESCE(fcf.country,'Unknown' ) AS country
        , COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
        , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
        , COALESCE(fcf.device_type,mta.device_type) AS device_type
        , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.adgroup_id AS fcf_adgroupid, mta.order_channel AS mta_order_channel
        , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
        WHERE platform = 'bing'
        AND finance_detail IN ('PAID_SHOPPING','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED')
        AND sourcename <> '%canada%'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
        ) fcf
  FULL JOIN (   SELECT  arrived_channel
                        , utm_content
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
                WHERE platform = 'bing'
                AND finance_detail IN ('PAID_SHOPPING','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED')
                GROUP BY 1,2,3,4,5,6,7
                ) mta
                ON fcf.stats_date = mta.activity_date_pacific
                AND fcf.banner = mta.arrived_channel
                AND fcf.device_type = mta.device_type
                -- the remainder of the joins are platform dependent
                AND fcf.adgroup_id = mta.utm_content
                AND fcf.finance_detail = mta.finance_detail
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) WITH DATA PRIMARY INDEX(adgroup_id, arrived_channel) ON COMMIT PRESERVE ROWS
;

-- An additional 'improvements' step is included to account fOR searches that happen
-- before midnight AND the order is placed after midnight.  This step ensures that the the order is correctly attributed
-- even though it occurred on a different date.
-- drop table bing_search_improvements;
CREATE MULTISET VOLATILE TABLE bing_search_improvements AS (
SELECT a.*, row_number() over (partition by a.banner, a.adgroup_id, a.campaign_id order by a.stats_date desc) row_count
FROM (	SELECT distinct banner
		        , sourcename
		        , finance_detail
		        , adgroup_id
		        , adgroup_name
		        , campaign_name
		        , campaign_id
		        , funding
		        , ar_flag
		        , funnel_type
		        , nmn_flag
		        , country
		        , stats_date
		FROM funnel_cost_fact
		WHERE platform = 'bing'
		and finance_detail IN ('PAID_SHOPPING','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED')
		and sourcename <> '%canada%'
        ) a
) WITH DATA PRIMARY INDEX(adgroup_id, banner) ON COMMIT PRESERVE ROWS
;

--drop table bing_search_dad_4box_improvements;
CREATE MULTISET VOLATILE TABLE bing_search_dad_4box_improvements AS (
SELECT	t1.activity_date_pacific
        , t1.platform
        , t1.arrived_channel
        , t1.order_channel
        , CASE WHEN t1.finance_detail = 'Unknown' AND t2.finance_detail IS NOT NULL THEN t2.finance_detail ELSE t1.finance_detail END AS finance_detail
        , CASE WHEN t1.sourcename = 'Unknown' AND t2.sourcename IS NOT NULL THEN t2.sourcename ELSE t1.sourcename END AS sourcename
        , CASE WHEN t1.campaign_name = 'Unknown' AND t2.campaign_name IS NOT NULL THEN t2.campaign_name ELSE t1.campaign_name END AS campaign_name
        , CASE WHEN t1.campaign_id = 'Unknown' AND t2.campaign_id IS NOT NULL THEN t2.campaign_id ELSE t1.campaign_id END AS campaign_id
        , CASE WHEN t1.adgroup_name = 'Unknown' AND t2.adgroup_name IS NOT NULL THEN t2.adgroup_name ELSE t1.adgroup_name END AS adgroup_name
        , CASE WHEN t1.adgroup_id = 'Unknown' AND t2.adgroup_id IS NOT NULL THEN t2.adgroup_id ELSE t1.adgroup_id END AS adgroup_id
        , 'Unknown' AS ad_name
        , 'Unknown' AS ad_id
        , CASE WHEN t1.funding = 'Unknown' AND t2.funding IS NOT NULL THEN t2.funding ELSE t1.funding END AS funding
        , CASE WHEN t1.ar_flag = 'Others' AND t2.ar_flag IS NOT NULL THEN t2.ar_flag ELSE t1.ar_flag END AS ar_flag
        , CASE WHEN t1.country = 'Unknown' AND t2.country IS NOT NULL THEN t2.country ELSE t1.country END AS country
        , CASE WHEN t1.funnel_type = 'Unknown' AND t2.funnel_type IS NOT NULL THEN t2.funnel_type ELSE t1.funnel_type END AS funnel_type
        , t1.nmn_flag
        , t1.device_type

        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

        , SUM(t1.attributed_demand) AS attributed_demand
        , SUM(t1.attributed_orders) AS attributed_orders
        , SUM(t1.attributed_units) AS attributed_units
        , SUM(t1.attributed_pred_net) AS attributed_pred_net
        , SUM(t1.sessions) AS sessions
        , SUM(t1.bounced_sessions) AS bounced_sessions
        , SUM(t1.session_orders) AS session_orders
FROM	bing_search_dad_4box t1
-- LY Rack may be inflated
LEFT JOIN (	SELECT	*
            FROM	bing_search_improvements
            WHERE   row_count = 1) t2 ON t1.adgroup_id = t2.adgroup_id
                                                        and t1.arrived_channel = t2.banner
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;


/******************************************
 *
 * TIKTOK
 *
 ******************************************/
CREATE MULTISET VOLATILE TABLE tiktok_dad_4box AS (
SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
        , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
        , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
        , COALESCE(mta.order_channel,'Unknown') AS order_channel
        , COALESCE(fcf.finance_detail,mta.finance_detail,'Unknown') AS finance_detail
        , fcf.sourcename
        , COALESCE(fcf.campaign_name,mta.utm_campaign,'Unknown') as campaign_name
        , fcf.campaign_id
        , fcf.adgroup_name
        , COALESCE(fcf.adgroup_id,mta.utm_term,'Unknown') AS adgroup_id
        , COALESCE(fcf.ad_name,mta.utm_content,'Unknown') AS ad_name
        , fcf.ad_id
        , fcf.funding
        , fcf.ar_flag
        , fcf.country
        , fcf.funnel_type
        , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
        , COALESCE(fcf.device_type,mta.device_type) AS device_type
        , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.ad_name AS fcf_ad_name, mta.order_channel AS mta_order_channel
        , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_ad_name order by mta_order_channel ASC) AS fcf_row_num

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
FROM (  SELECT stats_date
               , file_name
               , platform
               , banner
               , sourcename
               , 'SOCIAL_PAID' AS finance_detail
               , campaign_name
               , campaign_id
               , adgroup_name
               , adgroup_id
               , ad_name
               , ad_id
               , CASE WHEN banner = 'N.COM' THEN 'Unknown'
                       WHEN banner = 'R.COM' THEN 'MOBILE'
               END AS device_type
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
        WHERE platform = 'tiktok'
        AND sourcename not LIKE '%canada%'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        ) fcf
full JOIN (SELECT  arrived_channel
                        , left(utm_term,16) as utm_term
                        , utm_campaign
                        , case when utm_content like '17%' then SUBSTR(utm_content, 18) else utm_content end as utm_content
                        , platform
                        , CASE WHEN arrived_channel = 'N.COM' THEN 'Unknown'
                                WHEN arrived_channel = 'R.COM' THEN 'MOBILE'
                        END AS device_type
                        , order_channel
                        , 'SOCIAL_PAID' AS finance_detail
                        , activity_date_pacific
                        , SUM(attributed_demand) AS attributed_demand
                        , SUM(attributed_orders) AS attributed_orders
                        , SUM(attributed_pred_net) AS attributed_pred_net
                        , SUM(attributed_units) AS attributed_units
                        , SUM(session_orders) AS session_orders
                        , SUM(sessions) AS sessions
                        , SUM(bounced_sessions) AS bounced_sessions
                FROM mkt_utm
                WHERE platform = 'tiktok'
                AND  finance_detail LIKE 'SOCIAL_PAID'
                GROUP BY 1,2,3,4,5,6,7,8,9
                ) mta ON fcf.stats_date = mta.activity_date_pacific
                AND fcf.banner = mta.arrived_channel
                AND fcf.device_type = mta.device_type
                -- the remainder of the joins are platform dependent
                AND fcf.adgroup_id = left(mta.utm_term,16)
                and lower(fcf.campaign_name) = lower(mta.utm_campaign)
                and lower(fcf.ad_name) = lower(mta.utm_content)
                AND fcf.finance_detail = mta.finance_detail
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) WITH DATA PRIMARY INDEX(adgroup_id, ad_name, arrived_channel) ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE tiktok_improvements AS (
SELECT a.*, row_number() over (partition by a.banner, a.ad_name,a.finance_detail order by a.cost desc) row_count
FROM (	SELECT distinct sourcename
                , banner
                , finance_detail
                , ad_id
                , ad_name
                , adgroup_id
                , adgroup_name
                , campaign_name
                , campaign_id
                , funding
                , ar_flag
                , funnel_type
                , nmn_flag
                , country
                , SUM(cost) AS cost
        FROM funnel_cost_fact
        WHERE platform = 'tiktok'
        and sourcename not LIKE '%canada%'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14) a
) WITH DATA PRIMARY INDEX(adgroup_id, ad_name, banner) ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE tiktok_dad_4box_improvements AS (
SELECT	t1.activity_date_pacific
        , t1.platform
        , t1.arrived_channel
        , t1.order_channel
        , COALESCE(t1.finance_detail,t2.finance_detail,'Unknown') AS finance_detail
        , COALESCE(t1.sourcename,t2.sourcename,'Unknown') AS sourcename
        , COALESCE(t1.campaign_name,t2.campaign_name,'Unknown') AS campaign_name
        , COALESCE(t1.campaign_id,t2.campaign_id,'Unknown') AS campaign_id
        , COALESCE(t1.adgroup_name,t2.adgroup_name,'Unknown') AS adgroup_name
        , COALESCE(t1.adgroup_id,t2.adgroup_id,'Unknown') AS adgroup_id
        , COALESCE(t1.ad_name,t2.ad_name,'Unknown') AS ad_name
        , COALESCE(t1.ad_id,t2.ad_id,'Unknown') AS ad_id
        , COALESCE(t1.funding,t2.funding,'Unknown') AS funding
        , COALESCE(t1.ar_flag,t2.ar_flag,'Others') AS ar_flag
        , COALESCE(t1.country,t2.country,'Unknown') AS country
        , COALESCE(t1.funnel_type,t2.funnel_type,'Unknown') AS funnel_type
        , t1.nmn_flag
        , t1.device_type

        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

        , SUM(t1.attributed_demand) AS attributed_demand
        , SUM(t1.attributed_orders) AS attributed_orders
        , SUM(t1.attributed_units) AS attributed_units
        , SUM(t1.attributed_pred_net) AS attributed_pred_net
        , SUM(t1.sessions) AS sessions
        , SUM(t1.bounced_sessions) AS bounced_sessions
        , SUM(t1.session_orders) AS session_orders
FROM	tiktok_dad_4box t1
LEFT JOIN (     SELECT	*
                FROM	tiktok_improvements
                WHERE 	row_count = 1) t2 ON t1.adgroup_id = t2.adgroup_id
                                     AND t1.ad_name = t2.ad_name
                                     AND t1.arrived_channel = t2.banner
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;
/******************************************
 *
 * Snapchat
 *
 ******************************************/

CREATE MULTISET VOLATILE TABLE snapchat_dad_4box AS (
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
                , COALESCE(fcf.finance_detail,mta.finance_detail,'Unknown') AS finance_detail
                , COALESCE(fcf.sourcename,'Unknown') AS sourcename
                , COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
                , COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
                , COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
                , COALESCE(fcf.adgroup_id,mta.utm_term,'Unknown') AS adgroup_id
                , 'Unknown' AS ad_name
                , 'Unknown' AS ad_id
                , COALESCE(fcf.funding,'Unknown') AS funding
                , COALESCE(fcf.ar_flag,'Others') AS ar_flag
                , COALESCE(fcf.country,'Unknown' ) AS country
                , COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
                , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
                , COALESCE(fcf.device_type,mta.device_type) AS device_type
                , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.adgroup_id AS fcf_adgroupid, mta.order_channel AS mta_order_channel
                , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
                        , 'SOCIAL_PAID' AS finance_detail
                        , campaign_name
                        , campaign_id
                        , adgroup_name
                        , adgroup_id
                        , 'MOBILE' AS device_type
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
                        WHERE platform = 'snapchat'
                        AND sourcename not LIKE '%canada%'
                        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
                        ) fcf
        FULL JOIN ( SELECT  arrived_channel
                            , left(utm_term,36) AS utm_term
                            , platform
                            , 'MOBILE' AS device_type
                            , order_channel
                            , 'SOCIAL_PAID' AS finance_detail
                            , activity_date_pacific
                            , SUM(attributed_demand) AS attributed_demand
                            , SUM(attributed_orders) AS attributed_orders
                            , SUM(attributed_pred_net) AS attributed_pred_net
                            , SUM(attributed_units) AS attributed_units
                            , SUM(session_orders) AS session_orders
                            , SUM(sessions) AS sessions
                            , SUM(bounced_sessions) AS bounced_sessions
                    FROM mkt_utm
                    WHERE platform = 'snapchat'
                    GROUP BY 1,2,3,4,5,6,7
                    ) mta ON fcf.stats_date = mta.activity_date_pacific
                            AND fcf.banner = mta.arrived_channel
                            AND fcf.device_type = mta.device_type
                            -- the remainder of the joins are platform dependent
                            AND fcf.adgroup_id = mta.utm_term
                            AND fcf.finance_detail = mta.finance_detail
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 )a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;

/******************************************
 *
 * Pinterest
 *
 ******************************************/

 CREATE MULTISET VOLATILE TABLE pinterest_dad_4box AS (
 SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
         , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
         , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
         , COALESCE(mta.order_channel,'Unknown') AS order_channel
         , COALESCE(fcf.finance_detail,mta.finance_detail,'Unknown') AS finance_detail
         , COALESCE(fcf.sourcename,'Unknown') AS sourcename
         , COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
         , COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
         , COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
         , COALESCE(fcf.adgroup_id,mta.utm_term,'Unknown') AS adgroup_id
         , COALESCE(fcf.adgroup_id,'Unknown') AS ad_name
         , COALESCE(fcf.ad_id,mta.utm_content,'Unknown') AS ad_id
         , COALESCE(fcf.funding,'Unknown') AS funding
         , COALESCE(fcf.ar_flag,'Others') AS ar_flag
         , COALESCE(fcf.country,'Unknown' ) AS country
         , COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
         , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
         , COALESCE(fcf.device_type,mta.device_type) AS device_type
         , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.adgroup_id AS fcf_adgroupid, mta.order_channel AS mta_order_channel
         , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
                , platform
                , banner
                , sourcename
                , 'SOCIAL_PAID' AS finance_detail
                , campaign_name
                , campaign_id
                , adgroup_name
                , adgroup_id
                , ad_name
                , ad_id
                , 'Unknown' AS device_type
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
           WHERE platform = 'pinterest'
           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
           ) fcf
   FULL JOIN (   SELECT  arrived_channel
                         , left(utm_term,13) AS utm_term
                         , left(utm_campaign,13) AS utm_campaign
                         , left(utm_content,13) AS utm_content
                         , platform
                         , 'Unknown' AS device_type
                         , order_channel
                         , 'SOCIAL_PAID' AS finance_detail
                         , activity_date_pacific
                         , SUM(attributed_demand) AS attributed_demand
                         , SUM(attributed_orders) AS attributed_orders
                         , SUM(attributed_pred_net) AS attributed_pred_net
                         , SUM(attributed_units) AS attributed_units
                         , SUM(session_orders) AS session_orders
                         , SUM(sessions) AS sessions
                         , SUM(bounced_sessions) AS bounced_sessions
                FROM mkt_utm
                WHERE platform = 'pinterest'
                GROUP BY 1,2,3,4,5,6,7,8,9
                ) mta
          ON fcf.stats_date = mta.activity_date_pacific
          AND fcf.banner = mta.arrived_channel
          AND fcf.device_type = mta.device_type
        -- the remainder of the joins are platform dependent
         AND fcf.adgroup_id = mta.utm_term
         AND fcf.finance_detail = mta.finance_detail
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
 ) WITH DATA PRIMARY INDEX(adgroup_id, arrived_channel) ON COMMIT PRESERVE ROWS
 ;

 -- An additional 'improvements' step is included to account fOR searches that happen
 -- before midnight AND the order is placed after midnight.  This step ensures that the the order is correctly attributed
 -- even though it occurred on a different date.
 CREATE MULTISET VOLATILE TABLE pinterest_improvements AS (
 SELECT	distinct banner
         , sourcename
         , 'SOCIAL_PAID' AS finance_detail
         , adgroup_id
         , adgroup_name
         , campaign_name
         , campaign_id
         , ad_name
         , ad_id
         , funding
         , ar_flag
         , nmn_flag
         , funnel_type
         , country
 FROM funnel_cost_fact
 WHERE platform = 'pinterest'
 ) WITH DATA PRIMARY INDEX(adgroup_id, banner) ON COMMIT PRESERVE ROWS
 ;

 CREATE MULTISET VOLATILE TABLE pinterest_dad_4box_improvements AS (
 SELECT	t1.activity_date_pacific
         , t1.platform
         , t1.arrived_channel
         , t1.order_channel
         , CASE WHEN t1.finance_detail = 'Unknown' AND t2.finance_detail IS NOT NULL THEN t2.finance_detail ELSE t1.finance_detail END AS finance_detail
         , CASE WHEN t1.sourcename = 'Unknown' AND t2.sourcename IS NOT NULL THEN t2.sourcename ELSE t1.sourcename END AS sourcename
         , CASE WHEN t1.campaign_name = 'Unknown' AND t2.campaign_name IS NOT NULL THEN t2.campaign_name ELSE t1.campaign_name END AS campaign_name
         , CASE WHEN t1.campaign_id = 'Unknown' AND t2.campaign_id IS NOT NULL THEN t2.campaign_id ELSE t1.campaign_id END AS campaign_id
         , CASE WHEN t1.adgroup_name = 'Unknown' AND t2.adgroup_name IS NOT NULL THEN t2.adgroup_name ELSE t1.adgroup_name END AS adgroup_name
         , CASE WHEN t1.adgroup_id = 'Unknown' AND t2.adgroup_id IS NOT NULL THEN t2.adgroup_id ELSE t1.adgroup_id END AS adgroup_id
         , CASE WHEN t1.ad_name = 'Unknown' AND t2.ad_name IS NOT NULL THEN t2.ad_name ELSE t1.ad_name END AS ad_name
         , CASE WHEN t1.ad_id = 'Unknown' AND t2.ad_id IS NOT NULL THEN t2.ad_id ELSE t1.ad_id END AS ad_id
         , CASE WHEN t1.funding = 'Unknown' AND t2.funding IS NOT NULL THEN t2.funding ELSE t1.funding END AS funding
         , CASE WHEN t1.ar_flag = 'Others' AND t2.ar_flag IS NOT NULL THEN t2.ar_flag ELSE t1.ar_flag END AS ar_flag
         , CASE WHEN t1.country = 'Unknown' AND t2.country IS NOT NULL THEN t2.country ELSE t1.country END AS country
         , CASE WHEN t1.funnel_type = 'Unknown' AND t2.funnel_type IS NOT NULL THEN t2.funnel_type ELSE t1.funnel_type END AS funnel_type
         , t1.nmn_flag
         , t1.device_type

         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

         , SUM(t1.attributed_demand) AS attributed_demand
         , SUM(t1.attributed_orders) AS attributed_orders
         , SUM(t1.attributed_units) AS attributed_units
         , SUM(t1.attributed_pred_net) AS attributed_pred_net
         , SUM(t1.sessions) AS sessions
         , SUM(t1.bounced_sessions) AS bounced_sessions
         , SUM(t1.session_orders) AS session_orders
 FROM    pinterest_dad_4box t1
 LEFT JOIN (     SELECT	*
                 FROM	pinterest_improvements ) t2 ON t1.adgroup_id = t2.adgroup_id
                                                 and t1.arrived_channel = t2.banner
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
 ) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
 ;

 /******************************************
  *
  * FACEBOOK
  *
  ******************************************/

 CREATE MULTISET VOLATILE TABLE facebook_search_dad_4box AS (
 SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
         , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
         , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
         , COALESCE(mta.order_channel,'Unknown') AS order_channel
         , COALESCE(fcf.finance_detail,mta.finance_detail) AS finance_detail
         , fcf.sourcename
         , fcf.campaign_name
         , fcf.campaign_id
         , fcf.adgroup_name
         , fcf.adgroup_id
         , fcf.ad_name
         , COALESCE(fcf.ad_id,mta.utm_content) AS ad_id
         , COALESCE(fcf.funding,  CASE WHEN mta.utm_term LIKE '%@_ND@_%' ESCAPE '@'  THEN 'ND'
                       WHEN mta.utm_term LIKE '%@_EX@_%' ESCAPE '@' OR mta.utm_term LIKE '%icf%' THEN 'EX'
                       WHEN mta.utm_term LIKE '%@_P@_%' ESCAPE '@' OR mta.utm_term LIKE '%dpa%' OR mta.utm_term LIKE '%daba%' THEN 'Persistent'
                       WHEN mta.utm_term LIKE '%@_VF@_%' ESCAPE '@' THEN 'Coop'
                       ELSE null
                       END  ) AS funding
         , COALESCE(fcf.ar_flag,Case
                       WHEN mta.utm_term LIKE '%@_Brand@_%' ESCAPE '@' THEN 'Brand'
                       WHEN mta.utm_term LIKE '%@_acq@_%' ESCAPE '@' OR mta.utm_term LIKE '%dpa%acq%' THEN 'Acquisition'
                       WHEN mta.utm_term LIKE '%@_ret@_%' ESCAPE '@' THEN 'Retention'
                       ELSE 'Others'
                       End) AS ar_flag
         , fcf.country
         , fcf.funnel_type
         , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
         , COALESCE(fcf.device_type,mta.device_type) AS device_type
         , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.ad_id AS fcf_ad_id, mta.order_channel AS mta_order_channel
         , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_ad_id order by mta_order_channel ASC) AS fcf_row_num

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
                , 'facebook' AS platform
                , banner
                , sourcename
                , NULL AS finance_detail                                    -- why do this and then add it later?
                , campaign_name
                , campaign_id
                , oreplace(adgroup_name,' ','') AS adgroup_name
                , adgroup_id
                , ad_name
                , ad_id
                , device_type
                , funnel_type
                , funding
                , ar_flag
                , nmn_flag
                , CASE WHEN banner = 'R.com' THEN 'US' ELSE country END AS country       --- why is there a platform specific definition?
                , SUM(cost) AS cost
                , SUM(impressions) AS impressions
                , SUM(clicks) AS clicks
                , SUM(conversions) AS conversions
                , SUM(conversion_value) AS conversion_value
                , SUM(video_views) AS video_views
                , SUM(video100) AS video100
                , SUM(video75) AS video75
         FROM    funnel_cost_fact
         WHERE   platform = 'facegram'
         and     ad_id not IN (  SELECT ad_id
                                 FROM	funnel_cost_fact
                                 WHERE	platform = 'facegram'
                                 GROUP BY 1
                                 having SUM(cost) <=0 AND SUM(impressions)>0)

         GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
           ) fcf
   FULL JOIN (SELECT arrived_channel
                     , left(utm_content,13) AS utm_content
                     , left(utm_term,13) AS utm_term
                     , platform
                     , device_type
                     , order_channel
                     , NULL AS finance_detail
                     , activity_date_pacific
                     , SUM(attributed_demand) AS attributed_demand
                     , SUM(attributed_orders) AS attributed_orders
                     , SUM(attributed_pred_net) AS attributed_pred_net
                     , SUM(attributed_units) AS attributed_units
                     , SUM(session_orders) AS session_orders
                     , SUM(sessions) AS sessions
                     , SUM(bounced_sessions) AS bounced_sessions
                FROM mkt_utm
                WHERE platform = 'facebook' and utm_term is not null
                GROUP BY 1,2,3,4,5,6,7,8
                ) mta
          ON fcf.stats_date = mta.activity_date_pacific
          AND fcf.banner = mta.arrived_channel
          AND fcf.device_type = mta.device_type
        -- the remainder of the joins are platform dependent
         AND left(fcf.ad_id,13)=left(mta.utm_content,13)       -- is the left join logic needed here?  Can it be removed for future streamlining?
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
 ) WITH DATA PRIMARY INDEX(ad_id, arrived_channel) ON COMMIT PRESERVE ROWS
 ;

 -- An additional 'improvements' step is included to account fOR searches that happen
 -- before midnight AND the order is placed after midnight.  This step ensures that the the order is correctly attributed
 -- even though it occurred on a different date.
 CREATE MULTISET VOLATILE TABLE facebook_improvements AS (
  SELECT a.*, row_number() over (partition by a.banner, a.campaign_id, a.ad_id order by cost desc) row_num
 FROM (
         SELECT  'SOCIAL_PAID' as finance_detail
                 , banner
                 , sourcename
                 , adgroup_id
                 , adgroup_name
                 , ad_id
                 , ad_name
                 , campaign_id
                 , campaign_name
                 , funnel_type
                 , funding
                 , ar_flag
                 , nmn_flag
                 , country
                 , SUM(cost) cost
 FROM	funnel_cost_fact
 WHERE	platform = 'facegram'
 and	ad_id not IN (  SELECT ad_id
                     FROM	funnel_cost_fact
                     WHERE	platform = 'facegram'
                     GROUP BY 1
                     having SUM(cost) <=0 AND SUM(impressions)>0 )
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14 )a
 ) WITH DATA PRIMARY INDEX(ad_id, banner) ON COMMIT PRESERVE ROWS
 ;

 -- drop table facebook_dad_4box_improvements;
 CREATE MULTISET VOLATILE TABLE facebook_dad_4box_improvements AS (
 SELECT	t1.activity_date_pacific
         , t1.platform
         , t1.arrived_channel
         , t1.order_channel
         , COALESCE(t1.finance_detail,'SOCIAL_PAID') AS finance_detail
         , COALESCE(t1.sourcename,t2.sourcename,'Unknown') AS sourcename
         , COALESCE(t1.campaign_name,t2.campaign_name,'Unknown') AS campaign_name
         , COALESCE(t1.campaign_id,t2.campaign_id,'Unknown') AS campaign_id
         , COALESCE(t1.adgroup_name,t2.adgroup_name,'Unknown') AS adgroup_name
         , COALESCE(t1.adgroup_id,t2.adgroup_id,'Unknown') AS adgroup_id
         , COALESCE(t1.ad_name,t2.ad_name,'Unknown') AS ad_name
         , COALESCE(t1.ad_id,t2.ad_id,'Unknown') AS ad_id
         , COALESCE(t1.funding,t2.funding,'Unknown') AS funding
         , COALESCE(t1.ar_flag,t2.ar_flag,'Others') AS ar_flag
         , COALESCE(t1.country,t2.country,'Unknown') AS country
         , COALESCE(t1.funnel_type,t2.funnel_type,'Unknown') AS funnel_type
         , t1.nmn_flag
         , t1.device_type

         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
         , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

         , SUM(t1.attributed_demand) AS attributed_demand
         , SUM(t1.attributed_orders) AS attributed_orders
         , SUM(t1.attributed_units) AS attributed_units
         , SUM(t1.attributed_pred_net) AS attributed_pred_net
         , SUM(t1.sessions) AS sessions
         , SUM(t1.bounced_sessions) AS bounced_sessions
         , SUM(t1.session_orders) AS session_orders
 FROM	facebook_search_dad_4box t1
 LEFT JOIN (     SELECT  *
                 FROM    facebook_improvements
                 WHERE   row_num = 1) t2 ON t1.ad_id = t2.ad_id
                 AND     t1.arrived_channel = t2.banner
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
 ) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
 ;

/******************************************
 *
 * GEMINI/YAHOO  Display
 *
 ******************************************/

CREATE MULTISET VOLATILE TABLE gemini_dad_4box AS (
SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
        , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
        , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
        , COALESCE(mta.order_channel,'Unknown') AS order_channel
        , COALESCE(fcf.finance_detail,mta.finance_detail) AS finance_detail
        , COALESCE(fcf.sourcename,'Unknown') AS sourcename
        , COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
        , COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
        , COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
        , COALESCE(fcf.adgroup_id,mta.utm_term,'Unknown') AS adgroup_id
        , 'Unknown' AS ad_name
        , COALESCE(fcf.ad_id,'Unknown') AS ad_id
        , COALESCE(fcf.funding,  CASE WHEN mta.utm_term LIKE '%@_ND@_%' ESCAPE '@'  THEN 'ND'
                      WHEN mta.utm_term LIKE '%@_EX@_%' ESCAPE '@' OR mta.utm_term LIKE '%icf%' THEN 'EX'
                      WHEN mta.utm_term LIKE '%@_P@_%' ESCAPE '@' OR mta.utm_term LIKE '%dpa%' OR mta.utm_term LIKE '%daba%' THEN 'Persistent'
                      WHEN mta.utm_term LIKE '%@_VF@_%' ESCAPE '@' THEN 'Coop'
                      ELSE null
                      END  ) AS funding
        , COALESCE(fcf.ar_flag,Case
                      WHEN mta.utm_term LIKE '%@_Brand@_%' ESCAPE '@' THEN 'Brand'
                      WHEN mta.utm_term LIKE '%@_acq@_%' ESCAPE '@' OR mta.utm_term LIKE '%dpa%acq%' THEN 'Acquisition'
                      WHEN mta.utm_term LIKE '%@_ret@_%' ESCAPE '@' THEN 'Retention'
                      ELSE 'Others'
                      End) AS ar_flag
        , COALESCE(fcf.country,'US' ) AS country
        , COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
        , COALESCE(fcf.nmn_flag,'Unknown') AS nmn_flag
        , COALESCE(fcf.device_type,mta.device_type) AS device_type
        , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.adgroup_id AS fcf_adgroupid, mta.order_channel AS mta_order_channel
        , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
                , ad_id
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
        WHERE platform IN ('yahoo','gemini')
        AND  finance_detail = 'DISPLAY'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
        ) fcf
FULL JOIN (     SELECT  arrived_channel
                        , utm_content
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
                WHERE platform IN ('gemini')
                AND   finance_detail = 'DISPLAY'
                GROUP BY 1,2,3,4,5,6,7,8
                ) mta
                ON fcf.stats_date = mta.activity_date_pacific
                AND fcf.banner = mta.arrived_channel
                AND fcf.device_type = mta.device_type
                -- the remainder of the joins are platform dependent
                AND fcf.adgroup_id=mta.utm_term
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) WITH DATA PRIMARY INDEX(adgroup_id, arrived_channel) ON COMMIT PRESERVE ROWS
;
-- An additional 'improvements' step is included to account fOR searches that happen
-- before midnight AND the order is placed after midnight.  This step ensures that the the order is correctly attributed
-- even though it occurred on a different date.
CREATE MULTISET VOLATILE TABLE gemini_improvements AS (
SELECT a.*, row_number() over (partition by a.banner, a.adgroup_id order by a.stats_date desc) row_num
FROM (	SELECT  distinct stats_date
                , banner
                , sourcename
                , campaign_id
                , campaign_name
                , adgroup_id
                , adgroup_name
                , funnel_type
        FROM funnel_cost_fact
        WHERE platform IN ('yahoo','gemini') ) AS a
) WITH DATA PRIMARY INDEX(adgroup_id, banner) ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE gemini_dad_4box_improvements AS (
SELECT	t1.activity_date_pacific
        , t1.platform
        , t1.arrived_channel
        , t1.order_channel
        , COALESCE(t1.finance_detail,'DISPLAY') AS finance_detail
        , CASE WHEN t1.sourcename = 'Unknown' AND t2.sourcename IS NOT NULL THEN T2.sourcename ELSE t1.sourcename END AS sourcename
        , CASE WHEN t1.campaign_name = 'Unknown' AND t2.campaign_name IS NOT NULL THEN T2.campaign_name ELSE t1.campaign_name END AS campaign_name
        , CASE WHEN t1.campaign_id = 'Unknown' AND t2.campaign_id IS NOT NULL THEN T2.campaign_id ELSE t1.campaign_id END AS campaign_id
        , CASE WHEN t1.adgroup_name = 'Unknown' AND t2.adgroup_name IS NOT NULL THEN T2.adgroup_name ELSE t1.adgroup_name END AS adgroup_name
        , CASE WHEN t1.adgroup_id = 'Unknown' AND t2.adgroup_id IS NOT NULL THEN T2.adgroup_id ELSE t1.adgroup_id END AS adgroup_id
        , t1.ad_name
        , t1.ad_id
        , t1.funding
        , t1.ar_flag
        , t1.country
        , CASE WHEN t1.funnel_type = 'Unknown' AND t2.funnel_type IS NOT NULL THEN T2.funnel_type ELSE t1.funnel_type END AS funnel_type
        , t1.nmn_flag
        , t1.device_type

        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

        , SUM(t1.attributed_demand) AS attributed_demand
        , SUM(t1.attributed_orders) AS attributed_orders
        , SUM(t1.attributed_units) AS attributed_units
        , SUM(t1.attributed_pred_net) AS attributed_pred_net
        , SUM(t1.sessions) AS sessions
        , SUM(t1.bounced_sessions) AS bounced_sessions
        , SUM(t1.session_orders) AS session_orders
FROM	gemini_dad_4box t1
LEFT JOIN (     SELECT	*
                FROM    gemini_improvements
                WHERE   row_num = 1) t2 ON t1.adgroup_id = t2.adgroup_id
                AND     t1.arrived_channel = t2.banner
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;

/******************************************
 *
 * GEMINI/YAHOO  Search
 *
 ******************************************/

CREATE MULTISET VOLATILE TABLE gemini_search_dad_4box AS (
SELECT	COALESCE(fcf.stats_date,mta.activity_date_pacific) activity_date_pacific
        , COALESCE(fcf.platform,mta.platform,'Unknown') AS platform
        , COALESCE(fcf.banner,mta.arrived_channel,'Unknown') AS arrived_channel
        , COALESCE(mta.order_channel,'Unknown') AS order_channel
        , COALESCE(fcf.finance_detail,mta.finance_detail) AS finance_detail
        , COALESCE(fcf.sourcename,'Unknown') AS sourcename
        , COALESCE(fcf.campaign_name,'Unknown') AS campaign_name
        , COALESCE(fcf.campaign_id,'Unknown') AS campaign_id
        , COALESCE(fcf.adgroup_name,'Unknown') AS adgroup_name
        , COALESCE(fcf.adgroup_id,mta.utm_content,'Unknown') AS adgroup_id
        , 'Unknown' AS ad_name
        , COALESCE(fcf.ad_id,'Unknown') AS ad_id
        , COALESCE(fcf.funding,  CASE WHEN mta.utm_term LIKE '%@_ND@_%' ESCAPE '@'  THEN 'ND'
                      WHEN mta.utm_term LIKE '%@_EX@_%' ESCAPE '@' OR mta.utm_term LIKE '%icf%' THEN 'EX'
                      WHEN mta.utm_term LIKE '%@_P@_%' ESCAPE '@' OR mta.utm_term LIKE '%dpa%' OR mta.utm_term LIKE '%daba%' THEN 'Persistent'
                      WHEN mta.utm_term LIKE '%@_VF@_%' ESCAPE '@' THEN 'Coop'
                      ELSE null
                      END  ) AS funding
        , COALESCE(fcf.ar_flag,Case
                      WHEN mta.utm_term LIKE '%@_Brand@_%' ESCAPE '@' THEN 'Brand'
                      WHEN mta.utm_term LIKE '%@_acq@_%' ESCAPE '@' OR mta.utm_term LIKE '%dpa%acq%' THEN 'Acquisition'
                      WHEN mta.utm_term LIKE '%@_ret@_%' ESCAPE '@' THEN 'Retention'
                      ELSE 'Others'
                      End) AS ar_flag
        , COALESCE(fcf.country,'US' ) AS country
        , COALESCE(fcf.funnel_type,'Unknown') AS funnel_type
        , COALESCE(fcf.device_type,mta.device_type) AS device_type
        , COALESCE(fcf.nmn_flag, 'No') AS nmn_flag
        , fcf.stats_date, fcf.banner, fcf.device_type AS fcf_device_type, fcf.adgroup_id AS fcf_adgroupid, mta.order_channel AS mta_order_channel
        , ROW_NUMBER() OVER(PARTITION BY fcf.stats_date, fcf.banner, fcf_device_type, fcf_adgroupid order by mta_order_channel ASC) AS fcf_row_num

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
  FROM (SELECT  stats_date
                , file_name
                , 'gemini' as platform
                , banner
                , sourcename
                , finance_detail
                , campaign_name
                , campaign_id
                , adgroup_name
                , adgroup_id
                , ad_id
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
        WHERE platform IN ('yahoo','gemini')
        AND  finance_detail IN ('PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED')
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
        ) fcf
FULL JOIN (     SELECT arrived_channel
                        , utm_content
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
                WHERE platform IN ('gemini')
                AND   finance_detail IN ('PAID_SHOPPING','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED' )
                GROUP BY 1,2,3,4,5,6,7,8
                ) mta
                ON fcf.stats_date = mta.activity_date_pacific
                AND fcf.banner = mta.arrived_channel
                AND fcf.device_type = mta.device_type
                -- the remainder of the joins are platform dependent
                AND fcf.adgroup_id=mta.utm_content
                AND fcf.finance_detail = mta.finance_detail
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) WITH DATA PRIMARY INDEX(adgroup_id, finance_detail, device_type, arrived_channel) ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (adgroup_id, finance_detail, device_type, arrived_channel) on gemini_search_dad_4box;

CREATE MULTISET VOLATILE TABLE gemini_search_dad_4box_improvements AS (
SELECT	t1.activity_date_pacific
        , t1.platform
        , t1.arrived_channel
        , t1.order_channel
        , t1.finance_detail
        , t1.sourcename
        , CASE WHEN t1.campaign_name = 'Unknown' AND t2.campaign_name IS NOT NULL THEN T2.campaign_name ELSE t1.campaign_name END AS campaign_name
        , CASE WHEN t1.campaign_id = 'Unknown' AND t2.campaign_id IS NOT NULL THEN T2.campaign_id ELSE t1.campaign_id END AS campaign_id
        , CASE WHEN t1.adgroup_name = 'Unknown' AND t2.adgroup_name IS NOT NULL THEN T2.adgroup_name ELSE t1.adgroup_name END AS adgroup_name
        , t1.adgroup_id
        , t1.ad_name
        , t1.ad_id
        , CASE WHEN t1.funding = 'Unknown' AND t2.funding IS NOT NULL THEN T2.funding ELSE t1.funding END AS funding
        , CASE WHEN t1.ar_flag = 'Others' AND t2.ar_flag IS NOT NULL THEN T2.ar_flag ELSE t1.ar_flag END AS ar_flag
        , t1.country
        , CASE WHEN t1.funnel_type = 'Unknown' AND t2.funnel_type IS NOT NULL THEN T2.funnel_type ELSE t1.funnel_type END AS funnel_type
        , t1.nmn_flag
        , CASE WHEN t1.device_type = 'Unknown' AND t2.device_type IS NOT NULL THEN T2.device_type ELSE t1.device_type END AS device_type

        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.COST ELSE NULL END) AS cost
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.impressions ELSE NULL END) AS impressions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.clicks ELSE NULL END) AS clicks
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversions ELSE NULL END) AS conversions
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.conversion_value ELSE NULL END) AS conversion_value
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video_views ELSE NULL END) AS video_views
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video100 ELSE NULL END) AS video100
        , SUM(CASE WHEN fcf_row_num = 1 THEN t1.video75 ELSE NULL END) AS video75

        , SUM(t1.attributed_demand) AS attributed_demand
        , SUM(t1.attributed_orders) AS attributed_orders
        , SUM(t1.attributed_units) AS attributed_units
        , SUM(t1.attributed_pred_net) AS attributed_pred_net
        , SUM(t1.sessions) AS sessions
        , SUM(t1.bounced_sessions) AS bounced_sessions
        , SUM(t1.session_orders) AS session_orders
FROM	gemini_search_dad_4box t1
LEFT JOIN (     SELECT 	distinct finance_detail
                        , arrived_channel
                        , campaign_name
                        , campaign_id
                        , adgroup_name
                        , adgroup_id
                        , device_type
                        , funnel_type
                        , funding
                        , ar_flag
                FROM gemini_search_dad_4box
                WHERE campaign_name<>'Unknown' ) t2 ON t1.adgroup_id = t2.adgroup_id
                                                AND t1.finance_detail = t2.finance_detail
                                                AND t1.device_type = t2.device_type
                                                and t1.arrived_channel = t1.arrived_channel
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;

/******************************************
 *
 * Unknowns
 *
 ******************************************/

CREATE MULTISET VOLATILE TABLE Unknown_dad_4box AS (
SELECT	activity_date_pacific
        , COALESCE(mta.platform,'Unknown') AS platform
        , COALESCE(mta.arrived_channel,'Unknown') AS arrived_channel
        , COALESCE(mta.order_channel,'Unknown') AS order_channel
        , finance_detail
        , 'Unknown' AS sourcename
        , NULL AS campaign_name
        , NULL AS campaign_id
        , NULL AS adgroup_name
        , NULL AS adgroup_id
        , NULL AS ad_name
        , NULL AS ad_id
        , NULL AS funding
        , NULL AS ar_flag
        , NULL AS country
        , NULL AS funnel_type
        , NULL AS nmn_flag
        , COALESCE(mta.device_type,'Unknown') AS device_type

        , NULL cost
        , NULL AS impressions
        , NULL AS clicks
        , NULL AS conversions
        , NULL AS conversion_value
        , NULL AS video_views
        , NULL AS video100
        , NULL AS video75

        , SUM(mta.attributed_demand) AS attributed_demand
        , SUM(mta.attributed_orders) AS attributed_orders
        , SUM(mta.attributed_units) AS attributed_units
        , SUM(mta.attributed_pred_net) AS attributed_pred_net
        , SUM(mta.sessions) AS sessions
        , SUM(mta.bounced_sessions) AS bounced_sessions
        , SUM(mta.session_orders) AS session_orders
FROM	mkt_utm mta
WHERE platform IN ('Unknown')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX(activity_date_pacific) ON COMMIT PRESERVE ROWS
;

/************************************************************************************/

DELETE
FROM    {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Adwords Display
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	adwords_display_dad_4box
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Adwords Search
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         -- Reclassifying this flag after the improvements logic.  This is the same logic as used above for the funnel_cost_fact temp table.
        , CASE WHEN campaign_name LIKE '%_2928_%'
                        OR campaign_name LIKE '%_2929_%'
                        OR campaign_name LIKE '%_2932_%'
                        OR campaign_name LIKE '%_2922_%'
                THEN 'delete'                -- oath logic
                WHEN adgroup_name like '%@_RTG@_%' ESCAPE '@'
                                OR adgroup_name like '%retargeting%'
                                OR adgroup_name like 'ALL_PRODUCTS|DRT_60DAY' then 'Retargeting' -- piniterest logic
                WHEN campaign_name LIKE '%ACQ%'
                        OR adgroup_name LIKE '%@_Acq@_%' ESCAPE '@'
                        OR adgroup_name LIKE '%|ACQ|%'			-- pinterest logic
                        OR adgroup_name LIKE '%INTEREST_KW%'    -- pinterest logic
                        OR sourcename IN ('Nordstrom Acquisition - 4967171468','Nordstrom Canada Search - 1893217362')   -- adwords logic
                        OR sourcename IN ('Nordstrom Kenshoo #2', 'Social 2018 - Facebook', 'Social 2018 - Instagram')   -- facebook logic
                THEN 'Acquisition'
                WHEN campaign_name LIKE '%RET%'
                        OR adgroup_name LIKE '%@_Ret@_%' ESCAPE '@'
                        OR adgroup_name LIKE '%|Ret|%'						-- pinterest logic
                        OR sourcename IN ('Nordstrom 2017 DPA Retention','Nordstrom Retention Native')  -- gemini logic
                        THEN 'Retention'
                WHEN adgroup_name LIKE '%Conquesting%' THEN 'Conquesting'
                WHEN (campaign_name LIKE '%@_PSB@_%' ESCAPE '@')
                        OR campaign_name LIKE '%@_Brand@_%' ESCAPE '@'
                        OR adgroup_name LIKE '%@_Brand@_%' ESCAPE '@'
                        OR (campaign_name LIKE '%TERMS%')                    --- would this apply to all platforms fOR defining brand?
                        THEN 'Brand'
                        ELSE 'Others'
        END AS ar_flag
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	adwords_search_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
and campaign_name not like '%pmax%'
;

-- Adwords Shoopping PMAX
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         -- Reclassifying this flag after the improvements logic.  This is the same logic as used above for the funnel_cost_fact temp table.
        , CASE WHEN campaign_name LIKE '%_2928_%'
                        OR campaign_name LIKE '%_2929_%'
                        OR campaign_name LIKE '%_2932_%'
                        OR campaign_name LIKE '%_2922_%'
                THEN 'delete'                -- oath logic
                WHEN adgroup_name like '%@_RTG@_%' ESCAPE '@'
                                OR adgroup_name like '%retargeting%'
                                OR adgroup_name like 'ALL_PRODUCTS|DRT_60DAY' then 'Retargeting' -- piniterest logic
                WHEN campaign_name LIKE '%ACQ%'
                        OR adgroup_name LIKE '%@_Acq@_%' ESCAPE '@'
                        OR adgroup_name LIKE '%|ACQ|%'			-- pinterest logic
                        OR adgroup_name LIKE '%INTEREST_KW%'    -- pinterest logic
                        OR sourcename IN ('Nordstrom Acquisition - 4967171468','Nordstrom Canada Search - 1893217362')   -- adwords logic
                        OR sourcename IN ('Nordstrom Kenshoo #2', 'Social 2018 - Facebook', 'Social 2018 - Instagram')   -- facebook logic
                THEN 'Acquisition'
                WHEN campaign_name LIKE '%RET%'
                        OR adgroup_name LIKE '%@_Ret@_%' ESCAPE '@'
                        OR adgroup_name LIKE '%|Ret|%'						-- pinterest logic
                        OR sourcename IN ('Nordstrom 2017 DPA Retention','Nordstrom Retention Native')  -- gemini logic
                        THEN 'Retention'
                WHEN adgroup_name LIKE '%Conquesting%' THEN 'Conquesting'
                WHEN (campaign_name LIKE '%@_PSB@_%' ESCAPE '@')
                        OR campaign_name LIKE '%@_Brand@_%' ESCAPE '@'
                        OR adgroup_name LIKE '%@_Brand@_%' ESCAPE '@'
                        OR (campaign_name LIKE '%TERMS%')                    --- would this apply to all platforms fOR defining brand?
                        THEN 'Brand'
                        ELSE 'Others'
        END AS ar_flag
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	adwords_shopping_dad_4box_improvements_pmax
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Oath/Verizon
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	oath_display_dad_4box
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Bing
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	bing_search_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Tik Tok
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	tiktok_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Snapchat
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	snapchat_dad_4box
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Pinterest
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	pinterest_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Facebook
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	facebook_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Gemini/Yahoo Display
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	gemini_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Gemini/Yahoo Search
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	gemini_search_dad_4box_improvements
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;

-- Unknowns
INSERT INTO {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
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
         , CURRENT_TIMESTAMP AS dw_sys_load_tmstp
FROM	Unknown_dad_4box
WHERE   activity_date_pacific BETWEEN {start_date} AND {end_date}
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_pacific, platform, arrived_channel, finance_detail, adgroup_id), -- column names used for primary index
                    COLUMN (activity_date_pacific),
                    COLUMN (activity_date_pacific, finance_detail)
ON {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY;
SET QUERY_BAND = NONE FOR SESSION;
