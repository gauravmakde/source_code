-- DESCRIPTION: T2DL_DAS_MTA.SSA_MKT_ATTR_FACT
-- In the script, we are doing marketing attribution of arrived sessions with orders and demand by mapping the utm parameter parsed from the url
-- to different granularity level(utm_term, utm_channel, utm_campaign, mrkt_type, finance_rollup, finance_detail)
-- by using utm_channel_lookup and marketing_channel_hierarchy tables. And also binning non-arrived sessions with orders and demand as "DIRECT LOAD"/BASE

create table if not exists ace_etl.ssa_mkt_attr_fact
(
    channelcountry string
    , channel string
    , experience string
    , session_id string
    , referrer string
    , siteid string
    , utm_source string
    , utm_medium string
    , sp_source string
    , utm_channel string
    , utm_campaign string
    , sp_campaign string
    , utm_term string
    , utm_content string
    , gclid string
    , bounce_ind string
    , funnel_type string
    , web_orders bigint
    , web_demand_usd DECIMAL(32, 6)
    , session_type string
    , mrkt_touch string
    , mrkt_type string
    , finance_rollup string
    , finance_detail string
    , dw_sys_load_tmstp timestamp
    , activity_date_pacific date
)
using ORC
location 's3://ace-etl/ssa_mkt_attr_fact/'
partitioned by (activity_date_pacific);

MSCK REPAIR TABLE ace_etl.ssa_mkt_attr_fact;

-- Aggregate orders and demand per session
CREATE OR REPLACE TEMPORARY VIEW order_submitted AS
(
SELECT
    activity_date_partition,
    session_id,
    channelcountry,
    channel,
    experience,
    bounce_ind,
    web_orders,
    web_demand_usd
FROM ACP_VECTOR.CUSTOMER_SESSION_FACT
WHERE 1=1
    AND session_id <> 'UNKNOWN'
    AND activity_date_partition between '2024-08-18' AND '2024-08-18'
);


-- Parse out utm parameters from url
-- left join with order_submitted to get order count per sessions
CREATE OR REPLACE TEMPORARY VIEW utm_param AS
(
SELECT DISTINCT
    asf.activity_date_pacific,
    asf.session_id,
    asf.channelcountry,
    asf.channel,
    asf.experience,
    nullif(asf.referrer,'') as referrer,
    nullif(asf.url_destination,'') as url_destination,
    nullif(asf.utm_channel,'') as utm_channel,
    nullif(asf.siteid,'') as siteid,
    nullif(asf.utm_campaign,'') as utm_campaign,
    nullif(asf.sp_campaign,'') as sp_campaign,
    nullif(asf.utm_term,'') as utm_term,
    nullif(asf.utm_content,'') as utm_content,
    nullif(asf.utm_source,'') as utm_source,
    nullif(asf.utm_medium,'') as utm_medium,
    nullif(asf.sp_source,'') as sp_source,
    nullif(asf.gclid,'') as gclid,
    nullif(os.bounce_ind,'') as bounce_ind,
    nullif(os.web_orders,'') as web_orders,
    nullif(os.web_demand_usd,'') as web_demand_usd,
    nullif(asf.session_type,'') as session_type
 FROM
 (
    SELECT
        asf.activity_date_pacific,
        asf.session_id,
        asf.channelcountry,
        asf.channel,
        asf.experience,
        CASE
            WHEN LOWER(asf.referrer) LIKE '%google%' THEN 'google'
            WHEN LOWER(asf.referrer) LIKE '%bing%' THEN 'bing'
            WHEN LOWER(asf.referrer) LIKE '%yahoo%' THEN 'yahoo'
            WHEN LOWER(asf.referrer) LIKE '%facebook%'  THEN 'facebook'
            WHEN LOWER(asf.referrer) LIKE '%instagram%'  THEN 'instagram'
            WHEN LOWER(asf.referrer) LIKE '%pinterest%' THEN 'pinterest'
            WHEN LOWER(asf.referrer) LIKE '%twitter.com%' THEN 'twitter'
            ELSE asf.referrer
                END AS referrer,
        COALESCE(asf.requesteddestination,asf.actualdestination) AS url_destination,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'utm_channel=[^&]*',0), 'utm_channel=') AS utm_channel,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'siteid=[^&]*',0), 'siteid=') AS siteid,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'utm_campaign=[^&]*',0), 'utm_campaign=') AS utm_campaign,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'sp_campaign=[^&]*',0), 'sp_campaign=') AS sp_campaign,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'utm_term=[^&]*',0), 'utm_term=') AS utm_term,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'utm_content=[^&]*',0), 'utm_content=') AS utm_content,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'utm_source=[^&]*',0), 'utm_source=') AS utm_source,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'utm_medium=[^&]*',0), 'utm_medium=') AS utm_medium,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'sp_source=[^&]*',0), 'sp_source=') AS sp_source,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'gclid=[^&]*',0), 'gclid=') AS gclid,
        asf.session_type
    FROM ace_etl.analytical_session_fact asf
    WHERE 1=1
        AND asf.first_arrived_event in (0,1)
        AND asf.activity_date_pacific BETWEEN '2024-08-18' AND '2024-08-18'
        and asf.active_session_flag = 1
) asf
    LEFT JOIN order_submitted os
        ON os.session_id = asf.session_id
);


-- For the unmatched events, hard code to attribute it to different categories
CREATE OR REPLACE TEMPORARY VIEW mkt_cat as (
    SELECT
    utm_param.activity_date_pacific,
    utm_param.session_id,
    utm_param.channelcountry,
    utm_param.channel,
    utm_param.experience,
    utm_param.referrer,
    utm_param.utm_channel,
    utm_param.siteid,
    utm_param.utm_campaign,
    utm_param.sp_campaign,
    utm_param.utm_term,
    utm_param.utm_content,
    utm_param.utm_source,
    utm_param.utm_medium,
    utm_param.sp_source,
    utm_param.gclid,
    ucl.funnel_type,
    utm_param.bounce_ind,
    utm_param.web_orders,
    utm_param.web_demand_usd,
    utm_param.session_type,
    CASE
        WHEN utm_param.referrer = 'App-PushNotification' OR utm_param.utm_channel LIKE '%apppush%' THEN 'UNPAID'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.referrer IS NULL AND utm_param.utm_channel IS NULL THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer NOT IN ('google','bing','yahoo','facebook','instagram','pinterest',
                                                                'App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('google','bing','yahoo') THEN 'UNPAID'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('facebook','instagram','pinterest','twitter') THEN 'UNPAID'
            ELSE mch.marketing_type
                END AS mrkt_type,
    CASE
        WHEN utm_param.referrer = 'App-PushNotification' OR utm_param.utm_channel LIKE '%apppush%' THEN 'EMAIL'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.referrer IS NULL AND utm_param.utm_channel IS NULL THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer NOT IN ('google','bing','yahoo','facebook','instagram','pinterest',
                                                                'App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('google','bing','yahoo') THEN 'SEO'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('facebook','instagram','pinterest','twitter') THEN 'UNPAID_OTHER'
            ELSE mch.finance_rollup
                END AS finance_rollup,
    CASE
        WHEN (utm_param.referrer = 'App-PushNotification' AND utm_param.utm_channel IS NULL OR utm_param.utm_channel = 'apppush_ret_p')
        	OR utm_param.utm_channel = 'apppush_plan_ret_p' THEN 'APP_PUSH_PLANNED'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.referrer IS NULL AND utm_param.utm_channel IS NULL THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer NOT IN ('google','bing','yahoo','facebook','instagram','pinterest',
                                                                'App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('google','bing','yahoo') THEN 'SEO_SEARCH'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('facebook','instagram','pinterest','twitter') THEN 'SOCIAL_ORGANIC'
        ELSE mch.finance_detail
            END AS finance_detail
FROM utm_param
    LEFT JOIN ace_etl.utm_channel_lookup ucl
        ON lower(ucl.utm_mkt_chnl) = lower(utm_param.utm_channel)
    LEFT JOIN ace_etl.marketing_channel_hierarchy mch
        ON lower(mch.join_channel) = lower(ucl.bi_channel)
);


-- Categorized null value on mrkt_touch,mrkt_type,finance_rollup,finance_detail to be UNATT
CREATE OR REPLACE TEMPORARY VIEW ssa_mkt_attr_fact_output AS
(
SELECT
    mkt_cat.activity_date_pacific,
    mkt_cat.channelcountry,
    mkt_cat.channel,
    mkt_cat.experience,
    mkt_cat.session_id,
    mkt_cat.referrer,
    mkt_cat.siteid,
    mkt_cat.utm_source,
    mkt_cat.utm_medium,
    mkt_cat.sp_source,
    mkt_cat.utm_channel,
    mkt_cat.utm_campaign,
    mkt_cat.sp_campaign,
    mkt_cat.utm_term,
    mkt_cat.utm_content,
    mkt_cat.gclid,
    mkt_cat.bounce_ind,
    mkt_cat.funnel_type,
    mkt_cat.web_orders,
    mkt_cat.web_demand_usd,
    mkt_cat.session_type,
    CASE
        WHEN mkt_cat.finance_rollup IS NULL THEN 'U'
        WHEN mkt_cat.finance_rollup = 'BASE' THEN 'N'
        ELSE 'Y'
            END AS mrkt_touch,
    CASE
        WHEN mkt_cat.mrkt_type IS NULL THEN 'UNATTR/TIEOUT'
        ELSE mkt_cat.mrkt_type
            END AS mrkt_type,
    CASE
        WHEN mkt_cat.finance_rollup IS NULL THEN 'UNATTR/TIEOUT'
        ELSE mkt_cat.finance_rollup
            END AS finance_rollup,
    CASE
        WHEN mkt_cat.finance_detail IS NULL THEN 'UNATTRIBUTED'
        ELSE mkt_cat.finance_detail
            END AS finance_detail,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM mkt_cat
);

insert overwrite table ace_etl.ssa_mkt_attr_fact partition (activity_date_pacific)
select /*+ REPARTITION(100) */
    channelcountry
    , channel
    , experience
    , session_id
    , referrer
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
    , bounce_ind
    , funnel_type
    , web_orders
    , cast(web_demand_usd as double) web_demand_usd
    , session_type
    , mrkt_touch
    , mrkt_type
    , finance_rollup
    , finance_detail
    , dw_sys_load_tmstp
    , activity_date_pacific
from ssa_mkt_attr_fact_output;


-- Writing output to teradata landing table.
-- This should match the "sql_table_reference" indicated on the .json file.
insert overwrite table ssa_mkt_attr_fact_ldg_output
select * from ssa_mkt_attr_fact_output
;
