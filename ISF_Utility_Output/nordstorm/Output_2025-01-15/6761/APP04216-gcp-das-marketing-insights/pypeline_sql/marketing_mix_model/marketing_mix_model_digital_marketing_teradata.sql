SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_digital_marketing_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_digitial_marketing_teradata_job;'
FOR SESSION VOLATILE;

ET;

create multiset volatile table funnel_cost_mmm as (
select
        stats_date
        , platform
        , banner
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
        , ar_flag as bar
        , CASE WHEN funding = 'EX' then 'Yes' else 'No' end nmn_flag
        , CASE WHEN campaign_name like '%beauty%' then 'Yes' else 'No' end beauty_flag
        , CASE WHEN video_views > 0 then 'Yes' else 'No' end video_flag
        , SUM(cost) AS cost
        , SUM(impressions) AS impressions
        , SUM(clicks) AS clicks
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
                                         OR  (sourcename LIKE '%Nordstrom - MSAN%')
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
                        WHEN advertising_channel IN ('SEARCH','SHOPPING','PERFORMANCE_MAX') then
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
                                                OR media_type LIKE '%Price comparison%')
                                                 OR (campaign_name LIKE '%shopping%')
                                        THEN 'PAID_SHOPPING'
                                END
                        WHEN (media_type LIKE '%display%' OR campaign_name LIKE '%display%' or campaign_name like '%_EX_DS_%'
                        or campaign_name like '%EX_DV360%') THEN
                                CASE WHEN sourcename LIKE '%Nordstrom US Video - 5038267247%'
                                        THEN 'VIDEO'
                                        ELSE 'DISPLAY'
                                END
                        WHEN Media_Type='video' THEN 'VIDEO'
                        WHEN file_name like '%criteo%' THEN 'DISPLAY'
                        WHEN STRTOK(file_name,'_',2) in ('pinterest','snapchat','facegram','tiktok') THEN 'PAID_SOCIAL'
                        WHEN (campaign_name LIKE '%@_Video@_%' ESCAPE '@' or campaign_name LIKE '%CTV%' or campaign_name LIKE '%Video%'
                       or campaign_name LIKE '%Programmatic%')
                                THEN 'VIDEO'
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
                        THEN 'APP'
                        WHEN device_type LIKE '%tv%'
                                OR device_type LIKE '%computer%'
                                OR device_type LIKE '%desktop%'
                        THEN 'WEB'
                        ELSE 'Unknown'
                END AS device_type
                -- some campaign_names are low_nd while the adgroup_name is mid_nd.  How to handle?
                -- This simplified CASE WHEN statement seems to negate the need fOR the specific campagin name call outs FROM the original sql.
                , CASE WHEN campaign_name  LIKE '%low_nd%'
                                OR adgroup_name  LIKE '%low_nd%'
                                --OR campaign_name LIKE 'LOW@_%' ESCAPE '@'
                        THEN 'LOW'
                WHEN  campaign_name LIKE '%mid_nd%'
                                OR campaign_name LIKE '%_mid_%'
                                OR campaign_name LIKE '%midfunnel%'
                                OR adgroup_name LIKE '%mid_nd%'
                                --OR campaign_name LIKE 'MID@_%' ESCAPE '@'
                        THEN 'MID'
                WHEN campaign_name LIKE '%up_nd%'
                                OR adgroup_name LIKE '%up_nd%'
                                --OR campaign_name LIKE 'UP@_%' ESCAPE '@'
                        THEN 'UP'
                        ELSE 'Unknown'
                END AS funnel_type
                , CASE
                WHEN campaign_name like '%@_nd@_%' ESCAPE '@' or adgroup_name like '%@_nd@_%' ESCAPE '@'then 'ND'
                WHEN campaign_name like '%@_ex@_%' ESCAPE '@' or adgroup_name like '%@_ex@_%' ESCAPE '@' then 'EX'
                WHEN adgroup_name LIKE '%@_P@_%'
                                OR campaign_name LIKE '%@_P@_%' ESCAPE '@'
                                OR campaign_name LIKE '%@_Persistent@_%' ESCAPE '@'
                                OR sourcename IN ('Nordstrom Terms - 5601420631','Nordstrom Main - 5417253570')  -- adwords logic
                                OR sourcename IN ('DPA','DABA', 'Local Dynamic Ads', 'Social - 2018 Canada')      -- facebook logic
                                OR sourcename  in ('Nordstrom 2017 DPA Retention')  -- gemini logic
                                THEN 'Persistent'
                        WHEN adgroup_name LIKE '%vendor%'
                                OR adgroup_name LIKE '%_VF_%'
                                OR campaign_name LIKE '%_VF_%'
                                OR campaign_name LIKE '%icf%'
                                OR sourcename IN ('Nordstrom Acquisition - 4967171468','Nordstrom Canada Search - 1893217362','Nordstrom Shopping Coop - 9385258156')  -- adwords logic
                                THEN 'Coop'
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
                                OR campaign_name LIKE '%_Brand_%'
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
                , video_views
                , video100
                , video75
        from	t2dl_das_funnel_io.funnel_cost_fact_vw
        where stats_date between (current_date - 7) and (current_date - 1)
        and country <> 'CA'
        and file_name <> 'nmn_fp_pinterest'
) a
where platform <> 'rakuten'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index(stats_date,banner,device_type,adgroup_id,finance_detail) on commit preserve rows;

ET;
------EMAIL----------
------EMAIL----------
------EMAIL----------
CREATE MULTISET VOLATILE TABLE email_mkt AS (
SELECT
last_updated_date as send_date
,case
when brand = 'Nordstrom Rack' then 'R.COM'
else 'N.COM'
end banner
, 'LOW' funnel
, case when campaign_name like '%NMN%' then 'EX' else 'ND' end as funding
, 'EMAIL' as finance_detail
, 'Unknown' as BAR
,case when campaign_type = '' then 'unknown' else ('email'||' '||campaign_type) end platform
, campaign_name
, sum(total_sent_count)sends
,sum(total_sent_count)*0.00025 as cost
, sum(total_delivered_count) deliveries
, sum(total_clicked_count)clicks
, sum(total_opened_count) opens
, sum(unique_opened_count) opens_unique
, sum(ced_total_unsubscribed_count + cpp_total_unsubscribed_count) as unsubscribed

FROM PRD_NAP_USR_VWS.EMAIL_CONTACT_EVENT_HDR_FCT a
WHERE last_updated_date IS NOT NULL
	AND   campaign_type like '%marketing%'
  	AND   campaign_segment <> 'CA'
	AND   a.last_updated_date between (current_date - 7) and (current_date - 1)
group  by 1,2,3,4,5,6,7,8
)
WITH data PRIMARY INDEX (platform, send_date)
ON COMMIT PRESERVE ROWS;

ET;
-----UNION ALL DIFFERENT DIGITAL MARKETING SOURCES-------
create multiset volatile table dig_marketing as (
select
stats_date,
platform,
banner,
case when campaign_name like '%MSAN%' then 'PAID_SHOPPING'
when campaign_name like 'Nordstrom Rack | ENT | PG |%' then 'VIDEO'
when campaign_name like '%ND_Video_%' then 'VIDEO'
else finance_detail end as finance_detail,
OREPLACE(campaign_name,',','_')  AS campaign_name,
campaign_id,
OREPLACE(adgroup_name,',','_') AS adgroup_name,
adgroup_id,
OREPLACE(ad_name,',','_') AS ad_name,
ad_id,
device_type,
funnel_type,
funding,
bar,
case when platform = 'criteo' then 'Yes' else nmn_flag end as nmn_flag,
beauty_flag,
video_flag,
"cost",
impressions,
clicks,
video_views,
video100,
video75 ,
null as sends,
null as deliveries,
null as opens,
null as unsubscribed
from funnel_cost_mmm
where stats_date between (current_date - 7) and (current_date - 1)

UNION ALL

select
send_date as stats_date,
platform,
banner,
finance_detail,
campaign_name,
cast(null as varchar(4)) as campaign_id,
cast(null as varchar(4)) as adgroup_name,
cast(null as varchar(4)) as adgroup_id,
cast(null as varchar(4)) as ad_name,
cast(null as varchar(4)) as ad_id,
cast(null as varchar(4)) as device_type,
funnel as funnel_type,
funding,
BAR,
CASE WHEN funding = 'EX' then 'Yes' else 'No' end nmn_flag,
CASE WHEN campaign_name like '%beauty%' then 'Yes' else 'No' end beauty_flag,
cast(null as varchar(4)) as video_flag,
"cost",
null as impressions,
null as clicks,
null as video_views,
null as video100,
null as video75,
sends,
deliveries,
opens,
unsubscribed
from email_mkt
where stats_date between (current_date - 7) and (current_date - 1)

union all


select
communication_send_date,
case when campaign_type = 'DESERIALIZATION_DEFAULT_VALUE' then 'app push marketing planned'
when campaign_type = 'MARKETING_TRIGGERED' then 'app push marketing trigerred'
end as platform,
case when channel_brand = 'NORDSTROM_RACK' then 'R.COM' else 'N.COM' end as banner,
'APP PUSH' as finance_detail,
push_message_name as campaign_name,
cast(null as varchar(4)) as campaign_id,
cast(null as varchar(4)) as adgroup_name,
cast(null as varchar(4)) as adgroup_id,
cast(null as varchar(4)) as ad_name,
cast(null as varchar(4)) as ad_id,
'APP' as device_type,
'LOW' as funnel_type,'ND' as funding,
cast(null as varchar(4)) as BAR,
CASE WHEN funding = 'EX' then 'Yes' else 'No' end nmn_flag,
CASE WHEN push_message_name like '%beauty%' then 'Yes' else 'No' end beauty_flag,
cast(null as varchar(4)) as video_flag,
sum(push_sends_count) * 0.00025  as cost,
null as impressions,null as clicks,
null as video_views,null as video100,
null as video75,
sum(push_sends_count) as sends,
sum(push_deliveries_count) as deliveries,
sum(push_opens_count) as opend,
null as unsubscribed
from PRD_NAP_USR_VWS.CNTCT_EVNT_MOBILE_PUSH_AGG_FACT
where communication_send_date is not null
and campaign_type in ('DESERIALIZATION_DEFAULT_VALUE','MARKETING_TRIGGERED')
and communication_send_date between (current_date - 7) and (current_date - 1) -- data is correct and reliable starting this date
-- removing all test pushes
and push_message_name not like '% test%'
AND push_message_name not like '%\_test%' ESCAPE '\'
and push_message_name not like 'test%'
and push_message_name not like '[%'
--new addition by Nhan
and push_message_name not like 'Convey_Delivery_Delay%'
and push_message_name not like 'Convey_Reassurance%'
and push_message_name not like 'FOMO-Tools%'
and push_message_name <> 'OmniBOPUS-Production'
and push_message_name <> 'Welcome to OPT!'

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
WITH data PRIMARY INDEX (stats_date,platform,banner)
ON COMMIT PRESERVE ROWS;

ET;

DELETE FROM {proto_schema}.MMM_DIGITAL_MARKETING_LDG ALL;

INSERT INTO {proto_schema}.MMM_DIGITAL_MARKETING_LDG
select
stats_date,
platform,
banner,
case when campaign_name like '%CTV%' then 'CTV'
 when campaign_name like '%Video%' then 'VIDEO'
 when campaign_name like '%Display%' then 'DISPLAY'
else finance_detail end as finance_detail,
campaign_name,
campaign_id,
adgroup_name,
adgroup_id,
ad_name,
ad_id,
device_type,
funnel_type,
funding,bar,
nmn_flag,
beauty_flag,
video_flag,
"cost",
impressions,
clicks,
video_views,
video100,
video75,
sends,
deliveries,
opens,
unsubscribed,
CASE
    WHEN finance_detail IN ('PAID_SHOPPING', 'PAID_SEARCH_BRANDED', 'PAID_SEARCH_UNBRANDED') THEN
        CASE
            WHEN campaign_name LIKE '%psu_generic%' THEN 'GENERIC'
            WHEN campaign_name LIKE '%psu_supplier%' THEN 'SUPPLIER'
            WHEN campaign_name LIKE '%PMAX%' THEN 'PMAX'
            WHEN campaign_name LIKE '%_vf_' THEN 'VF'
            WHEN campaign_name LIKE '%shopping_STANDARD%' THEN 'STANDARD'
            WHEN campaign_name LIKE '%psb_brandplus%' THEN 'BRANDPLUS'
            WHEN campaign_name LIKE '%psb_BRAND%' THEN 'BRAND'
            WHEN campaign_name LIKE '%shopping_LIA%' THEN 'LIA'
            WHEN campaign_name LIKE '%MSAN%' THEN 'MSAN'
            WHEN campaign_name LIKE '%shopping_SMART%' THEN 'SMART'
            WHEN campaign_name LIKE '%AUDNET%' THEN 'AUDNET'
            WHEN campaign_name LIKE '%shopping_SHOWCASE%' THEN 'SHOWCASE'  END
ELSE 'UNKNOWN'
END AS ad_format,
case
when campaign_name in ('Up_ND_Social_Nordstrom_4561_xx_xx_NYC-Men',
						'PMAX - Flagship - NY Store Support - Store Driving_mid_nd_psu_pmaxstore_2024Q2_4552_US',
						'PMAX - Mens - NY Store Support - Store Driving_mid_nd_psu_pmaxstore_2024Q2_4552_US',
						'ACQ_MID_ND_Video_N__US_2024 H1 Store Driving Video_NYC',
						'Up_ND_Social_Nordstrom_4552_xx_xx_NYC') then 'NYC StoreDriving'
when campaign_name like '%boost%' then 'Boost'
when campaign_name like '%Anni%' then 'Anniversary'
when campaign_name like '%Anniversary%' then 'Anniversary'
when banner = 'N.COM' AND campaign_name like '%Bty%' then 'Beauty'
when banner = 'N.COM' AND campaign_name like '%Beauty%' then 'Beauty'
when campaign_name like '%NSO-%' then 'NSO'
when campaign_name like '%NSO%' then 'NSO'
when campaign_name like '%StoreDriving%' then 'StoreDriving'
when campaign_name like '%4538%' then 'StoreDriving'
when campaign_name like '%4445%' then 'StoreDriving'
when campaign_name like '%4390%' then 'StoreDriving'
when campaign_name like '%4380%' then 'StoreDriving'
when campaign_name like '%4576%' then 'StoreDriving'
when campaign_name like '%Store-%' then 'StoreDriving'
when campaign_name like '%Store%' then 'StoreDriving'
--when campaign_name like '%CTV%' then 'CTV'
--when banner = 'N.COM' AND campaign_name like '%CTR%' then 'CTR'
else 'other'
end as category,
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from dig_marketing;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
