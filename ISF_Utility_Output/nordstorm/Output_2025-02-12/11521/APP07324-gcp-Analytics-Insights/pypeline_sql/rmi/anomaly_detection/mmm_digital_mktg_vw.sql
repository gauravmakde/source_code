SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_digital_mktg_vw_11521_ACE_ENG;
     Task_Name=mmm_digital_mktg_vw;'
     FOR SESSION VOLATILE;

--T2/View Name: T2DL_DAS_MOA_KPI.mmm_digital_mktg_vw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-11-26
--Note:
-- This view supports the Anamoly Detection Dashboard.


REPLACE VIEW {kpi_scorecard_t2_schema}.mmm_digital_mktg_vw
AS
LOCK ROW FOR ACCESS 
 SELECT 
week_idnt,		
platform,
banner,
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
bar,
nmn_flag,
beauty_flag,
video_flag,
ad_format,
category,
sum(cost) AS cost,
sum(impressions) AS impressions,
sum(clicks) AS clicks,
sum(video_views) AS video_views,
sum(video100) AS video100, 
sum(video75) AS video75,
sum(sends) AS sends,
sum(deliveries) AS deliveries,
sum(opens) AS opens,
sum(unsubscribed) AS unsubscribed,
sum(LW_cost) AS LW_cost,
sum(LW_impressions) AS LW_impressions,
sum(LW_clicks) AS LW_clicks,
sum(LW_video_views) AS LW_video_views,
sum(LW_video100) AS LW_video100, 
sum(LW_video75) AS LW_video75,
sum(LW_sends) AS LW_sends,
sum(LW_deliveries) AS LW_deliveries,
sum(LW_opens) AS LW_opens,
sum(LW_unsubscribed) AS LW_unsubscribed,
sum(LY_cost) AS LY_cost,
sum(LY_impressions) AS LY_impressions,
sum(LY_clicks) AS LY_clicks,
sum(LY_video_views) AS LY_video_views,
sum(LY_video100) AS LY_video100, 
sum(LY_video75) AS LY_video75,
sum(LY_sends) AS LY_sends,
sum(LY_deliveries) AS LY_deliveries,
sum(LY_opens) AS LY_opens,
sum(LY_unsubscribed) AS LY_unsubscribed      
 FROM (
SELECT b.week_idnt,
platform,
banner,
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
bar,
nmn_flag,
beauty_flag,
video_flag,
ad_format,
category,
sum(cost) AS cost,
sum(impressions) AS impressions,
sum(clicks) AS clicks,
sum(video_views) AS video_views,
sum(video100) AS video100, 
sum(video75) AS video75,
sum(sends) AS sends,
sum(deliveries) AS deliveries,
sum(opens) AS opens,
sum(unsubscribed) AS unsubscribed,
CAST(0 AS FLOAT) AS LW_cost,
CAST(0 AS FLOAT) AS LW_impressions,
CAST(0 AS FLOAT) AS LW_clicks,
CAST(0 AS FLOAT) AS LW_video_views,
CAST(0 AS FLOAT) AS LW_video100, 
CAST(0 AS FLOAT) AS LW_video75,
CAST(0 AS FLOAT) AS LW_sends,
CAST(0 AS FLOAT) AS LW_deliveries,
CAST(0 AS FLOAT) AS LW_opens,
CAST(0 AS FLOAT) AS LW_unsubscribed,
CAST(0 AS FLOAT) AS LY_cost,
CAST(0 AS FLOAT) AS LY_impressions,
CAST(0 AS FLOAT) AS LY_clicks,
CAST(0 AS FLOAT) AS LY_video_views,
CAST(0 AS FLOAT) AS LY_video100, 
CAST(0 AS FLOAT) AS LY_video75,
CAST(0 AS FLOAT) AS LY_sends,
CAST(0 AS FLOAT) AS LY_deliveries,
CAST(0 AS FLOAT) AS LY_opens,
CAST(0 AS FLOAT) AS LY_unsubscribed
       FROM {kpi_scorecard_t2_schema}.mmm_digital_marketing  a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.stats_date = b.day_date
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19             
		UNION ALL
SELECT
CASE WHEN MOD(b.week_idnt,100) > 51 THEN b.week_idnt +49
ELSE b.week_idnt +1
END AS week_idnt, 
platform,
banner,
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
bar,
nmn_flag,
beauty_flag,
video_flag,
ad_format,
category,			  
CAST(0 AS FLOAT) AS cost,
CAST(0 AS FLOAT) AS impressions,
CAST(0 AS FLOAT) AS clicks,
CAST(0 AS FLOAT) AS video_views,
CAST(0 AS FLOAT) AS video100, 
CAST(0 AS FLOAT) AS video75,
CAST(0 AS FLOAT) AS sends,
CAST(0 AS FLOAT) AS deliveries,
CAST(0 AS FLOAT) AS opens,
CAST(0 AS FLOAT) AS unsubscribed,
sum(cost) AS LW_cost,
sum(impressions) AS LW_impressions,
sum(clicks) AS LW_clicks,
sum(video_views) AS LW_video_views,
sum(video100) AS LW_video100, 
sum(video75) AS LW_video75,
sum(sends) AS LW_sends,
sum(deliveries) AS LW_deliveries,
sum(opens) AS LW_opens,
sum(unsubscribed) AS LW_unsubscribed,
CAST(0 AS FLOAT) AS LY_cost,
CAST(0 AS FLOAT) AS LY_impressions,
CAST(0 AS FLOAT) AS LY_clicks,
CAST(0 AS FLOAT) AS LY_video_views,
CAST(0 AS FLOAT) AS LY_video100, 
CAST(0 AS FLOAT) AS LY_video75,
CAST(0 AS FLOAT) AS LY_sends,
CAST(0 AS FLOAT) AS LY_deliveries,
CAST(0 AS FLOAT) AS LY_opens,
CAST(0 AS FLOAT) AS LY_unsubscribed 
       FROM {kpi_scorecard_t2_schema}.mmm_digital_marketing  a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.stats_date = b.day_date
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
       UNION ALL
SELECT 
CASE WHEN MOD(b.week_idnt,100) > 52 THEN b.week_idnt +51
ELSE b.week_idnt +99
END AS week_idnt,
platform,
banner,
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
bar,
nmn_flag,
beauty_flag,
video_flag,
ad_format,
category,	
CAST(0 AS FLOAT) AS cost,
CAST(0 AS FLOAT) AS impressions,
CAST(0 AS FLOAT) AS clicks,
CAST(0 AS FLOAT) AS video_views,
CAST(0 AS FLOAT) AS video100, 
CAST(0 AS FLOAT) AS video75,
CAST(0 AS FLOAT) AS sends,
CAST(0 AS FLOAT) AS deliveries,
CAST(0 AS FLOAT) AS opens,
CAST(0 AS FLOAT) AS unsubscribed,
CAST(0 AS FLOAT) AS LW_cost,
CAST(0 AS FLOAT) AS LW_impressions,
CAST(0 AS FLOAT) AS LW_clicks,
CAST(0 AS FLOAT) AS LW_video_views,
CAST(0 AS FLOAT) AS LW_video100, 
CAST(0 AS FLOAT) AS LW_video75,
CAST(0 AS FLOAT) AS LW_sends,
CAST(0 AS FLOAT) AS LW_deliveries,
CAST(0 AS FLOAT) AS LW_opens,
CAST(0 AS FLOAT) AS LW_unsubscribed,
sum(cost) AS LY_cost,
sum(impressions) AS LY_impressions,
sum(clicks) AS LY_clicks,
sum(video_views) AS LY_video_views,
sum(video100) AS LY_video100, 
sum(video75) AS LY_video75,
sum(sends) AS LY_sends,
sum(deliveries) AS LY_deliveries,
sum(opens) AS LY_opens,
sum(unsubscribed) AS LY_unsubscribed  
       FROM {kpi_scorecard_t2_schema}.mmm_digital_marketing  a
         LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.stats_date = b.day_date
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19) c
        WHERE week_idnt between (SELECT MIN(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.mmm_digital_marketing  a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.stats_date = b.day_date)
					   and (SELECT MAX(week_idnt)
                     FROM {kpi_scorecard_t2_schema}.mmm_digital_marketing  a
                       LEFT JOIN prd_nap_usr_vws.DAY_CAL_454_DIM b ON a.stats_date = b.day_date)
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19;


SET QUERY_BAND = NONE FOR SESSION;

