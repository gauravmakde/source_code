SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=nmn_cost_data_vw_11521_ACE_ENG;
     Task_Name=nmn_cost_data_vw;'
     FOR SESSION VOLATILE;

/*

T2/View Name: funnel_cost_fact_vw
Team/Owner: AE
Date Created/Modified: 2023-02-09

Note:
A combined view of the rack_funnel_cost_fact and fp_funnel_cost_fact tables.

*/

REPLACE VIEW {nmn_t2_schema}.nmn_cost_data_vw
AS
LOCK ROW FOR ACCESS

select 
      stats_date,
      file_name,
      sourcetype,
      currency ,
      sourcename,
      media_type,
      device_type,
      campaign_name,
      campaign_id,
      campaign_type,
      sum(cost) as cost,
      sum(impressions) as impressions ,
      sum(clicks) as clicks ,
      sum(link_clicks) as link_clicks,
      sum(conversions) as conversions,
      sum(conversion_value) as conversion_value,
      sum(engagements) as engagemnets,
      sum(video_views) as video_views,
      sum(likes) as likes 
from {nmn_t2_schema}.fp_nmn_cost_data
where sourcename in ('Criteo - Retail Media Platform - Nordstrom', 'DV360 NMN', 'Nordstrom Media Network - 1900074490', 'Nordstrom Media Network - Nordstrom Shopping - 9836539670','Nordstrom Media Network - Discovery Ads - 3680971233', 'Incremental Levers - Campaign Level', 'Nordstrom RMN', 'Nordstrom Media Network')
group by 1,2,3,4,5,6,7,8,9,10

UNION ALL 

select 
      stats_date,
      file_name,
      sourcetype,
      currency ,
      sourcename,
      media_type,
      device_type,
      campaign_name,
      campaign_id,
      campaign_type,
      sum(cost) as cost,
      sum(impressions) as impressions ,
      sum(clicks) as clicks ,
      sum(link_clicks) as link_clicks,
      sum(conversions) as conversions,
      sum(conversion_value) as conversion_value,
      sum(engagements) as engagemnets,
      sum(video_views) as video_views,
      sum(likes) as likes 
from {nmn_t2_schema}.rack_nmn_cost_data
where sourcename in ('Criteo - Retail Media Platform - Nordstrom Rack', 'Nordstrom Rack Co-Op')
group by 1,2,3,4,5,6,7,8,9,10 
;


SET QUERY_BAND = NONE FOR SESSION;
