SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=funnel_reach_freq_vw_11521_ACE_ENG;
     Task_Name=funnel_reach_freq_vw;'
     FOR SESSION VOLATILE;

/*

T2/View Name: funnel_reach_freq_vw

Note:
A combined view of the reach-freq tables.

*/

CREATE VIEW {funnel_io_t2_schema}.funnel_reach_freq_vw
AS 
LOCK ROW FOR ACCESS
SELECT day_date,
       data_source_type,
       data_source_name,
       cast(platform as varchar(20)) as platform,
       trim(campaign_id) as campaign_id,
       campaign_name,
       campaign_objective,
       overall_reach,
       overall_freq,
       avg_daily_reach,
       avg_daily_freq
from(
	   Select day_date,
       data_source_type,
       data_source_name,
       CAST('fp_tiktok' as VARCHAR(20)) as platform,
       campaign_id,
       campaign_name,
       campaign_objective,
       daily_reach as overall_reach,
       daily_frequency as overall_freq,
       NULL as avg_daily_reach,
       NULL as avg_daily_freq
       from {funnel_io_t2_schema}.nmn_fp_tiktok_reach_and_freq
       Union ALL
       Select day_date,
       data_source_type,
       data_source_name,
       cast('fp_pinterest' as varchar(20)) as platform,
       campaign_id,
       campaign_name,
       campaign_objective,
       Null as overall_reach,
       Null as overall_freq,
       daily_reach as avg_daily_reach,
       daily_frequency as avg_daily_freq
       from {funnel_io_t2_schema}.nmn_fp_pinterest_reach_and_freq
       union all
       Select a.day_date,
       a.data_source_type,
       a.data_source_name,
       cast('fp_facebook' as varchar(20)) as platform,
       b.campaign_id,
       b.campaign_name,
       a.campaign_objective,
       a.daily_reach as overall_reach,
       a.daily_frequency as overall_freq,
       NULL as avg_daily_reach,
       NULL as avg_daily_freq
       from {funnel_io_t2_schema}.nmn_fp_facebook_reach_and_freq a
       inner join (
       	Select campaign_id, 
       		   campaign_name
       		   from {funnel_io_t2_schema}.fp_funnel_cost_fact 
       		   where stats_date>date'2022-01-30' 
       		   and file_name = 'fp_facegram'
       		   group by 1,2
       		   ) b
       on trim(a.campaign_id) = trim(b.campaign_id)
       Union all
       Select a.day_date,
       a.data_source_type,
       a.data_source_name,
       cast('rack_facebook' as varchar(20)) as platform,
       b.campaign_id,
       b.campaign_name,
       a.campaign_objective,
       a.daily_reach as overall_reach,
       a.daily_frequency as overall_freq,
       NULL as avg_daily_reach,
       NULL as avg_daily_freq
       from {funnel_io_t2_schema}.nmn_rack_facebook_reach_and_freq a
       inner join (
       	Select campaign_id, 
       		   campaign_name
       		   from {funnel_io_t2_schema}.rack_funnel_cost_fact 
       		   where stats_date>date'2022-01-30' 
       		   and file_name = 'rack_facegram'
       		   group by 1,2
       		   ) b
       on trim(a.campaign_id) = trim(b.campaign_id)
       ) rf
;

SET QUERY_BAND = NONE FOR SESSION;
