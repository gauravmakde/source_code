SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_facebook_reach_and_freq_11521_ACE_ENG;
     Task_Name=rack_facebook_reach_and_freq;'
     FOR SESSION VOLATILE;

/*
Table: T2DL_DAS_FUNNEL_IO.nmn_rack_facebook_reach_and_freq
Owner: Analytics Engineering
Modified: 2024-04-02

SQL moves data from the landing table to the final T2 table for the lookback period.
*/


DELETE
FROM  {funnel_io_t2_schema}.nmn_rack_facebook_reach_and_freq
;
INSERT INTO {funnel_io_t2_schema}.nmn_rack_facebook_reach_and_freq
SELECT
     day_date,
      data_source_type,
      data_source_name,
      campaign_id,
      campaign_name,
      campaign_objective,
      attribution_setting,
      daily_reach,
      daily_frequency
FROM  {funnel_io_t2_schema}.nmn_rack_facebook_reach_and_freq_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;
