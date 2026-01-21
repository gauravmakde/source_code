SET QUERY_BAND = 'App_ID=APP08743;
     DAG_ID=mta_alter_tables_11521_ACE_ENG;
     Task_Name=mta_alter_tables;'
     FOR SESSION VOLATILE;



ALTER TABLE T2DL_DAS_MTA.mta_acp_scoring_fact add utm_campaign VARCHAR(1000);

ALTER TABLE T2DL_DAS_MTA.mta_last_touch add utm_campaign VARCHAR(1000);

ALTER TABLE T2DL_DAS_MTA.MTA_UTM_DEMAND_AGG add utm_campaign VARCHAR(1000);
ALTER TABLE T2DL_DAS_MTA.mkt_utm_agg add utm_campaign VARCHAR(1000);

ALTER TABLE T2DL_DAS_MTA.mta_email_performance add campaign_name VARCHAR(1000);
ALTER TABLE T2DL_DAS_MTA.MTA_PAID_CAMPAIGN_DAILY add campaign_name VARCHAR(1000);


SET QUERY_BAND = NONE FOR SESSION;