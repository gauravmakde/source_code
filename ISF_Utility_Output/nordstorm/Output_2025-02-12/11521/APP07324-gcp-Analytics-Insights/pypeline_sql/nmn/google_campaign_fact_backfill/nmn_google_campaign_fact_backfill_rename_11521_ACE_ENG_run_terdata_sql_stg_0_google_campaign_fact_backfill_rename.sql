/*
T2/Table Name: T2DL_DAS_NMN.google_campaign_fact
Team/Owner: NMN / Customer Engagement Analytics
Usage: This script to rename existing T2 table through a one-time DAG
*/

SET QUERY_BAND = 'App_ID=APP08998;
     DAG_ID=nmn_google_campaign_fact_backfill_rename_11521_ACE_ENG;
     Task_Name=google_campaign_fact_backfill_rename;'
     FOR SESSION VOLATILE;

--- QB blob omitted for space

RENAME TABLE T2DL_DAS_NMN.google_campaign_fact to T2DL_DAS_NMN.google_campaign_fact_old;

SET QUERY_BAND = NONE FOR SESSION;
