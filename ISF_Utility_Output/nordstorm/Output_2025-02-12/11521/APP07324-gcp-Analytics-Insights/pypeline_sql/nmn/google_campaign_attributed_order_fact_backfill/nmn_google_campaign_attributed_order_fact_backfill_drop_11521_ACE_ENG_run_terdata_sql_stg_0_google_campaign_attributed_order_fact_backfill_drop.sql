/*
T2/Table Name: T2DL_DAS_NMN.google_campaign_attributed_order_fact
Team/Owner: NMN / Customer Engagement Analytics
Usage: This script to drop old T2 table through a one-time DAG
*/

SET QUERY_BAND = 'App_ID=APP08998;
     DAG_ID=nmn_google_campaign_attributed_order_fact_backfill_drop_11521_ACE_ENG;
     Task_Name=google_campaign_attributed_order_fact_backfill_drop;'
     FOR SESSION VOLATILE;

--- QB blob omitted for space

drop table T2DL_DAS_NMN.google_campaign_attributed_order_fact_old;

SET QUERY_BAND = NONE FOR SESSION;
