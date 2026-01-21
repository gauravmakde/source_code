SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=ddl_mwp_site_performance_11521_ACE_ENG;
     Task_Name=ddl_site_performance_weekly_detail;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_DIGENG.site_performance_weekly_detail
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

-- To Support the Site Performance WBR Dashboard
-- Weekly Refresh
*/

--drop table {mwp_t2_schema}.site_performance_weekly_detail;

CREATE MULTISET TABLE {mwp_t2_schema}.site_performance_weekly_detail
	 ,FALLBACK
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT
     ,DEFAULT MERGEBLOCKRATIO
	 (
	   navigationtype VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	   pagetype VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	   experience VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,	
	   brand_country  VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	   p25_pageinteractive FLOAT,
	   p50_pageinteractive FLOAT,
	   p75_pageinteractive FLOAT,
	   p90_pageinteractive FLOAT,
	   p95_pageinteractive FLOAT,
	   p99_pageinteractive FLOAT,
	   Soft_Navs INT,
	   page_views INT,
	   week_idnt INT,
	   dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	 )  
PRIMARY INDEX (week_idnt)
PARTITION BY Range_N(week_idnt BETWEEN 202101 AND 202852)
;

-- Table Comment (STANDARD)
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail IS 'MWP Site Performance - Weekly Performance';
-- Column comments (OPTIONAL)
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.navigationtype 			IS 'Type of Navigation (HARD/SOFT)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.pagetype 			IS 'Type of Webpage';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.experience 			IS 'Type of Device (MOBILE/DESKTOP)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.brand_country 			IS 'Channel & Country';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.p25_pageinteractive 			IS 'AVG Page Interactive Time (25% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.p50_pageinteractive 			IS 'AVG Page Interactive Time (50% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.p75_pageinteractive 			IS 'AVG Page Interactive Time (75% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.p90_pageinteractive 			IS 'AVG Page Interactive Time (90% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.p95_pageinteractive 			IS 'AVG Page Interactive Time (95% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.p99_pageinteractive 			IS 'AVG Page Interactive Time (99% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.Soft_Navs 				IS 'Total Number of SOFT Navigations';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.page_views 				IS 'Total Number of Page Views';
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.week_idnt 				IS 'Fiscal Week Number'; 
COMMENT ON  {mwp_t2_schema}.site_performance_weekly_detail.dw_sys_load_tmstp		IS 'Data load timestamp';



SET QUERY_BAND = NONE FOR SESSION;