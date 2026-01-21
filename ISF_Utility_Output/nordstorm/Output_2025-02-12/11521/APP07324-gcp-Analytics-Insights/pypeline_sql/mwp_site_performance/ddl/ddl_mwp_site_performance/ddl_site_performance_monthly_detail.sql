SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=ddl_mwp_site_performance_11521_ACE_ENG;
     Task_Name=ddl_site_performance_monthly_detail;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_DIGENG.site_performance_monthly_detail
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

-- To Support the Site Performance MBR Dashboard
-- Weekly Refresh
*/

--drop table {mwp_t2_schema}.site_performance_monthly_detail;

CREATE MULTISET TABLE {mwp_t2_schema}.site_performance_monthly_detail
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
	   month_idnt INT,
	   dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	 )
PRIMARY INDEX (month_idnt)
PARTITION BY Range_N(month_idnt BETWEEN 202101 AND 202812)
;

-- Table Comment (STANDARD)
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail IS 'MWP Site Performance - Monthly Performance';
-- Column comments (OPTIONAL)
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.navigationtype 			IS 'Type of Navigation (HARD/SOFT)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.pagetype 			IS 'Type of Webpage';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.experience 			IS 'Type of Device (MOBILE/DESKTOP)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.brand_country 			IS 'Channel & Country';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.p25_pageinteractive 			IS 'AVG Page Interactive Time (25% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.p50_pageinteractive 			IS 'AVG Page Interactive Time (50% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.p75_pageinteractive 			IS 'AVG Page Interactive Time (75% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.p90_pageinteractive 			IS 'AVG Page Interactive Time (90% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.p95_pageinteractive 			IS 'AVG Page Interactive Time (95% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.p99_pageinteractive 			IS 'AVG Page Interactive Time (99% of Total Interactions)';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.Soft_Navs 				IS 'Total Number of SOFT Navigations';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.page_views 				IS 'Total Number of Page Views';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.month_idnt 				IS 'Fiscal Month Number';
COMMENT ON  {mwp_t2_schema}.site_performance_monthly_detail.dw_sys_load_tmstp		IS 'Data load timestamp';


SET QUERY_BAND = NONE FOR SESSION;