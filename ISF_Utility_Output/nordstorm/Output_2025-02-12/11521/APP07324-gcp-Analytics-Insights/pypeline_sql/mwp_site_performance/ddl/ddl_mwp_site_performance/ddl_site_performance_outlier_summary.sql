SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=ddl_mwp_site_performance_11521_ACE_ENG;
     Task_Name=ddl_site_performance_outlier_summary;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_DIGENG.site_performance_outlier_summary
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

Note:
-- To Support the Site Performance Outlier Summary Dashboard
-- Daily Refresh
*/

--drop table {mwp_t2_schema}.site_performance_outlier_summary;

CREATE MULTISET TABLE {mwp_t2_schema}.site_performance_outlier_summary
	 ,FALLBACK
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT
     ,DEFAULT MERGEBLOCKRATIO
	 (
	   eventtime DATE,
	   navigationtype VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	   pagetype VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	   experience VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,	
	   brand_country  VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	   avg_page_interactive DECIMAL(12,4),
	   dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	 )  
PRIMARY INDEX (eventtime)
PARTITION BY Range_N(eventtime BETWEEN DATE '2021-01-31' AND DATE '2028-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary IS 'MWP Site Performance - Outlier Summary';
-- Column comments (OPTIONAL)
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary.eventtime 			IS 'Date of event';
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary.navigationtype 			IS 'Type of Navigation (HARD/SOFT)';
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary.pagetype 			IS 'Type of Webpage';
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary.experience 			IS 'Type of Device (MOBILE/DESKTOP)';
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary.brand_country 			IS 'Channel & Country';
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary.avg_page_interactive 			IS 'Average Page Interactive Time';
COMMENT ON  {mwp_t2_schema}.site_performance_outlier_summary.dw_sys_load_tmstp		IS 'Data load timestamp';

SET QUERY_BAND = NONE FOR SESSION;