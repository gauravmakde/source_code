SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_weekly_detail_11521_ACE_ENG;
     Task_Name=ddl_site_performance_weekly_detail_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_DIGENG.site_performance_weekly_detail_ldg
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mwp_t2_schema}', 'site_performance_weekly_detail_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {mwp_t2_schema}.site_performance_weekly_detail_ldg
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
	   week_idnt INT
	 )  
PRIMARY INDEX (week_idnt)  
;

SET QUERY_BAND = NONE FOR SESSION;