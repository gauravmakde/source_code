SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_outlier_summary_11521_ACE_ENG;
     Task_Name=ddl_site_performance_outlier_summary_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_DIGENG.site_performance_outlier_summary_ldg
Team/Owner: Digital Product Analytics
Date Modified: 04/06/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_DIGENG', 'site_performance_outlier_summary_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE T2DL_DAS_DIGENG.site_performance_outlier_summary_ldg
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
	   avg_page_interactive DECIMAL(12,4),
	   eventtime DATE
	 )
PRIMARY INDEX (eventtime)
;

SET QUERY_BAND = NONE FOR SESSION;