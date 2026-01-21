SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=google_iqv_cat_11521_ACE_ENG;
     Task_Name=ddl_google_iqv_cat_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.google_iqv_cat
Team/Owner: Analytics Engineering
Date Created/Modified:15/02/2024

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


--Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'google_iqv_cat_ldg', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.google_iqv_cat_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (	
	QueryLabel VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	QueryType VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	ReportDate DATE FORMAT 'YY/MM/DD',
	TimeGranularity VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	GeoCriteriaId FLOAT,
	GeoName VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	GeoType VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	IndexedQueryVolume FLOAT
      )

PRIMARY INDEX (QueryLabel,QueryType,ReportDate,TimeGranularity,GeoName,GeoType)
;


SET QUERY_BAND = NONE FOR SESSION;
