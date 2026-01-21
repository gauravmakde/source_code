SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=seo_data_11521_ACE_ENG;
     Task_Name=ddl_seo_data_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.seo_data
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'seo_data_ldg', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.seo_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	day_date DATE FORMAT 'YY/MM/DD',
Banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Keyword_Type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Impressions FLOAT,
Clicks FLOAT
      )

PRIMARY INDEX (day_date,Banner,Keyword_Type)
;


SET QUERY_BAND = NONE FOR SESSION;


