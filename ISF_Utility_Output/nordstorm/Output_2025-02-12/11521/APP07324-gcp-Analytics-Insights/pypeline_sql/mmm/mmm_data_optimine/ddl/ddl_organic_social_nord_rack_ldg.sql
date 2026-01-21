SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=organic_social_nord_rack_11521_ACE_ENG;
     Task_Name=ddl_organic_social_nord_rack_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.organic_social_nord_rack
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'organic_social_nord_rack_ldg', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.organic_social_nord_rack_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	Day_Date DATE FORMAT 'YY/MM/DD',
Channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
funnel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
funding_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Impressions FLOAT,
Reach FLOAT,
Likes FLOAT,
Cost FLOAT
     )
PRIMARY INDEX (Day_Date,Channel,Platform,funnel,funding_type)
;


SET QUERY_BAND = NONE FOR SESSION;


