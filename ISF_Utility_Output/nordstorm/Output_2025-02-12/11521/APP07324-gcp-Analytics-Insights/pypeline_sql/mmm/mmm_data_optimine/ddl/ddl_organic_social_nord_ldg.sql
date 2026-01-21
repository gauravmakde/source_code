SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=organic_social_nord_11521_ACE_ENG;
     Task_Name=ddl_organic_social_nord_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.organic_social_nord
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'organic_social_nord_ldg', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.organic_social_nord_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	Day_Date DATE FORMAT 'YY/MM/DD',
Facebook_reach FLOAT,
Facebook_visit FLOAT,
Pinterest_Impressions FLOAT,
Facebook_Likes FLOAT
      )

PRIMARY INDEX (Day_Date)
;


SET QUERY_BAND = NONE FOR SESSION;





