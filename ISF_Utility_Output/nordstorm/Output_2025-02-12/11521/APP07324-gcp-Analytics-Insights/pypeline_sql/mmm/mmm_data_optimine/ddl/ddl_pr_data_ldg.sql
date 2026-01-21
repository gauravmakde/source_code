SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=pr_data_11521_ACE_ENG;
     Task_Name=ddl_pr_data_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.pr_data
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/



-- Comment out prior to merging to production.
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'pr_data_ldg', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.pr_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	Start_Date DATE FORMAT 'YY/MM/DD',
	End_Date DATE FORMAT 'YY/MM/DD',
	Region VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
	City VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
	State_name VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
	marketing_type VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Media_Outlet VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Title_name VARCHAR(1200) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Total_Readership decimal(12,2),
	Link VARCHAR(1250) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Author VARCHAR(1250) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Campaign VARCHAR(450) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Notes VARCHAR(450) CHARACTER SET UNICODE NOT CASESPECIFIC
      )

PRIMARY INDEX (Start_Date,End_Date,Region,City)
;

SET QUERY_BAND = NONE FOR SESSION;













