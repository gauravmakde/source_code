SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=camelot_data_11521_ACE_ENG;
     Task_Name=ddl_camelot_data_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.camelot_data
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


--Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'camelot_data_ldg', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.camelot_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	start_date DATE FORMAT 'YY/MM/DD',
end_date DATE FORMAT 'YY/MM/DD',
banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
campaign VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
BAR VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Ad_Type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Funnel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
External_Funding FLOAT,
Impressions FLOAT,
Cost FLOAT
      )

PRIMARY INDEX (start_date,end_date,banner,campaign,dma,platform,channel,BAR,Ad_Type,Funnel)
;


SET QUERY_BAND = NONE FOR SESSION;
