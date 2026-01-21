SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=gl_sourced_11521_ACE_ENG;
     Task_Name=ddl_gl_sourced_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.gl_sourced
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'gl_sourced_ldg', OUT_RETURN_MSG);

create multiset table {kpi_scorecard_t2_schema}.gl_sourced_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	Fiscal_year INTEGER,
    Box_type VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC ,
	Metric VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	fiscal_month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	TY_Cost FLOAT,
    Plan_Cost FLOAT,
	LY_Cost FLOAT,
	TY_Sales FLOAT,
	Plan_Sales FLOAT,
	LY_Sales FLOAT  
      )

PRIMARY INDEX (Fiscal_year,Box_type,Metric,fiscal_month)
;


SET QUERY_BAND = NONE FOR SESSION;

