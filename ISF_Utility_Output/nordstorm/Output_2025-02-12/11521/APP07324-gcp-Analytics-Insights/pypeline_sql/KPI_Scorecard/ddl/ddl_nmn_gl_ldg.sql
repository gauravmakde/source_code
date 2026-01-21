SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=nmn_gl_11521_ACE_ENG;
     Task_Name=ddl_nmn_gl_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.nmn_gl
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'nmn_gl_ldg', OUT_RETURN_MSG);

create multiset table {kpi_scorecard_t2_schema}.nmn_gl_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	Fiscal_year INTEGER,    
	fiscal_month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,	
	TY_Sales FLOAT,
	LY_Sales FLOAT,
	Plan_Sales FLOAT,
	TY_Earnings FLOAT,
	LY_Earnings FLOAT,
	Plan_Earnings FLOAT 
      )

PRIMARY INDEX (Fiscal_year,fiscal_month)
;


SET QUERY_BAND = NONE FOR SESSION;


