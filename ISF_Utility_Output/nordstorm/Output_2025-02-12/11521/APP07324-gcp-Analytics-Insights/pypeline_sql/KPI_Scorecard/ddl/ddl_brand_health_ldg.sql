SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=brand_health_11521_ACE_ENG;
     Task_Name=ddl_brand_health_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.brand_health
Team/Owner: Analytics Engineering
Date Created/Modified:04/01/2024

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'brand_health_ldg', OUT_RETURN_MSG);

create multiset table {kpi_scorecard_t2_schema}.brand_health_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	Fiscal_Year INTEGER,
    Fiscal_Month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Banner  VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Aware FLOAT,
	Familiar FLOAT,
	Composite FLOAT,
	Shop FLOAT,
	Ly_Aware FLOAT,
	Ly_Familiar FLOAT,
	Ly_Composite FLOAT,
	Ly_Shop FLOAT
      )

PRIMARY INDEX (Fiscal_Year,Fiscal_Month,Banner)
;


SET QUERY_BAND = NONE FOR SESSION;




