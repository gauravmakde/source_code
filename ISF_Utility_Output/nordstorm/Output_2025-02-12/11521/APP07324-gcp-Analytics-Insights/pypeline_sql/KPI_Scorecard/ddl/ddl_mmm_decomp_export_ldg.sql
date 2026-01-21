SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_decomp_export_11521_ACE_ENG;
     Task_Name=ddl_mmm_decomp_export_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.mmm_decomp_export
Team/Owner: Analytics Engineering
Date Created/Modified:22/12/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'mmm_decomp_export_ldg', OUT_RETURN_MSG);

create multiset table {kpi_scorecard_t2_schema}.mmm_decomp_export_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	Fiscal_Month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Channel VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Touchpoint VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Spend FLOAT,
	Events FLOAT,
	US_FLS_Net_Revenue FLOAT,
	US_N_com_New_Customers FLOAT,
	US_N_com_Net_Revenue FLOAT,
	US_FLS_New_Customers FLOAT,
	US_NRHL_New_Customers FLOAT,
	US_NRHL_Net_Revenue FLOAT,
	US_Rack_New_Customers FLOAT,
	US_Rack_Net_Revenue FLOAT,
	Total_US_New_Customers FLOAT,
	Total_US_Net_Revenue FLOAT,
	US_Rack_Web_Traffic FLOAT,
	US_FLS_Web_Traffic FLOAT
      )

PRIMARY INDEX (Fiscal_Month,Channel,Touchpoint)
;


SET QUERY_BAND = NONE FOR SESSION;




