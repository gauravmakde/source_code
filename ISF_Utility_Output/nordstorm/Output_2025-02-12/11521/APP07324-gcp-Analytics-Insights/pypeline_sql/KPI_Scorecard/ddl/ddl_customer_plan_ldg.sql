SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=customer_plan_11521_ACE_ENG;
     Task_Name=ddl_customer_plan_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.customer_plan
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'customer_plan_ldg', OUT_RETURN_MSG);

create multiset table {kpi_scorecard_t2_schema}.customer_plan_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	fiscal_year INTEGER,
    fiscal_month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Box_type  VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Total_Customers FLOAT,
	Retained_Customers FLOAT,
	Acquired_Customers FLOAT,
	Reactivated_Customers FLOAT,
	Total_Trips FLOAT,
	Retained_Trips FLOAT,
	Acquired_Trips FLOAT,
	Reactivated_Trips FLOAT,
	Total_Sales FLOAT,
	Retained_Sales FLOAT,
	Acquired_Sales FLOAT,
	Reactivated_Sales FLOAT
      )

PRIMARY INDEX (fiscal_year,fiscal_month,Box_type)
;


SET QUERY_BAND = NONE FOR SESSION;




