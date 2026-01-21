SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=customer_plan_11521_ACE_ENG;
     Task_Name=ddl_customer_plan;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.customer_plan
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023


*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.customer_plan;

create multiset table {kpi_scorecard_t2_schema}.customer_plan
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
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
	Reactivated_Sales FLOAT,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(fiscal_year,fiscal_month,Box_type)
;

COMMENT ON  {kpi_scorecard_t2_schema}.customer_plan IS 'Customer_Plan Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;
