SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=ddl_mmm_gross_demand_kpi_11521_ACE_ENG;
     Task_Name=ddl_mmm_gross_demand_kpi;'
     FOR SESSION VOLATILE;
	
/*

T2/Table Name: T2DL_DAS_MOA_KPI.mmm_gross_demand_kpi
Team/Owner: Analytics Engineering
Date Created/Modified:27/11/2024

*/

-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_MOA_KPI', 'mmm_gross_demand_kpi', OUT_RETURN_MSG);



CREATE MULTISET TABLE {kpi_scorecard_t2_schema}.mmm_gross_demand_kpi
,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(tran_date DATE FORMAT 'YY/MM/DD',
business_unit_desc VARCHAR(150)CHARACTER SET UNICODE NOT CASESPECIFIC,
store_num INTEGER,
bill_zip_code VARCHAR(15)CHARACTER SET UNICODE NOT CASESPECIFIC,
price_type VARCHAR(20)CHARACTER SET UNICODE NOT CASESPECIFIC,
order_platform_type VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
loyalty_status VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
engagement_cohort VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
cust_status VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
division_name VARCHAR(150)CHARACTER SET UNICODE NOT CASESPECIFIC,
subdivision_name VARCHAR(150)CHARACTER SET UNICODE NOT CASESPECIFIC,
total_msrp_amt DECIMAL(15,2),
jwn_reported_gross_demand_amt DECIMAL(18,9),
jwn_demand_units INTEGER,
dw_batch_date DATE FORMAT 'YY/MM/DD',
dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX (tran_date, business_unit_desc);

SET QUERY_BAND = NONE FOR SESSION;