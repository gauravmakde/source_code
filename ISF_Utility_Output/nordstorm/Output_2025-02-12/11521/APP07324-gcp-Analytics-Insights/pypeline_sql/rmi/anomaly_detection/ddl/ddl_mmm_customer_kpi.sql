SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_customer_kpi_11521_ACE_ENG;
     Task_Name=ddl_mmm_customer_kpi;'
     FOR SESSION VOLATILE;
	
/*

T2/Table Name: T2DL_DAS_MOA_KPI.mmm_customer_kpi
Team/Owner: Analytics Engineering
Date Created/Modified:27/11/2024

*/

-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_MOA_KPI', 'mmm_customer_kpi', OUT_RETURN_MSG);



CREATE MULTISET TABLE {kpi_scorecard_t2_schema}.mmm_customer_kpi
,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(activity_date DATE FORMAT 'YY/MM/DD',
store_num INTEGER,
business_unit_desc VARCHAR(150)CHARACTER SET UNICODE NOT CASESPECIFIC,
tran_type_code VARCHAR(8)CHARACTER SET UNICODE NOT CASESPECIFIC,
engagement_cohort VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
order_platform_type VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
cust_status VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
loyalty_status VARCHAR(20)CHARACTER SET UNICODE NOT CASESPECIFIC,
cust_cnt INTEGER,
dw_batch_date DATE FORMAT 'YY/MM/DD',
dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX (activity_date, business_unit_desc);

COMMENT ON  {kpi_scorecard_t2_schema}.mmm_customer_kpi IS 'Supports anomaly detection dashboard';

SET QUERY_BAND = NONE FOR SESSION;