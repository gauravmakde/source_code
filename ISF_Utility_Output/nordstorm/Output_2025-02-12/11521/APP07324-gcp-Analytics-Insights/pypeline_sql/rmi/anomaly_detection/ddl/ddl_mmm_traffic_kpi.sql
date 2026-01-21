SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=ddl_mmm_traffic_kpi_11521_ACE_ENG;
     Task_Name=ddl_mmm_traffic_kpi;'
     FOR SESSION VOLATILE;
	
/*

T2/Table Name: T2DL_DAS_MOA_KPI.mmm_traffic_kpi
Team/Owner: Analytics Engineering
Date Created/Modified:26/12/2024

*/

-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_MOA_KPI', 'mmm_traffic_kpi', OUT_RETURN_MSG);



CREATE MULTISET TABLE {kpi_scorecard_t2_schema}.mmm_traffic_kpi
,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(activity_date_pacific DATE FORMAT 'YY/MM/DD',
store_num INTEGER,
business_unit_desc VARCHAR(150)CHARACTER SET UNICODE NOT CASESPECIFIC,
platform_type VARCHAR(150)CHARACTER SET UNICODE NOT CASESPECIFIC,
cust_status VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
engagement_cohort VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
loyalty_status VARCHAR(30)CHARACTER SET UNICODE NOT CASESPECIFIC,
total_traffic BIGINT,
trips INTEGER,
dw_batch_date DATE FORMAT 'YY/MM/DD',
dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( activity_date_pacific , business_unit_desc);

SET QUERY_BAND = NONE FOR SESSION;