SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=ddl_customer_age_dim_11521_ACE_ENG;
     Task_Name=ddl_customer_age_dim;'
     FOR SESSION VOLATILE;

/*
CCO Strategy Customer Age DDL file   
This file creates the production table T2DL_DAS_STRATEGY.customer_age_dim
*/

-- For testing the table. Comment out before final merge.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'customer_age_dim', OUT_RETURN_MSG);

 CREATE MULTISET TABLE {cco_t2_schema}.customer_age_dim,
 FALLBACK ,
 NO BEFORE JOURNAL,
 NO AFTER JOURNAL,
 CHECKSUM = DEFAULT,
 DEFAULT MERGEBLOCKRATIO,
 MAP = TD_MAP1
(
	acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, age_start_day DATE
	, age_end_day DATE
	, age_value DECIMAL(12,3) compress
	, age_source VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	, age_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	, age_updated_date DATE
	, dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX (acp_id, age_source, age_type, age_start_day, age_end_day);

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;

