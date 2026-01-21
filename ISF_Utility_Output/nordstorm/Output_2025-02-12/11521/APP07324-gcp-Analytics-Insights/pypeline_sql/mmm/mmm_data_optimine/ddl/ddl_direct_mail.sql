SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=direct_mail_11521_ACE_ENG;
     Task_Name=ddl_direct_mail;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.direct_mail
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'direct_mail', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.direct_mail
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	start_date DATE FORMAT 'YY/MM/DD',
	campaign_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	channel_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	bar VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	sales_channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	region VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	circulation FLOAT,
	cost FLOAT,
	store_no VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	funding_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
	funnel_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,	    
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(start_date,campaign_name,channel_type,bar,sales_channel,region
,funding_type,funnel_type)
;

COMMENT ON  {mmm_t2_schema}.direct_mail IS 'direct_mail Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;


