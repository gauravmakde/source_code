SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=camelot_data_11521_ACE_ENG;
     Task_Name=ddl_camelot_data;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMMM.camelot_data
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'camelot_data', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.camelot_data
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	start_date DATE FORMAT 'YY/MM/DD',
end_date DATE FORMAT 'YY/MM/DD',
banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
campaign VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
BAR VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Ad_Type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Funnel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
External_Funding FLOAT,
Impressions FLOAT,
Cost FLOAT,	  
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(start_date,end_date,banner,campaign,dma,platform,channel,BAR,Ad_Type,Funnel)
;

COMMENT ON  {mmm_t2_schema}.camelot_data IS 'camelot_data Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;



