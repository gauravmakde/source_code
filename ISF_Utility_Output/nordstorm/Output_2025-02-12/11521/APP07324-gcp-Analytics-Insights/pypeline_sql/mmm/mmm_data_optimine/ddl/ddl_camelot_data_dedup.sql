SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=ddl_camelot_data_dedup_11521_ACE_ENG;
     Task_Name=ddl_camelot_data_dedup;'
     FOR SESSION VOLATILE;


/*

T2/Table Name: T2DL_DAS_MMM.camelot_data_dedup
Team/Owner: Analytics Engineering
Date Modified: 2024-05-01


*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'camelot_data_dedup', OUT_RETURN_MSG);

CREATE MULTISET TABLE {mmm_t2_schema}.camelot_data_dedup ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      DAY_DT DATE FORMAT 'YY/MM/DD',
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
      dw_sys_load_tmstp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))
PRIMARY INDEX ( DAY_DT ,banner ,campaign ,dma ,
platform ,channel ,BAR ,Ad_Type ,Funnel );

COMMENT ON {mmm_t2_schema}.camelot_data_dedup IS 'camelot_data_dedup Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;
