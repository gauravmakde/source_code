SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=ddl_mmm_digital_marketing_11521_ACE_ENG;
     Task_Name=ddl_mmm_digital_marketing;'
     FOR SESSION VOLATILE;


--T2/Table Name: T2DL_DAS_MOA_KPI.mmm_digital_marketing
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2024-12-03
--Note:
-- This table supports the Anamoly Detection Dashboard.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'mmm_digital_marketing', OUT_RETURN_MSG);

CREATE MULTISET TABLE {kpi_scorecard_t2_schema}.mmm_digital_marketing ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
	  stats_date DATE FORMAT 'YY/MM/DD',
      platform VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      banner VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_name VARCHAR(8000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      adgroup_name VARCHAR(8000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      adgroup_id VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ad_name VARCHAR(8000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ad_id VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      device_type VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      funnel_type VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      funding VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      bar VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      nmn_flag VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      beauty_flag VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      video_flag VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cost DECIMAL(38,2),
      impressions BIGINT,
      clicks BIGINT,
      video_views FLOAT,
      video100 FLOAT,
      video75 FLOAT,
      sends INTEGER,
      deliveries INTEGER,
      opens INTEGER,
      unsubscribed INTEGER,
      ad_format VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      category VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_batch_date DATE FORMAT 'YY/MM/DD',
      dw_sys_load_tmstp TIMESTAMP(6))
PRIMARY INDEX ( stats_date,platform,banner,finance_detail,campaign_name,campaign_id,adgroup_name,adgroup_id,ad_name,ad_id,device_type,funnel_type,funding,bar,
nmn_flag,beauty_flag,video_flag,ad_format,category
 );


COMMENT ON  {kpi_scorecard_t2_schema}.mmm_digital_marketing IS 'Anamoly Detection Dashboard';

SET QUERY_BAND = NONE FOR SESSION;
