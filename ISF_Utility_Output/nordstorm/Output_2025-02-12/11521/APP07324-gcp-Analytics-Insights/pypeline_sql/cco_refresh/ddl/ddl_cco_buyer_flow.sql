SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=ddl_cco_buyer_flow_daily_11521_ACE_ENG;
Task_Name=run_teradata_ddl_cco_buyer_flow;'
FOR SESSION VOLATILE;


--DROP TABLE {cco_t2_schema}.cco_buyer_flow;


CREATE MULTISET TABLE {cco_t2_schema}.cco_buyer_flow ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      reporting_year_shopped VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      buyer_flow VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel',
          '4) Reactivated-to-Channel'),
      AARE_acquired INTEGER COMPRESS (0, 1),
      AARE_activated INTEGER COMPRESS (0, 1),
      AARE_retained INTEGER COMPRESS (0, 1),
      AARE_engaged INTEGER COMPRESS (0, 1),
      dw_sys_load_tmstp TIMESTAMP(6),
      dw_sys_updt_tmstp TIMESTAMP(6))
PRIMARY INDEX ( acp_id ,reporting_year_shopped ,channel );


collect statistics column (acp_id) on {cco_t2_schema}.cco_buyer_flow;
collect statistics column (reporting_year_shopped) on {cco_t2_schema}.cco_buyer_flow;
collect statistics column (channel) on {cco_t2_schema}.cco_buyer_flow;
collect statistics column (buyer_flow) on {cco_t2_schema}.cco_buyer_flow;
collect statistics column (acp_id,reporting_year_shopped,channel) on {cco_t2_schema}.cco_buyer_flow;


SET QUERY_BAND = NONE FOR SESSION;
