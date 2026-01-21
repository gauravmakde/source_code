SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=ddl_cco_new_to_channel_lkup_11521_ACE_ENG;
     Task_Name=ddl_cco_new_to_channel_lkup;'
     FOR SESSION VOLATILE;





/*
CCO New To Channel Lkup DDL file   
This file creates the production table T2DL_DAS_STRATEGY.cco_new_to_channel_lkup
*/

CREATE MULTISET TABLE {cco_t2_schema}.cco_new_to_channel_lkup ,FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(
acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
ntx_date DATE FORMAT 'yyyy-mm-dd'
)
PRIMARY INDEX ( acp_id ,channel );


SET QUERY_BAND = NONE FOR SESSION;