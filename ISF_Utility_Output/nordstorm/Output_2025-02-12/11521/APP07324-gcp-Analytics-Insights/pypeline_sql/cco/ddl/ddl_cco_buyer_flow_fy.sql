SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=ddl_cco_buyer_flow_fy_11521_ACE_ENG;
     Task_Name=ddl_cco_buyer_flow_fy;'
     FOR SESSION VOLATILE;





/*
CCO Buyer Flow DDL file   
This file creates the production table T2DL_DAS_STRATEGY.cco_buyer_flow_fy
*/

create MULTISET table {cco_t2_schema}.cco_buyer_flow_fy,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
    acp_id VARCHAR(50) CHARACTER SET UNICODE
    ,fiscal_year_shopped VARCHAR(10) CHARACTER SET UNICODE
    ,channel VARCHAR(20) CHARACTER SET UNICODE
    ,buyer_flow VARCHAR(27) CHARACTER SET UNICODE compress ('1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel',
        '4) Reactivated-to-Channel')
    ,AARE_acquired INTEGER compress (0,1)
    ,AARE_activated INTEGER compress (0,1)
    ,AARE_retained INTEGER compress (0,1)
    ,AARE_engaged INTEGER compress (0,1)
) primary index (acp_id,fiscal_year_shopped,channel) ;

SET QUERY_BAND = NONE FOR SESSION;