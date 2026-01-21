SET QUERY_BAND = 'App_ID=APP09267;
     DAG_ID=ddl_consented_customer_relationship_hist_11521_ACE_ENG;
     Task_Name=ddl_consented_customer_relationship_hist;'
     FOR SESSION VOLATILE;


--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'consented_customer_relationship_hist', OUT_RETURN_MSG);
/*
Table definition for T2DL_DAS_CLIENTELING.consented_customer_relationship_hist
*/
CREATE MULTISET TABLE {cli_t2_schema}.consented_customer_relationship_hist
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
    (
    acp_id                                  VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , seller_id                               VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , first_clienteling_consent_dt            DATE NOT NULL
    , first_seller_consent_dt                 DATE NOT NULL
    , seller_num                              INTEGER COMPRESS
    , opt_in_date_pst			                DATE NOT NULL
    , opt_out_date_pst                        DATE
    , clienteling_history                     VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , seller_history                          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , rewards_level                           VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , email_pref_cur                          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
    , phone_pref_cur                          VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC

    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
    PRIMARY INDEX (acp_id) 
    PARTITION BY RANGE_N(opt_in_date_pst BETWEEN DATE '2018-02-04' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN)
    ;


-- table comment
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist IS 'Remote Sell Transactions';
-- Column comments
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.acp_id IS 'analytical customer profile identifier';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.seller_id IS 'worker id of the consented seller';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.first_clienteling_consent_dt IS 'date on which customer first consented to any Nord seller thereby first entering the program';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.first_seller_consent_dt IS 'date on which customer first consented to given Nord seller';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.seller_num IS 'Nth seller to which customer consented';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.opt_in_date_pst IS 'date on which customer consented to given seller';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.opt_out_date_pst IS 'date on which customer opted out of consent to given seller';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.clienteling_history IS 'amount paid, less employee discount in local currency';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.seller_history IS 'amount paid, less employee discount in USD currency';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.rewards_level IS 'current rewards level'; 
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.email_pref_cur IS 'current email marketability';
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.phone_pref_cur IS 'current phone marketability'; 
COMMENT ON  {cli_t2_schema}.consented_customer_relationship_hist.dw_sys_load_tmstp IS 'timestamp when data was last updated'; 

SET QUERY_BAND = NONE FOR SESSION;