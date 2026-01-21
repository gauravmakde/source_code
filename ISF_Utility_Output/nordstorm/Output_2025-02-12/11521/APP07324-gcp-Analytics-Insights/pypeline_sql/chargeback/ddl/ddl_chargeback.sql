SET QUERY_BAND = 'App_ID=APP09301;
     DAG_ID=ddl_chargeback_11521_ACE_ENG;
     Task_Name=ddl_chargeback;'
     FOR SESSION VOLATILE;

/*

Table: {trust_engine_t2_schema}.chargeback
Owner: Rujira Achawanantakun
Modified: 2024-09-25
Note:
- This ddl is used to create chargeback table structure into tier 2 datalab.

*/
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{trust_engine_t2_schema}', 'chargeback', OUT_RETURN_MSG);
*/
CREATE MULTISET TABLE {trust_engine_t2_schema}.chargeback
	 ,FALLBACK
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT
     ,DEFAULT MERGEBLOCKRATIO
    (
        case_id VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
        loss_type_cd VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
        loss_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        ft_reported_date DATE format 'YYYY-MM-DD',
        case_open_date DATE format 'YYYY-MM-DD',
        fraud_tag_date DATE format 'YYYY-MM-DD',
        transaction_date DATE format 'YYYY-MM-DD',
        ft_tran_id VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
        trxn_amt DECIMAL(12, 2),
        reference23_nr VARCHAR(23) CHARACTER SET UNICODE NOT CASESPECIFIC,
        industry_transaction_id VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
        authorization_cd VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC,
        mail_phone_indicator_cd VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
        pos_entry_mode_cd VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
        merchant_account_id VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC,
        merchant_nm VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        merch_cat_code INTEGER,
        merchant_postal_cd VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
        dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (case_id, ft_tran_id);

CREATE INDEX(fraud_tag_date)
       ,INDEX(fraud_tag_date, loss_type_cd)
       ,INDEX(fraud_tag_date, loss_type)
on {trust_engine_t2_schema}.chargeback;


-- Table Comment
COMMENT ON  {trust_engine_t2_schema}.chargeback IS 'Chargeback';
SET QUERY_BAND = NONE FOR SESSION;