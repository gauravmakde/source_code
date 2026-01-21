SET QUERY_BAND = 'App_ID=APP09301;
     DAG_ID=chargeback_load_11521_ACE_ENG;
     Task_Name=chargeback_load;'
     FOR SESSION VOLATILE;

/*

Table: {trust_engine_t2_schema}.chargeback_load
Owner: Rujira Achawanantakun
Modified: 2024-09-03
- This landing table is created as part of the chargeback job that loads
data from S3 to teradata. The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{trust_engine_t2_schema}', 'chargeback_load', OUT_RETURN_MSG);

CREATE MULTISET TABLE {trust_engine_t2_schema}.chargeback_load
	 ,FALLBACK
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT
     ,DEFAULT MERGEBLOCKRATIO
    (
    case_id VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC,
    loss_type_cd VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
    loss_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    ft_reported_date DATE format 'YYYY-MM-DD',
    case_open_date DATE format 'YYYY-MM-DD',
    fraud_tag_date DATE format 'YYYY-MM-DD',
    transaction_date DATE format 'YYYY-MM-DD',
    ft_tran_id VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
    trxn_amt DECIMAL(12, 2),
    reference23_nr VARCHAR(23) CHARACTER SET UNICODE NOT CASESPECIFIC,
    industry_transaction_id VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
    authorization_cd VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC,
    mail_phone_indicator_cd VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
    pos_entry_mode_cd VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
    merchant_account_id VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC,
    merchant_nm VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
    merch_cat_code INTEGER,
    merchant_postal_cd VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC 
    )
PRIMARY INDEX (case_id, ft_tran_id)
;

SET QUERY_BAND = NONE FOR SESSION;
