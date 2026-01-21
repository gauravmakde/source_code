SET QUERY_BAND = 'App_ID=APP09301;
     DAG_ID=ddl_chargeback_transaction_11521_ACE_ENG;
     Task_Name=ddl_chargeback_transaction;'
     FOR SESSION VOLATILE;

/*

Table: {trust_engine_t2_schema}.chargeback_transaction
Owner: Rujira Achawanantakun
Modified: 2024-09-16
Note:
- This ddl is used to create chargeback_transaction table structure into tier 2 datalab.

*/
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{trust_engine_t2_schema}', 'chargeback_transaction', OUT_RETURN_MSG);
*/
CREATE MULTISET TABLE {trust_engine_t2_schema}.chargeback_transaction
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
        global_tran_id BIGINT NOT NULL,
        business_day_date DATE FORMAT 'YYYY-MM-DD' NOT NULL,
        settlement_result_received_id VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
        network_reference_number VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
        visa_transaction_id VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
        online_shopper_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        deterministic_profile_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        token_value VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
        tender VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        tender_item_usd_amt DECIMAL(12, 2),
        tender_entry_method_code INTEGER,
        entry_mode VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC,
        entry_method_desc VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
        line_item_order_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('StoreInitStoreTake','CustInitWebOrder','RPOS','StoreInitDTCAuto','StoreInitSameStrSend','CustInitPhoneOrder','StoreInitDTCManual'),
        business_unit_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_address_city VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_address_state CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_postal_code VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
        intent_store_num INTEGER,
        ringing_store_num INTEGER,
        register_num INTEGER,
        tran_num INTEGER,
        line_item_seq_num SMALLINT COMPRESS (1 ,2 ,3 ,4 ,5 ,6 ,7 ),
        merch_dept_num VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
        commission_slsprsn_num VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('2079333','2079334','2079335','0000'),
        employee_discount_flag BYTEINT COMPRESS (0, 1),
        employee_discount_num VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS '0' ,
        dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (case_id, ft_tran_id);

CREATE INDEX(fraud_tag_date)
       ,INDEX(fraud_tag_date, loss_type_cd)
       ,INDEX(fraud_tag_date, loss_type)
       ,INDEX(business_day_date)
       ,INDEX(business_day_date, loss_type_cd)
       ,INDEX(business_day_date, loss_type)
on {trust_engine_t2_schema}.chargeback_transaction;

-- Table Comment
COMMENT ON  {trust_engine_t2_schema}.chargeback_transaction IS 'Chargeback Transaction';
SET QUERY_BAND = NONE FOR SESSION;