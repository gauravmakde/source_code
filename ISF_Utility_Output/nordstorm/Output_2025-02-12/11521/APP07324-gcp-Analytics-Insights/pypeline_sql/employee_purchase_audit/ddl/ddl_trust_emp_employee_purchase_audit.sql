SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=ddl_trust_emp_employee_purchase_audit_11521_ACE_ENG;
     Task_Name=ddl_trust_emp_employee_purchase_audit;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {trust_emp_t2_schema}.employee_purchase_audit
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 8/20/2024

Note:
-- Purpose of the table: Employee discount summary by year, month, channel, and merchant
-- Update Cadence: Daily
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{trust_emp_t2_schema}', 'employee_purchase_audit', OUT_RETURN_MSG);

-- T3DL_ACE_FROPS.employee_purchase_audit definition

CREATE MULTISET TABLE {trust_emp_t2_schema}.employee_purchase_audit ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      first_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      last_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      discount_percent CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC,
      business_date DATE FORMAT 'YY/MM/DD',
      intent_store INTEGER,
      ringing_store INTEGER,
      ringing_date DATE FORMAT 'YY/MM/DD',
      sales_person VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
      original_register_num DECIMAL(8,0),
      original_transaction_id BIGINT,
      status_code CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      emp_discount_flag CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
      emp_discount_amount DECIMAL(12,2),
      emp_discount_number VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
      upc_no VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      fee_code VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      line_item_num SMALLINT,
      line_net_amount DECIMAL(12,2),
      tran_total_amount DECIMAL(12,2),
      item_tax_amt DECIMAL(12,2),
      tran_total_tax_amt DECIMAL(12,2),
      tax_exempt_flag VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      original_bus_date DATE FORMAT 'YY/MM/DD',
      original_store INTEGER,
      tran_type CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tran_total_emp_disc_amt DECIMAL(12,2),
      tax_store INTEGER,
      tran_total_manual_tax_amt DECIMAL(12,2),
      sku_desc VARCHAR(5000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sku_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      fulfillment_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tran_currency CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      original_currency CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      order_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      city VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      state VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      zip VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      zip_3 VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC,
      original_tran_num VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      original_destination_city VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      original_destination_state VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      original_destination_zip VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      line_item_activity_type_code CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      item_source VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC,
      business_unit_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX (original_transaction_id,line_item_num, sku_num )
partition by RANGE_N(business_date BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY);


-- Table Comment (STANDARD)
--COMMENT ON  {t2_schema}.final_table_name IS 'Description of table';
---- Column comments (OPTIONAL)
--COMMENT ON  {t2_schema}.final_table_name.column_1 IS 'column description';
--COMMENT ON  {t2_schema}.final_table_name.column_2 IS 'column description';
--COMMENT ON  {t2_schema}.final_table_name.column_3 IS 'column description';
--COMMENT ON  {t2_schema}.final_table_name.column_4 IS 'column description';
--COMMENT ON  {t2_schema}.final_table_name.column_5 IS 'column description';


COLLECT STATISTICS  COLUMN (business_date),
                    COLUMN (original_transaction_id), -- column names used for primary index
                    COLUMN (line_item_num) ,
                    COLUMN (sku_num)-- column names used for partition
on {trust_emp_t2_schema}.employee_purchase_audit;


/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
