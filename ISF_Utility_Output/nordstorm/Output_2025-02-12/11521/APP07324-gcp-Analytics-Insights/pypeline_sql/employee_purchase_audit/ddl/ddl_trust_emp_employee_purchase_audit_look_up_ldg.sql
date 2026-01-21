/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=ddl_trust_emp_employee_purchase_audit_look_up_ldg_11521_ACE_ENG;
     Task_Name=ddl_trust_emp_employee_purchase_audit_look_up_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: {trust_emp_t2_schema}.employee_purchase_audit_look_up_ldg
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 8/19/2024

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{trust_emp_t2_schema}', 'employee_purchase_audit_look_up_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {trust_emp_t2_schema}.employee_purchase_audit_look_up_ldg ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      employee_discount_num VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tender_item_account_number_v2 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tender VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX (employee_discount_num,tender_item_account_number_v2 );

/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;