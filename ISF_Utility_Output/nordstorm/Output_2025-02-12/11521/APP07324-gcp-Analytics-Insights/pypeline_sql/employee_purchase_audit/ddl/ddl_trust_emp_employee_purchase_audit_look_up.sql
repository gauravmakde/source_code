SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=ddl_trust_emp_employee_purchase_audit_look_up_11521_ACE_ENG;
     Task_Name=ddl_trust_emp_employee_purchase_audit_look_up;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {trust_emp_t2_schema}.employee_purchase_audit_look_up
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 8/20/2024

Note:
-- Purpose of the table: Employee discount summary by year, month, channel, and merchant
-- Update Cadence: Daily
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_TRUST_EMP', 'employee_purchase_audit_look_up', OUT_RETURN_MSG);

-- T3DL_ACE_FROPS.employee_purchase_audit_look_up definition

CREATE MULTISET TABLE T2DL_DAS_TRUST_EMP.employee_purchase_audit_look_up ,FALLBACK ,
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


COLLECT STATISTICS  COLUMN (employee_discount_num),
                    COLUMN (tender_item_account_number_v2)
on T2DL_DAS_TRUST_EMP.employee_purchase_audit_look_up;


/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
