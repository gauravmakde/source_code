/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=trust_emp_employee_purchase_audit_look_up_11521_ACE_ENG;
     Task_Name=trust_emp_employee_purchase_audit_look_up;'
     FOR SESSION VOLATILE;


/*

T2/Table Name: {trust_emp_t2_schema}.employee_purchase_audit
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 8/19/2024

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

Teradata SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (employee_discount_num),
                    COLUMN (tender_item_account_number_v2)-- column names used for primary index
on T2DL_DAS_TRUST_EMP.employee_purchase_audit_look_up_ldg
;


insert into T2DL_DAS_TRUST_EMP.employee_purchase_audit_look_up
select  employee_discount_num
        , tender_item_account_number_v2
        , tender
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    T2DL_DAS_TRUST_EMP.employee_purchase_audit_look_up_ldg
;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
