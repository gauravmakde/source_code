
-- This file uses Spark sql to pull data from S3 or Hive and move it to a new s3 location in a format needed for TPT.

--- Examples of reading data from S3:
 create or replace temporary view employee_purchase_audit_look_up_ldg_csv
         (
         employee_discount_num string,
         tender_item_account_number_v2 string,
         tender string,
         dw_sys_load_tmstp timestamp
         )
 USING CSV
 OPTIONS (path "s3://analytics-insights-triggers/trust_emp_employee_audit_look_up/employee_purchase_audit_look_up.csv",
         sep ",",
         header "true");

-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table employee_purchase_audit_look_up_ldg_output
select
employee_discount_num
, tender_item_account_number_v2
, tender
, dw_sys_load_tmstp
from employee_purchase_audit_look_up_ldg_csv
;
