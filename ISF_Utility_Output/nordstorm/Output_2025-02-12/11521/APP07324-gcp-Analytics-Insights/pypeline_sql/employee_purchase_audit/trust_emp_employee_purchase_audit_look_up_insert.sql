/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=trust_emp_employee_purchase_audit_look_up_insert_11521_ACE_ENG;
     Task_Name=trust_emp_employee_purchase_audit_look_up_insert;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: {trust_emp_t2_schema}.employee_purchase_audit
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 8/19/2024

Notes:
-- update records in the TPT_CONTROL_TBL for this job.  Only the top three values need to be updated.  
-- The remainder of the values are standard and apply to most TPT jobs.

*/

DELETE FROM  {trust_emp_t2_schema}.TPT_CONTROL_TBL
where job_name = 'employee_purchase_audit_look_up_ldg'  -- matches job name from tpt_load_ldg stage on .json file
;

INSERT INTO {trust_emp_t2_schema}.TPT_CONTROL_TBL (
  job_name,   -- matches job name from tpt_load_ldg stage on .json file
  database_name,
  object_name,   -- landing table name
  object_type,
  batchusername,
  truncateindicator,
  errorlimit,
  textdelimiter_hexa_flag,
  textdelimiter,
  AcceptExcessColumns,
  AcceptMissingColumns,
  QuotedData,
  OpenQuoteMark,
  s3bucket,
  s3prefix,
  s3singlepartfile,
  s3connectioncount,
  collist_indicator_flag,
  collist,
  rcd_load_tmstp,
  rcd_update_tmstp, 
  HeaderRcdIndicator,
  AllFilesHeaderRcdIndicator,
  APP_ID,
  DAG_ID,
  TASK_NAME
)
VALUES (
	'employee_purchase_audit_look_up_ldg',  -- matches job name from tpt_load_ldg stage on .json file
	'{trust_emp_t2_schema}',
	'employee_purchase_audit_look_up_ldg',  -- landing table name
	'T',
	 '*', 
	'Y',
	'1',
	'Y',
	'1F',
	'N',
	'N',
	'N',
	null,
	null,
	null,
	null,
	'20',
	'N',
	null,
	current_timestamp,
	current_timestamp,
	'N',
	'N',
	'APP02602',
	'trust_emp_employee_purchase_audit_look_up_insert_11521_ACE_ENG',
	'trust_emp_employee_purchase_audit_look_up_insert'
 ); 

 /* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;