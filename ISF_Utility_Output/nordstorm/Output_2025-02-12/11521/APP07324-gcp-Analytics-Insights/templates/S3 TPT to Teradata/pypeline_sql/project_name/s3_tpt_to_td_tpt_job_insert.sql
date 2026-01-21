/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=your_dag_name_11521_ACE_ENG;
     Task_Name=your_sql_file_name_without_extension;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

Notes:
-- update records in the TPT_CONTROL_TBL for this job.  Only the top six values need to be updated.  
-- The remainder of the values are standard and apply to most TPT jobs.

*/

DELETE FROM  {t2_schema}.TPT_CONTROL_TBL 
where job_name = 'final_table_name_ldg'  -- matches job name from tpt_load_ldg stage on .json file
;

INSERT INTO {t2_schema}.TPT_CONTROL_TBL (
  job_name,   -- matches job name from tpt_load_ldg stage on .json file
  database_name,
  object_name,   -- landing table name
  APP_ID,  -- matches App_ID in QUERY_BAND SETTINGS on .sql file
  DAG_ID,  -- matches DAG_ID in QUERY_BAND SETTINGS on .sql file
  TASK_NAME,  -- matches Task_Name in QUERY_BAND SETTINGS on .sql file
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
  AllFilesHeaderRcdIndicator
)
VALUES (
	'final_table_name_ldg',  -- matches job name from tpt_load_ldg stage on .json file
	'{t2_schema}',
	'final_table_name_ldg',  -- landing table name
  'app_id', -- matches App_ID in QUERY_BAND SETTINGS on .sql file
  'dag_id', -- matches DAG_ID in QUERY_BAND SETTINGS on .sql file
  'task_name' , -- matches Task_Name in QUERY_BAND SETTINGS on .sql file
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
	'N'
 ); 

 /* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;