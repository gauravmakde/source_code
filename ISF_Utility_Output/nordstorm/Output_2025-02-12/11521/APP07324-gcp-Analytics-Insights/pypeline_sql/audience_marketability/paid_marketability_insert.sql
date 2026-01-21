/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08718;
DAG_ID=paid_marketability_11521_ACE_ENG;
Task_Name=paid_marketability_insert;'
FOR SESSION VOLATILE;



DELETE FROM  {mktg_audience_t2_schema}.TPT_CONTROL_TBL
where job_name = 'paid_marketability_ldg'  -- matches job name from tpt_load_ldg stage on .json file
;

INSERT INTO {mktg_audience_t2_schema}.TPT_CONTROL_TBL (
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
  AllFilesHeaderRcdIndicator
)
VALUES (
	'paid_marketability_ldg',  -- matches job name from tpt_load_ldg stage on .json file
	'{mktg_audience_t2_schema}',
	'paid_marketability_ldg',  -- landing table name
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
