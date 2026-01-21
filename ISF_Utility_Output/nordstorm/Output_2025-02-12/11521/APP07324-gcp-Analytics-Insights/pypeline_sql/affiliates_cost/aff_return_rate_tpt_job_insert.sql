SET QUERY_BAND = 'App_ID=APP08823;
     DAG_ID=affiliates_cost_11521_ACE_ENG;
     Task_Name=aff_return_rate_tpt_job_insert;'
     FOR SESSION VOLATILE;

DELETE FROM  {funnel_io_t2_schema}.TPT_CONTROL_TBL where job_name = 'tpt_affiliates_return_rate_ldg';

INSERT INTO {funnel_io_t2_schema}.TPT_CONTROL_TBL (
  job_name,
  database_name,
  object_name,
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
	'tpt_affiliates_return_rate_ldg',
	'{funnel_io_t2_schema}',
	'affiliates_return_rate_ldg',
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

SET QUERY_BAND = NONE FOR SESSION;