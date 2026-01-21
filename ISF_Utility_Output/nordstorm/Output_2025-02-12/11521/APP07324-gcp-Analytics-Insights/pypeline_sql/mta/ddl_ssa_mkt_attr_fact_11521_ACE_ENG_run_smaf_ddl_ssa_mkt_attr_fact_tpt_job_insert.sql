/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08134;
     DAG_ID=ssa_mkt_attr_fact_11521_ACE_ENG;
     Task_Name=ddl_ssa_mkt_attr_ldg;'
     FOR SESSION VOLATILE;


-- update record in the TPT_CONTROL_TBL for this job.  Only the top three values need to be updated.
-- The remainder of the values are standard and apply to most TPT jobs.
DELETE FROM T2DL_DAS_MTA.TPT_CONTROL_TBL WHERE job_name = 'ssa_mkt_attr_fact_ldg';

INSERT INTO T2DL_DAS_MTA.TPT_CONTROL_TBL (
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
	'ssa_mkt_attr_fact_ldg',  -- matches job name from tpt_load_ldg stage on .json file
	'T2DL_DAS_MTA',
	'ssa_mkt_attr_fact_ldg',  -- landing table name
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
