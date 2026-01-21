SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_usl_experian_mosaic_segments_11521_ACE_ENG;
     Task_Name=experian_mosaic_segments_tpt_job_insert;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_USL.experian_mosaic_segments
Team/Owner: Customer Analytics/ Brian McGrane
Date Created/Modified: 05/31/2024

*/

DELETE FROM  {usl_t2_schema}.TPT_CONTROL_TBL where job_name = 'experian_mosaic_segments_ldg';

INSERT INTO {usl_t2_schema}.TPT_CONTROL_TBL (
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
	'experian_mosaic_segments_ldg',
	'{usl_t2_schema}',
	'experian_mosaic_segments_ldg',
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
