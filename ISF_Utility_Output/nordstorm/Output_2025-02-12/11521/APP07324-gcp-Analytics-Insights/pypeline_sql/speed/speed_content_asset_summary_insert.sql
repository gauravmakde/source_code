
SET QUERY_BAND = 'App_ID=APP08907;
     DAG_ID=speed_content_asset_summary_tpt_11521_ACE_ENG;
     Task_Name=speed_content_asset_summary_tpt;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_DIGENG.SPEED_CONTENT_ASSET_SUMMARY
Team/Owner: Digital Optimization/Soren Stime
Date Created/Modified: 07/26/2023,TBD

Note:
SPEED Content Asset Summary aggregates the data in content session analytical 
to view data on the asset level. Due to this, if further aggregations are done 
it is recommended to only use event counts as your measure of choice. 
*/

DELETE FROM  {deg_t2_schema}.TPT_CONTROL_TBL 
where job_name = 'speed_content_asset_summary_ldg'  -- matches job name from tpt_load_ldg stage on .json file
;

INSERT INTO {deg_t2_schema}.TPT_CONTROL_TBL (
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
	'speed_content_asset_summary_ldg',  -- matches job name from tpt_load_ldg stage on .json file
	'{deg_t2_schema}',
	'speed_content_asset_summary_ldg',  -- landing table name
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
