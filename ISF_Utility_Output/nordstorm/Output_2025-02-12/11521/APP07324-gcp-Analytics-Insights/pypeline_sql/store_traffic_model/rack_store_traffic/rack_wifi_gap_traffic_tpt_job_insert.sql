/*
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_wifi_gap_traffic_11521_ACE_ENG;
     Task_Name=rack_wifi_gap_traffic_tpt_job_insert;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.rack_wifi_gap_traffic
Team/Owner: tech_ffp_analytics/Selina Song
Date Created/Modified: 08/31/2023

Notes:
-- update records in the TPT_CONTROL_TBL for this job.  Only the top three values need to be updated.
-- The remainder of the values are standard and apply to most TPT jobs.

*/

DELETE FROM  {fls_traffic_model_t2_schema}.TPT_CONTROL_TBL
where job_name = 'rack_wifi_gap_traffic_ldg'  -- matches job name from tpt_load_ldg stage on .json file
;

INSERT INTO {fls_traffic_model_t2_schema}.TPT_CONTROL_TBL (
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
	'rack_wifi_gap_traffic_ldg',  -- matches job name from tpt_load_ldg stage on .json file
	'{fls_traffic_model_t2_schema}',
	'rack_wifi_gap_traffic_ldg',  -- landing table name
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