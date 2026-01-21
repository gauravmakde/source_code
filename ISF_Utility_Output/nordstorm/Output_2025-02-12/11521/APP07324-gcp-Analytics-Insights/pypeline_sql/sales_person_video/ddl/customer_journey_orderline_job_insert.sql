
SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=ddl_sales_person_video_customer_journey_orderline_11521_ACE_ENG;
     Task_Name=customer_journey_orderline_load;'
     FOR SESSION VOLATILE;

/*

T2/Table Name:sales_person_video_customer_journey_orderline
Team/Owner: Anfernee Calloway
Date Created/Modified: 12/5/23

Notes:
-- update records in the TPT_CONTROL_TBL for this job.  Only the top three values need to be updated.  
-- The remainder of the values are standard and apply to most TPT jobs.

*/

DELETE FROM  {spv_t2_schema}.TPT_CONTROL_TBL 
where job_name = 'customer_journey_orderline_landing'  -- matches job name from tpt_load_ldg stage on .json file
;

INSERT INTO {spv_t2_schema}.TPT_CONTROL_TBL (
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
	'customer_journey_orderline_landing',  -- matches job name from tpt_load_ldg stage on .json file
	'{spv_t2_schema}',
	'customer_journey_orderline_landing',  -- landing table name
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

