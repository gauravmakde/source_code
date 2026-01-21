SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_facebook_reach_and_freq_11521_ACE_ENG;
     Task_Name=rack_facebook_reach_and_freq_tpt_job_insert;'
     FOR SESSION VOLATILE;


DELETE FROM  {funnel_io_t2_schema}.TPT_CONTROL_TBL 
where job_name = 'nmn_rack_facebook_reach_and_freq_ldg'  
;


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
  'nmn_rack_facebook_reach_and_freq_ldg',  
  '{funnel_io_t2_schema}',
  'nmn_rack_facebook_reach_and_freq_ldg',  
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
