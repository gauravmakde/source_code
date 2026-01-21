SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_nmn_cost_data_11521_ACE_ENG;
     Task_Name=nmn_fp_tpt_job_insert;'
     FOR SESSION VOLATILE;

DELETE FROM  T2DL_DAS_NMN.TPT_CONTROL_TBL where job_name = 'nmn_fp_nmn_fp_google_ads_data_ldg';

INSERT INTO T2DL_DAS_NMN.TPT_CONTROL_TBL (
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
	'nmn_fp_nmn_fp_google_ads_data_ldg',
	'T2DL_DAS_NMN',
	'nmn_fp_google_ads_data_ldg',
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


DELETE FROM  T2DL_DAS_NMN.TPT_CONTROL_TBL where job_name = 'nmn_fp_nmn_fp_criteo_data_ldg';

INSERT INTO T2DL_DAS_NMN.TPT_CONTROL_TBL (
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
	'nmn_fp_nmn_fp_criteo_data_ldg',
	'T2DL_DAS_NMN',
	'nmn_fp_criteo_data_ldg',
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


 DELETE FROM  T2DL_DAS_NMN.TPT_CONTROL_TBL where job_name = 'nmn_fp_nmn_fp_dv_three_sixty_data_ldg';

INSERT INTO T2DL_DAS_NMN.TPT_CONTROL_TBL (
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
	'nmn_fp_nmn_fp_dv_three_sixty_data_ldg',
	'T2DL_DAS_NMN',
	'nmn_fp_dv_three_sixty_data_ldg',
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




DELETE FROM  T2DL_DAS_NMN.TPT_CONTROL_TBL where job_name = 'nmn_fp_nmn_fp_meta_data_ldg';

INSERT INTO T2DL_DAS_NMN.TPT_CONTROL_TBL (
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
	'nmn_fp_nmn_fp_meta_data_ldg',
	'T2DL_DAS_NMN',
	'nmn_fp_meta_data_ldg',
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


DELETE FROM  T2DL_DAS_NMN.TPT_CONTROL_TBL where job_name = 'nmn_fp_nmn_fp_tiktok_data_ldg';

INSERT INTO T2DL_DAS_NMN.TPT_CONTROL_TBL (
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
	'nmn_fp_nmn_fp_tiktok_data_ldg',
	'T2DL_DAS_NMN',
	'nmn_fp_tiktok_data_ldg',
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

 DELETE FROM  T2DL_DAS_NMN.TPT_CONTROL_TBL where job_name = 'nmn_fp_nmn_fp_pinterest_data_ldg';

INSERT INTO T2DL_DAS_NMN.TPT_CONTROL_TBL (
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
	'nmn_fp_nmn_fp_pinterest_data_ldg',
	'T2DL_DAS_NMN',
	'nmn_fp_pinterest_data_ldg',
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



 
