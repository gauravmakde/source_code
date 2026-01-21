-- For AE Test S3 - TPT - Teradata use case only
SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=test_nsk_migration_11521_ACE_ENG;
     Task_Name=test_script;'
     FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_BIE_DEV', 'ae_test_tpt_td', OUT_RETURN_MSG);
CREATE MULTISET TABLE T2DL_DAS_BIE_DEV.ae_test_tpt_td,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(
    is_featured	char(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , has_rms	char(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , analyst	varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , asset_type	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , business_area	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subject_area	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dashboard_name	varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "description"	varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "data_source"	varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "database" varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , update_frequency	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dashboard_url	varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(dashboard_url)
; 

-- table comment
COMMENT ON  T2DL_DAS_BIE_DEV.ae_test_tpt_td IS 'ae_test_tpt_td_output_table';

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_BIE_DEV', 'ae_test_tpt_td_ldg', OUT_RETURN_MSG);
CREATE MULTISET TABLE T2DL_DAS_BIE_DEV.ae_test_tpt_td_ldg,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(
    is_featured	char(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , has_rms	char(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , analyst	varchar(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , asset_type	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , business_area	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subject_area	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dashboard_name	varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "description"	varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "data_source"	varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "database" varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
    , update_frequency	varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dashboard_url	varchar(200) CHARACTER SET UNICODE NOT CASESPECIFIC
)
PRIMARY INDEX(dashboard_url)
;

COMMENT ON  T2DL_DAS_BIE_DEV.ae_test_td_td IS 'ae_test_tpt_td_ldg_output_table';

DELETE FROM  T2DL_DAS_BIE_DEV.TPT_CONTROL_TBL where job_name = 'ae_test_tpt_td_ldg';

INSERT INTO T2DL_DAS_BIE_DEV.TPT_CONTROL_TBL (
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
	'ae_test_tpt_td_ldg',
	'T2DL_DAS_BIE_DEV',
	'ae_test_tpt_td_ldg',
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
