SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=dashboard_catalog_11521_ACE_ENG;
     Task_Name=ddl_dashboard_catalog_ldg;'
     FOR SESSION VOLATILE;

/*

Table:  T2DL_DAS_PRODUCT_FUNNEL.dashboard_catalog_ldg
Owner: Analytics Engineering
Modified: 2023-02-08

This table is created as part of the dashboard catalog job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.
*/

DELETE FROM  {t2_schema}.TPT_CONTROL_TBL where job_name = 'dashboard_catalog_ldg';

INSERT INTO {t2_schema}.TPT_CONTROL_TBL (
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
	'dashboard_catalog_ldg',
	'{t2_schema}',
	'dashboard_catalog_ldg',
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

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{t2_schema}', 'dashboard_catalog_ldg', OUT_RETURN_MSG);
CREATE MULTISET TABLE {t2_schema}.dashboard_catalog_ldg,
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

SET QUERY_BAND = NONE FOR SESSION;