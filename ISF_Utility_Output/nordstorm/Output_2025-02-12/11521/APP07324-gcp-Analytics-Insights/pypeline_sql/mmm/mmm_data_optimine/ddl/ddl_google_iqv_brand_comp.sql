SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=google_iqv_brand_comp_11521_ACE_ENG;
     Task_Name=ddl_google_iqv_brand_comp;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.google_iqv_brand_comp
Team/Owner: Analytics Engineering
Date Created/Modified:15/02/2024

*/
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'google_iqv_brand_comp', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.google_iqv_brand_comp
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Channel_flag VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	QueryLabel VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	QueryType VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	ReportDate DATE FORMAT 'YY/MM/DD',
	TimeGranularity VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	GeoCriteriaId FLOAT,
	GeoName VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	GeoType VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	IndexedQueryVolume FLOAT,	 
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Channel_flag,QueryLabel,QueryType,ReportDate,TimeGranularity,GeoName,GeoType)
;

COMMENT ON  {mmm_t2_schema}.google_iqv_brand_comp IS 'google_iqv_brand_comp Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;
