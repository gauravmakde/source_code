SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=seo_data_11521_ACE_ENG;
     Task_Name=ddl_seo_data;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.seo_data
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'seo_data', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.seo_data
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
       day_date DATE FORMAT 'YY/MM/DD',
Banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Keyword_Type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Impressions FLOAT,
Clicks FLOAT,	 
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(day_date,Banner,Keyword_Type)
;

COMMENT ON  {mmm_t2_schema}.seo_data IS 'seo_data Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;


