SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=gfk_brand_metrics_11521_ACE_ENG;
     Task_Name=ddl_gfk_brand_metrics;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.gfk_brand_metrics
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.

-- drop table {mmm_t2_schema}.gfk_brand_metrics;
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'gfk_brand_metrics', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.gfk_brand_metrics
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	day_date DATE FORMAT 'YY/MM/DD',
	Segment VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Metric VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Metric_Value INTEGER,    
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(day_date,Segment,Metric)
;

COMMENT ON  {mmm_t2_schema}.gfk_brand_metrics IS 'gfk_brand_metrics Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;




