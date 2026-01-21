SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=google_iqv_brand_comp_11521_ACE_ENG;
     Task_Name=google_iqv_brand_comp;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.google_iqv_brand_comp
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.google_iqv_brand_comp
;
INSERT INTO {mmm_t2_schema}.google_iqv_brand_comp
SELECT Channel_flag,
	QueryLabel,
	QueryType,
	ReportDate,
	TimeGranularity,
	GeoCriteriaId,
	GeoName,
	GeoType,
	IndexedQueryVolume,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.google_iqv_brand_comp_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;
