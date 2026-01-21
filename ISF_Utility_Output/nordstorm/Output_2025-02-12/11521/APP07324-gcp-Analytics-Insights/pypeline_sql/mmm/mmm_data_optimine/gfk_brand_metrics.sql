SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=gfk_brand_metrics_11521_ACE_ENG;
     Task_Name=gfk_brand_metrics;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.gfk_brand_metrics
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.gfk_brand_metrics
;
INSERT INTO {mmm_t2_schema}.gfk_brand_metrics
SELECT day_date,
	Segment,
	Metric,
	Metric_Value,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.gfk_brand_metrics_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;



