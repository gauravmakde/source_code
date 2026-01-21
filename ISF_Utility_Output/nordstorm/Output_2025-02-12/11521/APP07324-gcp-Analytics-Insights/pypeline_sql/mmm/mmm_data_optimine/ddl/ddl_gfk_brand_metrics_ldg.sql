SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=gfk_brand_metrics_11521_ACE_ENG;
     Task_Name=ddl_gfk_brand_metrics_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.gfk_brand_metrics 
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'gfk_brand_metrics_ldg', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.gfk_brand_metrics_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
	day_date DATE FORMAT 'YY/MM/DD',
	Segment VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Metric VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Metric_Value INTEGER
      )

PRIMARY INDEX (day_date,Segment,Metric)
;


SET QUERY_BAND = NONE FOR SESSION;



