-- Reading data from S3 and creating the view

create or replace temporary view google_iqv_cat_view
(	
	QueryLabel string,
	QueryType string,
	ReportDate string,
	TimeGranularity string,
	GeoCriteriaId string,
	GeoName string,
	GeoType string,
	IndexedQueryVolume string	
   	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/Google_IQV_Category_*.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE google_iqv_cat_ldg_output
SELECT	
	QueryLabel,
	QueryType,
	to_date(ReportDate,"M/d/yyyy") as ReportDate ,
	TimeGranularity,
	GeoCriteriaId,
	GeoName,
	GeoType,
	IndexedQueryVolume   
FROM google_iqv_cat_view
WHERE 1=1
;

