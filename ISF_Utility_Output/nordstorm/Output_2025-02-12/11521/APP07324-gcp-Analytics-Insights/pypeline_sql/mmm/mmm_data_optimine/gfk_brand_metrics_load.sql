-- Reading data from S3 and creating the view

create or replace temporary view gfk_brand_metrics_view
(
	day_date string,
	Segment string,
	Metric string,
	Metric_Value string
   	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/GFK*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE gfk_brand_metrics_ldg_output
SELECT
   to_date(day_date,"M/d/yyyy") as day_date,
	Segment,
	Metric,
	Metric_Value
FROM gfk_brand_metrics_view
WHERE 1=1
;


