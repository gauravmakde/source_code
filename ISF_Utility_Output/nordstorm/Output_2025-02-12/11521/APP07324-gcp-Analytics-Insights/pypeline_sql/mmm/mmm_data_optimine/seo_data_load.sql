create or replace temporary view seo_data_view
(
    day_date string,
Banner string,
Keyword_Type string,
Impressions float,
Clicks float
	   	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/seo_data.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE seo_data_ldg_output
SELECT
	to_date(day_date,"M/d/yyyy") as day_date,
Banner,
Keyword_Type,
Impressions,
Clicks    
FROM seo_data_view
WHERE 1=1
;

