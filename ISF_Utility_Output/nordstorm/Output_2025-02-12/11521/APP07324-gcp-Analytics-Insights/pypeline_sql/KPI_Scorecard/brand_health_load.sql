-- Reading data from S3 and creating the view

create or replace temporary view brand_health_view
(
   Fiscal_Year string,
    Fiscal_Month string,
    Banner  string,
    Aware string,
	Familiar string,
	Composite string,
	Shop string,
	Ly_Aware string,
	Ly_Familiar string,
	Ly_Composite string,
	Ly_Shop string  
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/gfk_brand_metrics/brand*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE brand_health_ldg_output
SELECT
   Fiscal_Year,
    Fiscal_Month,
    Banner,
    Aware,
	Familiar,
	Composite,
	Shop,
	Ly_Aware,
	Ly_Familiar,
	Ly_Composite,
	Ly_Shop
FROM brand_health_view
WHERE 1=1
;



