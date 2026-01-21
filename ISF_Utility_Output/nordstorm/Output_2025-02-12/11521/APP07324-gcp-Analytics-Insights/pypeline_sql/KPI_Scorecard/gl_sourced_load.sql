-- Reading data from S3 and creating the view

create or replace temporary view gl_sourced_view
(
   Fiscal_year string
	,Box_type string
	,Metric string
	,fiscal_month string
	,TY_Cost string
	,Plan_Cost string
	,LY_Cost string
	,TY_Sales string
	,Plan_Sales string	
	,LY_Sales string   
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/GL_Sourced/GL*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE gl_sourced_ldg_output
SELECT
    Fiscal_year,
    Box_type ,
	Metric,
	fiscal_month,
	TY_Cost,
    Plan_Cost,
	LY_Cost,
	TY_Sales,
	Plan_Sales,
	LY_Sales
FROM gl_sourced_view
WHERE 1=1
;
