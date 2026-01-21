-- Reading data from S3 and creating the view

create or replace temporary view nmn_gl_view
(
   Fiscal_year string	
	,fiscal_month string	
	,TY_Sales string	
	,LY_Sales string   
	,Plan_Sales string	
	,TY_Earnings string	
	,LY_Earnings string	
	, Plan_Earnings string	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/NMN/NMN*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_gl_ldg_output
SELECT
    Fiscal_year,    
	fiscal_month,	
	TY_Sales,
	LY_Sales,
	Plan_Sales,
	TY_Earnings,
	LY_Earnings,
	Plan_Earnings
FROM nmn_gl_view
WHERE 1=1
;
