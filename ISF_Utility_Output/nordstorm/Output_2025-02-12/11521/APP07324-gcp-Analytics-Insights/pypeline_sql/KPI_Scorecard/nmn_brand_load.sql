-- Reading data from S3 and creating the view

create or replace temporary view nmn_brand_view
(
   Fiscal_year string	
	,fiscal_month string			
	,No_of_Brand string   
	,LY_No_of_Brand string	
	,Plan_No_of_Brand string	
	,Total_Spend string			
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/NMN_Brand/NMN*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_brand_ldg_output
SELECT
    Fiscal_year,    
	fiscal_month,	
	No_of_Brand,
	LY_No_of_Brand,
	Plan_No_of_Brand,
	Total_Spend
FROM nmn_brand_view
WHERE 1=1
;
