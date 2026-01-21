-- Reading data from S3 and creating the view

create or replace temporary view loyalty_view
(
   Fiscal_Year string
	,Fiscal_Month string
	,Banner string
	,Loyalty_Deferred_Rev string
	,Gift_Card_Deferred_Rev string
	,Loyalty_OPEX string
	,Credit_Ebit string
	,Tender_Expense_Avoidance string
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/Loyalty/Loyalty*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE loyalty_ldg_output
SELECT
   Fiscal_Year,
	Fiscal_Month,
	Banner,
	Loyalty_Deferred_Rev,
	Gift_Card_Deferred_Rev,
	Loyalty_OPEX,
	Credit_Ebit,
	Tender_Expense_Avoidance
FROM loyalty_view
WHERE 1=1
;



