-- Reading data from S3 and creating the view

create or replace temporary view customer_plan_view
(
   fiscal_year string
	,fiscal_month string
	,Box_type string
	,Total_Customers string
	,Retained_Customers string
	,Acquired_Customers string
	,Reactivated_Customers string
	,Total_Trips string
	,Retained_Trips string
	,Acquired_Trips string
	,Reactivated_Trips string
	,Total_Sales string
	,Retained_Sales string
	,Acquired_Sales string
	,Reactivated_Sales string   
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/buyerflow_plan/Customer*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE customer_plan_ldg_output
SELECT
   fiscal_year,
	fiscal_month,
	Box_type,
	Total_Customers,
	Retained_Customers,
	Acquired_Customers,
	Reactivated_Customers,
	Total_Trips,
	Retained_Trips,
	Acquired_Trips,
	Reactivated_Trips,
	Total_Sales,
	Retained_Sales,
	Acquired_Sales,
	Reactivated_Sales
FROM customer_plan_view
WHERE 1=1
;



