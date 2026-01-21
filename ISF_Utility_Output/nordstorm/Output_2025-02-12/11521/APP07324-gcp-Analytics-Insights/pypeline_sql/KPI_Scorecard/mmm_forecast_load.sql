-- Reading data from S3 and creating the view

create or replace temporary view mmm_forecast_view
(
   Brand string,
    Funnel string,
    Channel  string,
    Fiscal_Month string,
	Spend string,
	FLS_Net_Revenue string,
	FLS_New_Customers string,
	NCOM_Net_Revenue string,
	NCOM_New_Customers string,
	Rack_Net_Revenue string,
	Rack_New_Customers string,
	RCOM_Net_Revenue string,
	RCOM_New_Customers string
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/MMM/MMM_Forecast*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE mmm_forecast_ldg_output
SELECT
   Brand,
    Funnel,
    Channel,
    Fiscal_Month,
	Spend,
	FLS_Net_Revenue,
	FLS_New_Customers,
	NCOM_Net_Revenue,
	NCOM_New_Customers,
	Rack_Net_Revenue,
	Rack_New_Customers,
	RCOM_Net_Revenue,
	RCOM_New_Customers 
FROM mmm_forecast_view
WHERE 1=1
;



