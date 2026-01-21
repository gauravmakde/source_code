-- Reading data from S3 and creating the view

create or replace temporary view mmm_decomp_export_view
(
   Fiscal_Month string,
	Channel string,
    Touchpoint string,
	Spend string,
	Events string,
	US_FLS_Net_Revenue string,
	US_N_com_New_Customers string,
	US_N_com_Net_Revenue string,
	US_FLS_New_Customers string,
	US_NRHL_New_Customers string,
	US_NRHL_Net_Revenue string,
	US_Rack_New_Customers string,
	US_Rack_Net_Revenue string,
	Total_US_New_Customers string,
	Total_US_Net_Revenue string,
	US_Rack_Web_Traffic string,
	US_FLS_Web_Traffic string  
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/KPI_Scorecard/MMM/MMM_Actual*", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE mmm_decomp_export_ldg_output
SELECT
   Fiscal_Month,
	Channel,
    Touchpoint,
	Spend,
	Events,
	US_FLS_Net_Revenue,
	US_N_com_New_Customers,
	US_N_com_Net_Revenue,
	US_FLS_New_Customers,
	US_NRHL_New_Customers,
	US_NRHL_Net_Revenue,
	US_Rack_New_Customers,
	US_Rack_Net_Revenue,
	Total_US_New_Customers,
	Total_US_Net_Revenue,
	US_Rack_Web_Traffic,
	US_FLS_Web_Traffic  
FROM mmm_decomp_export_view
WHERE 1=1
;



