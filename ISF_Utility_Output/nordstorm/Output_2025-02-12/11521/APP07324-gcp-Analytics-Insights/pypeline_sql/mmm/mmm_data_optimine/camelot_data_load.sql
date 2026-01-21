create or replace temporary view camelot_data_view
(
	start_date string,
end_date string,
banner string,
campaign string,
dma string,
platform string,
channel string,
BAR string,
Ad_Type string,
Funnel string,
External_Funding string,
Impressions string,
Cost string
	)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/Camelot_data_New.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE camelot_data_ldg_output
SELECT
TO_DATE(CAST(UNIX_TIMESTAMP(start_date, 'M/d/yyyy') AS TIMESTAMP)) AS start_date,
TO_DATE(CAST(UNIX_TIMESTAMP(end_date, 'M/d/yyyy') AS TIMESTAMP)) AS end_date,
banner,
campaign,
dma,
platform,
channel,
BAR,
Ad_Type,
Funnel,
External_Funding,
Impressions,
Cost    
FROM camelot_data_view
WHERE 1=1
;

