create or replace temporary view pr_data_view
(
	Start_Date string, 
	End_Date string,
	Region string,
	City string,
	State_name string,
	marketing_type string,
	Media_Outlet string,
	Title_name string,
	Total_Readership float,
	Link string,
	Author string,
	Campaign string,
	Notes string
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/PR_data.csv", 	
    sep ",",
    header "true" 
)
;


-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE pr_data_ldg_output
SELECT
    to_date(Start_Date,"M/d/yyyy") as Start_Date,
	to_date(End_Date,"M/d/yyyy") as End_Date,
	Region,
	City,
	State_name,
	marketing_type,
	Media_Outlet,
	Title_name,
	cast(Total_Readership as decimal(12,2)) as Total_Readership,
	Link,
	Author,
	Campaign,
	Notes
FROM pr_data_view
WHERE 1=1
;




