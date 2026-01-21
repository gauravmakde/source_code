create or replace temporary view organic_social_nord_view
(
Day_Date string,
Facebook_reach string,
Facebook_visit string,
Pinterest_Impressions string,
Facebook_Likes string	
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/Nordstrom_organic_social.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE organic_social_nord_ldg_output
SELECT
 to_date(Day_Date,"M/d/yyyy") as Day_Date,
Facebook_reach,
Facebook_visit,
Pinterest_Impressions,
Facebook_Likes
FROM organic_social_nord_view
WHERE 1=1
;



