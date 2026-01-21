create or replace temporary view organic_social_nord_rack_view
(
Day_Date string,
Channel string,
Platform string,
funnel string,
funding_type string,
Impressions string,
Reach string,
Likes string,
Cost string
)
 USING CSV 
 OPTIONS (
    path "s3://mim-production/mmm_data_consolidation/Nordstrom_Rack_organic_social.csv", 
    sep ",",
    header "true" 
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE organic_social_nord_rack_ldg_output
SELECT
to_date(Day_Date,"M/d/yyyy") as Day_Date,
Channel,
Platform,
funnel,
funding_type,
Impressions,
Reach,
Likes,
Cost
FROM organic_social_nord_rack_view
WHERE 1=1
;


