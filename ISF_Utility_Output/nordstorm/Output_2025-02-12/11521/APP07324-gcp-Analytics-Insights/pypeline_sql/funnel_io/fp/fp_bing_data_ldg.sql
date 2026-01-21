-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW fp_bing_data_view_csv
(
    stats_date date
    , sourcetype string
    , currency string
    , sourcename string
    , media_type string
    , campaign_name string
    , campaign_id string
    , adgroup_name string
    , adgroup_id string
    , device_type string
    , cost string
    , impressions string
    , clicks string
    , conversions string
    , conversion_value string
    , account_name string
    , campaign_type string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_bing/bing_{year}*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table. 
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE fp_bing_data_ldg_output 

SELECT
    stats_date
    , sourcetype
    , currency
    , sourcename
    , media_type
    , campaign_name
    , campaign_id
    , adgroup_name
    , adgroup_id
    , device_type
    , cost
    , impressions
    , clicks
    , conversions
    , conversion_value
    , account_name
    , campaign_type

FROM fp_bing_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} and {end_date}
;




