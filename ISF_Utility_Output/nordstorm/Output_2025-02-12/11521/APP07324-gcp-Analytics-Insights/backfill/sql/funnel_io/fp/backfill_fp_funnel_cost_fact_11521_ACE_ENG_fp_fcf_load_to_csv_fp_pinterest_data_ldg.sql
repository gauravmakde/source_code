-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW fp_pinterest_data_view_csv
(
    stats_date date
    , sourcetype string
    , currency string
    , media_type string
    , sourcename string
    , campaign_name string
    , campaign_id string
    , adgroup_name string
    , adgroup_id string
    , cost string
    , impressions string
    , clicks string
    , video_views string
    , video75 string
    , video100 string
    , conversions string
    , conversion_value string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_pinterest/pinterest_2024*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE fp_pinterest_data_ldg_output 

SELECT
    stats_date
    , sourcetype
    , currency
    , media_type
    , sourcename
    , campaign_name
    , campaign_id
    , adgroup_name
    , adgroup_id
    , cost
    , impressions
    , clicks
    , video_views
    , video75
    , video100
    , conversions
    , conversion_value

FROM fp_pinterest_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN '2024-01-01' AND '2024-07-01'
;





