-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW fp_gemini_data_view_csv
(
   stats_date date
    , currency string
    , media_type string
    , sourcename string
    , sourcetype string
    , adgroup_id string
    , adgroup_name string
    , campaign_id string
    , campaign_name string
    , clicks string
    , conversion_value string
    , conversions string
    , device_type string
    , impressions string
    , cost string
    , video100 string
    , video75 string
    , video_views string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_gemini/gemini_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT INTO TABLE fp_gemini_data_ldg_output

SELECT
    stats_date
    , currency
    , media_type
    , sourcename
    , sourcetype
    , adgroup_id
    , adgroup_name
    , campaign_id
    , campaign_name
    , clicks
    , conversion_value
    , conversions
    , device_type
    , impressions
    , cost
    , video100
    , video75
    , video_views

FROM fp_gemini_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;






