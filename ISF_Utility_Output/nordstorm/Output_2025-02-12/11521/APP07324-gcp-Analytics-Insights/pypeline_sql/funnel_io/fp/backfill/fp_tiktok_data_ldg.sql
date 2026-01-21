-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW fp_tiktok_data_view_csv
(
   stats_date DATE
    , sourcetype string
    , currency string
    , sourcename string
    , media_type string
    , ad_id string
    , ad_name string
    , adgroup_id string
    , adgroup_name string
    , campaign_id string
    , campaign_name string
    , clicks string
    , conversions string
    , impressions string
    , cost string
    , video_views string
    , video100 string
    , video75 string
    , likes string
    , conversion_value string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_tiktok/tiktok_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT INTO TABLE fp_tiktok_data_ldg_output

SELECT
   stats_date
    , sourcetype
    , currency
    , sourcename
    , media_type
    , ad_id
    , ad_name
    , adgroup_id
    , adgroup_name
    , campaign_id
    , campaign_name
    , clicks
    , conversions
    , impressions
    , cost
    , video_views
    , video100
    , video75
    , likes
    , conversion_value

FROM fp_tiktok_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;






