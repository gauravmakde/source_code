-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW fp_snapchat_data_view_csv
(
    stats_date date
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
    , video100 string
    , cost string
    , impressions string
    , clicks string
    , video_views string
    , video75 string
    , conversions string
    , conversion_value string
    , video_views_15s string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_snapchat/snapchat_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT INTO TABLE fp_snapchat_data_ldg_output

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
    , video100
    , cost
    , impressions
    , clicks
    , video_views
    , video75
    , conversions
    , conversion_value
    , video_views_15s

FROM fp_snapchat_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;





