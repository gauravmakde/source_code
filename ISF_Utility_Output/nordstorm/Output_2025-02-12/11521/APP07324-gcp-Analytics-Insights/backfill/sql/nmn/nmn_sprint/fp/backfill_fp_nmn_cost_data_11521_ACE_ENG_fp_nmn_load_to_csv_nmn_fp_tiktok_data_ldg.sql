-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW nmn_fp_tiktok_data_view_csv
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
    , campaign_type string
    , clicks string
    , conversions string
    , Engagements string
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
    path "s3://s3-to-csv-pubs/NMN/fp_TikTok/nmn_tiktok_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_fp_tiktok_data_ldg_output 

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
    , campaign_type
    , clicks
    , conversions
    , Engagements
    , impressions
    , cost
    , video_views
    , video100
    , video75
    , likes
    , conversion_value

FROM nmn_fp_tiktok_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN date'2022-01-01' AND date'2024-10-03'
;







