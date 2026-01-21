-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW nmn_fp_pinterest_data_view_csv
(
    stats_date date
    , sourcetype string
    , currency string
    , media_type string
    , sourcename string
    , campaign_name string
    , campaign_id string
    , campaign_type string
    , adgroup_name string
    , adgroup_id string
    , cost string
    , impressions string
    , clicks string
    , Engagements string
    , video_views string
    , video75 string
    , video100 string
    , conversions string
    , conversion_value string
)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/fp_Pinterest/nmn_fp_pinterest_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_fp_pinterest_data_ldg_output 

SELECT
    stats_date
    , sourcetype
    , currency
    , media_type
    , sourcename
    , campaign_name
    , campaign_id
    , campaign_type
    , adgroup_name
    , adgroup_id
    , cost
    , impressions
    , clicks
    , Engagements
    , video_views
    , video75
    , video100
    , conversions
    , conversion_value

FROM nmn_fp_pinterest_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN date'2022-01-01' AND date'2024-10-03'
;






