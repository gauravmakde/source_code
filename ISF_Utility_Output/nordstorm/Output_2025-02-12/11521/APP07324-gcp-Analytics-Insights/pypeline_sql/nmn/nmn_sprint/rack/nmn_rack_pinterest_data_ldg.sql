-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW nmn_rack_pinterest_data_view_csv
(
    stats_date date
    , currency string
    , media_type string
    , sourcename string
    , sourcetype string
    , campaign_name string
    , campaign_id string
    , campaign_type string
    , adgroup_name string
    , adgroup_id string
    , cost string
    , impressions string
    , clicks string
    , engagements string
    , video_views string
    , video100 string
    , video75 string
    , conversions string
    , conversion_value string
)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/Rack_Pinterest/nmn_rack_pinterest_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_rack_pinterest_data_ldg_output 

SELECT
    stats_date
    , currency
    , media_type
    , sourcename
    , sourcetype 
    , campaign_name
    , campaign_id
    , campaign_type 
    , adgroup_name
    , adgroup_id
    , cost
    , impressions
    , clicks
    , engagements
    , video_views
    , video100
    , video75
    , conversions
    , conversion_value

FROM nmn_rack_pinterest_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} and {end_date}
;
