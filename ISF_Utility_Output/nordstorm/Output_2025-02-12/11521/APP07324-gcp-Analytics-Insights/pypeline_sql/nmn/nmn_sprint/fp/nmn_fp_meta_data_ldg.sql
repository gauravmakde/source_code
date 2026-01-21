-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW nmn_fp_meta_data_view_csv
(
   stats_date date
    , sourcetype string
    , sourcename string
    , media_type string
    , campaign_name string
    , campaign_id string
    , campaign_type string
    , adgroup_name string
    , adgroup_id string
    , ad_name string
    , ad_id string
    , device_type string
    , platform string
    , currency string
    , cost string
    , impressions string
    , clicks string
    , video100 string
    , video75 string
    , video_views string
    , link_clicks string
    , engagements string
    , conversions string
    , conversion_value string
)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/Facegram/nmn_meta_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_fp_meta_data_ldg_output 

SELECT
    stats_date
    , sourcetype
    , sourcename
    , media_type
    , campaign_name
    , campaign_id
    , campaign_type
    , adgroup_name
    , adgroup_id
    , ad_name
    , ad_id
    , device_type
    , platform
    , currency
    , cost
    , impressions
    , clicks
    , video100
    , video75
    , video_views
    , link_clicks 
    , engagements 
    , conversions
    , conversion_value

FROM nmn_fp_meta_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}

;








