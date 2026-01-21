-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW rack_snapchat_data_view_csv
(    
    stats_date date
    , sourcetype string
    , currency string
    , sourcename string
    , media_type string
    , video_views_15s string    
    , ad_id string
    , ad_name string
    , adgroup_id string
    , adgroup_name string
    , campaign_id string
    , campaign_name string
    , cost string
    , impressions string
    , clicks string
    , video_views string 
    , video100 string      
    , video75 string
    , conversions string
    , conversion_value string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_rack_Snapchat/Snapchat_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT INTO TABLE rack_snapchat_data_ldg_output

SELECT
    stats_date
    , sourcetype
    , currency
    , sourcename
    , media_type
    , video_views_15s    
    , ad_id
    , ad_name
    , adgroup_id
    , adgroup_name
    , campaign_id
    , campaign_name
    , cost
    , impressions
    , clicks
    , video_views
    , video100    
    , video75
    , conversions
    , conversion_value

FROM rack_snapchat_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} and {end_date}
;



