-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW rack_tiktok_data_view_csv
(
   stats_date DATE
    , currency string
    , media_type string    
    , sourcename string
    , sourcetype string
    , campaign_name string    
    , campaign_id string    
    , adgroup_name string    
    , adgroup_id string    
    , ad_name string
    , ad_id string
    , cost string
    , impressions string
    , clicks string    
    , video_views string
    , video100 string
    , video75 string
    , conversions string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_rack_Tiktok/tiktok_2024*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE rack_tiktok_data_ldg_output 

SELECT
   stats_date
    , currency
    , media_type   
    , sourcename    
    , sourcetype
    , campaign_name   
    , campaign_id
    , adgroup_name
    , adgroup_id
    , ad_name
    , ad_id
    , cost
    , impressions
    , clicks
    , video_views
    , video100
    , video75
    , conversions

FROM rack_tiktok_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN '2024-01-01' and '2024-07-01'
;



