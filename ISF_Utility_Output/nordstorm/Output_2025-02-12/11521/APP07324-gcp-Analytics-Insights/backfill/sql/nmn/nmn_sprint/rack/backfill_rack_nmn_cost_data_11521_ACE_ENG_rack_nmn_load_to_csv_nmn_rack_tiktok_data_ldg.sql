-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW nmn_rack_tiktok_data_view_csv
(
   stats_date DATE
    , currency string
    , media_type string    
    , sourcename string
    , sourcetype string
    , campaign_name string    
    , campaign_id string  
    , campaign_type string  
    , adgroup_name string    
    , adgroup_id string    
    , ad_name string
    , ad_id string
    , cost string
    , impressions string
    , clicks string   
    , engagements string
    , video_views string
    , video100 string
    , video75 string
    , conversions string
)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/Rack_TikTok/nmn_rack_tiktok_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_rack_tiktok_data_ldg_output 

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
    , ad_name
    , ad_id
    , cost
    , impressions
    , clicks
    , engagements
    , video_views
    , video100
    , video75
    , conversions

FROM nmn_rack_tiktok_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN date'2022-01-01' and date'2024-10-15'
;

