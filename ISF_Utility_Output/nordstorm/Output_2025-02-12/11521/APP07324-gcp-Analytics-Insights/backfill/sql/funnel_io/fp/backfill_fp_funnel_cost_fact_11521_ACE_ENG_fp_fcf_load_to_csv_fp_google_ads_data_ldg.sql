-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW fp_google_ads_data_view_csv
(
   stats_date date
    , sourcename string
    , sourcetype string
	, currency string
    , media_type string
    , campaign_name string
    , campaign_id string
    , adgroup_name string
    , adgroup_id string
    , ad_account_name string
    , cost string
    , impressions string
    , clicks string
    , conversions string
    , conversion_value string
    , video_views string
)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/Google/nmn_Google_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE fp_google_ads_data_ldg_output 

SELECT
      stats_date
    , sourcename  
    , sourcetype  
	, currency  
    , media_type
    , campaign_name  
    , campaign_id  
    , adgroup_name  
    , adgroup_id  
    , ad_account_name
    , cost  
    , impressions  
    , clicks  
    , conversions  
    , conversion_value  
    , video_views
FROM fp_google_ads_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN '2024-01-01' AND '2024-07-01'
;
