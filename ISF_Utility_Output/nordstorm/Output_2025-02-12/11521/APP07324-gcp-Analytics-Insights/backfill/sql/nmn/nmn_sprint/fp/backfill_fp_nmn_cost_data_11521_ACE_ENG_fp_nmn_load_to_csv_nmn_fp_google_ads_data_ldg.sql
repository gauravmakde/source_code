-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW nmn_fp_google_ads_data_view_csv
(
   stats_date date
    , sourcename string
    , sourcetype string
	, currency string
    , media_type string
    , campaign_name string
    , campaign_id string
    , campaign_type string
    , adgroup_name string
    , adgroup_id string
    , ad_account_name string
    , cost string
    , impressions string
    , clicks string
    , Engagements string
    , conversions string
    , conversion_value string
    , video_views string
)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/GoogleAds/nmn_Google_Ads_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_fp_google_ads_data_ldg_output 

SELECT
      stats_date
    , sourcename  
    , sourcetype  
	, currency  
    , media_type
    , campaign_name  
    , campaign_id  
    , campaign_type
    , adgroup_name  
    , adgroup_id  
    , ad_account_name
    , cost  
    , impressions  
    , clicks  
    , Engagements
    , conversions  
    , conversion_value  
    , video_views
FROM nmn_fp_google_ads_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN date'2022-01-01' AND date'2024-10-03'
;






