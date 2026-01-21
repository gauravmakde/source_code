-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW fp_dv_three_sixty_data_view_csv
(
   stats_date date
    , sourcetype string
    , sourcename string
	, currency string
    , account_name string
    , campaign_name string
    , campaign_id string
    , country string
    , creative string
    , adgroup_name string
    , cost string
    , impressions string
    , clicks string
    , conversions string
    , conversion_value string
	, video_skips string
    , video_views string
    , video25 string
    , video50 string
    , video75 string
	, video100 string
)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/DV360/nmn_dv360_*",
    sep ",",
    header "true"
)
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE fp_dv_three_sixty_data_ldg_output 

SELECT
      stats_date
    , sourcetype
    , sourcename
	, currency
    , account_name
    , campaign_name
    , campaign_id
    , country
    , creative 
    , adgroup_name 
    , cost
    , impressions
    , clicks
    , conversions 
    , conversion_value
	, video_skips
    , video_views 
    , video25 
    , video50 
    , video75 
	, video100 

FROM fp_dv_three_sixty_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;
