-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW rack_verizon_data_view_csv
(
   stats_date date
    , currency string
    , sourcename string
    , campaign_id string
    , campaign_name string
    , adgroup_id string
    , adgroup_name string
    , ad_id string
    , ad_name string
    , media_type string
    , cost string
    , impressions string
    , clicks string
    , video100 string
    , video75 string
    , video_views string
    , conversions string
    , conversion_value string
    , device_type string    
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_rack_Verizon/verizon_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT INTO TABLE rack_verizon_data_ldg_output

SELECT
    stats_date
    , currency
    , sourcename
    , campaign_id
    , campaign_name
    , adgroup_id    
    , adgroup_name
    , ad_id
    , ad_name
    , cost
    , impressions
    , clicks
    , video100
    , video75
    , video_views    
    , conversions
    , conversion_value
    , device_type    

FROM rack_verizon_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} and {end_date}
;



