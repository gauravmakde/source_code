-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW fp_adwords_data_view_csv
(
   stats_date date
    , sourcetype string
    , currency string
    , sourcename string
    , campaign_id string
    , campaign_name string
    , advertising_channel string
    , adgroup_name string
    , adgroup_id string
    , ad_name string
    , ad_id string
    , device_type string
    , cost string
    , impressions string
    , clicks string
    , conversions string
    , conversion_value string
    , media_type string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_adwords/adwords_2024*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE fp_adwords_data_ldg_output 

SELECT
    stats_date
    , sourcetype
    , currency
    , sourcename
    , campaign_id
    , campaign_name
    , advertising_channel
    , adgroup_name
    , adgroup_id
    , ad_name
    , ad_id
    , device_type
    , cost
    , impressions
    , clicks
    , conversions
    , conversion_value
    , media_type

FROM fp_adwords_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN '2024-01-01' and '2024-07-01'
;





