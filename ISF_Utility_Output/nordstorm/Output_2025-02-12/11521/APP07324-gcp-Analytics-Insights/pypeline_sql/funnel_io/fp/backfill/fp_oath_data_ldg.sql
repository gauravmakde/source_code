-- Reading data from S3 and creating the view

CREATE OR REPLACE TEMPORARY VIEW fp_oath_data_view_csv
(
   stats_date Date
  , currency string
  , sourcename string
  , campaign_id string
  , campaign_name string
  , adgroup_id string
  , adgroup_name string
  , ad_id string
  , ad_name string
  , media_type string
  , device_type string
  , cost string
  , impressions string
  , clicks string
  , video100 string
  , video75 string
  , video_views string
  , conversions string
  , conversion_value string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_oath/oath_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT INTO TABLE fp_oath_data_ldg_output

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
  , media_type
  , device_type
  , cost
  , impressions
  , clicks
  , video100
  , video75
  , video_views
  , conversions
  , conversion_value

FROM fp_oath_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN {start_date} AND {end_date}
;





