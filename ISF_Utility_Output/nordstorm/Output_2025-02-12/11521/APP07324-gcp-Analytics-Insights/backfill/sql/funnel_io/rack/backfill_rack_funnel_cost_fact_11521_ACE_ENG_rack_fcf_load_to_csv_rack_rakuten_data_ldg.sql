-- Reading data from S3 and creating the view
CREATE OR REPLACE TEMPORARY VIEW rack_rakuten_data_view_csv
(
   stats_date DATE
    , sourcetype string
    , currency string
    , platform string
    , platform_id string
    , adgroup_name string
    , adgroup_id string
    , order_number string
    , estimated_net_total_cost string
    , gross_commissions string
    , gross_sales string
    , sales string
    , campaign_name string
    , clicks string
)
USING CSV
OPTIONS (
    path "s3://funnel-io-exports/nordstrom_rack_Rakuten/rakuten_2024*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE rack_rakuten_data_ldg_output 

SELECT
   stats_date
    , sourcetype
    , currency
    , platform
    , platform_id
    , adgroup_name
    , adgroup_id
    , order_number
    , estimated_net_total_cost
    , gross_commissions
    , gross_sales
    , sales
    , campaign_name
    , clicks

FROM rack_rakuten_data_view_csv

WHERE 1=1
    AND stats_date BETWEEN '2024-01-01' and '2024-07-01'
;



