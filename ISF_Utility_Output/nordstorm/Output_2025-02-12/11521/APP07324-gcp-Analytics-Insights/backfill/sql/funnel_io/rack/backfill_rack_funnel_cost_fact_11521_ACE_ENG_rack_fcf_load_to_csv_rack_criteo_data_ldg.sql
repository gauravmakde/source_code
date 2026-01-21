CREATE OR REPLACE TEMPORARY VIEW rack_criteo_data_view_csv
(
      day_date date,
      data_source_type string,
      currency string,
      data_source_name string,
      campaign_id string,
      campaign_name string,
	campaign_type string,
      spend string,
      clicks string,
      impressions string,
      attributed_orders string,
      attributed_sales string,
      attributed_units string)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/Rack_Criteo/rack_criteo_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE rack_criteo_data_ldg_output 

SELECT
      day_date,
      data_source_type,
      currency,
      data_source_name,
      campaign_id,
      campaign_name,
	campaign_type,
      spend,
      clicks,
      impressions,
      attributed_orders,
      attributed_sales,
      attributed_units
FROM rack_criteo_data_view_csv
WHERE 1=1
AND day_date BETWEEN '2024-01-01' AND '2024-07-01';
