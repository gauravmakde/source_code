CREATE OR REPLACE TEMPORARY VIEW nmn_rack_criteo_data_view_csv
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
      engagements string,
      attributed_orders string,
      attributed_sales string,
      attributed_units string)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/NMN_Rack_Criteo/nmn_rack_criteo_*",
    sep ",",
    header "true"
)
;
-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
INSERT OVERWRITE TABLE nmn_rack_criteo_data_ldg_output 

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
      engagements,
      attributed_orders,
      attributed_sales,
      attributed_units
FROM nmn_rack_criteo_data_view_csv
WHERE 1=1
AND day_date BETWEEN date'2022-01-01' AND date'2024-10-15';

