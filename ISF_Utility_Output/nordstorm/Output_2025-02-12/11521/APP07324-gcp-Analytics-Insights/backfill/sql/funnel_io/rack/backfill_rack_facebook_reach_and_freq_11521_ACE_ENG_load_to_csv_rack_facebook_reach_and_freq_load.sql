CREATE OR REPLACE TEMPORARY VIEW nmn_rack_facebook_reach_and_freq
(
      day_date date,
      data_source_type string,
      data_source_name string,
      campaign_id string,
      campaign_name string,
      campaign_objective string,
      attribution_setting string,
      daily_reach string,
      daily_frequency string )
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/Rack_Facegram/Reach_And_Frequency/rack_facegram_RF_*",
    sep ",",
    header "true"
)
;


INSERT OVERWRITE TABLE rack_facebook_reach_and_freq_ldg_output 

SELECT
    day_date,
      data_source_type,
      data_source_name,
      campaign_id,
      campaign_name,
      campaign_objective,
      attribution_setting,
      daily_reach,
      daily_frequency
FROM nmn_rack_facebook_reach_and_freq
WHERE 1=1
;
