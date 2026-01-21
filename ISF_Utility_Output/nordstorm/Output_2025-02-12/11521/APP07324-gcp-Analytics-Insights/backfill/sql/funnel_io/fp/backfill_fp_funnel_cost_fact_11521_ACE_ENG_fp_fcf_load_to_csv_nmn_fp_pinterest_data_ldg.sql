CREATE OR REPLACE TEMPORARY VIEW nmn_fp_pinterest_data_view_csv
(
      day_date date,
      data_source_type string,
      data_source_name string,
      campaign_id string,
      campaign_name string,
	campaign_status string,
      campaign_objective string,
      spend string,
      total_impressions string,
      clicks string,
      adds_to_cart string,
      web_purchases string,
      offline_purchases string,
      web_purchase_conversion_value string,
      offline_purchase_conversion_value string,
      conversions string,
      total_conversions_value string,
      engagements string,
      video_views string,
      total_video_played_at_100 string,
      total_video_played_at_75 string,
      total_video_played_at_50 string,
      total_video_played_at_25 string,
      daily_reach string,
      daily_frequency string)
USING CSV
OPTIONS (
    path "s3://s3-to-csv-pubs/NMN/Pinterest/nmn_pinterest_*",
    sep ",",
    header "true"
)
;

INSERT OVERWRITE TABLE nmn_fp_pinterest_data_ldg_output 

SELECT
      day_date,
      data_source_type,
      data_source_name,
      campaign_id,
      campaign_name,
	campaign_status,
      campaign_objective,
      spend,
      total_impressions,
      clicks,
      adds_to_cart,
      web_purchases,
      offline_purchases,
      web_purchase_conversion_value,
      offline_purchase_conversion_value,
      conversions,
      total_conversions_value,
      engagements,
      video_views,
      total_video_played_at_100,
      total_video_played_at_75,
      total_video_played_at_50,
      total_video_played_at_25,
      daily_reach,
      daily_frequency
FROM nmn_fp_pinterest_data_view_csv
WHERE 1=1
;
