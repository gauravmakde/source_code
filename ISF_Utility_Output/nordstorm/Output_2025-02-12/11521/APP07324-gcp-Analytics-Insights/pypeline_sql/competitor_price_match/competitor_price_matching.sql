create or replace temporary view price_match_dump_view 
(
  style_num bigint,
  color_code int,
  color_description string,
  size string,
  size_display_name string,
  perc_of_sales decimal(12,2),
  title string,
  brand string,
  merch_l1_name string,
  merch_l2_name string,
  reg_price decimal(12,2),
  on_sale int,
  comp_name string,
  comp_scrape_date timestamp,
  comp_offer_price decimal(12,2),
  effective_date timestamp,
  on_promo int,
  promo_name string,
  promo_effective_date date,
  comp_url string,
  comp_sku bigint,
  comp_color string,
  vpn string,
  rms_sku bigint,
  base_price decimal(12,2)
        )
USING csv
OPTIONS (path "s3://nord-price-comp-intel-prod/ext-dataweave-feed/archives/{year}-*/match_dump*_sorted.txt",
        sep "|",
        header "true"
        )
;

create table if not exists {hive_schema}.competitor_price_matching_match_dump
(
  style_num bigint,
  color_code int,
  color_description string,
  size string,
  size_display_name string,
  perc_of_sales decimal(12,2),
  title string,
  brand string,
  merch_l1_name string,
  merch_l2_name string,
  reg_price decimal(12,2),
  on_sale int,
  comp_name string,
  comp_scrape_tstamp_est timestamp,
  comp_offer_price decimal(12,2),
  effective_tstamp_est timestamp,
  on_promo int,
  promo_name string,
  promo_effective_date_est date,
  comp_url string,
  comp_sku bigint,
  comp_color string,
  vpn string,
  rms_sku bigint,
  base_price decimal(12,2),
  sys_tstamp_pacific timestamp,
  comp_scrape_date_partition_est date
        )
USING PARQUET
location 's3://{s3_bucket_root_var}/comp_price_match/match_dump/'
partitioned by (comp_scrape_date_partition_est);

--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table {hive_schema}.competitor_price_matching_match_dump;


insert overwrite table {hive_schema}.competitor_price_matching_match_dump partition (comp_scrape_date_partition_est)
select
  style_num,
  color_code,
  color_description,
  size ,
  size_display_name ,
  perc_of_sales,
  title,
  brand,
  merch_l1_name,
  merch_l2_name,
  reg_price,
  on_sale ,
  comp_name ,
  comp_scrape_date AS comp_scrape_tstamp_est,
  comp_offer_price ,
  effective_date AS effective_tstamp_est,
  on_promo ,
  promo_name ,
  promo_effective_date AS promo_effective_date_est,
  comp_url ,
  comp_sku ,
  comp_color ,
  vpn ,
  rms_sku,
  base_price,
  from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Pacific') AS sys_tstamp_pacific,
  DATE(comp_scrape_date) AS comp_scrape_date_partition_est
from price_match_dump_view
WHERE DATE(comp_scrape_date) BETWEEN {start_date} and {end_date};