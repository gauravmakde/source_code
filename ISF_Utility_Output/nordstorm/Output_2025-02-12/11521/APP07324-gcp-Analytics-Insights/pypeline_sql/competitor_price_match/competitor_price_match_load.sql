create or replace temporary view price_match_dump_view 
(
  style_num string,
  color_code string,
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
  rms_sku string,
  base_price decimal(12,2)
        )
USING csv
OPTIONS (path "s3://nord-price-comp-intel-prod/ext-dataweave-feed/archives/{year}-*/match_dump*_sorted.txt",
        sep "|",
        header "true"
        )
; 


insert overwrite table competitor_price_matching_ldg_output
select
  style_num AS style_group_num,
  color_code AS nordstrom_direct_color_code,
  size AS product_size ,
  brand AS product_brand_name,
  reg_price AS regular_price,
  comp_name AS competitor_name,
  comp_scrape_date AS comp_scrape_tstamp_est,
  comp_offer_price,
  effective_date AS effective_timestamp_est,
  on_promo  ,
  promo_name ,
  promo_effective_date ,
  vpn ,
  rms_sku AS rms_sku_num,
  base_price AS competitor_base_price, 
  DATE(comp_scrape_date) AS comp_scrape_date_est
from price_match_dump_view
WHERE DATE(comp_scrape_date) BETWEEN date_sub({start_date},1) and date({end_date})