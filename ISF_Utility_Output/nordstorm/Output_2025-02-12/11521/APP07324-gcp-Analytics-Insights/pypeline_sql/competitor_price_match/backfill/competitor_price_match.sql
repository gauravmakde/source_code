SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=competitor_price_matching_11521_ACE_ENG;
     Task_Name=competitor_price_match;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_PRICE_MATCHING.competitor_price_match_dump
Owner: Analytics Engineering
Modified: 2023-11-01

SQL moves data from the landing table to the final T2 table for the lookback period.
*/

DELETE FROM {price_matching_t2_schema}.competitor_price_match_dump 
WHERE comp_scrape_date_est BETWEEN '2022-12-01' and '2023-08-04';

INSERT INTO {price_matching_t2_schema}.competitor_price_match_dump
SELECT DISTINCT
style_group_num,
nordstrom_direct_color_code,
product_size,
product_brand_name,
regular_price,
competitor_name,
comp_scrape_tstamp_est,
comp_offer_price,
effective_timestamp_est,
on_promo ,
promo_name ,
promo_effective_date,
vpn,
rms_sku_num,
competitor_base_price, 
CAST(comp_scrape_tstamp_est AS DATE FORMAT 'YYYY/MM/DD') AS comp_scrape_date_est,
CURRENT_TIMESTAMP as dw_sys_load_timestamp_pst
FROM  {price_matching_t2_schema}.competitor_price_match_dump_ldg
WHERE 1=1
AND comp_scrape_date_est BETWEEN '2022-12-01' and '2023-08-04'
;


COLLECT STATISTICS COLUMN (rms_sku_num,comp_scrape_date_est) ON {price_matching_t2_schema}.competitor_price_match_dump;
COLLECT STATISTICS COLUMN (rms_sku_num) ON {price_matching_t2_schema}.competitor_price_match_dump;
COLLECT STATISTICS COLUMN (comp_scrape_date_est) ON {price_matching_t2_schema}.competitor_price_match_dump;


SET QUERY_BAND = NONE FOR SESSION;