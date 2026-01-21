BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_11521_ACE_ENG;
---   Task_Name=run_cco_job_7_cco_cust_chan_yr_attributes_fy;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cco_strategy_line_item_extract (
acp_id STRING,
date_shopped DATE,
fiscal_year_shopped STRING,
banner STRING,
channel STRING,
store_num INTEGER,
gross_sales BIGNUMERIC,
gross_incl_gc BIGNUMERIC,
return_amt BIGNUMERIC,
net_sales BIGNUMERIC,
gross_items INTEGER,
return_items INTEGER,
net_items INTEGER,
event_anniversary INTEGER,
event_holiday INTEGER,
npg_flag INTEGER,
div_num INTEGER
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cco_strategy_line_item_extract
(SELECT acp_id,
  date_shopped,
  fiscal_year_shopped,
  banner,
  channel,
  store_num,
  gross_sales,
  gross_incl_gc,
  return_amt,
  net_sales,
  gross_items,
  return_items,
  net_items,
  event_anniversary,
  event_holiday,
   CASE
   WHEN LOWER(npg_flag) = LOWER('Y')
   THEN 1
   ELSE 0
   END AS npg_flag,
  div_num
 FROM `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id,fiscal_year_shopped,channel) on cco_strategy_line_item_extract;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_level_derived (
acp_id STRING,
fiscal_year_shopped STRING,
channel STRING,
cust_chan_gross_sales BIGNUMERIC,
cust_chan_return_amt BIGNUMERIC,
cust_chan_net_sales BIGNUMERIC,
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary INTEGER,
cust_chan_holiday INTEGER,
cust_chan_npg INTEGER,
cust_chan_div_count INTEGER,
cust_chan_net_sales_ly BIGNUMERIC,
cust_chan_net_sales_ny BIGNUMERIC,
cust_chan_anniversary_ly INTEGER,
cust_chan_holiday_ly INTEGER
) ;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_chan_level_derived_nopi (
acp_id STRING,
fiscal_year_shopped STRING,
channel STRING,
cust_chan_gross_sales BIGNUMERIC,
cust_chan_return_amt BIGNUMERIC,
cust_chan_net_sales BIGNUMERIC,
cust_chan_trips INTEGER,
cust_chan_gross_items INTEGER,
cust_chan_return_items INTEGER,
cust_chan_net_items INTEGER,
cust_chan_anniversary INTEGER,
cust_chan_holiday INTEGER,
cust_chan_npg INTEGER,
cust_chan_div_count INTEGER
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_chan_level_derived_nopi
(SELECT *
 FROM (SELECT acp_id,
     fiscal_year_shopped,
     channel,
     SUM(gross_sales) AS cust_chan_gross_sales,
     SUM(return_amt) AS cust_chan_return_amt,
     SUM(net_sales) AS cust_chan_net_sales,
     COUNT(DISTINCT CASE
       WHEN gross_incl_gc > 0
       THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
       ELSE NULL
       END) AS cust_chan_trips,
     SUM(gross_items) AS cust_chan_gross_items,
     SUM(return_items) AS cust_chan_return_items,
     SUM(gross_items - return_items) AS cust_chan_net_items,
     MAX(event_anniversary) AS cust_chan_anniversary,
     MAX(event_holiday) AS cust_chan_holiday,
     MAX(npg_flag) AS cust_chan_npg,
     COUNT(DISTINCT div_num) AS cust_chan_div_count
    FROM cco_strategy_line_item_extract
    GROUP BY acp_id,
     fiscal_year_shopped,
     channel
    UNION ALL
    SELECT acp_id,
     fiscal_year_shopped,
      CASE
      WHEN LOWER(banner) = LOWER('NORDSTROM')
      THEN '5) Nordstrom Banner'
      WHEN LOWER(banner) = LOWER('RACK')
      THEN '6) Rack Banner'
      ELSE NULL
      END AS channel,
     SUM(gross_sales) AS cust_chan_gross_sales,
     SUM(return_amt) AS cust_chan_return_amt,
     SUM(net_sales) AS cust_chan_net_sales,
     COUNT(DISTINCT CASE
       WHEN gross_incl_gc > 0
       THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
       ELSE NULL
       END) AS cust_chan_trips,
     SUM(gross_items) AS cust_chan_gross_items,
     SUM(return_items) AS cust_chan_return_items,
     SUM(gross_items - return_items) AS cust_chan_net_items,
     MAX(event_anniversary) AS cust_chan_anniversary,
     MAX(event_holiday) AS cust_chan_holiday,
     MAX(npg_flag) AS cust_chan_npg,
     COUNT(DISTINCT div_num) AS cust_chan_div_count
    FROM cco_strategy_line_item_extract
    GROUP BY acp_id,
     fiscal_year_shopped,
     channel
    UNION ALL
    SELECT acp_id,
     fiscal_year_shopped,
     '7) JWN' AS channel,
     SUM(gross_sales) AS cust_chan_gross_sales,
     SUM(return_amt) AS cust_chan_return_amt,
     SUM(net_sales) AS cust_chan_net_sales,
     COUNT(DISTINCT CASE
       WHEN gross_incl_gc > 0
       THEN acp_id || FORMAT('%11d', store_num) || CAST(date_shopped AS STRING)
       ELSE NULL
       END) AS cust_chan_trips,
     SUM(gross_items) AS cust_chan_gross_items,
     SUM(return_items) AS cust_chan_return_items,
     SUM(gross_items - return_items) AS cust_chan_net_items,
     MAX(event_anniversary) AS cust_chan_anniversary,
     MAX(event_holiday) AS cust_chan_holiday,
     MAX(npg_flag) AS cust_chan_npg,
     COUNT(DISTINCT div_num) AS cust_chan_div_count
    FROM cco_strategy_line_item_extract
    GROUP BY acp_id,
     fiscal_year_shopped,
     channel) AS t5);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO cust_chan_level_derived
(SELECT acp_id,
  fiscal_year_shopped,
  channel,
  cust_chan_gross_sales,
  cust_chan_return_amt,
  cust_chan_net_sales,
  cust_chan_trips,
  cust_chan_gross_items,
  cust_chan_return_items,
  cust_chan_net_items,
  cust_chan_anniversary,
  cust_chan_holiday,
  cust_chan_npg,
  cust_chan_div_count,
  LAG(cust_chan_net_sales) OVER (PARTITION BY acp_id, channel ORDER BY fiscal_year_shopped) AS cust_chan_net_sales_ly,
  LEAD(cust_chan_net_sales) OVER (PARTITION BY acp_id, channel ORDER BY fiscal_year_shopped) AS cust_chan_net_sales_ny,
  LAG(cust_chan_anniversary) OVER (PARTITION BY acp_id, channel ORDER BY fiscal_year_shopped) AS
  cust_chan_anniversary_ly,
  LAG(cust_chan_holiday) OVER (PARTITION BY acp_id, channel ORDER BY fiscal_year_shopped) AS cust_chan_holiday_ly
 FROM cust_chan_level_derived_nopi);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id,fiscal_year_shopped,channel) on cust_chan_level_derived;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_jwn_rank
AS
SELECT acp_id,
 fiscal_year_shopped,
 cust_chan_net_sales,
 ROW_NUMBER() OVER (PARTITION BY fiscal_year_shopped ORDER BY cust_chan_net_sales DESC) AS rank_record
FROM cust_chan_level_derived
WHERE LOWER(channel) = LOWER('7) JWN')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY fiscal_year_shopped ORDER BY cust_chan_net_sales DESC)) <= 500000;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS experian_likelihood_scores
AS
SELECT acp_id,
  CASE
  WHEN social_media_predictions_facebook_usage_likelihood IS NULL OR LOWER(social_media_predictions_facebook_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(social_media_predictions_facebook_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_social_media_predictions_facebook_usage_score,
  CASE
  WHEN social_media_predictions_instagram_usage_likelihood IS NULL OR LOWER(social_media_predictions_instagram_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(social_media_predictions_instagram_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_social_media_predictions_instagram_usage_score,
  CASE
  WHEN social_media_predictions_pinterest_usage_likelihood IS NULL OR LOWER(social_media_predictions_pinterest_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(social_media_predictions_pinterest_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_social_media_predictions_pinterest_usage_score,
  CASE
  WHEN social_media_predictions_linkedin_usage_likelihood IS NULL OR LOWER(social_media_predictions_linkedin_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(social_media_predictions_linkedin_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_social_media_predictions_linkedin_usage_score,
  CASE
  WHEN social_media_predictions_twitter_usage_likelihood IS NULL OR LOWER(social_media_predictions_twitter_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(social_media_predictions_twitter_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_social_media_predictions_twitter_usage_score,
  CASE
  WHEN social_media_predictions_snapchat_usage_likelihood IS NULL OR LOWER(social_media_predictions_snapchat_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(social_media_predictions_snapchat_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_social_media_predictions_snapchat_usage_score,
  CASE
  WHEN social_media_predictions_youtube_usage_likelihood IS NULL OR LOWER(social_media_predictions_youtube_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(social_media_predictions_youtube_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_social_media_predictions_youtube_usage_score,
  CASE
  WHEN truetouch_strategy_brand_loyalists_likelihood IS NULL OR LOWER(truetouch_strategy_brand_loyalists_likelihood) =
    LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_brand_loyalists_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_brand_loyalists_score,
  CASE
  WHEN truetouch_strategy_deal_seekers_likelihood IS NULL OR LOWER(truetouch_strategy_deal_seekers_likelihood) = LOWER('Unknown'
     )
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_deal_seekers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_deal_seekers_score,
  CASE
  WHEN truetouch_strategy_moment_shoppers_likelihood IS NULL OR LOWER(truetouch_strategy_moment_shoppers_likelihood) =
    LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_moment_shoppers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_moment_shoppers_score,
  CASE
  WHEN truetouch_strategy_mainstream_adopters_likelihood IS NULL OR LOWER(truetouch_strategy_mainstream_adopters_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_mainstream_adopters_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_mainstream_adopters_score,
  CASE
  WHEN truetouch_strategy_novelty_seekers_likelihood IS NULL OR LOWER(truetouch_strategy_novelty_seekers_likelihood) =
    LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_novelty_seekers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_novelty_seekers_score,
  CASE
  WHEN truetouch_strategy_organic_and_natural_likelihood IS NULL OR LOWER(truetouch_strategy_organic_and_natural_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_organic_and_natural_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_organic_and_natural_score,
  CASE
  WHEN truetouch_strategy_quality_matters_likelihood IS NULL OR LOWER(truetouch_strategy_quality_matters_likelihood) =
    LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_quality_matters_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_quality_matters_score,
  CASE
  WHEN truetouch_strategy_recreational_shoppers_likelihood IS NULL OR LOWER(truetouch_strategy_recreational_shoppers_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_recreational_shoppers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_recreational_shoppers_score,
  CASE
  WHEN truetouch_strategy_savvy_researchers_likelihood IS NULL OR LOWER(truetouch_strategy_savvy_researchers_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_savvy_researchers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_savvy_researchers_score,
  CASE
  WHEN truetouch_strategy_trendsetters_likelihood IS NULL OR LOWER(truetouch_strategy_trendsetters_likelihood) = LOWER('Unknown'
     )
  THEN NULL
  ELSE (CAST(CASE
        WHEN LOWER(truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%extremely%')
        THEN 5
        WHEN LOWER(truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%highly%')
        THEN 4
        WHEN LOWER(truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%very%')
        THEN 3
        WHEN LOWER(truetouch_strategy_trendsetters_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
        THEN 2
        WHEN LOWER(truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%somewhat%')
        THEN 1
        ELSE 0
        END AS FLOAT64) * CASE
       WHEN LOWER(truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%unlikely%')
       THEN - 1
       ELSE 1
       END + CASE
      WHEN LOWER(truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%unlikely%')
      THEN 5.5
      ELSE 4.5
      END) * 0.1
  END AS cust_current_truetouch_strategy_trendsetters_score
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_cust_usr_vws.customer_experian_demographic_prediction_dim;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_chan_yr_attributes_fy;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_chan_yr_attributes_fy
(SELECT DISTINCT a.acp_id,
  a.fiscal_year_shopped,
  a.channel,
  a.cust_chan_gross_sales,
  a.cust_chan_return_amt,
  a.cust_chan_net_sales,
  a.cust_chan_trips,
  a.cust_chan_gross_items,
  a.cust_chan_return_items,
  a.cust_chan_net_items,
  a.cust_chan_anniversary,
  a.cust_chan_holiday,
  a.cust_chan_npg,
   CASE
   WHEN a.cust_chan_div_count = 1
   THEN 1
   ELSE 0
   END AS cust_chan_singledivision,
   CASE
   WHEN a.cust_chan_div_count > 1
   THEN 1
   ELSE 0
   END AS cust_chan_multidivision,
  a.cust_chan_net_sales_ly,
  a.cust_chan_net_sales_ny,
  a.cust_chan_anniversary_ly,
  a.cust_chan_holiday_ly,
  b.buyer_flow AS cust_chan_buyer_flow,
  b.aare_acquired AS cust_chan_acquired_aare,
  b.aare_activated AS cust_chan_activated_aare,
  b.aare_retained AS cust_chan_retained_aare,
  b.aare_engaged AS cust_chan_engaged_aare,
  c.cust_gender,
  c.cust_age,
  c.cust_lifestage,
  c.cust_age_group,
  c.cust_nms_market,
  c.cust_dma,
  c.cust_country,
  c.cust_dma_rank,
  c.cust_loyalty_type,
  c.cust_loyalty_level,
  c.cust_loy_member_enroll_dt,
  c.cust_loy_cardmember_enroll_dt,
  b.aare_acquired AS cust_acquired_this_year,
  c.cust_acquisition_date,
  c.cust_acquisition_fiscal_year,
  c.cust_acquisition_channel,
  c.cust_acquisition_banner,
  c.cust_acquisition_brand,
  c.cust_tenure_bucket_months,
  c.cust_tenure_bucket_years,
  c.cust_activation_date,
  c.cust_activation_channel,
  c.cust_activation_banner,
  c.cust_engagement_cohort,
  c.cust_channel_count,
  c.cust_channel_combo,
  c.cust_banner_count,
  c.cust_banner_combo,
  c.cust_employee_flag,
  c.cust_jwn_trip_bucket,
  c.cust_jwn_net_spend_bucket,
  c.cust_jwn_gross_sales,
  c.cust_jwn_return_amt,
  c.cust_jwn_net_sales,
  c.cust_jwn_net_sales_apparel,
  c.cust_jwn_trips,
  c.cust_jwn_gross_items,
  c.cust_jwn_return_items,
  c.cust_jwn_net_items,
  c.cust_tender_nordstrom,
  c.cust_tender_nordstrom_note,
  c.cust_tender_3rd_party_credit,
  c.cust_tender_debit_card,
  c.cust_tender_gift_card,
  c.cust_tender_cash,
  c.cust_tender_paypal,
  c.cust_tender_check,
  c.cust_svc_group_exp_delivery,
  c.cust_svc_group_order_pickup,
  c.cust_svc_group_selling_relation,
  c.cust_svc_group_remote_selling,
  c.cust_svc_group_alterations,
  c.cust_svc_group_in_store,
  c.cust_svc_group_restaurant,
  c.cust_service_free_exp_delivery,
  c.cust_service_next_day_pickup,
  c.cust_service_same_day_bopus,
  c.cust_service_curbside_pickup,
  c.cust_service_style_boards,
  c.cust_service_gift_wrapping,
  c.cust_service_pop_in,
  c.cust_marketplace_flag,
  c.cust_platform_desktop,
  c.cust_platform_mow,
  c.cust_platform_ios,
  c.cust_platform_android,
  c.cust_platform_pos,
  c.cust_anchor_brand,
  c.cust_strategic_brand,
  c.cust_store_customer,
  c.cust_digital_customer,
  c.cust_clv_jwn,
  c.cust_clv_fp,
  c.cust_clv_op,
  CAST(h.mcv_net_1year_fp + h.mcv_net_1year_op AS NUMERIC) AS cust_next_year_net_spend_jwn,
  d.rfm_1year_segment AS cust_current_rfm_1year_segment,
  d.rfm_4year_segment AS cust_current_rfm_4year_segment,
   CASE
   WHEN d.last_order_date_jwn > CURRENT_DATE('PST8PDT')
   THEN NULL
   ELSE CAST(TRUNC(CAST(ROUND(DATE_DIFF(CURRENT_DATE('PST8PDT'), d.last_order_date_jwn, MONTH) + (EXTRACT(DAY FROM CURRENT_DATE('PST8PDT')) - EXTRACT(DAY FROM d.last_order_date_jwn)) / 31, 9)AS FLOAT64)) AS INTEGER)
   END AS cust_current_months_since_last_shopped_jwn,
   CASE
   WHEN d.last_order_date_fp > CURRENT_DATE('PST8PDT')
   THEN NULL
   ELSE CAST(TRUNC(CAST(ROUND(DATE_DIFF(CURRENT_DATE('PST8PDT'), d.last_order_date_fp, MONTH) + (EXTRACT(DAY FROM CURRENT_DATE('PST8PDT')) - EXTRACT(DAY FROM d.last_order_date_fp)) / 31, 9)AS FLOAT64)) AS INTEGER)
   END AS cust_current_months_since_last_shopped_fp,
   CASE
   WHEN d.last_order_date_op > CURRENT_DATE('PST8PDT')
   THEN NULL
   ELSE CAST(TRUNC(CAST(ROUND(DATE_DIFF(CURRENT_DATE('PST8PDT'), d.last_order_date_op, MONTH) + (EXTRACT(DAY FROM CURRENT_DATE('PST8PDT')) - EXTRACT(DAY FROM d.last_order_date_op)) / 31, 9)AS FLOAT64)) AS INTEGER)
   END AS cust_current_months_since_last_shopped_op,
   CASE
   WHEN d.last_order_date_nstores > CURRENT_DATE('PST8PDT')
   THEN NULL
   ELSE CAST(TRUNC(CAST(ROUND(DATE_DIFF(CURRENT_DATE('PST8PDT'), d.last_order_date_nstores, MONTH) + (EXTRACT(DAY FROM CURRENT_DATE('PST8PDT')) - EXTRACT(DAY FROM d.last_order_date_nstores)) / 31, 9)AS FLOAT64)) AS INTEGER)
   END AS cust_current_months_since_last_shopped_nstores,
   CASE
   WHEN d.last_order_date_ncom > CURRENT_DATE('PST8PDT')
   THEN NULL
   ELSE CAST(TRUNC(CAST(ROUND(DATE_DIFF(CURRENT_DATE('PST8PDT'), d.last_order_date_ncom, MONTH) + (EXTRACT(DAY FROM CURRENT_DATE('PST8PDT')) - EXTRACT(DAY FROM d.last_order_date_ncom)) / 31, 9)AS FLOAT64)) AS INTEGER)
   END AS cust_current_months_since_last_shopped_ncom,
   CASE
   WHEN d.last_order_date_rstores > CURRENT_DATE('PST8PDT')
   THEN NULL
   ELSE CAST(TRUNC(CAST(ROUND(DATE_DIFF(CURRENT_DATE('PST8PDT'), d.last_order_date_rstores, MONTH) + (EXTRACT(DAY FROM CURRENT_DATE('PST8PDT')) - EXTRACT(DAY FROM d.last_order_date_rstores)) / 31, 9)AS FLOAT64)) AS INTEGER)
   END AS cust_current_months_since_last_shopped_rstores,
   CASE
   WHEN d.last_order_date_rcom > CURRENT_DATE('PST8PDT')
   THEN NULL
   ELSE CAST(TRUNC(CAST(ROUND(DATE_DIFF(CURRENT_DATE('PST8PDT'), d.last_order_date_rcom, MONTH) + (EXTRACT(DAY FROM CURRENT_DATE('PST8PDT')) - EXTRACT(DAY FROM d.last_order_date_rcom)) / 31, 9) AS FLOAT64)) AS INTEGER)
   END AS cust_current_months_since_last_shopped_rcom,
  e.martial_status AS cust_current_marital_status,
  e.household_adult_count AS cust_current_household_adult_count, 
  e.household_children_count AS cust_current_household_children_count,
  e.household_person_count AS cust_current_household_person_count,
  e.occupation_type AS cust_current_occupation_type,
  e.education_model_likelihood AS cust_current_education_model_likelihood,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_discretionary_spend_estimate_spend_on_apparel = ''
    THEN '0'
    ELSE e.household_discretionary_spend_estimate_spend_on_apparel
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_discretionary_spend_estimate_spend_on_apparel,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_discretionary_spend_estimate_score = ''
    THEN '0'
    ELSE e.household_discretionary_spend_estimate_score
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_discretionary_spend_estimate_score,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_estimated_income_national_percentile = ''
    THEN '0'
    ELSE e.household_estimated_income_national_percentile
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_estimated_income_national_percentile,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_estimated_income_amount_in_thousands = ''
    THEN '0'
    ELSE e.household_estimated_income_amount_in_thousands
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_estimated_income_amount_in_thousands,
   CASE
   WHEN LOWER(e.household_estimated_income_range) IN (LOWER('$1,000-$14,999'), LOWER('$15,000-$24,999'), LOWER('$25,000-$34,999'
      ), LOWER('$35,000-$49,999'), LOWER('$50,000-$74,999'))
   THEN '1) < $75,000'
   WHEN LOWER(e.household_estimated_income_range) IN (LOWER('$75,000-$99,999'))
   THEN '2) $75,000 - $99,999'
   WHEN LOWER(e.household_estimated_income_range) IN (LOWER('$100,000-$124,999'), LOWER('$125,000-$149,999'))
   THEN '3) $100,000-$149,999'
   WHEN LOWER(e.household_estimated_income_range) IN (LOWER('$150,000-$174,999'), LOWER('$175,000-$199,999'))
   THEN '4) $150,000-$199,999'
   WHEN LOWER(e.household_estimated_income_range) IN (LOWER('$200,000-$249,999'), LOWER('$250,000+'))
   THEN '5) $200,000+'
   ELSE 'Unknown'
   END AS cust_current_household_estimated_income_range,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_young_adult_clothing_shopper_likelihood_rank_asc = ''
    THEN '0'
    ELSE e.household_buyer_young_adult_clothing_shopper_likelihood_rank_asc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_young_adult_clothing_shoppers_likelihood,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_prestige_makeup_user_likelihood_rank_asc = ''
    THEN '0'
    ELSE e.household_buyer_prestige_makeup_user_likelihood_rank_asc
    END  AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_prestige_makeup_user_likelihood,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_luxury_store_shoppers_likelihood_rank_asc = ''
    THEN '0'
    ELSE e.household_buyer_luxury_store_shoppers_likelihood_rank_asc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_luxury_store_shoppers_likelihood,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_loyalty_card_user_likelihood_rank_asc = ''
    THEN '0'
    ELSE e.household_buyer_loyalty_card_user_likelihood_rank_asc
    END  AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_loyalty_card_user_likelihood,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_online_overall_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_online_overall_propensity_model_rank_desc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_online_overall_propensity,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_retail_overall_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_retail_overall_propensity_model_rank_desc
    END  AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_retail_overall_propensity,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_consumer_expenditure_apparel_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_consumer_expenditure_apparel_propensity_model_rank_desc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_consumer_expenditure_apparel_propensity,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_online_apparel_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_online_apparel_propensity_model_rank_desc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_online_apparel_propensity,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_retail_apparel_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_retail_apparel_propensity_model_rank_desc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_retail_apparel_propensity,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_consumer_expenditure_shoes_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_consumer_expenditure_shoes_propensity_model_rank_desc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_consumer_expenditure_shoes_propensity,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_online_shoe_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_online_shoe_propensity_model_rank_desc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_online_shoes_propensity,
  CAST(TRUNC(CAST(CASE
    WHEN e.household_buyer_retail_shoes_propensity_model_rank_desc = ''
    THEN '0'
    ELSE e.household_buyer_retail_shoes_propensity_model_rank_desc
    END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_retail_shoes_propensity,
  e.social_media_predictions_facebook_usage_likelihood AS
  cust_current_social_media_predictions_facebook_usage_likelihood,
  e.social_media_predictions_instagram_usage_likelihood AS
  cust_current_social_media_predictions_instagram_usage_likelihood,
  e.social_media_predictions_pinterest_usage_likelihood AS
  cust_current_social_media_predictions_pinterest_usage_likelihood,
  e.social_media_predictions_linkedin_usage_likelihood AS
  cust_current_social_media_predictions_linkedin_usage_likelihood,
  e.social_media_predictions_twitter_usage_likelihood AS cust_current_social_media_predictions_twitter_usage_likelihood
  ,
  e.social_media_predictions_snapchat_usage_likelihood AS
  cust_current_social_media_predictions_snapchat_usage_likelihood,
  e.social_media_predictions_youtube_usage_likelihood AS cust_current_social_media_predictions_youtube_usage_likelihood
  ,
  e.retail_shopper_type AS cust_current_retail_shopper_type,
  e.household_mosaic_lifestyle_segment AS cust_current_household_mosaic_lifestyle_segment,
  j.mosaic_group AS cust_current_household_mosaic_lifestyle_group,
  j.mosaic_group_summary AS cust_current_household_mosaic_lifestyle_group_summary,
  e.truetouch_strategy_brand_loyalists_likelihood AS cust_current_truetouch_strategy_brand_loyalists_likelihood,
  e.truetouch_strategy_deal_seekers_likelihood AS cust_current_truetouch_strategy_deal_seekers_likelihood,
  e.truetouch_strategy_moment_shoppers_likelihood AS cust_current_truetouch_strategy_moment_shoppers_likelihood,
  e.truetouch_strategy_mainstream_adopters_likelihood AS cust_current_truetouch_strategy_mainstream_adopters_likelihood
  ,
  e.truetouch_strategy_novelty_seekers_likelihood AS cust_current_truetouch_strategy_novelty_seekers_likelihood,
  e.truetouch_strategy_organic_and_natural_likelihood AS cust_current_truetouch_strategy_organic_and_natural_likelihood
  ,
  e.truetouch_strategy_quality_matters_likelihood AS cust_current_truetouch_strategy_quality_matters_likelihood,
  e.truetouch_strategy_recreational_shoppers_likelihood AS
  cust_current_truetouch_strategy_recreational_shoppers_likelihood,
  e.truetouch_strategy_savvy_researchers_likelihood AS cust_current_truetouch_strategy_savvy_researchers_likelihood,
  e.truetouch_strategy_trendsetters_likelihood AS cust_current_truetouch_strategy_trendsetters_likelihood,
  CAST(TRUNC(CAST(f.affinity_accessories AS FLOAT64)) AS INTEGER) AS cust_current_affinity_accessories,
  CAST(TRUNC(CAST(f.affinity_active AS FLOAT64)) AS INTEGER) AS cust_current_affinity_active,
  CAST(TRUNC(CAST(f.affinity_apparel AS FLOAT64)) AS INTEGER) AS cust_current_affinity_apparel,
  CAST(TRUNC(CAST(f.affinity_beauty AS FLOAT64)) AS INTEGER) AS cust_current_affinity_beauty,
  CAST(TRUNC(CAST(f.affinity_designer AS FLOAT64)) AS INTEGER) AS cust_current_affinity_designer,
  CAST(TRUNC(CAST(f.affinity_home AS FLOAT64)) AS INTEGER) AS cust_current_affinity_home,
  CAST(TRUNC(CAST(f.affinity_kids AS FLOAT64)) AS INTEGER) AS cust_current_affinity_kids,
  CAST(TRUNC(CAST(f.affinity_mens AS FLOAT64)) AS INTEGER) AS cust_current_affinity_mens,
  CAST(TRUNC(CAST(f.affinity_shoes AS FLOAT64)) AS INTEGER) AS cust_current_affinity_shoes,
  CAST(TRUNC(CAST(f.affinity_womens AS FLOAT64)) AS INTEGER) AS cust_current_affinity_womens,
  g.predicted_ct_segment AS cust_current_predicted_core_target_segment,
  ROUND(CAST(i.cust_current_social_media_predictions_facebook_usage_score AS NUMERIC), 2) AS
  cust_current_social_media_predictions_facebook_usage_score,
  ROUND(CAST(i.cust_current_social_media_predictions_instagram_usage_score AS NUMERIC), 2) AS
  cust_current_social_media_predictions_instagram_usage_score,
  ROUND(CAST(i.cust_current_social_media_predictions_pinterest_usage_score AS NUMERIC), 2) AS
  cust_current_social_media_predictions_pinterest_usage_score,
  ROUND(CAST(i.cust_current_social_media_predictions_linkedin_usage_score AS NUMERIC), 2) AS
  cust_current_social_media_predictions_linkedin_usage_score,
  ROUND(CAST(i.cust_current_social_media_predictions_twitter_usage_score AS NUMERIC), 2) AS
  cust_current_social_media_predictions_twitter_usage_score,
  ROUND(CAST(i.cust_current_social_media_predictions_snapchat_usage_score AS NUMERIC), 2) AS
  cust_current_social_media_predictions_snapchat_usage_score,
  ROUND(CAST(i.cust_current_social_media_predictions_youtube_usage_score AS NUMERIC), 2) AS
  cust_current_social_media_predictions_youtube_usage_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_brand_loyalists_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_brand_loyalists_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_deal_seekers_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_deal_seekers_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_moment_shoppers_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_moment_shoppers_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_mainstream_adopters_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_mainstream_adopters_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_novelty_seekers_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_novelty_seekers_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_organic_and_natural_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_organic_and_natural_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_quality_matters_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_quality_matters_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_recreational_shoppers_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_recreational_shoppers_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_savvy_researchers_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_savvy_researchers_score,
  ROUND(CAST(i.cust_current_truetouch_strategy_trendsetters_score AS NUMERIC), 2) AS
  cust_current_truetouch_strategy_trendsetters_score,
   CASE
   WHEN k.rank_record <= 500000
   THEN 1
   ELSE 0
   END AS cust_jwn_top500k,
   CASE
   WHEN k.rank_record <= 1000
   THEN 1
   ELSE 0
   END AS cust_jwn_top1k,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM cust_chan_level_derived AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_buyer_flow_fy AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b
      .channel) AND LOWER(a.fiscal_year_shopped) = LOWER(b.fiscal_year_shopped)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_customer_level_attributes_fy AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.fiscal_year_shopped
     ) = LOWER(c.fiscal_year_shopped)
  LEFT JOIN t2dl_das_cal.customer_attributes_transactions AS d ON LOWER(a.acp_id) = LOWER(d.acp_id)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS e ON LOWER(a.acp_id) = LOWER(e.acp_id)
  LEFT JOIN t2dl_das_cal.customer_attributes_merch AS f ON LOWER(a.acp_id) = LOWER(f.acp_id)
  LEFT JOIN t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment AS g ON LOWER(a
    .acp_id) = LOWER(g.acp_id)
  LEFT JOIN t2dl_das_cal.customer_attributes_scores AS h ON LOWER(a.acp_id) = LOWER(h.acp_id)
  LEFT JOIN experian_likelihood_scores AS i ON LOWER(a.acp_id) = LOWER(i.acp_id)
  LEFT JOIN t2dl_das_usl.experian_mosaic_segments AS j ON LOWER(e.household_mosaic_lifestyle_segment) = LOWER(j.mosaic_segment
    )
  LEFT JOIN cust_jwn_rank AS k ON LOWER(a.acp_id) = LOWER(k.acp_id) AND LOWER(a.fiscal_year_shopped) = LOWER(k.fiscal_year_shopped
     ));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column  (acp_id), column  (fiscal_year_shopped), column  (channel), column  (cust_gender), column  (cust_age), column  (cust_lifestage), column  (cust_age_group), column  (cust_NMS_market), column  (cust_dma), column  (cust_country), column  (cust_dma_rank), column  (cust_loyalty_type), column  (cust_loyalty_level), column  (cust_employee_flag), column  (acp_id, fiscal_year_shopped, channel) on t2dl_das_strategy.cco_cust_chan_yr_attributes_fy;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
