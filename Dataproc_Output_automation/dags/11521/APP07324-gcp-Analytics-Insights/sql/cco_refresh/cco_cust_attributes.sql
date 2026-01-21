
/*SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
---Task_Name=run_cco_cust_attributes;'*/
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;
/************************************************************************************/
/************************************************************************************
 * Build table with acp_id level data in support of cco_cust_channel_week_attributes_vw.
 ************************************************************************************/
/************************************************************************************/
TRUNCATE TABLE  `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_attributes;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO  `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_attributes
(
acp_id,
cust_next_year_net_spend_jwn,
cust_current_rfm_1year_segment,
cust_current_rfm_4year_segment,
cust_current_weeks_since_last_shopped_jwn,
cust_current_weeks_since_last_shopped_fp,
cust_current_weeks_since_last_shopped_op,
cust_current_weeks_since_last_shopped_nstores,
cust_current_weeks_since_last_shopped_ncom,
cust_current_weeks_since_last_shopped_rstores,
cust_current_weeks_since_last_shopped_rcom,
cust_current_marital_status,
cust_current_household_adult_count,
cust_current_household_children_count,
cust_current_household_person_count,
cust_current_occupation_type,
cust_current_education_model_likelihood,
cust_current_household_discretionary_spend_estimate_spend_on_apparel,
cust_current_household_discretionary_spend_estimate_score,
cust_current_household_estimated_income_national_percentile,
cust_current_household_estimated_income_amount_in_thousands,
cust_current_household_estimated_income_range,
cust_current_household_buyer_young_adult_clothing_shoppers_likelihood,
cust_current_household_buyer_prestige_makeup_user_likelihood,
cust_current_household_buyer_luxury_store_shoppers_likelihood,
cust_current_household_buyer_loyalty_card_user_likelihood,
cust_current_household_buyer_online_overall_propensity,
cust_current_household_buyer_retail_overall_propensity,
cust_current_household_buyer_consumer_expenditure_apparel_propensity,
cust_current_household_buyer_online_apparel_propensity,
cust_current_household_buyer_retail_apparel_propensity,
cust_current_household_buyer_consumer_expenditure_shoes_propensity,
cust_current_household_buyer_online_shoes_propensity,
cust_current_household_buyer_retail_shoes_propensity,
cust_current_social_media_predictions_facebook_usage_likelihood,
cust_current_social_media_predictions_instagram_usage_likelihood,
cust_current_social_media_predictions_pinterest_usage_likelihood,
cust_current_social_media_predictions_linkedin_usage_likelihood,
cust_current_social_media_predictions_twitter_usage_likelihood,
cust_current_social_media_predictions_snapchat_usage_likelihood,
cust_current_social_media_predictions_youtube_usage_likelihood,
cust_current_retail_shopper_type,
cust_current_household_mosaic_lifestyle_segment,
cust_current_household_mosaic_lifestyle_group,
cust_current_household_mosaic_lifestyle_group_summary,
cust_current_truetouch_strategy_brand_loyalists_likelihood,
cust_current_truetouch_strategy_deal_seekers_likelihood,
cust_current_truetouch_strategy_moment_shoppers_likelihood,
cust_current_truetouch_strategy_mainstream_adopters_likelihood,
cust_current_truetouch_strategy_novelty_seekers_likelihood,
cust_current_truetouch_strategy_organic_and_natural_likelihood,
cust_current_truetouch_strategy_quality_matters_likelihood,
cust_current_truetouch_strategy_recreational_shoppers_likelihood,
cust_current_truetouch_strategy_savvy_researchers_likelihood,
cust_current_truetouch_strategy_trendsetters_likelihood,
cust_current_affinity_accessories,
cust_current_affinity_active,
cust_current_affinity_apparel,
cust_current_affinity_beauty,
cust_current_affinity_designer,
cust_current_affinity_home,
cust_current_affinity_kids,
cust_current_affinity_mens,
cust_current_affinity_shoes,
cust_current_affinity_womens,
cust_current_predicted_core_target_segment,
cust_current_social_media_predictions_facebook_usage_score,
cust_current_social_media_predictions_instagram_usage_score,
cust_current_social_media_predictions_pinterest_usage_score,
cust_current_social_media_predictions_linkedin_usage_score,
cust_current_social_media_predictions_twitter_usage_score,
cust_current_social_media_predictions_snapchat_usage_score,
cust_current_social_media_predictions_youtube_usage_score,
cust_current_truetouch_strategy_brand_loyalists_score,
cust_current_truetouch_strategy_deal_seekers_score,
cust_current_truetouch_strategy_moment_shoppers_score,
cust_current_truetouch_strategy_mainstream_adopters_score,
cust_current_truetouch_strategy_novelty_seekers_score,
cust_current_truetouch_strategy_organic_and_natural_score,
cust_current_truetouch_strategy_quality_matters_score,
cust_current_truetouch_strategy_recreational_shoppers_score,
cust_current_truetouch_strategy_savvy_researchers_score,
cust_current_truetouch_strategy_trendsetters_score,
dw_sys_load_tmstp
)
WITH acp_ids AS (SELECT DISTINCT acp_id
FROM  `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_cust_channel_week_attributes)
SELECT a.acp_id,
  f.mcv_net_1year_fp + f.mcv_net_1year_op AS cust_next_year_net_spend_jwn,
 b.rfm_1year_segment AS cust_current_rfm_1year_segment,
 b.rfm_4year_segment AS cust_current_rfm_4year_segment,
  CASE
  WHEN b.last_order_date_jwn > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE CAST(trunc(DATE_DIFF(CURRENT_DATE('PST8PDT'), b.last_order_date_jwn, DAY) / 7) AS INT64)
  END AS cust_current_weeks_since_last_shopped_jwn,
  CASE
  WHEN b.last_order_date_fp > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE CAST(trunc(DATE_DIFF(CURRENT_DATE('PST8PDT'), b.last_order_date_fp, DAY) / 7) AS INT64)
  END AS cust_current_weeks_since_last_shopped_fp,
  CASE
  WHEN b.last_order_date_op > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE CAST(trunc(DATE_DIFF(CURRENT_DATE('PST8PDT'), b.last_order_date_op, DAY) / 7) AS INT64)
  END AS cust_current_weeks_since_last_shopped_op,
  CASE
  WHEN b.last_order_date_nstores > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE CAST(trunc(DATE_DIFF(CURRENT_DATE('PST8PDT'), b.last_order_date_nstores, DAY) / 7) AS INT64)
  END AS cust_current_weeks_since_last_shopped_nstores,
  CASE
  WHEN b.last_order_date_ncom > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE CAST(trunc(DATE_DIFF(CURRENT_DATE('PST8PDT'), b.last_order_date_ncom, DAY) / 7) AS INT64)
  END AS cust_current_weeks_since_last_shopped_ncom,
  CASE
  WHEN b.last_order_date_rstores > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE CAST(trunc(DATE_DIFF(CURRENT_DATE('PST8PDT'), b.last_order_date_rstores, DAY) / 7) AS INT64)
  END AS cust_current_weeks_since_last_shopped_rstores,
  CASE
  WHEN b.last_order_date_rcom > CURRENT_DATE('PST8PDT')
  THEN NULL
  ELSE CAST(trunc(DATE_DIFF(CURRENT_DATE('PST8PDT'), b.last_order_date_rcom, DAY) / 7) AS INT64)
  END AS cust_current_weeks_since_last_shopped_rcom,
 c.martial_status AS cust_current_marital_status,
 c.household_adult_count AS cust_current_household_adult_count,
 c.household_children_count AS cust_current_household_children_count,
 c.household_person_count AS cust_current_household_person_count,
 c.occupation_type AS cust_current_occupation_type,
 c.education_model_likelihood AS cust_current_education_model_likelihood,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_discretionary_spend_estimate_spend_on_apparel = ''
   THEN '0'
   ELSE c.household_discretionary_spend_estimate_spend_on_apparel
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_discretionary_spend_estimate_spend_on_apparel,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_discretionary_spend_estimate_score = ''
   THEN '0'
   ELSE c.household_discretionary_spend_estimate_score
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_discretionary_spend_estimate_score,
   CAST(TRUNC(CAST(CASE
   WHEN c.household_estimated_income_national_percentile = ''
   THEN '0'
   ELSE c.household_estimated_income_national_percentile
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_estimated_income_national_percentile,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_estimated_income_amount_in_thousands = ''
   THEN '0'
   ELSE c.household_estimated_income_amount_in_thousands
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_estimated_income_amount_in_thousands,
  CASE
  WHEN LOWER(c.household_estimated_income_range) IN (LOWER('$1,000-$14,999'), LOWER('$15,000-$24,999'), LOWER('$25,000-$34,999'
     ), LOWER('$35,000-$49,999'), LOWER('$50,000-$74,999'))
  THEN '1) < $75,000'
  WHEN LOWER(c.household_estimated_income_range) IN (LOWER('$75,000-$99,999'))
  THEN '2) $75,000 - $99,999'
  WHEN LOWER(c.household_estimated_income_range) IN (LOWER('$100,000-$124,999'), LOWER('$125,000-$149,999'))
  THEN '3) $100,000-$149,999'
  WHEN LOWER(c.household_estimated_income_range) IN (LOWER('$150,000-$174,999'), LOWER('$175,000-$199,999'))
  THEN '4) $150,000-$199,999'
  WHEN LOWER(c.household_estimated_income_range) IN (LOWER('$200,000-$249,999'), LOWER('$250,000+'))
  THEN '5) $200,000+'
  ELSE 'Unknown'
  END AS cust_current_household_estimated_income_range,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_young_adult_clothing_shopper_likelihood_rank_asc = ''
   THEN '0'
   ELSE c.household_buyer_young_adult_clothing_shopper_likelihood_rank_asc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_young_adult_clothing_shoppers_likelihood,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_prestige_makeup_user_likelihood_rank_asc = ''
   THEN '0'
   ELSE c.household_buyer_prestige_makeup_user_likelihood_rank_asc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_prestige_makeup_user_likelihood,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_luxury_store_shoppers_likelihood_rank_asc = ''
   THEN '0'
   ELSE c.household_buyer_luxury_store_shoppers_likelihood_rank_asc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_luxury_store_shoppers_likelihood,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_loyalty_card_user_likelihood_rank_asc = ''
   THEN '0'
   ELSE c.household_buyer_loyalty_card_user_likelihood_rank_asc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_loyalty_card_user_likelihood,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_online_overall_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_online_overall_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_online_overall_propensity,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_retail_overall_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_retail_overall_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_retail_overall_propensity,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_consumer_expenditure_apparel_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_consumer_expenditure_apparel_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_consumer_expenditure_apparel_propensity,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_online_apparel_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_online_apparel_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_online_apparel_propensity,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_retail_apparel_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_retail_apparel_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_retail_apparel_propensity,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_consumer_expenditure_shoes_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_consumer_expenditure_shoes_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_consumer_expenditure_shoes_propensity,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_online_shoe_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_online_shoe_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_online_shoes_propensity,
  CAST(TRUNC(CAST(CASE
   WHEN c.household_buyer_retail_shoes_propensity_model_rank_desc = ''
   THEN '0'
   ELSE c.household_buyer_retail_shoes_propensity_model_rank_desc
   END AS FLOAT64)) AS INTEGER) AS cust_current_household_buyer_retail_shoes_propensity,
 c.social_media_predictions_facebook_usage_likelihood AS cust_current_social_media_predictions_facebook_usage_likelihood
 ,
 c.social_media_predictions_instagram_usage_likelihood AS
 cust_current_social_media_predictions_instagram_usage_likelihood,
 c.social_media_predictions_pinterest_usage_likelihood AS
 cust_current_social_media_predictions_pinterest_usage_likelihood,
 c.social_media_predictions_linkedin_usage_likelihood AS cust_current_social_media_predictions_linkedin_usage_likelihood
 ,
 c.social_media_predictions_twitter_usage_likelihood AS cust_current_social_media_predictions_twitter_usage_likelihood,
 c.social_media_predictions_snapchat_usage_likelihood AS cust_current_social_media_predictions_snapchat_usage_likelihood
 ,
 c.social_media_predictions_youtube_usage_likelihood AS cust_current_social_media_predictions_youtube_usage_likelihood,
 c.retail_shopper_type AS cust_current_retail_shopper_type,
 c.household_mosaic_lifestyle_segment AS cust_current_household_mosaic_lifestyle_segment,
 g.mosaic_group AS cust_current_household_mosaic_lifestyle_group,
 g.mosaic_group_summary AS cust_current_household_mosaic_lifestyle_group_summary,
 c.truetouch_strategy_brand_loyalists_likelihood AS cust_current_truetouch_strategy_brand_loyalists_likelihood,
 c.truetouch_strategy_deal_seekers_likelihood AS cust_current_truetouch_strategy_deal_seekers_likelihood,
 c.truetouch_strategy_moment_shoppers_likelihood AS cust_current_truetouch_strategy_moment_shoppers_likelihood,
 c.truetouch_strategy_mainstream_adopters_likelihood AS cust_current_truetouch_strategy_mainstream_adopters_likelihood,
 c.truetouch_strategy_novelty_seekers_likelihood AS cust_current_truetouch_strategy_novelty_seekers_likelihood,
 c.truetouch_strategy_organic_and_natural_likelihood AS cust_current_truetouch_strategy_organic_and_natural_likelihood,
 c.truetouch_strategy_quality_matters_likelihood AS cust_current_truetouch_strategy_quality_matters_likelihood,
 c.truetouch_strategy_recreational_shoppers_likelihood AS
 cust_current_truetouch_strategy_recreational_shoppers_likelihood,
 c.truetouch_strategy_savvy_researchers_likelihood AS cust_current_truetouch_strategy_savvy_researchers_likelihood,
 c.truetouch_strategy_trendsetters_likelihood AS cust_current_truetouch_strategy_trendsetters_likelihood,
 CAST(TRUNC(CAST(d.affinity_accessories AS FLOAT64)) AS INTEGER) AS cust_current_affinity_accessories,
 CAST(TRUNC(CAST(d.affinity_active AS FLOAT64)) AS INTEGER) AS cust_current_affinity_active,
 CAST(TRUNC(CAST(d.affinity_apparel AS FLOAT64)) AS INTEGER) AS cust_current_affinity_apparel,
 CAST(TRUNC(CAST(d.affinity_beauty AS FLOAT64)) AS INTEGER) AS cust_current_affinity_beauty,
 CAST(TRUNC(CAST(d.affinity_designer AS FLOAT64)) AS INTEGER) AS cust_current_affinity_designer,
 CAST(TRUNC(CAST(d.affinity_home AS FLOAT64)) AS INTEGER) AS cust_current_affinity_home,
 CAST(TRUNC(CAST(d.affinity_kids AS FLOAT64)) AS INTEGER) AS cust_current_affinity_kids,
 CAST(TRUNC(CAST(d.affinity_mens AS FLOAT64)) AS INTEGER) AS cust_current_affinity_mens,
 CAST(TRUNC(CAST(d.affinity_shoes AS FLOAT64)) AS INTEGER) AS cust_current_affinity_shoes,
 CAST(TRUNC(CAST(d.affinity_womens AS FLOAT64)) AS INTEGER) AS cust_current_affinity_womens,
 e.predicted_ct_segment AS cust_current_predicted_core_target_segment,
  CASE
  WHEN c.social_media_predictions_facebook_usage_likelihood IS NULL OR LOWER(c.social_media_predictions_facebook_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.social_media_predictions_facebook_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.social_media_predictions_facebook_usage_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_social_media_predictions_facebook_usage_score,
  CASE
  WHEN c.social_media_predictions_instagram_usage_likelihood IS NULL OR LOWER(c.social_media_predictions_instagram_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.social_media_predictions_instagram_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.social_media_predictions_instagram_usage_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_social_media_predictions_instagram_usage_score,
  CASE
  WHEN c.social_media_predictions_pinterest_usage_likelihood IS NULL OR LOWER(c.social_media_predictions_pinterest_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.social_media_predictions_pinterest_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.social_media_predictions_pinterest_usage_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_social_media_predictions_pinterest_usage_score,
  CASE
  WHEN c.social_media_predictions_linkedin_usage_likelihood IS NULL OR LOWER(c.social_media_predictions_linkedin_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.social_media_predictions_linkedin_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.social_media_predictions_linkedin_usage_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_social_media_predictions_linkedin_usage_score,
  CASE
  WHEN c.social_media_predictions_twitter_usage_likelihood IS NULL OR LOWER(c.social_media_predictions_twitter_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.social_media_predictions_twitter_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.social_media_predictions_twitter_usage_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_social_media_predictions_twitter_usage_score,
  CASE
  WHEN c.social_media_predictions_snapchat_usage_likelihood IS NULL OR LOWER(c.social_media_predictions_snapchat_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.social_media_predictions_snapchat_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.social_media_predictions_snapchat_usage_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_social_media_predictions_snapchat_usage_score,
  CASE
  WHEN c.social_media_predictions_youtube_usage_likelihood IS NULL OR LOWER(c.social_media_predictions_youtube_usage_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.social_media_predictions_youtube_usage_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.social_media_predictions_youtube_usage_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_social_media_predictions_youtube_usage_score,
  CASE
  WHEN c.truetouch_strategy_brand_loyalists_likelihood IS NULL OR LOWER(c.truetouch_strategy_brand_loyalists_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_brand_loyalists_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_brand_loyalists_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_brand_loyalists_score,
  CASE
  WHEN c.truetouch_strategy_deal_seekers_likelihood IS NULL OR LOWER(c.truetouch_strategy_deal_seekers_likelihood) =
    LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_deal_seekers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_deal_seekers_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_deal_seekers_score,
  CASE
  WHEN c.truetouch_strategy_moment_shoppers_likelihood IS NULL OR LOWER(c.truetouch_strategy_moment_shoppers_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_moment_shoppers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_moment_shoppers_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_moment_shoppers_score,
  CASE
  WHEN c.truetouch_strategy_mainstream_adopters_likelihood IS NULL OR LOWER(c.truetouch_strategy_mainstream_adopters_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_mainstream_adopters_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_mainstream_adopters_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_mainstream_adopters_score,
  CASE
  WHEN c.truetouch_strategy_novelty_seekers_likelihood IS NULL OR LOWER(c.truetouch_strategy_novelty_seekers_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_novelty_seekers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_novelty_seekers_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_novelty_seekers_score,
  CASE
  WHEN c.truetouch_strategy_organic_and_natural_likelihood IS NULL OR LOWER(c.truetouch_strategy_organic_and_natural_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_organic_and_natural_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_organic_and_natural_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_organic_and_natural_score,
  CASE
  WHEN c.truetouch_strategy_quality_matters_likelihood IS NULL OR LOWER(c.truetouch_strategy_quality_matters_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_quality_matters_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_quality_matters_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_quality_matters_score,
  CASE
  WHEN c.truetouch_strategy_recreational_shoppers_likelihood IS NULL OR LOWER(c.truetouch_strategy_recreational_shoppers_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_recreational_shoppers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_recreational_shoppers_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_recreational_shoppers_score,
  CASE
  WHEN c.truetouch_strategy_savvy_researchers_likelihood IS NULL OR LOWER(c.truetouch_strategy_savvy_researchers_likelihood
     ) = LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_savvy_researchers_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_savvy_researchers_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_savvy_researchers_score,
  CASE
  WHEN c.truetouch_strategy_trendsetters_likelihood IS NULL OR LOWER(c.truetouch_strategy_trendsetters_likelihood) =
    LOWER('Unknown')
  THEN NULL
  ELSE ROUND(CAST((CAST(CASE
          WHEN LOWER(c.truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%extremely%')
          THEN 5
          WHEN LOWER(c.truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%highly%')
          THEN 4
          WHEN LOWER(c.truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%very%')
          THEN 3
          WHEN LOWER(c.truetouch_strategy_trendsetters_likelihood) IN (LOWER('likely'), LOWER('unlikely'))
          THEN 2
          WHEN LOWER(c.truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%somewhat%')
          THEN 1
          ELSE 0
          END AS FLOAT64) * CASE
         WHEN LOWER(c.truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%unlikely%')
         THEN - 1
         ELSE 1
         END + CASE
        WHEN LOWER(c.truetouch_strategy_trendsetters_likelihood) LIKE LOWER('%unlikely%')
        THEN 5.5
        ELSE 4.5
        END) * 0.1 AS NUMERIC), 2)
  END AS cust_current_truetouch_strategy_trendsetters_score,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM acp_ids AS a
 LEFT JOIN  `{{params.gcp_project_id}}`.t2dl_das_cal.customer_attributes_transactions AS b 
 ON LOWER(a.acp_id) = LOWER(b.acp_id)
 LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS c 
 ON LOWER(a.acp_id) = LOWER(c.acp_id)
 LEFT JOIN  `{{params.gcp_project_id}}`.t2dl_das_cal.customer_attributes_merch AS d 
 ON LOWER(a.acp_id) = LOWER(d.acp_id)
 LEFT JOIN  t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment AS e ON LOWER(a
   .acp_id) = LOWER(e.acp_id)
 LEFT JOIN  `{{params.gcp_project_id}}`.t2dl_das_cal.customer_attributes_scores AS f 
 ON LOWER(a.acp_id) = LOWER(f.acp_id)
 LEFT JOIN  `{{params.gcp_project_id}}`.t2dl_das_usl.experian_mosaic_segments AS g 
 ON LOWER(c.household_mosaic_lifestyle_segment) = LOWER(g.mosaic_segment);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
