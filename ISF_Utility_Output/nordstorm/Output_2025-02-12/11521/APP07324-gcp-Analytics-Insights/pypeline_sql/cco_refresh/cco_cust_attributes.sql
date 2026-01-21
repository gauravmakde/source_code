SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_week_grain_11521_ACE_ENG;
Task_Name=run_cco_cust_attributes;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************
 * Build table with acp_id level data in support of cco_cust_channel_week_attributes_vw.
 ************************************************************************************/
/************************************************************************************/


DELETE FROM {cco_t2_schema}.cco_cust_attributes ALL;

INSERT INTO {cco_t2_schema}.cco_cust_attributes
WITH acp_ids AS (
  SELECT DISTINCT acp_id
  FROM {cco_t2_schema}.cco_cust_channel_week_attributes
)
SELECT a.acp_id
     , f.mcv_net_1year_fp + f.mcv_net_1year_op AS cust_next_year_net_spend_jwn
     , b.rfm_1year_segment AS cust_current_rfm_1year_segment
     , b.rfm_4year_segment AS cust_current_rfm_4year_segment
     , CASE WHEN last_order_date_jwn > CURRENT_DATE() THEN NULL
            ELSE CAST(((CURRENT_DATE() - b.last_order_date_jwn) / 7) AS INTEGER)
        END AS cust_current_weeks_since_last_shopped_jwn
     , CASE WHEN last_order_date_fp > CURRENT_DATE() THEN NULL
            ELSE CAST(((CURRENT_DATE() - b.last_order_date_fp) / 7) AS INTEGER)
        END AS cust_current_weeks_since_last_shopped_fp
     , CASE WHEN last_order_date_op > CURRENT_DATE() THEN NULL
            ELSE CAST(((CURRENT_DATE() - b.last_order_date_op) / 7) AS INTEGER)
        END AS cust_current_weeks_since_last_shopped_op
     , CASE WHEN last_order_date_nstores > CURRENT_DATE() THEN NULL
            ELSE CAST(((CURRENT_DATE() - b.last_order_date_nstores) / 7) AS INTEGER)
        END AS cust_current_weeks_since_last_shopped_nstores
     , CASE WHEN last_order_date_ncom > CURRENT_DATE() THEN NULL
            ELSE CAST (((CURRENT_DATE() - b.last_order_date_ncom) / 7) AS INTEGER)
        END AS cust_current_weeks_since_last_shopped_ncom
     , CASE WHEN last_order_date_rstores > CURRENT_DATE() THEN NULL
            ELSE CAST(((CURRENT_DATE() - b.last_order_date_rstores) / 7) AS INTEGER)
        END AS cust_current_weeks_since_last_shopped_rstores
     , CASE WHEN last_order_date_rcom > CURRENT_DATE() THEN NULL
            ELSE CAST(((CURRENT_DATE() - b.last_order_date_rcom) / 7) AS INTEGER)
        END AS cust_current_weeks_since_last_shopped_rcom
     , c.martial_status AS cust_current_marital_status
     , c.household_adult_count AS cust_current_household_adult_count
     , c.household_children_count AS cust_current_household_children_count
     , c.household_person_count AS cust_current_household_person_count
     , c.occupation_type AS cust_current_occupation_type
     , c.education_model_likelihood AS cust_current_education_model_likelihood
     , CAST(c.household_discretionary_spend_estimate_spend_on_apparel AS INTEGER) AS cust_current_household_discretionary_spend_estimate_spend_on_apparel
     , CAST(c.household_discretionary_spend_estimate_score AS INTEGER) AS cust_current_household_discretionary_spend_estimate_score
     , CAST(c.household_estimated_income_national_percentile AS INTEGER) AS cust_current_household_estimated_income_national_percentile
     , CAST(c.household_estimated_income_amount_in_thousands AS INTEGER) AS cust_current_household_estimated_income_amount_in_thousands
     , CASE WHEN c.household_estimated_income_range IN ('$1,000-$14,999', '$15,000-$24,999', '$25,000-$34,999', '$35,000-$49,999', '$50,000-$74,999') THEN '1) < $75,000'
            WHEN c.household_estimated_income_range IN ('$75,000-$99,999') THEN '2) $75,000 - $99,999'
            WHEN c.household_estimated_income_range IN ('$100,000-$124,999', '$125,000-$149,999') THEN '3) $100,000-$149,999'
            WHEN c.household_estimated_income_range IN ('$150,000-$174,999', '$175,000-$199,999') THEN '4) $150,000-$199,999'
            WHEN c.household_estimated_income_range IN ('$200,000-$249,999', '$250,000+') THEN '5) $200,000+'
            ELSE 'Unknown'
        END AS cust_current_household_estimated_income_range
     , CAST(c.household_buyer_young_adult_clothing_shopper_likelihood_rank_asc AS INTEGER) AS cust_current_household_buyer_young_adult_clothing_shoppers_likelihood
     , CAST(c.household_buyer_prestige_makeup_user_likelihood_rank_asc AS INTEGER) AS cust_current_household_buyer_prestige_makeup_user_likelihood
     , CAST(c.household_buyer_luxury_store_shoppers_likelihood_rank_asc AS INTEGER) AS cust_current_household_buyer_luxury_store_shoppers_likelihood
     , CAST(c.household_buyer_loyalty_card_user_likelihood_rank_asc AS INTEGER) AS cust_current_household_buyer_loyalty_card_user_likelihood
     , CAST(c.household_buyer_online_overall_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_online_overall_propensity
     , CAST(c.household_buyer_retail_overall_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_retail_overall_propensity
     , CAST(c.household_buyer_consumer_expenditure_apparel_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_consumer_expenditure_apparel_propensity
     , CAST(c.household_buyer_online_apparel_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_online_apparel_propensity
     , CAST(c.household_buyer_retail_apparel_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_retail_apparel_propensity
     , CAST(c.household_buyer_consumer_expenditure_shoes_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_consumer_expenditure_shoes_propensity
     , CAST(c.household_buyer_online_shoe_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_online_shoes_propensity
     , CAST(c.household_buyer_retail_shoes_propensity_model_rank_desc AS INTEGER) AS cust_current_household_buyer_retail_shoes_propensity
     , c.social_media_predictions_facebook_usage_likelihood AS cust_current_social_media_predictions_facebook_usage_likelihood
     , c.social_media_predictions_instagram_usage_likelihood AS cust_current_social_media_predictions_instagram_usage_likelihood
     , c.social_media_predictions_pinterest_usage_likelihood AS cust_current_social_media_predictions_pinterest_usage_likelihood
     , c.social_media_predictions_linkedin_usage_likelihood AS cust_current_social_media_predictions_linkedin_usage_likelihood
     , c.social_media_predictions_twitter_usage_likelihood AS cust_current_social_media_predictions_twitter_usage_likelihood
     , c.social_media_predictions_snapchat_usage_likelihood AS cust_current_social_media_predictions_snapchat_usage_likelihood
     , c.social_media_predictions_youtube_usage_likelihood AS cust_current_social_media_predictions_youtube_usage_likelihood
     , c.retail_shopper_type AS cust_current_retail_shopper_type
     , c.household_mosaic_lifestyle_segment AS cust_current_household_mosaic_lifestyle_segment
     , g.mosaic_group AS cust_current_household_mosaic_lifestyle_group
     , g.mosaic_group_summary AS cust_current_household_mosaic_lifestyle_group_summary
     , c.truetouch_strategy_brand_loyalists_likelihood AS cust_current_truetouch_strategy_brand_loyalists_likelihood
     , c.truetouch_strategy_deal_seekers_likelihood AS cust_current_truetouch_strategy_deal_seekers_likelihood
     , c.truetouch_strategy_moment_shoppers_likelihood AS cust_current_truetouch_strategy_moment_shoppers_likelihood
     , c.truetouch_strategy_mainstream_adopters_likelihood AS cust_current_truetouch_strategy_mainstream_adopters_likelihood
     , c.truetouch_strategy_novelty_seekers_likelihood AS cust_current_truetouch_strategy_novelty_seekers_likelihood
     , c.truetouch_strategy_organic_and_natural_likelihood AS cust_current_truetouch_strategy_organic_and_natural_likelihood
     , c.truetouch_strategy_quality_matters_likelihood AS cust_current_truetouch_strategy_quality_matters_likelihood
     , c.truetouch_strategy_recreational_shoppers_likelihood AS cust_current_truetouch_strategy_recreational_shoppers_likelihood
     , c.truetouch_strategy_savvy_researchers_likelihood AS cust_current_truetouch_strategy_savvy_researchers_likelihood
     , c.truetouch_strategy_trendsetters_likelihood AS cust_current_truetouch_strategy_trendsetters_likelihood
     , CAST(d.affinity_accessories AS INTEGER) AS cust_current_affinity_accessories
     , CAST(d.affinity_active AS INTEGER) AS cust_current_affinity_active
     , CAST(d.affinity_apparel AS INTEGER) AS cust_current_affinity_apparel
     , CAST(d.affinity_beauty AS INTEGER) AS cust_current_affinity_beauty
     , CAST(d.affinity_designer AS INTEGER) AS cust_current_affinity_designer
     , CAST(d.affinity_home AS INTEGER) AS cust_current_affinity_home
     , CAST(d.affinity_kids AS INTEGER) AS cust_current_affinity_kids
     , CAST(d.affinity_mens AS INTEGER) AS cust_current_affinity_mens
     , CAST(d.affinity_shoes AS INTEGER) AS cust_current_affinity_shoes
     , CAST(d.affinity_womens AS INTEGER) AS cust_current_affinity_womens
     , e.predicted_ct_segment AS cust_current_predicted_core_target_segment
     , CASE WHEN (c.social_media_predictions_facebook_usage_likelihood IS NULL OR c.social_media_predictions_facebook_usage_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.social_media_predictions_facebook_usage_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.social_media_predictions_facebook_usage_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.social_media_predictions_facebook_usage_likelihood LIKE '%very%' THEN 3
                                 WHEN c.social_media_predictions_facebook_usage_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.social_media_predictions_facebook_usage_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.social_media_predictions_facebook_usage_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.social_media_predictions_facebook_usage_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_social_media_predictions_facebook_usage_score
     , CASE WHEN (c.social_media_predictions_instagram_usage_likelihood IS NULL OR c.social_media_predictions_instagram_usage_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.social_media_predictions_instagram_usage_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.social_media_predictions_instagram_usage_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.social_media_predictions_instagram_usage_likelihood LIKE '%very%' THEN 3
                                 WHEN c.social_media_predictions_instagram_usage_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.social_media_predictions_instagram_usage_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.social_media_predictions_instagram_usage_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.social_media_predictions_instagram_usage_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_social_media_predictions_instagram_usage_score
     , CASE WHEN (c.social_media_predictions_pinterest_usage_likelihood IS NULL OR c.social_media_predictions_pinterest_usage_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.social_media_predictions_pinterest_usage_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.social_media_predictions_pinterest_usage_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.social_media_predictions_pinterest_usage_likelihood LIKE '%very%' THEN 3
                                 WHEN c.social_media_predictions_pinterest_usage_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.social_media_predictions_pinterest_usage_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.social_media_predictions_pinterest_usage_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.social_media_predictions_pinterest_usage_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_social_media_predictions_pinterest_usage_score
     , CASE WHEN (c.social_media_predictions_linkedin_usage_likelihood IS NULL OR c.social_media_predictions_linkedin_usage_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.social_media_predictions_linkedin_usage_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.social_media_predictions_linkedin_usage_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.social_media_predictions_linkedin_usage_likelihood LIKE '%very%' THEN 3
                                 WHEN c.social_media_predictions_linkedin_usage_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.social_media_predictions_linkedin_usage_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.social_media_predictions_linkedin_usage_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.social_media_predictions_linkedin_usage_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_social_media_predictions_linkedin_usage_score
     , CASE WHEN (c.social_media_predictions_twitter_usage_likelihood IS NULL OR c.social_media_predictions_twitter_usage_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.social_media_predictions_twitter_usage_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.social_media_predictions_twitter_usage_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.social_media_predictions_twitter_usage_likelihood LIKE '%very%' THEN 3
                                 WHEN c.social_media_predictions_twitter_usage_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.social_media_predictions_twitter_usage_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.social_media_predictions_twitter_usage_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.social_media_predictions_twitter_usage_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_social_media_predictions_twitter_usage_score
     , CASE WHEN (c.social_media_predictions_snapchat_usage_likelihood IS NULL OR c.social_media_predictions_snapchat_usage_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.social_media_predictions_snapchat_usage_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.social_media_predictions_snapchat_usage_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.social_media_predictions_snapchat_usage_likelihood LIKE '%very%' THEN 3
                                 WHEN c.social_media_predictions_snapchat_usage_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.social_media_predictions_snapchat_usage_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.social_media_predictions_snapchat_usage_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.social_media_predictions_snapchat_usage_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_social_media_predictions_snapchat_usage_score
     , CASE WHEN (c.social_media_predictions_youtube_usage_likelihood IS NULL OR c.social_media_predictions_youtube_usage_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.social_media_predictions_youtube_usage_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.social_media_predictions_youtube_usage_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.social_media_predictions_youtube_usage_likelihood LIKE '%very%' THEN 3
                                 WHEN c.social_media_predictions_youtube_usage_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.social_media_predictions_youtube_usage_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.social_media_predictions_youtube_usage_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.social_media_predictions_youtube_usage_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_social_media_predictions_youtube_usage_score
     , CASE WHEN (c.truetouch_strategy_brand_loyalists_likelihood IS NULL OR c.truetouch_strategy_brand_loyalists_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_brand_loyalists_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_brand_loyalists_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_brand_loyalists_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_brand_loyalists_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_brand_loyalists_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_brand_loyalists_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_brand_loyalists_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_brand_loyalists_score
     , CASE WHEN (c.truetouch_strategy_deal_seekers_likelihood IS NULL OR c.truetouch_strategy_deal_seekers_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_deal_seekers_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_deal_seekers_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_deal_seekers_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_deal_seekers_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_deal_seekers_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_deal_seekers_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_deal_seekers_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_deal_seekers_score
     , CASE WHEN (c.truetouch_strategy_moment_shoppers_likelihood IS NULL OR c.truetouch_strategy_moment_shoppers_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_moment_shoppers_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_moment_shoppers_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_moment_shoppers_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_moment_shoppers_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_moment_shoppers_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_moment_shoppers_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_moment_shoppers_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_moment_shoppers_score
     , CASE WHEN (c.truetouch_strategy_mainstream_adopters_likelihood IS NULL OR c.truetouch_strategy_mainstream_adopters_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_mainstream_adopters_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_mainstream_adopters_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_mainstream_adopters_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_mainstream_adopters_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_mainstream_adopters_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_mainstream_adopters_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_mainstream_adopters_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_mainstream_adopters_score
     , CASE WHEN (c.truetouch_strategy_novelty_seekers_likelihood IS NULL OR c.truetouch_strategy_novelty_seekers_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_novelty_seekers_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_novelty_seekers_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_novelty_seekers_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_novelty_seekers_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_novelty_seekers_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_novelty_seekers_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_novelty_seekers_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_novelty_seekers_score
     , CASE WHEN (c.truetouch_strategy_organic_and_natural_likelihood IS NULL OR c.truetouch_strategy_organic_and_natural_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_organic_and_natural_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_organic_and_natural_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_organic_and_natural_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_organic_and_natural_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_organic_and_natural_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_organic_and_natural_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_organic_and_natural_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_organic_and_natural_score
     , CASE WHEN (c.truetouch_strategy_quality_matters_likelihood IS NULL OR c.truetouch_strategy_quality_matters_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_quality_matters_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_quality_matters_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_quality_matters_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_quality_matters_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_quality_matters_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_quality_matters_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_quality_matters_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_quality_matters_score
     , CASE WHEN (c.truetouch_strategy_recreational_shoppers_likelihood IS NULL OR c.truetouch_strategy_recreational_shoppers_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_recreational_shoppers_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_recreational_shoppers_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_recreational_shoppers_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_recreational_shoppers_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_recreational_shoppers_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_recreational_shoppers_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_recreational_shoppers_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_recreational_shoppers_score
     , CASE WHEN (c.truetouch_strategy_savvy_researchers_likelihood IS NULL OR c.truetouch_strategy_savvy_researchers_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_savvy_researchers_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_savvy_researchers_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_savvy_researchers_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_savvy_researchers_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_savvy_researchers_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_savvy_researchers_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_savvy_researchers_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_savvy_researchers_score
     , CASE WHEN (c.truetouch_strategy_trendsetters_likelihood IS NULL OR c.truetouch_strategy_trendsetters_likelihood = 'Unknown') THEN NULL
            ELSE CAST((CAST(CASE WHEN c.truetouch_strategy_trendsetters_likelihood LIKE '%extremely%' THEN 5
                                 WHEN c.truetouch_strategy_trendsetters_likelihood LIKE '%highly%' THEN 4
                                 WHEN c.truetouch_strategy_trendsetters_likelihood LIKE '%very%' THEN 3
                                 WHEN c.truetouch_strategy_trendsetters_likelihood IN ('likely','unlikely') THEN 2
                                 WHEN c.truetouch_strategy_trendsetters_likelihood LIKE '%somewhat%' THEN 1
                                 ELSE 0 END AS FLOAT)
                          * CASE WHEN c.truetouch_strategy_trendsetters_likelihood LIKE '%unlikely%' THEN -1 ELSE 1 END
                          + CASE WHEN c.truetouch_strategy_trendsetters_likelihood LIKE '%unlikely%' THEN 5.5 ELSE 4.5 END
                      ) * 0.1 AS DECIMAL(3,2))
        END AS cust_current_truetouch_strategy_trendsetters_score
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM acp_ids a
LEFT JOIN t2dl_das_cal.customer_attributes_transactions b
  ON a.acp_id = b.acp_id
LEFT JOIN prd_nap_cust_usr_vws.customer_experian_demographic_prediction_dim c
  ON a.acp_id = c.acp_id
LEFT JOIN t2dl_das_cal.customer_attributes_merch d
  ON a.acp_id = d.acp_id
LEFT JOIN t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment e
  ON a.acp_id = e.acp_id
LEFT JOIN t2dl_das_cal.customer_attributes_scores f
  ON a.acp_id = f.acp_id
LEFT JOIN t2dl_das_usl.experian_mosaic_segments g
  ON c.household_mosaic_lifestyle_segment = g.mosaic_segment;


SET QUERY_BAND = NONE FOR SESSION;
