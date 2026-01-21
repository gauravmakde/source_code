SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=ddl_cco_cust_channel_week_attributes_11521_ACE_ENG;
Task_Name=run_teradata_ddl_cco_cust_channel_week_attributes_vw;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************
 * This file creates the production view cco_cust_channel_week_attributes_vw
 * to combine acp_id level data with the cco_cust_channel_week_attributes table data.
 ************************************************************************************/
/************************************************************************************/


REPLACE VIEW {cco_t2_schema}.cco_cust_channel_week_attributes_vw
AS
LOCKING ROW FOR ACCESS
SELECT a.acp_id
     , a.week_idnt
     , a.channel
     , a.cust_chan_gross_sales
     , a.cust_chan_return_amt
     , a.cust_chan_net_sales
     , a.cust_chan_trips
     , a.cust_chan_gross_items
     , a.cust_chan_return_items
     , a.cust_chan_net_items
     , a.cust_chan_anniversary
     , a.cust_chan_holiday
     , a.cust_chan_npg
     , a.cust_chan_singledivision
     , a.cust_chan_multidivision
     --, a.cust_chan_net_sales_wkly   -- not effective at week level
     --, a.cust_chan_net_sales_wkny   -- not effective at week level
     --, a.cust_chan_anniversary_wkly -- not effective at week level
     --, a.cust_chan_holiday_wkly     -- not effective at week level
     , a.cust_chan_buyer_flow
     , a.cust_chan_acquired_aare
     , a.cust_chan_activated_aare
     , a.cust_chan_retained_aare
     , a.cust_chan_engaged_aare
     , a.cust_gender
     , a.cust_age
     , a.cust_lifestage
     , a.cust_age_group
     , a.cust_nms_market
     , a.cust_dma
     , a.cust_country
     , a.cust_dma_rank
     , a.cust_loyalty_type
     , a.cust_loyalty_level
     , a.cust_loy_member_enroll_dt
     , a.cust_loy_cardmember_enroll_dt
     , a.cust_acquired_this_year
     , a.cust_acquisition_date
     , a.cust_acquisition_fiscal_year
     , a.cust_acquisition_channel
     , a.cust_acquisition_banner
     , a.cust_acquisition_brand
     , a.cust_tenure_bucket_months
     , a.cust_tenure_bucket_years
     , a.cust_activation_date
     , a.cust_activation_channel
     , a.cust_activation_banner
     --, a.cust_engagement_cohort     -- not effective at week level
     , a.cust_channel_count
     , a.cust_channel_combo
     , a.cust_banner_count
     , a.cust_banner_combo
     , a.cust_employee_flag
     , a.cust_jwn_trip_bucket
     , a.cust_jwn_net_spend_bucket
     , a.cust_jwn_gross_sales
     , a.cust_jwn_return_amt
     , a.cust_jwn_net_sales
     , a.cust_jwn_net_sales_apparel
     , a.cust_jwn_net_sales_shoes
     , a.cust_jwn_net_sales_beauty
     , a.cust_jwn_net_sales_designer
     , a.cust_jwn_net_sales_accessories
     , a.cust_jwn_net_sales_home
     , a.cust_jwn_net_sales_merch_projects
     , a.cust_jwn_net_sales_leased_boutiques
     , a.cust_jwn_net_sales_other_non_merch
     , a.cust_jwn_net_sales_restaurant
     , a.cust_jwn_transaction_apparel_ind
     , a.cust_jwn_transaction_shoes_ind
     , a.cust_jwn_transaction_beauty_ind
     , a.cust_jwn_transaction_designer_ind
     , a.cust_jwn_transaction_accessories_ind
     , a.cust_jwn_transaction_home_ind
     , a.cust_jwn_transaction_merch_projects_ind
     , a.cust_jwn_transaction_leased_boutiques_ind
     , a.cust_jwn_transaction_other_non_merch_ind
     , a.cust_jwn_transaction_restaurant_ind
     , a.cust_jwn_trips
     , a.cust_jwn_gross_items
     , a.cust_jwn_return_items
     , a.cust_jwn_net_items
     , a.cust_tender_nordstrom
     , a.cust_tender_nordstrom_note
     , a.cust_tender_3rd_party_credit
     , a.cust_tender_debit_card
     , a.cust_tender_gift_card
     , a.cust_tender_cash
     , a.cust_tender_paypal
     , a.cust_tender_check
     , a.cust_svc_group_exp_delivery
     , a.cust_svc_group_order_pickup
     , a.cust_svc_group_selling_relation
     , a.cust_svc_group_remote_selling
     , a.cust_svc_group_alterations
     , a.cust_svc_group_in_store
     , a.cust_svc_group_restaurant
     , a.cust_service_free_exp_delivery
     , a.cust_service_next_day_pickup
     , a.cust_service_same_day_bopus
     , a.cust_service_curbside_pickup
     , a.cust_service_style_boards
     , a.cust_service_gift_wrapping
     , a.cust_service_pop_in
     , a.cust_marketplace_flag
     , a.cust_platform_desktop
     , a.cust_platform_MOW
     , a.cust_platform_IOS
     , a.cust_platform_Android
     , a.cust_platform_POS
     , a.cust_anchor_brand
     , a.cust_strategic_brand
     , a.cust_store_customer
     , a.cust_digital_customer
     , a.cust_clv_jwn
     , a.cust_clv_fp
     , a.cust_clv_op
     , b.cust_next_year_net_spend_jwn
     , b.cust_current_rfm_1year_segment
     , b.cust_current_rfm_4year_segment
     , b.cust_current_weeks_since_last_shopped_op
     , b.cust_current_weeks_since_last_shopped_ncom
     , b.cust_current_weeks_since_last_shopped_rstores
     , b.cust_current_weeks_since_last_shopped_rcom
     , b.cust_current_marital_status
     , b.cust_current_household_adult_count
     , b.cust_current_household_children_count
     , b.cust_current_household_person_count
     , b.cust_current_occupation_type
     , b.cust_current_education_model_likelihood
     , b.cust_current_household_discretionary_spend_estimate_spend_on_apparel
     , b.cust_current_household_discretionary_spend_estimate_score
     , b.cust_current_household_estimated_income_national_percentile
     , b.cust_current_household_estimated_income_amount_in_thousands
     , b.cust_current_household_estimated_income_range
     , b.cust_current_household_buyer_young_adult_clothing_shoppers_likelihood
     , b.cust_current_household_buyer_prestige_makeup_user_likelihood
     , b.cust_current_household_buyer_luxury_store_shoppers_likelihood
     , b.cust_current_household_buyer_loyalty_card_user_likelihood
     , b.cust_current_household_buyer_online_overall_propensity
     , b.cust_current_household_buyer_retail_overall_propensity
     , b.cust_current_household_buyer_consumer_expenditure_apparel_propensity
     , b.cust_current_household_buyer_online_apparel_propensity
     , b.cust_current_household_buyer_retail_apparel_propensity
     , b.cust_current_household_buyer_consumer_expenditure_shoes_propensity
     , b.cust_current_household_buyer_online_shoes_propensity
     , b.cust_current_household_buyer_retail_shoes_propensity
     , b.cust_current_social_media_predictions_facebook_usage_likelihood
     , b.cust_current_social_media_predictions_instagram_usage_likelihood
     , b.cust_current_social_media_predictions_pinterest_usage_likelihood
     , b.cust_current_social_media_predictions_linkedin_usage_likelihood
     , b.cust_current_social_media_predictions_twitter_usage_likelihood
     , b.cust_current_social_media_predictions_snapchat_usage_likelihood
     , b.cust_current_social_media_predictions_youtube_usage_likelihood
     , b.cust_current_retail_shopper_type
     , b.cust_current_household_mosaic_lifestyle_segment
     , b.cust_current_household_mosaic_lifestyle_group
     , b.cust_current_household_mosaic_lifestyle_group_summary
     , b.cust_current_truetouch_strategy_brand_loyalists_likelihood
     , b.cust_current_truetouch_strategy_deal_seekers_likelihood
     , b.cust_current_truetouch_strategy_moment_shoppers_likelihood
     , b.cust_current_truetouch_strategy_mainstream_adopters_likelihood
     , b.cust_current_truetouch_strategy_novelty_seekers_likelihood
     , b.cust_current_truetouch_strategy_organic_and_natural_likelihood
     , b.cust_current_truetouch_strategy_quality_matters_likelihood
     , b.cust_current_truetouch_strategy_recreational_shoppers_likelihood
     , b.cust_current_truetouch_strategy_savvy_researchers_likelihood
     , b.cust_current_truetouch_strategy_trendsetters_likelihood
     , b.cust_current_affinity_accessories
     , b.cust_current_affinity_active
     , b.cust_current_affinity_apparel
     , b.cust_current_affinity_beauty
     , b.cust_current_affinity_designer
     , b.cust_current_affinity_home
     , b.cust_current_affinity_kids
     , b.cust_current_affinity_mens
     , b.cust_current_affinity_shoes
     , b.cust_current_affinity_womens
     , b.cust_current_predicted_core_target_segment
     , b.cust_current_social_media_predictions_facebook_usage_score
     , b.cust_current_social_media_predictions_instagram_usage_score
     , b.cust_current_social_media_predictions_pinterest_usage_score
     , b.cust_current_social_media_predictions_linkedin_usage_score
     , b.cust_current_social_media_predictions_twitter_usage_score
     , b.cust_current_social_media_predictions_snapchat_usage_score
     , b.cust_current_social_media_predictions_youtube_usage_score
     , b.cust_current_truetouch_strategy_brand_loyalists_score
     , b.cust_current_truetouch_strategy_deal_seekers_score
     , b.cust_current_truetouch_strategy_moment_shoppers_score
     , b.cust_current_truetouch_strategy_mainstream_adopters_score
     , b.cust_current_truetouch_strategy_novelty_seekers_score
     , b.cust_current_truetouch_strategy_organic_and_natural_score
     , b.cust_current_truetouch_strategy_quality_matters_score
     , b.cust_current_truetouch_strategy_recreational_shoppers_score
     , b.cust_current_truetouch_strategy_savvy_researchers_score
     , b.cust_current_truetouch_strategy_trendsetters_score
     --, a.cust_jwn_top500k  -- not effective at week level
     --, a.cust_jwn_top1k    -- not effective at week level
FROM {cco_t2_schema}.cco_cust_channel_week_attributes a
LEFT JOIN {cco_t2_schema}.cco_cust_attributes b
  ON a.acp_id = b.acp_id;


SET QUERY_BAND = NONE FOR SESSION;
