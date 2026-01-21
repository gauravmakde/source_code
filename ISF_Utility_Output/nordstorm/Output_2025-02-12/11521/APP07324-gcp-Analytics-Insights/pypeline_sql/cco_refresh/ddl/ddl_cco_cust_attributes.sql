SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=ddl_cco_cust_attributes_11521_ACE_ENG;
Task_Name=run_teradata_ddl_cco_cust_attributes;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************
 * This table supports view cco_cust_channel_week_attributes_vw
 * to combine acp_id level data with the cco_cust_channel_week_attributes table data.
 ************************************************************************************/
/************************************************************************************/


--/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP('{cco_t2_schema}', 'cco_cust_attributes', OUT_RETURN_MSG);
--*/


CREATE MULTISET TABLE {cco_t2_schema}.cco_cust_attributes,--NO FALLBACK ,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    cust_next_year_net_spend_jwn DECIMAL(14,4),
    cust_current_rfm_1year_segment VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS (
        '01 Champions', '02 Loyal Customers', '03 Potential Loyalist', '04 Recent Customers',
        '05 Promising', '06 Customers Needing Attention', '07 About To Sleep', '08 At Risk',
        '09 Cant Lose Them', '10 Hibernating', '11 Lost'),
    cust_current_rfm_4year_segment VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS (
        '01 Champions', '02 Loyal Customers', '03 Potential Loyalist', '04 Recent Customers',
        '05 Promising', '06 Customers Needing Attention', '07 About To Sleep', '08 At Risk',
        '09 Cant Lose Them', '10 Hibernating', '11 Lost'),
    cust_current_weeks_since_last_shopped_jwn SMALLINT,
    cust_current_weeks_since_last_shopped_fp SMALLINT,
    cust_current_weeks_since_last_shopped_op SMALLINT,
    cust_current_weeks_since_last_shopped_nstores SMALLINT,
    cust_current_weeks_since_last_shopped_ncom SMALLINT,
    cust_current_weeks_since_last_shopped_rstores SMALLINT,
    cust_current_weeks_since_last_shopped_rcom SMALLINT,
    cust_current_marital_status VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Married  Extremely Likely', 'Married Likely', 'Single Likely, never married',
        'Unknown Not scored', 'Unknown Scored'),
    cust_current_household_adult_count SMALLINT COMPRESS (0, 1, 2, 3, 4, 5, 6, 7, 8),
    cust_current_household_children_count SMALLINT COMPRESS (0, 1, 2, 3, 4, 5, 6, 7),
    cust_current_household_person_count SMALLINT COMPRESS (0, 1, 2, 3, 4, 5, 6, 7, 8),
    cust_current_occupation_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    cust_current_education_model_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    cust_current_household_discretionary_spend_estimate_spend_on_apparel INTEGER,
    cust_current_household_discretionary_spend_estimate_score INTEGER,
    cust_current_household_estimated_income_national_percentile SMALLINT,
    cust_current_household_estimated_income_amount_in_thousands INTEGER,
    cust_current_household_estimated_income_range VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) < $75,000', '2) $75,000 - $99,999', '3) $100,000-$149,999', '4) $150,000-$199,999',
        '5) $200,000+', 'Unknown'),
    cust_current_household_buyer_young_adult_clothing_shoppers_likelihood INTEGER,
    cust_current_household_buyer_prestige_makeup_user_likelihood INTEGER,
    cust_current_household_buyer_luxury_store_shoppers_likelihood INTEGER,
    cust_current_household_buyer_loyalty_card_user_likelihood INTEGER,
    cust_current_household_buyer_online_overall_propensity INTEGER,
    cust_current_household_buyer_retail_overall_propensity INTEGER,
    cust_current_household_buyer_consumer_expenditure_apparel_propensity INTEGER,
    cust_current_household_buyer_online_apparel_propensity INTEGER,
    cust_current_household_buyer_retail_apparel_propensity INTEGER,
    cust_current_household_buyer_consumer_expenditure_shoes_propensity INTEGER,
    cust_current_household_buyer_online_shoes_propensity INTEGER,
    cust_current_household_buyer_retail_shoes_propensity INTEGER,
    cust_current_social_media_predictions_facebook_usage_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_social_media_predictions_instagram_usage_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_social_media_predictions_pinterest_usage_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_social_media_predictions_linkedin_usage_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_social_media_predictions_twitter_usage_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_social_media_predictions_snapchat_usage_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_social_media_predictions_youtube_usage_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_retail_shopper_type VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Just the Essentials: consumers who primarily purchase necessities',
        'Original Traditionalists: loyal to their brands, stores, services, and their country',
        'Status Strivers:  shopping is fun and recreational',
        'Upscale Clicks and Bricks: knowledgeable consumers, who buy in-store or online',
        'Virtual Shoppers: go for the bargains and the Internet helps them find discounts',
        'Unknown'),
    cust_current_household_mosaic_lifestyle_segment VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    cust_current_household_mosaic_lifestyle_group VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    cust_current_household_mosaic_lifestyle_group_summary VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
    cust_current_truetouch_strategy_brand_loyalists_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_deal_seekers_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_moment_shoppers_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_mainstream_adopters_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_novelty_seekers_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_organic_and_natural_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_quality_matters_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_recreational_shoppers_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_savvy_researchers_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_truetouch_strategy_trendsetters_likelihood VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Very Unlikely', 'Extremely Unlikely', 'Somewhat Likely', 'Extremely Likely',
        'Somewhat Unlikely', 'Highly Likely', 'Very Likely', 'Likely', 'Highly Unlikely'),
    cust_current_affinity_accessories INTEGER,
    cust_current_affinity_active INTEGER,
    cust_current_affinity_apparel INTEGER,
    cust_current_affinity_beauty INTEGER,
    cust_current_affinity_designer INTEGER,
    cust_current_affinity_home INTEGER,
    cust_current_affinity_kids INTEGER,
    cust_current_affinity_mens INTEGER,
    cust_current_affinity_shoes INTEGER,
    cust_current_affinity_womens INTEGER,
    cust_current_predicted_core_target_segment VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'bargainista', 'curator', 'enthusiast', 'essentialist', 'premium_shopper', 'streamliner'),
    cust_current_social_media_predictions_facebook_usage_score DECIMAL(3,2),
    cust_current_social_media_predictions_instagram_usage_score DECIMAL(3,2),
    cust_current_social_media_predictions_pinterest_usage_score DECIMAL(3,2),
    cust_current_social_media_predictions_linkedin_usage_score DECIMAL(3,2),
    cust_current_social_media_predictions_twitter_usage_score DECIMAL(3,2),
    cust_current_social_media_predictions_snapchat_usage_score DECIMAL(3,2),
    cust_current_social_media_predictions_youtube_usage_score DECIMAL(3,2),
    cust_current_truetouch_strategy_brand_loyalists_score DECIMAL(3,2),
    cust_current_truetouch_strategy_deal_seekers_score DECIMAL(3,2),
    cust_current_truetouch_strategy_moment_shoppers_score DECIMAL(3,2),
    cust_current_truetouch_strategy_mainstream_adopters_score DECIMAL(3,2),
    cust_current_truetouch_strategy_novelty_seekers_score DECIMAL(3,2),
    cust_current_truetouch_strategy_organic_and_natural_score DECIMAL(3,2),
    cust_current_truetouch_strategy_quality_matters_score DECIMAL(3,2),
    cust_current_truetouch_strategy_recreational_shoppers_score DECIMAL(3,2),
    cust_current_truetouch_strategy_savvy_researchers_score DECIMAL(3,2),
    cust_current_truetouch_strategy_trendsetters_score DECIMAL(3,2),
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX (acp_id);


COLLECT STATISTICS COLUMN (acp_id) ON {cco_t2_schema}.cco_cust_attributes;


SET QUERY_BAND = NONE FOR SESSION;
