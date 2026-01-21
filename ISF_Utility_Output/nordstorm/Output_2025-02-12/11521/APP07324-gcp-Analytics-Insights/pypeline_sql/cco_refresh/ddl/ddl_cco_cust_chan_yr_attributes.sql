SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=ddl_cco_cust_chan_yr_attributes_daily_11521_ACE_ENG;
Task_Name=run_teradata_ddl_cco_cust_chan_yr_attributes;'
FOR SESSION VOLATILE;


--DROP TABLE {cco_t2_schema}.cco_cust_chan_yr_attributes;


CREATE MULTISET TABLE {cco_t2_schema}.cco_cust_chan_yr_attributes ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      reporting_year_shopped VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cust_chan_gross_sales DECIMAL(38,2),
      cust_chan_return_amt DECIMAL(38,2),
      cust_chan_net_sales DECIMAL(38,2),
      cust_chan_trips INTEGER,
      cust_chan_gross_items INTEGER,
      cust_chan_return_items INTEGER,
      cust_chan_net_items INTEGER,
      cust_chan_anniversary BYTEINT COMPRESS (0, 1),
      cust_chan_holiday BYTEINT COMPRESS (0, 1),
      cust_chan_npg BYTEINT COMPRESS (0, 1),
      cust_chan_singledivision BYTEINT COMPRESS (0, 1),
      cust_chan_multidivision BYTEINT COMPRESS (0, 1),
      cust_chan_net_sales_ly DECIMAL(38,2),
      cust_chan_net_sales_ny DECIMAL(38,2),
      cust_chan_anniversary_ly INTEGER COMPRESS (0, 1),
      cust_chan_holiday_ly INTEGER COMPRESS (0, 1),
      cust_chan_buyer_flow VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel',
          '4) Reactivated-to-Channel'),
      cust_chan_acquired_aare BYTEINT COMPRESS (0, 1),
      cust_chan_activated_aare BYTEINT COMPRESS (0, 1),
      cust_chan_retained_aare BYTEINT COMPRESS (0, 1),
      cust_chan_engaged_aare BYTEINT COMPRESS (0, 1),
      cust_gender VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          'Female', 'Male', 'Unknown'),
      cust_age SMALLINT,
      cust_lifestage VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '01) Young Adult', '02) Early Career', '03) Mid Career', '04) Late Career', '05) Retired',
          'Unknown'),
      cust_age_group VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '0) <18 yrs', '1) 18-24 yrs', '2) 25-34 yrs', '3) 35-44 yrs', '4) 45-54 yrs',
          '5) 55-64 yrs', '6) 65+ yrs', 'Unknown'),
      cust_NMS_market VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cust_dma VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cust_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cust_dma_rank VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) Top 5', '2) 6-10', '3) 11-20', '4) 21-30', '5) 31-50', '6) 51-100',
          '7) > 100', 'DMA missing'),
      cust_loyalty_type VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          'a) Cardmember', 'b) Member', 'c) Non-Loyalty'),
      cust_loyalty_level VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) MEMBER', '2) INFLUENCER', '3) AMBASSADOR', '4) ICON'),
      cust_loy_member_enroll_dt DATE FORMAT 'YY/MM/DD',
      cust_loy_cardmember_enroll_dt DATE FORMAT 'YY/MM/DD',
      cust_acquired_this_year BYTEINT COMPRESS (0, 1),
      cust_acquisition_date DATE FORMAT 'YY/MM/DD',
      cust_acquisition_fiscal_year SMALLINT,
      cust_acquisition_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) Nordstrom Stores', '2) Nordstrom.com', '3) Rack Stores', '4) Rack.com'),
      cust_acquisition_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          'NORDSTROM', 'RACK'),
      cust_acquisition_brand VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cust_tenure_bucket_months VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) 0-3 months', '2) 4-6 months', '3) 7-12 months', '4) 13-24 months', '5) 25-36 months',
          '6) 37-48 months', '7) 49-60 months', '8) 61+ months'),
      cust_tenure_bucket_years VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) <= 1 year', '2) 1-2 years', '3) 2-5 years', '4) 5-10 years', '5) 10+ years'),
      cust_activation_date DATE FORMAT 'YY/MM/DD',
      cust_activation_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) Nordstrom Stores', '2) Nordstrom.com', '3) Rack Stores', '4) Rack.com'),
      cust_activation_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          'NORDSTROM', 'RACK'),
      cust_engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          'Acquire & Activate', 'Highly-Engaged', 'Lightly-Engaged', 'Moderately-Engaged'),
      cust_channel_count INTEGER COMPRESS (1, 2, 3, 4),
      cust_channel_combo VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cust_banner_count INTEGER COMPRESS (1, 2),
      cust_banner_combo VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
          '1) Nordstrom-only', '2) Rack-only', '3) Dual-Banner'),
      cust_employee_flag INTEGER COMPRESS (0, 1),
      cust_jwn_trip_bucket VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
           '00 trips', '01 trips', '02 trips', '03 trips', '04 trips', '05 trips', '06 trips',
           '07 trips', '08 trips', '09 trips', '10+ trips'),
      cust_jwn_net_spend_bucket VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
           '0) $0', '1) $0-50', '2) $50-100', '3) $100-250', '4) $250-500', '5) $500-1K',
           '6) 1-2K', '7) 2-5K', '8) 5-10K', '9) 10K+'),
      cust_jwn_gross_sales DECIMAL(38,2),
      cust_jwn_return_amt DECIMAL(38,2),
      cust_jwn_net_sales DECIMAL(38,2),
      cust_jwn_net_sales_apparel DECIMAL(38,2),
      cust_jwn_trips INTEGER,
      cust_jwn_gross_items INTEGER,
      cust_jwn_return_items INTEGER,
      cust_jwn_net_items INTEGER,
      cust_tender_nordstrom BYTEINT COMPRESS (0, 1),
      cust_tender_nordstrom_note BYTEINT COMPRESS (0, 1),
      cust_tender_3rd_party_credit BYTEINT COMPRESS (0, 1),
      cust_tender_debit_card BYTEINT COMPRESS (0, 1),
      cust_tender_gift_card BYTEINT COMPRESS (0, 1),
      cust_tender_cash BYTEINT COMPRESS (0, 1),
      cust_tender_paypal BYTEINT COMPRESS (0, 1),
      cust_tender_check BYTEINT COMPRESS (0, 1),
      cust_svc_group_exp_delivery BYTEINT COMPRESS (0, 1),
      cust_svc_group_order_pickup BYTEINT COMPRESS (0, 1),
      cust_svc_group_selling_relation BYTEINT COMPRESS (0, 1),
      cust_svc_group_remote_selling BYTEINT COMPRESS (0, 1),
      cust_svc_group_alterations BYTEINT COMPRESS (0, 1),
      cust_svc_group_in_store BYTEINT COMPRESS (0, 1),
      cust_svc_group_restaurant BYTEINT COMPRESS (0, 1),
      cust_service_free_exp_delivery BYTEINT COMPRESS (0, 1),
      cust_service_next_day_pickup BYTEINT COMPRESS (0, 1),
      cust_service_same_day_bopus BYTEINT COMPRESS (0, 1),
      cust_service_curbside_pickup BYTEINT COMPRESS (0, 1),
      cust_service_style_boards BYTEINT COMPRESS (0, 1),
      cust_service_gift_wrapping BYTEINT COMPRESS (0, 1),
      cust_service_pop_in BYTEINT COMPRESS (0, 1),
      cust_marketplace_flag BYTEINT COMPRESS (0, 1),
      cust_platform_desktop BYTEINT COMPRESS (0, 1),
      cust_platform_MOW BYTEINT COMPRESS (0, 1),
      cust_platform_IOS BYTEINT COMPRESS (0, 1),
      cust_platform_Android BYTEINT COMPRESS (0, 1),
      cust_platform_POS BYTEINT COMPRESS (0, 1),
      cust_anchor_brand BYTEINT COMPRESS (0, 1),
      cust_strategic_brand BYTEINT COMPRESS (0, 1),
      cust_store_customer BYTEINT COMPRESS (0, 1),
      cust_digital_customer BYTEINT COMPRESS (0, 1),
      cust_clv_jwn DECIMAL(14,4),
      cust_clv_fp DECIMAL(14,4),
      cust_clv_op DECIMAL(14,4),
      cust_next_year_net_spend_jwn DECIMAL(14,4),
      cust_current_rfm_1year_segment VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS (
          '01 Champions', '02 Loyal Customers', '03 Potential Loyalist', '04 Recent Customers',
          '05 Promising', '06 Customers Needing Attention', '07 About To Sleep', '08 At Risk',
          '09 Cant Lose Them', '10 Hibernating', '11 Lost'),
      cust_current_rfm_4year_segment VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS (
          '01 Champions', '02 Loyal Customers', '03 Potential Loyalist', '04 Recent Customers',
          '05 Promising', '06 Customers Needing Attention', '07 About To Sleep', '08 At Risk',
          '09 Cant Lose Them', '10 Hibernating', '11 Lost'),
      cust_current_months_since_last_shopped_jwn SMALLINT,
      cust_current_months_since_last_shopped_fp SMALLINT,
      cust_current_months_since_last_shopped_op SMALLINT,
      cust_current_months_since_last_shopped_nstores SMALLINT,
      cust_current_months_since_last_shopped_ncom SMALLINT,
      cust_current_months_since_last_shopped_rstores SMALLINT,
      cust_current_months_since_last_shopped_rcom SMALLINT,
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
      cust_jwn_top500k BYTEINT COMPRESS (0, 1),
      cust_jwn_top1k BYTEINT COMPRESS (0, 1),
      dw_sys_load_tmstp TIMESTAMP(6),
      dw_sys_updt_tmstp TIMESTAMP(6))
PRIMARY INDEX ( acp_id ,reporting_year_shopped ,channel );


collect statistics
column  (acp_id),
column  (reporting_year_shopped),
column  (channel),
column  (cust_gender),
column  (cust_age),
column  (cust_lifestage),
column  (cust_age_group),
column  (cust_NMS_market),
column  (cust_dma),
column  (cust_country),
column  (cust_dma_rank),
column  (cust_loyalty_type),
column  (cust_loyalty_level),
column  (cust_employee_flag),
column  (acp_id, reporting_year_shopped, channel)
on
{cco_t2_schema}.cco_cust_chan_yr_attributes;



SET QUERY_BAND = NONE FOR SESSION;
