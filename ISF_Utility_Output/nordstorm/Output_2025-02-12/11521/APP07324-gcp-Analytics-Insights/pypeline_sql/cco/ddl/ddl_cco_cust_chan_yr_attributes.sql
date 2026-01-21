SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=ddl_cco_cust_chan_yr_attributes_11521_ACE_ENG;
     Task_Name=ddl_cco_cust_chan_yr_attributes;'
     FOR SESSION VOLATILE;




/*
CCO CCO Customer Channel Year Attributes DDL file   
This file creates the production table T2DL_DAS_STRATEGY.CCO_cust_chan_yr_attributes
*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'cco_cust_chan_yr_attributes', OUT_RETURN_MSG);

create MULTISET table {cco_t2_schema}.cco_cust_chan_yr_attributes,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
   acp_id VARCHAR(50) character set unicode not casespecific  
  ,reporting_year_shopped VARCHAR(10)character set unicode not casespecific  
  ,channel VARCHAR(20)character set unicode not casespecific  
  ,cust_chan_gross_sales DECIMAL(38,2)
  ,cust_chan_return_amt DECIMAL(38,2)
  ,cust_chan_net_sales DECIMAL(38,2)
  ,cust_chan_trips INTEGER
  ,cust_chan_gross_items INTEGER
  ,cust_chan_return_items INTEGER
  ,cust_chan_net_items INTEGER
  ,cust_chan_anniversary BYTEINT
  ,cust_chan_holiday BYTEINT
  ,cust_chan_npg BYTEINT
  ,cust_chan_singledivision BYTEINT
  ,cust_chan_multidivision BYTEINT
  ,cust_chan_net_sales_ly DECIMAL(38,2) COMPRESS
  ,cust_chan_net_sales_ny DECIMAL(38,2) COMPRESS
  ,cust_chan_anniversary_ly INTEGER COMPRESS
  ,cust_chan_holiday_ly INTEGER COMPRESS
  ,cust_chan_buyer_flow VARCHAR(27)character set unicode not casespecific COMPRESS
  ,cust_chan_acquired_aare BYTEINT COMPRESS
  ,cust_chan_activated_aare BYTEINT COMPRESS
  ,cust_chan_retained_aare BYTEINT COMPRESS
  ,cust_chan_engaged_aare BYTEINT COMPRESS
  ,cust_gender VARCHAR(7)character set unicode not casespecific
  ,cust_age SMALLINT COMPRESS
  ,cust_lifestage VARCHAR(16)character set unicode not casespecific
  ,cust_age_group VARCHAR(13)character set unicode not casespecific
  ,cust_NMS_market VARCHAR(20)character set unicode not casespecific
  ,cust_dma VARCHAR(55)character set unicode not casespecific COMPRESS
  ,cust_country VARCHAR(2)character set unicode not casespecific COMPRESS
  ,cust_dma_rank VARCHAR(11)character set unicode not casespecific
  ,cust_loyalty_type VARCHAR(14)character set unicode not casespecific 
  ,cust_loyalty_level VARCHAR(13)character set unicode not casespecific COMPRESS
  ,cust_loy_member_enroll_dt DATE COMPRESS
  ,cust_loy_cardmember_enroll_dt DATE COMPRESS
  --,cust_contr_margin_decile INTEGER
  --,cust_contr_margin_amt DECIMAL(38,2)
  ,cust_acquired_this_year BYTEINT COMPRESS
  ,cust_acquisition_date DATE COMPRESS
  ,cust_acquisition_fiscal_year SMALLINT COMPRESS
  ,cust_acquisition_channel VARCHAR(19)character set unicode not casespecific COMPRESS
  ,cust_acquisition_banner VARCHAR(9)character set unicode not casespecific COMPRESS
  ,cust_acquisition_brand VARCHAR(40)character set unicode not casespecific COMPRESS
  ,cust_tenure_bucket_months VARCHAR(15)character set unicode not casespecific COMPRESS
  ,cust_tenure_bucket_years VARCHAR(13)character set unicode not casespecific COMPRESS
  ,cust_activation_date DATE COMPRESS
  ,cust_activation_channel VARCHAR(19)character set unicode not casespecific COMPRESS
  ,cust_activation_banner VARCHAR(9)character set unicode not casespecific COMPRESS
  ,cust_engagement_cohort VARCHAR(30)character set unicode not casespecific
  ,cust_channel_count INTEGER
  ,cust_channel_combo VARCHAR(32)character set unicode not casespecific
  ,cust_banner_count INTEGER
  ,cust_banner_combo VARCHAR(17)character set unicode not casespecific
  ,cust_employee_flag INTEGER
  ,cust_jwn_trip_bucket VARCHAR(9)character set unicode not casespecific COMPRESS
  ,cust_jwn_net_spend_bucket VARCHAR(11)character set unicode not casespecific COMPRESS
  ,cust_jwn_gross_sales DECIMAL(38,2)
  ,cust_jwn_return_amt DECIMAL(38,2)
  ,cust_jwn_net_sales DECIMAL(38,2)
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_trips INTEGER
  ,cust_jwn_gross_items INTEGER
  ,cust_jwn_return_items INTEGER
  ,cust_jwn_net_items INTEGER
  ,cust_tender_nordstrom BYTEINT
  ,cust_tender_nordstrom_note BYTEINT
  ,cust_tender_3rd_party_credit BYTEINT
  ,cust_tender_debit_card BYTEINT
  ,cust_tender_gift_card BYTEINT
  ,cust_tender_cash BYTEINT
  ,cust_tender_paypal BYTEINT
  ,cust_tender_check BYTEINT
  --,cust_event_ctr INTEGER
  ,cust_svc_group_exp_delivery BYTEINT
  ,cust_svc_group_order_pickup BYTEINT
  ,cust_svc_group_selling_relation BYTEINT
  ,cust_svc_group_remote_selling BYTEINT
  ,cust_svc_group_alterations BYTEINT
  ,cust_svc_group_in_store BYTEINT
  ,cust_svc_group_restaurant BYTEINT
  ,cust_service_free_exp_delivery BYTEINT
  ,cust_service_next_day_pickup BYTEINT
  ,cust_service_same_day_bopus BYTEINT
  ,cust_service_curbside_pickup BYTEINT
  ,cust_service_style_boards BYTEINT
  ,cust_service_gift_wrapping BYTEINT
  ,cust_service_pop_in BYTEINT
  ,cust_platform_desktop BYTEINT
  ,cust_platform_MOW BYTEINT
  ,cust_platform_IOS BYTEINT
  ,cust_platform_Android BYTEINT
  ,cust_platform_POS BYTEINT
  ,cust_anchor_brand BYTEINT
  ,cust_strategic_brand BYTEINT
  ,cust_store_customer BYTEINT
  ,cust_digital_customer BYTEINT
  ,cust_clv_jwn DECIMAL(14,4) COMPRESS
  ,cust_clv_fp DECIMAL(14,4) COMPRESS
  ,cust_clv_op DECIMAL(14,4) COMPRESS
  ,cust_next_year_net_spend_jwn DECIMAL(14,4) COMPRESS
  ,cust_current_rfm_1year_segment VARCHAR(30) CHARACTER SET LATIN COMPRESS
  ,cust_current_rfm_4year_segment VARCHAR(30) CHARACTER SET LATIN COMPRESS
  ,cust_current_months_since_last_shopped_jwn SMALLINT COMPRESS
  ,cust_current_months_since_last_shopped_fp SMALLINT COMPRESS
  ,cust_current_months_since_last_shopped_op SMALLINT COMPRESS
  ,cust_current_months_since_last_shopped_nstores SMALLINT COMPRESS
  ,cust_current_months_since_last_shopped_ncom SMALLINT COMPRESS
  ,cust_current_months_since_last_shopped_rstores SMALLINT COMPRESS
  ,cust_current_months_since_last_shopped_rcom SMALLINT COMPRESS
  ,cust_current_marital_status VARCHAR(50) COMPRESS
  ,cust_current_household_adult_count SMALLINT COMPRESS
  ,cust_current_household_children_count SMALLINT COMPRESS
  ,cust_current_household_person_count SMALLINT COMPRESS
  ,cust_current_occupation_type VARCHAR(50) COMPRESS
  ,cust_current_education_model_likelihood VARCHAR(50) COMPRESS
  ,cust_current_household_discretionary_spend_estimate_spend_on_apparel INTEGER COMPRESS
  ,cust_current_household_discretionary_spend_estimate_score INTEGER COMPRESS
  ,cust_current_household_estimated_income_national_percentile SMALLINT COMPRESS
  ,cust_current_household_estimated_income_amount_in_thousands INTEGER COMPRESS
  ,cust_current_household_estimated_income_range VARCHAR(50)
  ,cust_current_household_buyer_young_adult_clothing_shoppers_likelihood INTEGER COMPRESS
  ,cust_current_household_buyer_prestige_makeup_user_likelihood INTEGER COMPRESS
  ,cust_current_household_buyer_luxury_store_shoppers_likelihood INTEGER COMPRESS
  ,cust_current_household_buyer_loyalty_card_user_likelihood INTEGER COMPRESS
  ,cust_current_household_buyer_online_overall_propensity INTEGER COMPRESS
  ,cust_current_household_buyer_retail_overall_propensity INTEGER COMPRESS
  ,cust_current_household_buyer_consumer_expenditure_apparel_propensity INTEGER COMPRESS
  ,cust_current_household_buyer_online_apparel_propensity INTEGER COMPRESS
  ,cust_current_household_buyer_retail_apparel_propensity INTEGER COMPRESS
  ,cust_current_household_buyer_consumer_expenditure_shoes_propensity INTEGER COMPRESS
  ,cust_current_household_buyer_online_shoes_propensity INTEGER COMPRESS
  ,cust_current_household_buyer_retail_shoes_propensity INTEGER COMPRESS
  ,cust_current_social_media_predictions_facebook_usage_likelihood VARCHAR(50) COMPRESS
  ,cust_current_social_media_predictions_instagram_usage_likelihood VARCHAR(50) COMPRESS
  ,cust_current_social_media_predictions_pinterest_usage_likelihood VARCHAR(50) COMPRESS
  ,cust_current_social_media_predictions_linkedin_usage_likelihood VARCHAR(50) COMPRESS
  ,cust_current_social_media_predictions_twitter_usage_likelihood VARCHAR(50) COMPRESS
  ,cust_current_social_media_predictions_snapchat_usage_likelihood VARCHAR(50) COMPRESS
  ,cust_current_social_media_predictions_youtube_usage_likelihood VARCHAR(50) COMPRESS
  ,cust_current_retail_shopper_type VARCHAR(100) COMPRESS
  ,cust_current_household_mosaic_lifestyle_segment VARCHAR(50) COMPRESS
  ,cust_current_household_mosaic_lifestyle_group VARCHAR(50) COMPRESS
  ,cust_current_household_mosaic_lifestyle_group_summary VARCHAR(100) COMPRESS
  ,cust_current_truetouch_strategy_brand_loyalists_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_deal_seekers_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_moment_shoppers_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_mainstream_adopters_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_novelty_seekers_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_organic_and_natural_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_quality_matters_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_recreational_shoppers_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_savvy_researchers_likelihood VARCHAR(50) COMPRESS
  ,cust_current_truetouch_strategy_trendsetters_likelihood VARCHAR(50) COMPRESS
  ,cust_current_affinity_accessories INTEGER COMPRESS
  ,cust_current_affinity_active INTEGER COMPRESS
  ,cust_current_affinity_apparel INTEGER COMPRESS
  ,cust_current_affinity_beauty INTEGER COMPRESS
  ,cust_current_affinity_designer INTEGER COMPRESS
  ,cust_current_affinity_home INTEGER COMPRESS
  ,cust_current_affinity_kids INTEGER COMPRESS
  ,cust_current_affinity_mens INTEGER COMPRESS
  ,cust_current_affinity_shoes INTEGER COMPRESS
  ,cust_current_affinity_womens INTEGER COMPRESS
  ,cust_current_predicted_core_target_segment VARCHAR(16) COMPRESS
  ,cust_current_social_media_predictions_facebook_usage_score DECIMAL(3,2) COMPRESS
  ,cust_current_social_media_predictions_instagram_usage_score DECIMAL(3,2) COMPRESS
  ,cust_current_social_media_predictions_pinterest_usage_score DECIMAL(3,2) COMPRESS
  ,cust_current_social_media_predictions_linkedin_usage_score DECIMAL(3,2) COMPRESS
  ,cust_current_social_media_predictions_twitter_usage_score DECIMAL(3,2) COMPRESS
  ,cust_current_social_media_predictions_snapchat_usage_score DECIMAL(3,2) COMPRESS
  ,cust_current_social_media_predictions_youtube_usage_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_brand_loyalists_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_deal_seekers_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_moment_shoppers_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_mainstream_adopters_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_novelty_seekers_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_organic_and_natural_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_quality_matters_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_recreational_shoppers_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_savvy_researchers_score DECIMAL(3,2) COMPRESS
  ,cust_current_truetouch_strategy_trendsetters_score DECIMAL(3,2) COMPRESS
  ,cust_jwn_top500k BYTEINT
  ,cust_jwn_top1k BYTEINT
)
primary index (acp_id, reporting_year_shopped, channel);

SET QUERY_BAND = NONE FOR SESSION;