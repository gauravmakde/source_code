SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=ddl_customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=run_teradata_ddl_customer_sandbox_fact;'
FOR SESSION VOLATILE;


--DROP TABLE T2DL_DAS_STRATEGY.customer_sandbox_fact;

/*CREATE MULTISET TABLE T2DL_DAS_STRATEGY.customer_sandbox_fact ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      data_source VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('1) TRANSACTION', '2) NSEAM'),
      acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_banner_num INTEGER COMPRESS (1, 2),
      store_channel_num INTEGER COMPRESS (1, 2, 3, 4),
      store_channel_country INTEGER,
      store_num INTEGER,
      store_region_num INTEGER,
      div_num INTEGER,
      subdiv_num INTEGER,
      dept_num INTEGER,
      npg_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      date_shopped DATE FORMAT 'YY/MM/DD',
      month_num INTEGER,
      quarter_num INTEGER,
      year_num INTEGER,
      trip_id VARCHAR(69) CHARACTER SET UNICODE NOT CASESPECIFIC,
      gross_sales DECIMAL(38,2),
      return_amt DECIMAL(38,2),
      net_sales DECIMAL(38,2),
      gross_items INTEGER,
      return_items INTEGER,
      net_items INTEGER,
      RegPrice_net_spend DECIMAL(38,2),
      Promo_net_spend DECIMAL(38,2),
      Clearance_net_spend DECIMAL(38,2),
      RegPrice_gross_spend DECIMAL(38,2),
      Promo_gross_spend DECIMAL(38,2),
      Clearance_gross_spend DECIMAL(38,2),
      RegPrice_return_amt DECIMAL(38,2),
      Promo_return_amt DECIMAL(38,2),
      Clearance_return_amt DECIMAL(38,2),
      RegPrice_net_items INTEGER,
      Promo_net_items INTEGER,
      Clearance_net_items INTEGER,
      item_delivery_method_num INTEGER,
      employee_flag INTEGER,
      reporting_year_shopped_num INTEGER,
      audience_engagement_cohort_num INTEGER,
      cust_jwn_trip_bucket_num INTEGER,
      cust_jwn_net_spend_bucket_num INTEGER,
      cust_jwn_net_spend_bucket_ly_num INTEGER,
      cust_jwn_trip_bucket_ly_num INTEGER,
      cust_jwn_net_spend_bucket_fy_num INTEGER,
      cust_jwn_trip_bucket_num_fy_num INTEGER,
      audience_engagement_cohort_ly_num INTEGER,
      audience_engagement_cohort_fy_num INTEGER,
      cust_rack_trip_bucket_num INTEGER,
      cust_rack_net_spend_bucket_num INTEGER,
      cust_nord_trip_bucket_num INTEGER,
      cust_nord_net_spend_bucket_num INTEGER,
      cust_channel_combo_num INTEGER,
      cust_loyalty_type_num INTEGER,
      cust_loyalty_level_num INTEGER,
      cust_gender VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cust_age_group_num INTEGER,
      cust_employee_flag INTEGER,
      cust_tenure_bucket_years_num INTEGER,
      cust_acquisition_fiscal_year SMALLINT,
      cust_acquisition_channel_num INTEGER,
      cust_activation_channel_num INTEGER,
      cust_nms_market_num INTEGER,
      cust_nms_region_num INTEGER,
      cust_dma_num INTEGER,
      cust_region_num INTEGER,
      cust_country_num INTEGER,
      cust_tender_nordstrom INTEGER COMPRESS (0, 1),
      cust_tender_nordstrom_note INTEGER COMPRESS (0, 1),
      cust_tender_gift_card INTEGER COMPRESS (0, 1),
      cust_tender_other BYTEINT COMPRESS (0, 1),
      cust_had_a_cancel BYTEINT,
      cust_svc_group_exp_delivery INTEGER,
      cust_svc_group_order_pickup INTEGER,
      cust_svc_group_selling_relation INTEGER,
      cust_svc_group_remote_selling INTEGER,
      cust_svc_group_alterations INTEGER,
      cust_svc_group_in_store INTEGER,
      cust_svc_group_restaurant INTEGER,
      cust_service_free_exp_delivery INTEGER,
      cust_service_next_day_pickup INTEGER,
      cust_service_same_day_bopus INTEGER,
      cust_service_curbside_pickup INTEGER,
      cust_service_style_boards INTEGER,
      cust_service_gift_wrapping INTEGER,
      cust_styling INTEGER,
      cust_service_pop_in INTEGER COMPRESS (0, 1),
      cust_ns_buyerflow_num INTEGER,
      cust_ncom_buyerflow_num INTEGER,
      cust_rs_buyerflow_num INTEGER,
      cust_rcom_buyerflow_num INTEGER,
      cust_nord_buyerflow_num INTEGER,
      cust_rack_buyerflow_num INTEGER,
      cust_jwn_buyerflow_num INTEGER,
      cust_jwn_acquired_aare INTEGER,
      cust_jwn_activated_aare INTEGER,
      cust_jwn_retained_aare INTEGER,
      cust_jwn_engaged_aare INTEGER,
      shopped_fls BYTEINT,
      shopped_ncom BYTEINT,
      shopped_rack BYTEINT,
      shopped_rcom BYTEINT,
      shopped_fp BYTEINT,
      shopped_op BYTEINT,
      shopped_jwn BYTEINT,
      cust_made_a_return BYTEINT COMPRESS (0, 1),
      cust_platform_desktop INTEGER COMPRESS (0, 1),
      cust_platform_MOW INTEGER COMPRESS (0, 1),
      cust_platform_IOS INTEGER COMPRESS (0, 1),
      cust_platform_Android INTEGER COMPRESS (0, 1),
      cust_platform_POS INTEGER COMPRESS (0, 1),
      cust_service_ship2store_ncom BYTEINT,
      cust_service_ship2store_rcom BYTEINT,
      restaurant_only_flag BYTEINT COMPRESS (0, 1),
      closest_sol_dist_bucket_num INTEGER,
      closest_store_banner_num INTEGER COMPRESS (1, 2),
      nord_sol_dist_bucket_num INTEGER,
      rack_sol_dist_bucket_num INTEGER,
      shopped_nord_local BYTEINT COMPRESS (0, 1),
      ncom_ret_to_nstore BYTEINT COMPRESS (0, 1),
      ncom_ret_by_mail BYTEINT COMPRESS (0, 1),
      ncom_ret_to_rstore BYTEINT COMPRESS (0, 1),
      rcom_ret_to_nstore BYTEINT COMPRESS (0, 1),
      rcom_ret_by_mail BYTEINT COMPRESS (0, 1),
      rcom_ret_to_rstore BYTEINT COMPRESS (0, 1),
      cust_nord_Shoes_net_spend_ry DECIMAL(38,2),
      cust_nord_Beauty_net_spend_ry DECIMAL(38,2),
      cust_nord_Designer_Apparel_net_spend_ry DECIMAL(38,2),
      cust_nord_Apparel_net_spend_ry DECIMAL(38,2),
      cust_nord_Accessories_net_spend_ry DECIMAL(38,2),
      cust_nord_Home_net_spend_ry DECIMAL(38,2),
      cust_nord_Merch_Projects_net_spend_ry DECIMAL(38,2),
      cust_nord_Leased_Boutique_net_spend_ry DECIMAL(38,2),
      cust_nord_Restaurant_net_spend_ry DECIMAL(38,2),
      cust_rack_Shoes_net_spend_ry DECIMAL(38,2),
      cust_rack_Beauty_net_spend_ry DECIMAL(38,2),
      cust_rack_Designer_Apparel_net_spend_ry DECIMAL(38,2),
      cust_rack_Apparel_net_spend_ry DECIMAL(38,2),
      cust_rack_Accessories_net_spend_ry DECIMAL(38,2),
      cust_rack_Home_net_spend_ry DECIMAL(38,2),
      cust_rack_Merch_Projects_net_spend_ry DECIMAL(38,2),
      cust_Rack_Leased_Boutique_net_spend_ry DECIMAL(38,2),
      cust_Rack_Restaurant_net_spend_ry DECIMAL(38,2),
      cust_nord_RegPrice_net_spend_ry DECIMAL(38,2),
      cust_nord_Promo_net_spend_ry DECIMAL(38,2),
      cust_nord_Clearance_net_spend_ry DECIMAL(38,2),
      cust_rack_RegPrice_net_spend_ry DECIMAL(38,2),
      cust_rack_Promo_net_spend_ry DECIMAL(38,2),
      cust_rack_Clearance_net_spend_ry DECIMAL(38,2),
      cust_event_holiday INTEGER COMPRESS (0, 1),
      cust_event_anniversary INTEGER COMPRESS (0, 1),
      cust_contr_margin_decile INTEGER,
      cust_contr_margin_amt DECIMAL(14,4),
      dw_sys_load_tmstp TIMESTAMP(6),
      dw_sys_updt_tmstp TIMESTAMP(6))
PRIMARY INDEX (
      acp_id,
      store_num,
      div_num,
      subdiv_num,
      dept_num,
      date_shopped,
      trip_id,
      item_delivery_method_num,
      employee_flag,
      reporting_year_shopped_num,
      audience_engagement_cohort_num,
      cust_jwn_trip_bucket_num,
      cust_jwn_net_spend_bucket_num,
      cust_rack_trip_bucket_num,
      cust_rack_net_spend_bucket_num,
      cust_nord_trip_bucket_num,
      cust_nord_net_spend_bucket_num,
      cust_channel_combo_num,
      cust_loyalty_type_num,
      cust_loyalty_level_num,
      cust_gender,
      cust_age_group_num,
      cust_tenure_bucket_years_num,
      cust_acquisition_fiscal_year,
      cust_acquisition_channel_num,
      cust_activation_channel_num,
      cust_nms_market_num,
      cust_dma_num,
      cust_had_a_cancel,
      cust_svc_group_exp_delivery,
      cust_svc_group_order_pickup,
      cust_svc_group_selling_relation,
      cust_svc_group_remote_selling,
      cust_svc_group_alterations,
      cust_svc_group_in_store,
      cust_svc_group_restaurant,
      cust_service_free_exp_delivery,
      cust_service_next_day_pickup,
      cust_service_same_day_bopus,
      cust_service_curbside_pickup,
      cust_service_style_boards,
      cust_service_gift_wrapping,
      cust_styling,
      cust_ns_buyerflow_num,
      cust_ncom_buyerflow_num,
      cust_rs_buyerflow_num,
      cust_rcom_buyerflow_num,
      cust_nord_buyerflow_num,
      cust_rack_buyerflow_num,
      cust_jwn_buyerflow_num,
      cust_jwn_acquired_aare,
      cust_jwn_activated_aare,
      cust_jwn_retained_aare,
      cust_jwn_engaged_aare,
      shopped_fls,
      shopped_ncom,
      shopped_rack,
      shopped_rcom,
      shopped_fp,
      shopped_op,
      shopped_jwn,
      cust_service_ship2store_ncom,
      cust_service_ship2store_rcom,
      closest_sol_dist_bucket_num)
PARTITION BY RANGE_N(date_shopped BETWEEN DATE '2019-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY);
*/

COLLECT STATISTICS COLUMN(date_shopped) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(acp_id) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(store_banner_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(store_channel_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(store_channel_country) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(store_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(div_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(subdiv_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(dept_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(trip_id) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(item_delivery_method_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(employee_flag) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(reporting_year_shopped_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(audience_engagement_cohort_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_jwn_trip_bucket_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_jwn_net_spend_bucket_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_rack_trip_bucket_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_rack_net_spend_bucket_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_nord_trip_bucket_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_nord_net_spend_bucket_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_channel_combo_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_loyalty_type_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_loyalty_level_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_gender) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_age_group_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_employee_flag) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_tenure_bucket_years_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_acquisition_fiscal_year) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_acquisition_channel_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_activation_channel_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_nms_market_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_dma_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_had_a_cancel) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_svc_group_exp_delivery) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_svc_group_order_pickup) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_svc_group_selling_relation) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_svc_group_remote_selling) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_svc_group_alterations) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_svc_group_in_store) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_svc_group_restaurant) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_free_exp_delivery) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_next_day_pickup) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_same_day_bopus) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_curbside_pickup) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_style_boards) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_gift_wrapping) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_styling) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_ns_buyerflow_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_ncom_buyerflow_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_rs_buyerflow_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_rcom_buyerflow_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_nord_buyerflow_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_rack_buyerflow_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_jwn_buyerflow_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_jwn_acquired_aare) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_jwn_activated_aare) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_jwn_retained_aare) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_jwn_engaged_aare) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(shopped_fls) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(shopped_ncom) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(shopped_rack) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(shopped_rcom) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(shopped_fp) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(shopped_op) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(shopped_jwn) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_ship2store_ncom) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_service_ship2store_rcom) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(closest_sol_dist_bucket_num) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_event_holiday) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;
COLLECT STATISTICS COLUMN(cust_event_anniversary) ON T2DL_DAS_STRATEGY.customer_sandbox_fact;


SET QUERY_BAND = NONE FOR SESSION;
