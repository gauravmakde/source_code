SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=ddl_cco_customer_level_attributes_fy_11521_ACE_ENG;
     Task_Name=ddl_cco_customer_level_attributes_fy;'
     FOR SESSION VOLATILE;




/*
CCO Customer Level Attributes  DDL file   
This file creates the production table T2DL_DAS_STRATEGY.cco_customer_level_attributes_fy
*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'cco_customer_level_attributes_fy', OUT_RETURN_MSG);

create MULTISET table {cco_t2_schema}.cco_customer_level_attributes_fy,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
   acp_id VARCHAR(50)  character set unicode not casespecific  
  ,fiscal_year_shopped VARCHAR(10) character set unicode not casespecific  
  ,cust_gender VARCHAR(7) character set unicode not casespecific compress
  ,cust_age INTEGER
  ,cust_lifestage VARCHAR(16) character set unicode not casespecific compress
  ,cust_age_group VARCHAR(13) character set unicode not casespecific compress
  ,cust_NMS_market VARCHAR(20) character set unicode not casespecific compress
  ,cust_dma VARCHAR(55) character set unicode not casespecific compress
  ,cust_country VARCHAR(2) character set unicode not casespecific compress
  ,cust_dma_rank VARCHAR(11)character set unicode not casespecific compress
  ,cust_loyalty_type VARCHAR(14)character set unicode not casespecific compress
  ,cust_loyalty_level VARCHAR(13)character set unicode not casespecific compress
  ,cust_loy_member_enroll_dt DATE
  ,cust_loy_cardmember_enroll_dt DATE
  --,cust_contr_margin_decile INTEGER
  --,cust_contr_margin_amt DECIMAL(38,2)
  ,cust_acquisition_date DATE
  ,cust_acquisition_fiscal_year SMALLINT
  ,cust_acquisition_channel VARCHAR(19)character set unicode not casespecific compress
  ,cust_acquisition_banner VARCHAR(9)character set unicode not casespecific compress
  ,cust_acquisition_brand VARCHAR(40)character set unicode not casespecific compress
  ,cust_tenure_bucket_months VARCHAR(15)character set unicode not casespecific compress
  ,cust_tenure_bucket_years VARCHAR(13)character set unicode not casespecific compress
  ,cust_activation_date DATE
  ,cust_activation_channel VARCHAR(19)character set unicode not casespecific compress
  ,cust_activation_banner VARCHAR(9)character set unicode not casespecific compress
  ,cust_engagement_cohort VARCHAR(30)character set unicode not casespecific compress
  ,cust_channel_count INTEGER
  ,cust_channel_combo VARCHAR(32)character set unicode not casespecific compress
  ,cust_banner_count INTEGER
  ,cust_banner_combo VARCHAR(17)character set unicode not casespecific compress
  ,cust_employee_flag INTEGER
  ,cust_jwn_trip_bucket VARCHAR(9)character set unicode not casespecific compress
  ,cust_jwn_net_spend_bucket VARCHAR(11)character set unicode not casespecific compress
  ,cust_jwn_gross_sales DECIMAL(38,2)
  ,cust_jwn_return_amt DECIMAL(38,2)
  ,cust_jwn_net_sales DECIMAL(38,2)
  ,cust_jwn_net_sales_apparel DECIMAL(38,2)
  ,cust_jwn_trips INTEGER
  ,cust_jwn_gross_items INTEGER
  ,cust_jwn_return_items INTEGER
  ,cust_jwn_net_items INTEGER
  ,cust_tender_nordstrom INTEGER
  ,cust_tender_nordstrom_note INTEGER
  ,cust_tender_3rd_party_credit INTEGER
  ,cust_tender_debit_card INTEGER
  ,cust_tender_gift_card INTEGER
  ,cust_tender_cash INTEGER
  ,cust_tender_paypal INTEGER
  ,cust_tender_check INTEGER
  --,cust_event_ctr INTEGER
  ,cust_event_holiday INTEGER
  ,cust_event_anniversary INTEGER
  ,cust_svc_group_exp_delivery INTEGER
  ,cust_svc_group_order_pickup INTEGER
  ,cust_svc_group_selling_relation INTEGER
  ,cust_svc_group_remote_selling INTEGER
  ,cust_svc_group_alterations INTEGER
  ,cust_svc_group_in_store INTEGER
  ,cust_svc_group_restaurant INTEGER
  ,cust_service_free_exp_delivery INTEGER
  ,cust_service_next_day_pickup INTEGER
  ,cust_service_same_day_bopus INTEGER
  ,cust_service_curbside_pickup INTEGER
  ,cust_service_style_boards INTEGER
  ,cust_service_gift_wrapping INTEGER
  ,cust_service_pop_in INTEGER
  ,cust_platform_desktop INTEGER
  ,cust_platform_MOW INTEGER
  ,cust_platform_IOS INTEGER
  ,cust_platform_Android INTEGER
  ,cust_platform_POS INTEGER
  ,cust_anchor_brand BYTEINT
  ,cust_strategic_brand BYTEINT
  ,cust_store_customer BYTEINT
  ,cust_digital_customer BYTEINT
  ,cust_clv_jwn DECIMAL(14,4) COMPRESS
  ,cust_clv_fp DECIMAL(14,4) COMPRESS
  ,cust_clv_op DECIMAL(14,4) COMPRESS
)
primary index (acp_id, fiscal_year_shopped) ;

SET QUERY_BAND = NONE FOR SESSION;