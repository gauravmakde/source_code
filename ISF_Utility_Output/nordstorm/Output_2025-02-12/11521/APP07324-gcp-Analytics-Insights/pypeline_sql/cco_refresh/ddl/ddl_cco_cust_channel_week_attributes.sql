SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=ddl_cco_cust_channel_week_attributes_11521_ACE_ENG;
Task_Name=run_teradata_ddl_cco_cust_channel_week_attributes;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************
 * CCO Customer/Channel/Week-Level Attributes DDL file.
 * This file creates the production table T2DL_DAS_STRATEGY.cco_cust_channel_week_attributes.
 ************************************************************************************/
/************************************************************************************/


--/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP(
     '{cco_t2_schema}',
     'cco_cust_channel_week_attributes',
     OUT_RETURN_MSG);
--*/


CREATE MULTISET TABLE {cco_t2_schema}.cco_cust_channel_week_attributes,--NO FALLBACK ,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    week_idnt INTEGER,
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
    cust_chan_net_sales_wkly DECIMAL(38,2) COMPRESS,
    cust_chan_net_sales_wkny DECIMAL(38,2) COMPRESS,
    cust_chan_anniversary_wkly INTEGER COMPRESS,
    cust_chan_holiday_wkly INTEGER COMPRESS,
    cust_chan_buyer_flow VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) New-to-JWN', '2) New-to-Channel (not JWN)', '3) Retained-to-Channel',
        '4) Reactivated-to-Channel'),
    cust_chan_acquired_aare BYTEINT COMPRESS (0, 1),
    cust_chan_activated_aare BYTEINT COMPRESS (0, 1),
    cust_chan_retained_aare BYTEINT COMPRESS (0, 1),
    cust_chan_engaged_aare BYTEINT COMPRESS (0, 1),
    cust_gender VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Female', 'Male', 'Unknown'),
    cust_age SMALLINT COMPRESS,
    cust_lifestage VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '01) Young Adult', '02) Early Career', '03) Mid Career', '04) Late Career',
        '05) Retired', 'Unknown'),
    cust_age_group VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '0) <18 yrs', '1) 18-24 yrs', '2) 25-34 yrs', '3) 35-44 yrs', '4) 45-54 yrs',
        '5) 55-64 yrs', '6) 65+ yrs', 'Unknown'),
    cust_nms_market VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    cust_dma VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
    cust_country VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
    cust_dma_rank VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) Top 5', '2) 6-10', '3) 11-20', '4) 21-30', '5) 31-50', '6) 51-100',
        '7) > 100', 'DMA missing'),
    cust_loyalty_type VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'a) Cardmember', 'b) Member', 'c) Non-Loyalty'),
    cust_loyalty_level VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) MEMBER', '2) INFLUENCER', '3) AMBASSADOR', '4) ICON'),
    cust_loy_member_enroll_dt DATE COMPRESS,
    cust_loy_cardmember_enroll_dt DATE COMPRESS,
    cust_acquired_this_year BYTEINT COMPRESS (0, 1),
    cust_acquisition_date DATE COMPRESS,
    cust_acquisition_fiscal_year SMALLINT COMPRESS,
    cust_acquisition_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) Nordstrom Stores', '2) Nordstrom.com', '3) Rack Stores', '4) Rack.com'),
    cust_acquisition_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'NORDSTROM', 'RACK'),
    cust_acquisition_brand VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
    cust_tenure_bucket_months VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) 0-3 months', '2) 4-6 months', '3) 7-12 months', '4) 13-24 months',
        '5) 25-36 months', '6) 37-48 months', '7) 49-60 months', '8) 61+ months',
        'Unknown'),
    cust_tenure_bucket_years VARCHAR(13) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) <= 1 year', '2) 1-2 years', '3) 2-5 years', '4) 5-10 years', '5) 10+ years',
        'Unknown'),
    cust_activation_date DATE COMPRESS,
    cust_activation_channel VARCHAR(19) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) Nordstrom Stores', '2) Nordstrom.com', '3) Rack Stores', '4) Rack.com'),
    cust_activation_banner VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'NORDSTROM', 'RACK'),
    cust_engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        'Acquire & Activate', 'Highly-Engaged', 'Lightly-Engaged', 'Moderately-Engaged'),
    cust_channel_count INTEGER COMPRESS (1, 2, 3, 4),
    cust_channel_combo VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '01) NordStore-only', '02) N.com-only', '03) RackStore-only', '04) Rack.com-only',
        '05) NordStore+N.com', '06) NordStore+RackStore', '07) NordStore+Rack.com',
        '08) N.com+RackStore', '09) N.com+Rack.com', '10) RackStore+Rack.com',
        '11) NordStore+N.com+RackStore', '12) NordStore+N.com+Rack.com',
        '13) NordStore+RackStore+Rack.com', '14) N.com+RackStore+Rack.com',
        '15) 4-Box', '99) Error'),
    cust_banner_count INTEGER COMPRESS (1, 2),
    cust_banner_combo VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS (
        '1) Nordstrom-only', '2) Rack-only', '3) Dual-Banner', '99) Error'),
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
    cust_jwn_net_sales_shoes DECIMAL(38,2),
    cust_jwn_net_sales_beauty DECIMAL(38,2),
    cust_jwn_net_sales_designer DECIMAL(38,2),
    cust_jwn_net_sales_accessories DECIMAL(38,2),
    cust_jwn_net_sales_home DECIMAL(38,2),
    cust_jwn_net_sales_merch_projects DECIMAL(38,2),
    cust_jwn_net_sales_leased_boutiques DECIMAL(38,2),
    cust_jwn_net_sales_other_non_merch DECIMAL(38,2),
    cust_jwn_net_sales_restaurant DECIMAL(38,2),
    cust_jwn_transaction_apparel_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_shoes_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_beauty_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_designer_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_accessories_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_home_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_merch_projects_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_leased_boutiques_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_other_non_merch_ind INTEGER COMPRESS (0, 1),
    cust_jwn_transaction_restaurant_ind INTEGER COMPRESS (0, 1),
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
    cust_clv_jwn DECIMAL(14,4) COMPRESS,
    cust_clv_fp DECIMAL(14,4) COMPRESS,
    cust_clv_op DECIMAL(14,4) COMPRESS,
    cust_jwn_top500k BYTEINT COMPRESS (0, 1),
    cust_jwn_top1k BYTEINT COMPRESS (0, 1),
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX (acp_id, week_idnt, channel)
PARTITION BY RANGE_N(week_idnt BETWEEN 201601 AND 201652 EACH 1,
                                       201701 AND 201753 EACH 1,
                                       201801 AND 201852 EACH 1,
                                       201901 AND 201952 EACH 1,
                                       202001 AND 202052 EACH 1,
                                       202101 AND 202153 EACH 1,
                                       202201 AND 202253 EACH 1,
                                       202301 AND 202353 EACH 1,
                                       202401 AND 202453 EACH 1,
                                       202501 AND 202553 EACH 1,
                                       202601 AND 202653 EACH 1,
                                       202701 AND 202753 EACH 1,
                                       202801 AND 202853 EACH 1,
                                       202901 AND 202953 EACH 1,
                                       203001 AND 203053 EACH 1);


COLLECT STATISTICS COLUMN (acp_id) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (week_idnt) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (channel) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_gender) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_age) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_lifestage) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_age_group) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_nms_market) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_dma) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_country) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_dma_rank) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_loyalty_type) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_loyalty_level) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (cust_employee_flag) ON {cco_t2_schema}.cco_cust_channel_week_attributes;
COLLECT STATISTICS COLUMN (acp_id, week_idnt, channel) ON {cco_t2_schema}.cco_cust_channel_week_attributes;


SET QUERY_BAND = NONE FOR SESSION;
