CREATE MULTISET TABLE PRD_NAP_DSA_AI_FCT.FINANCE_SALES_DEMAND_FACT ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      row_id BIGINT GENERATED ALWAYS AS IDENTITY
           (START WITH 1 
            INCREMENT BY 1 
            MINVALUE -999999999999999999 
            MAXVALUE 999999999999999999 
            NO CYCLE),
      tran_date DATE FORMAT 'YYYY-MM-DD' NOT NULL,
      business_unit_desc VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('FULL LINE','RACK','OFFPRICE ONLINE','N.COM','N.CA','FULL LINE CANADA','RACK CANADA'),
      store_num INTEGER,
      bill_zip_code VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      line_item_currency_code VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      inventory_business_model VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'NOT_APPLICABLE' COMPRESS ('Wholesale','Wholesession','Drop ship','Concession','NOT_APPLICABLE','UNKNOWN_VALUE'),
      division_name VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('APPAREL','ACCESSORIES','BEAUTY','SHOES','HOME','DESIGNER','MERCH PROJECTS','LAST CHANCE','ALTERNATE MODELS','LEASED','LEASED BOUTIQUES','OTHER','OTHER/NON-MERCH','RESTAURA
NT'),
      subdivision_name VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('WOMENS APPAREL','WOMENS SPECIALIZED','WOMENS SHOES','MENS APPAREL','KIDS APPAREL','MENS SPECIALIZED','MENS SHOES','KIDS SHOES'),
      price_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'NOT_APPLICABLE' COMPRESS ('Regular Price','Clearance','Promotion','NOT_APPLICABLE','UNKNOWN_VALUE'),
      record_source VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('O','S','C','W','G'),
      platform_code VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('Desktop/Tablet','Mobile Web','Android','IOS','Store POS','Direct to Customer (DTC)','NOT_APPLICABLE','Other'),
      remote_selling_ind VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('Remote Selling','Self-Service'),
      fulfilled_from_location_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'NOT_APPLICABLE' COMPRESS ('DC','FC','Store','Vendor (Drop Ship)','Trunk Club','UNKNOWN_VALUE','NOT_APPLICABLE'),
      delivery_method VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'NOT_APPLICABLE' COMPRESS ('BOPUS','Ship to Store','Store Take','Ship to Customer/Other','NOT_APPLICABLE','StorePickup','Charge Send'),
      delivery_method_subtype VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'NOT_APPLICABLE' COMPRESS ('Charge send','Free 2 Day (F2DD)','Next Day Delivery','Next Day Pickup','NOT_APPLICABLE','Other','Paid Expedited','Same Day Pickup','Stand
ard Shipping','Store Take'),
      fulfillment_journey VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Charge send','Free 2 Day (F2DD)','Next Day Delivery','Next Day Pickup','NOT_APPLICABLE','Other','Paid Expedited','Same Day Pickup','Standard Shipping','Store Take'),
      loyalty_status VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT 'NOT_APPLICABLE' COMPRESS ('MEMBER','INSIDER','AMBASSADOR','INFLUENCER','ICON','UNKNOWN_VALUE','NOT_APPLICABLE'),
      buyerflow_code VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('1) New-to-JWN','2) New-to-Channel (not JWN)','3) Retained-to-Channel','4) Reactivated-to-Channel'),
      AARE_acquired_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      AARE_activated_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      AARE_retained_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      AARE_engaged_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Acquire & Activate','Acquired Mid-Qtr','Highly-Engaged','Lightly-Engaged','Moderately-Engaged'),
      mrtk_chnl_type_code VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('BASE','PAID','UNATTRIBUTED','UNPAID'),
      mrtk_chnl_finance_rollup_code VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('AFFILIATES','BASE','DISPLAY','EMAIL','NOT_APPLICABLE','PAID_OTHER','PAID_SEARCH','SEO','SHOPPING','SOCIAL','UNATTRIBUTED','UNPAID_OTHER'),
      mrtk_chnl_finance_detail_code VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('AFFILIATES','BASE','DISPLAY','EMAIL_TRANSACT','EMAIL_MARKETING','APP_PUSH_PLANNED','EMAIL_TRIGGER','APP_PUSH_TRANSACTIONAL','VIDEO','PAID_SEARCH_BRANDED','P
AID_SEARCH_UNBRANDED','SEO_SHOPPING','SEO_LOCAL','SEO_SEARCH','SHOPPING','SOCIAL_PAID','UNATTRIBUTED','SOCIAL_ORGANIC'),
      tran_tender_cash_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      tran_tender_check_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      tran_tender_nordstrom_card_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      tran_tender_non_nordstrom_credit_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      tran_tender_non_nordstrom_debit_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      tran_tender_nordstrom_gift_card_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      tran_tender_nordstrom_note_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      tran_tender_paypal_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC DEFAULT NULL  COMPRESS ('N','Y'),
      gross_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      gross_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      canceled_gross_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      canceled_gross_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      reported_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      reported_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      canceled_reported_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      canceled_reported_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      emp_disc_gross_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      emp_disc_gross_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      emp_disc_reported_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      emp_disc_reported_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      emp_disc_fulfilled_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      emp_disc_fulfilled_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      emp_disc_op_gmv_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      emp_disc_op_gmv_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      fulfilled_demand_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      fulfilled_demand_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      post_fulfill_price_adj_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      same_day_store_return_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      same_day_store_return_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      last_chance_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      last_chance_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      actual_product_returns_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      actual_product_returns_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      op_gmv_usd_amt DECIMAL(20,2) DEFAULT 0.00  COMPRESS 0.00 ,
      op_gmv_units INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      orders_count INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      canceled_fraud_orders_count INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      emp_disc_orders_count INTEGER DEFAULT 0  COMPRESS (0 ,1 ),
      dw_sys_load_tmstp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))
PRIMARY INDEX ( row_id )
PARTITION BY RANGE_N(tran_date  BETWEEN DATE '2021-01-31' AND DATE '2026-02-03' EACH INTERVAL '1' DAY ,
 UNKNOWN);

TABLE COMMENT
=============
?
COLUMN COMMENTS
===============
?
