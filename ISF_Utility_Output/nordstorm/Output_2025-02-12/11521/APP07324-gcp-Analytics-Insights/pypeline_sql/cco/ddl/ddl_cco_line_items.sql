SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=ddl_cco_line_items_11521_ACE_ENG;
     Task_Name=ddl_cco_line_items;'
     FOR SESSION VOLATILE;

/*
CCO Strategy Line Item DDL file   
This file creates the production table T2DL_DAS_STRATEGY.cco_strategy_line_item
*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'cco_line_items', OUT_RETURN_MSG);

create MULTISET table {cco_t2_schema}.cco_line_items,--NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
    global_tran_id BIGINT
    ,line_item_seq_num SMALLINT
    ,data_source varchar(20) CHARACTER SET UNICODE COMPRESS ('1) TRANSACTION','2) NSEAM')
    ,order_num VARCHAR(32) CHARACTER SET UNICODE compress
    ,acp_id VARCHAR(50) CHARACTER SET UNICODE compress
    ,banner VARCHAR(9) CHARACTER SET UNICODE compress ('NORDSTROM', 'RACK')
    ,channel VARCHAR(19) CHARACTER SET UNICODE compress ( '1) Nordstrom Stores', '2) Nordstrom.com','3) Rack Stores', '4) Rack.com')
    ,channel_country VARCHAR(2) CHARACTER SET UNICODE compress ('US', 'CA')
    ,store_num INTEGER compress
    ,store_region VARCHAR(30) CHARACTER SET UNICODE compress ('FULL LINE CANADA','MIDWEST FLS','MIDWEST RACKS','N.CA','N.COM', 'NORTHEAST FLS','NORTHEAST RACKS','NORTHWEST FLS', 'NORTHWEST RACKS','OFFPRICE ONLINE','RACK CANADA','SCAL RACKS','SOUTHEAST FLS','SOUTHEAST RACKS','SOUTHERN CALIFORNIA FLS','SOUTHWEST FLS','SOUTHWEST RACKS')
    ,div_num INTEGER compress
    ,div_desc VARCHAR(30) CHARACTER SET UNICODE compress ('ACCESSORIES','APPAREL','BEAUTY','DESIGNER','HOME','LEASED BOUTIQUES','MERCH PROJECTS','OTHER/NON-MERCH','RESTAURANT','SHOES')
    ,subdiv_num INTEGER compress
    ,subdiv_desc VARCHAR(30) CHARACTER SET UNICODE compress ('ACCESSORIES','BAG FEES','BEAUTY SERVICES','CREATIVE PROJECTS','DEPOSITS/SERVICES','FRAGRANCE','HOME','KIDS APPAREL','KIDS DESIGNER','KIDS SHOES','LEASED BOUTIQUES','MAKEUP&SKINCARE','MENS APPAREL','MENS DESIGNER','MENS SHOES','MENS SPECIALIZED','MERCH PROJECTS','NEW CONCEPTS','NII NON INVENTORY ITEMS','RESTAURANT','WMNS DESIGNER','WOMENS APPAREL','WOMENS SHOES','WOMENS SPECIALIZED')
    ,dept_num INTEGER compress
    ,dept_desc VARCHAR(50) CHARACTER SET UNICODE compress
     ('WMNS BETTER LC','YOUNG WOMENS SHOES','SPECIALTY COFFEE BAR','LINGERIE','WOMENS BETTER SHOES','DRESSES','YOUNG WOMENS APPAREL',
      'WMNS ACTIVEWEAR','MAKEUP','ADVANCED TECHNOLOGY/TOOLS','WMNS BETTER MC','JEWELRY','CAFE','MENS BETTER SPORTSWEAR','WMNS BETTER EC',
      'HDBG/SLG/LUG','CONTEMPORARY 1','WOMENS ACTIVE SHOES','ACCESSORIES','SKINCARE','WMNS FRAGRANCE','MENS ACTIVEWEAR','MENS DRESSWEAR',
      'YOUNG MENS APPAREL','BIG GIRL','BEAUTY GIFT','SLEEPWEAR','WMNS SWIM WEAR','NON ACTIVE KIDS SHOES','MENS SPECIALIZED',
      'DESIGNER','WMNS DENIM','ACTIVE KIDS SHOES','MENS BETTER SHOES','EYEWEAR','HOSIERY','MENS ACTIVE SHOES','MENS DENIM','BIG BOY' )
    ,brand_name VARCHAR(50) CHARACTER SET UNICODE compress
    ,npg_flag VARCHAR(1) CHARACTER SET UNICODE compress ('Y','N')
    ,price_type VARCHAR(1) CHARACTER SET UNICODE compress ('R','C','P')
    ,store_country VARCHAR(2) CHARACTER SET UNICODE compress ('US','CA')
    ,ntn_tran INTEGER compress
    ,date_shopped DATE compress
    -- ,month_shopped INTEGER compress
    -- ,fiscal_qtr_shopped INTEGER compress
    -- ,fiscal_yr_shopped INTEGER compress
    ,fiscal_month_num INTEGER compress --renamed from month_shopped
    ,fiscal_qtr_num INTEGER compress --renamed from fiscal_qtr_shopped
    ,fiscal_yr_num INTEGER compress --renamed from fiscal_yr_shopped
    ,fiscal_year_shopped VARCHAR(10) CHARACTER SET UNICODE compress --new column  
    ,reporting_year_shopped VARCHAR(10) CHARACTER SET UNICODE compress  
    ,item_price_band VARCHAR(11) CHARACTER SET UNICODE compress
    ,gross_sales DECIMAL(12,2) compress
    ,gross_incl_gc DECIMAL(12,2) compress
    ,return_amt DECIMAL(12,2) compress 0.00
    ,net_sales DECIMAL(13,2) compress 0.00
    ,gross_items INTEGER compress
    ,return_items INTEGER compress
    ,net_items INTEGER compress
    ,return_date DATE compress
    ,days_to_return INTEGER compress
    ,return_store INTEGER compress
    ,return_banner VARCHAR(9) CHARACTER SET UNICODE compress ('NORDSTROM', 'RACK')
    ,return_channel VARCHAR(19) CHARACTER SET UNICODE compress ( '1) Nordstrom Stores', '2) Nordstrom.com','3) Rack Stores', '4) Rack.com')
    ,item_delivery_method VARCHAR(40) CHARACTER SET UNICODE compress ('FC_BOPUS','FREE_2DAY_DELIVERY','FREE_2DAY_SHIP_TO_STORE','NEXT_DAY_SHIP_TO_STORE','PAID_EXPEDITED_SHIP_TO_HOME','PAID_EXPEDITED_SHIP_TO_STORE','SAME_DAY_BOPUS','SAME_DAY_DELIVERY','SHIP_TO_STORE')
    ,pickup_store INTEGER compress
    ,pickup_date DATE compress
    ,pickup_channel VARCHAR(19) CHARACTER SET UNICODE COMPRESS ( '1) Nordstrom Stores', '2) Nordstrom.com','3) Rack Stores', '4) Rack.com')
    ,pickup_banner VARCHAR(9) CHARACTER SET UNICODE compress ('NORDSTROM', 'RACK')
    ,platform VARCHAR(40) CHARACTER SET UNICODE compress ('WEB','IOS','MOW','POS','ANDROID','CSR_PHONE','THIRD_PARTY_VENDOR')
    ,employee_flag INTEGER compress
    ,tender_nordstrom INTEGER compress   (0,1)
    ,tender_nordstrom_note INTEGER compress  (0,1)
    ,tender_3rd_party_credit INTEGER compress (0,1)
    ,tender_debit_card INTEGER compress (0,1)
    ,tender_gift_card INTEGER compress (0,1)
    ,tender_cash INTEGER compress (0,1)
    ,tender_paypal INTEGER compress (0,1)
    ,tender_check INTEGER compress (0,1)
    --,event_ctr INTEGER  compress (0,1)
    ,event_holiday INTEGER compress (0,1)
    ,event_anniversary INTEGER  compress (0,1)
    ,svc_group_exp_delivery INTEGER compress (0,1)
    ,svc_group_order_pickup INTEGER  compress (0,1)
    ,svc_group_selling_relation INTEGER compress (0,1)
    ,svc_group_remote_selling INTEGER compress (0,1)
    ,svc_group_alterations INTEGER compress (0,1)
    ,svc_group_in_store INTEGER compress (0,1)
    ,svc_group_restaurant INTEGER compress (0,1)
    ,service_free_exp_delivery INTEGER compress (0,1)
    ,service_next_day_pickup INTEGER compress (0,1)
    ,service_same_day_bopus INTEGER compress (0,1)
    ,service_curbside_pickup INTEGER compress (0,1)
    ,service_style_boards INTEGER compress (0,1)
    ,service_gift_wrapping INTEGER compress (0,1)
    ,service_pop_in INTEGER compress (0,1)
)
primary index (global_tran_id, line_item_seq_num);


SET QUERY_BAND = NONE FOR SESSION;