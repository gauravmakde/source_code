SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_gifting_order_summary_11521_ACE_ENG;
     Task_Name=ddl_gifting_order_summary;'
     FOR SESSION VOLATILE;


CREATE MULTISET TABLE {gifting_digital_exp_t2_schema}.gifting_order_summary
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
      --Dimensions
       order_date_pacific           date
     , order_num                    varchar(40) character set unicode
     , source_platform_code         varchar(16) compress('ANDROID', 'BACKEND_SERVICE', 'CSR_PHONE', 'IOS', 'MOW', 'POS', 'UNKNOWN', 'WEB')
     , source_channel_country_code  char(2) compress('US', 'CA')
     , source_channel_code          varchar(10) compress('RACK', 'FULL_LINE')
     , ship_type                    varchar(13) compress('SHIP_TO_HOME', 'BOPUS', 'SHIP_TO_STORE')
     , destination_node_num         integer compress
     , gift_option_type             varchar(20) compress('GIFT_MESSAGE', 'GIFT_BOX', 'GIFT_KIT', 'GIFT_WRAP')--, 'EMAIL_GIFT_MESSAGE') --doesn't exist yet, but will in the future for rack
     --Measures
     , order_items                  integer compress
     , shipped_items                integer compress
     , return_items_30_day          integer compress
     --Converted to USD (for when displaying US or US+CA)
     , order_amount_usd             numeric(10,2) compress 
     , shipped_usd_sales            numeric(10,2) compress
     , return_usd_amount_30_day     numeric(10,2) compress
     , gift_option_amount_usd       numeric(10,2) compress
     --NOT Converted to USD (for when displaying just CA)
     , order_amount                 numeric(10,2) compress 
     , shipped_sales                numeric(10,2) compress
     , return_amount_30_day         numeric(10,2) compress
     , gift_option_amount           numeric(10,2) compress
     , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX(order_num) --what you would join on
PARTITION BY RANGE_N(order_date_pacific BETWEEN DATE '2020-10-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN) --what you would filter on
;



--comments
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary                             IS 'An order level table focused on gifting details.';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.order_date_pacific          IS 'The date when the order was created by web, converted to US Pacific timezone.';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.order_num                   IS 'The order number.';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.source_platform_code        IS 'Technical platform of the source of an event.';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.source_channel_country_code IS 'Country of the source of an event';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.source_channel_code         IS 'Channel of the source of an event.';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.ship_type                   IS 'A grouping of fulfillment method at the item level. Options are ship to home, ship to store, and bopus';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.destination_node_num        IS 'The physical store number where the item is to be delivered for pickup. This field is populated for BOPUS and Ship to Store orders.';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.gift_option_type            IS 'The gift option selected by the user for the item (Order Line).';

COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.order_items                 IS 'Quantity per order';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.shipped_items               IS 'Shipped quantity per order';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.return_items_30_day         IS 'Returned quantity per order when item was returned within 30 days';

COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.order_amount_usd            IS 'The price of the items in the order, without the employee discount, converted to USD';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.shipped_usd_sales           IS 'The price the customer was charged when the item was shipped (excludes discounts, cancels, and fraud), converted to USD';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.return_usd_amount_30_day    IS 'The price of the items returned converted to USD, without employee discount when item was returned within 30 days';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.gift_option_amount_usd      IS 'The cost of the gift option converted to USD';

COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.order_amount                IS 'The price of the items in the order, without the employee discount';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.shipped_sales               IS 'The price the customer was charged when the item was shipped (excludes discounts, cancels, and fraud)';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.return_amount_30_day        IS 'The price of the items returned, without employee discount when item was returned within 30 days';
COMMENT ON {gifting_digital_exp_t2_schema}.gifting_order_summary.gift_option_amount          IS 'The cost of the gift option';
-- source for most of the comments = https://confluence.nordstrom.com/pages/viewpage.action?pageId=586982947



SET QUERY_BAND = NONE FOR SESSION;