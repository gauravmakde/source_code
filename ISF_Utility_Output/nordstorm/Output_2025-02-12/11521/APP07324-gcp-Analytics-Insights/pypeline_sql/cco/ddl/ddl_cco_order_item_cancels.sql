SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=ddl_cco_order_item_cancels_11521_ACE_ENG;
     Task_Name=ddl_cco_order_item_cancels;'
     FOR SESSION VOLATILE;




/*
CCO Strategy Cancels DDL file   
This file creates the production table T2DL_DAS_STRATEGY.cco_cancels
*/

 CREATE MULTISET TABLE {cco_t2_schema}.cco_order_item_cancels,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    order_num VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
    order_line_id VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
    order_line_num SMALLINT,
    order_date_pacific      DATE NOT NULL,
    order_line_amount_usd DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
    employee_discount_flag BYTEINT,
    order_line_quantity SMALLINT,
    requested_max_promise_date_pacific DATE,
    upc_code VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    destination_node_num SMALLINT,
    destination_city VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
    destination_state VARCHAR(20) CHARACTER SET UNICODE,
    destination_zip_code VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    destination_country_code VARCHAR(20),
    item_delivery_method VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
    backorder_display_ind CHAR,
    source_channel_country_code VARCHAR(20),
    source_channel_code VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    source_platform_code VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    source_store_num SMALLINT,
    canceled_date_pacific DATE NOT NULL,
    cancel_reason_code VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
)
PRIMARY INDEX (acp_id, order_line_num)
PARTITION BY RANGE_N(order_date_pacific BETWEEN DATE '2018-02-04' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN);

SET QUERY_BAND = NONE FOR SESSION;