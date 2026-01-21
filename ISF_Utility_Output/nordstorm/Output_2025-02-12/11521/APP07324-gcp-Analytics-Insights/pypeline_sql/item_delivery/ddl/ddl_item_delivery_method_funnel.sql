SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_item_delivery_method_funnel_11521_ACE_ENG;
     Task_Name=ddl_item_delivery_method_funnel;'
     FOR SESSION VOLATILE;

/******************************************************************************

Description - This ddl creates a order-order line level table that helps identify item delivery method by item
Analyst - Niharika Srivastava, Strategic Analytics

*******************************************************************************/

CREATE MULTISET TABLE {item_delivery_t2_schema}.item_delivery_method_funnel_daily,
    FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
        (
        order_date_pacific                              DATE NOT NULL,
        order_tmstp_pacific                             TIMESTAMP,
        shopper_id                                      VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        source_channel_code                             VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
        source_channel_country_code                     VARCHAR(49) CHARACTER SET UNICODE NOT CASESPECIFIC,
        source_platform_code                            VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
        order_num                                       VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
        order_line_num                                  INTEGER,
        order_line_id                                   VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        delivery_method_code                            VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
        padded_upc                                      VARCHAR(512) CHARACTER SET UNICODE NOT CASESPECIFIC,
        rms_sku_num                                     VARCHAR(512) CHARACTER SET UNICODE NOT CASESPECIFIC,
        item_delivery_method                            VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
        order_pickup_ind                                BYTEINT COMPRESS,
        ship_to_store_ind                               BYTEINT COMPRESS,
        bopus_filled_from_stores_ind                    BYTEINT COMPRESS,
        customer_promised_next_day_pickup_ind           BYTEINT COMPRESS,
        curbside_ind                                    BYTEINT COMPRESS,
        fulfilled_node_num                              INTEGER COMPRESS,
        shipped_node_num                                INTEGER COMPRESS,
        destination_node_num                            INTEGER COMPRESS,
        picked_up_by_customer_node_num                  INTEGER COMPRESS,
        destination_zip_code                            VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        bill_zip_code                                   VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
        canceled_tmstp_pacific                          TIMESTAMP,
        canceled_date_pacific                           DATE,
        cancel_reason_code                              VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
        fraud_cancel_ind                                VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
        pre_release_cancel_ind                          VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
        order_line_currency_code                        VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
        order_line_amount                               DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
        order_line_amount_usd                           DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
        order_line_quantity                             DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. ),
        order_line_employee_discount_percentage         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
        order_line_employee_discount_amount             DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
        order_line_employee_discount_amount_usd         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00,
        requested_max_promise_tmstp_pacific             TIMESTAMP,
        gift_with_purchase_ind                          VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
        beauty_sample_ind                               VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
        backorder_ind                                   VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
        shipped_tmstp_pacific                           TIMESTAMP,
        arrived_at_order_pickup_tmstp_pacific           TIMESTAMP,
        fulfilled_tmstp_pacific                         TIMESTAMP,
        picked_up_by_customer_tmstp_pacific             TIMESTAMP,
        business_day_date                               DATE,
        global_tran_id                                  BIGINT,
        acp_id                                          VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        shipped_sales                                   DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        shipped_usd_sales                               DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        has_employee_discount                           DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        employee_discount_amt                           DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        employee_discount_usd_amt                       DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        shipped_qty                                     INTEGER,
        intent_store_num                                DECIMAL(8,0) DEFAULT 0.00 COMPRESS 0.00,
        return_ringing_store_num                        DECIMAL(8,0) DEFAULT 0.00 COMPRESS 0.00,
        return_date                                     DATE,
        return_qty                                      INTEGER,
        return_amt                                      DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        return_usd_amt                                  DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        return_employee_disc_amt                        DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        return_employee_disc_usd_amt                    DECIMAL(12,2) DEFAULT 0.00 COMPRESS 0.00,
        days_to_return                                  INTEGER,
        line_item_fulfillment_type                      VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
        line_item_seq_num                               SMALLINT,
        dw_sys_load_tmstp                               TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
        )
        UNIQUE PRIMARY INDEX (order_num,order_line_id,order_date_pacific)
        PARTITION BY RANGE_N(order_date_pacific BETWEEN DATE '2018-02-04' AND DATE '2025-12-31' EACH INTERVAL '1' DAY, UNKNOWN);

-- table comment
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily IS 'Governed descriptions and logic for item fulfillment methods'; --update
--Column comments
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.order_date_pacific IS 'the date in pacific standard time an order was placed';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.order_tmstp_pacific IS 'the timestamp in pacific standard time an order was placed';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.shopper_id IS 'the shopper id of a digital customer';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.source_channel_code IS '' ;
--COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.order_type_code IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.order_num IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.order_line_num IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.delivery_method_code IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.padded_upc IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.item_delivery_method IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.order_pickup_ind IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.ship_to_store_ind IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.bopus_filled_from_stores_ind IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.customer_promised_next_day_pickup_ind IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.curbside_ind IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.destination_node_num IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.picked_up_by_customer_node_num IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.destination_zip_code IS '';
COMMENT ON  {item_delivery_t2_schema}.item_delivery_method_funnel_daily.bill_zip_code IS 'the bill zip associated with the order';

SET QUERY_BAND = NONE FOR SESSION;