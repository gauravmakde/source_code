SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sales_person_video_customer_journey_orderline_11521_ACE_ENG;
     Task_Name=ddl_customer_journey_orderline_landing;'
     FOR SESSION VOLATILE;

/*

Table: spv.customer_journey_orderline_landing
Owner: Elianna Wang
Edit Date: 2022-11-14

*/
CALL SYS_MGMT.DROP_IF_EXISTS_SP('{spv_t2_schema}', 'customer_journey_orderline_landing', OUT_RETURN_MSG);

create multiset table {spv_t2_schema}.customer_journey_orderline_landing
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    shopper_id                              VARCHAR(100) CHARACTER SET UNICODE NOT NULL
    , web_style_num                         VARCHAR(10) CHARACTER SET UNICODE NOT NULL
    , click_spv_country                     VARCHAR(2) CHARACTER SET UNICODE NOT NULL
    , click_spv_channel                     VARCHAR(15) CHARACTER SET UNICODE NOT NULL
    , click_spv_platform                    VARCHAR(20) CHARACTER SET UNICODE NOT NULL
    , click_spv_year                        INTEGER
    , click_spv_month                       INTEGER
    , click_spv_day                         INTEGER
    , click_spv_date                        DATE FORMAT 'YYYY-MM-DD' NOT NULL
    , click_spv_count                       INTEGER
    , click_spv_time_start                  TIMESTAMP(3) WITH TIME ZONE
    , click_spv_time_end                    TIMESTAMP(3) WITH TIME ZONE
    , add_or_remove_bag_country             VARCHAR(2)
    , add_or_remove_bag_channel             VARCHAR(15)
    , add_or_remove_bag_platform            VARCHAR(20)
    , add_or_remove_bag_year                INTEGER
    , add_or_remove_bag_month               INTEGER
    , add_or_remove_bag_day                 INTEGER
    , add_or_remove                         VARCHAR(6)
    , add_or_remove_time                    TIMESTAMP(3) WITH TIME ZONE
    , ordernumber                           VARCHAR(15)
    , checkout_country                      VARCHAR(2)
    , checkout_channel                      VARCHAR(15)
    , checkout_platform                     VARCHAR(20)
    , checkout_year                         INTEGER
    , checkout_month                        INTEGER
    , checkout_day                          INTEGER
    , checkout_time                         TIMESTAMP(3) WITH TIME ZONE
    , item_price                            NUMERIC(15,2)
    , orderlineid                           VARCHAR(100)
    , order_amount                          NUMERIC(15,2)
    )
primary index(shopper_id, web_style_num, click_spv_country, click_spv_channel, click_spv_platform, click_spv_date, ordernumber, orderlineid)
partition by range_n(click_spv_date BETWEEN DATE'2022-02-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

SET QUERY_BAND = NONE FOR SESSION;
