SET QUERY_BAND = 'App_ID=APP08142;
     DAG_ID=ddl_recs_funnel_11521_ACE_ENG;
     Task_Name=ddl_recs_funnel;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_DSA_RECS_REPORTING.recs_funnel_daily
Team/Owner: Sachin Goyal
Date Created/Modified: 02/06/2023

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window: Daily

*/


create multiset table {recommendations_t2_schema}.recs_funnel_daily
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
        country CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
        channel VARCHAR(24) CHARACTER SET UNICODE NOT CASESPECIFIC ,
        platform CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC ,
        context_page_type VARCHAR(24) CHARACTER SET UNICODE NOT CASESPECIFIC,
        placement VARCHAR(24) CHARACTER SET UNICODE NOT CASESPECIFIC,
        strategy VARCHAR(24) CHARACTER SET UNICODE NOT CASESPECIFIC,
        user_id_type VARCHAR(24) CHARACTER SET UNICODE NOT CASESPECIFIC,
        currencycode VARCHAR(24) CHARACTER SET UNICODE NOT CASESPECIFIC ,
        etl_timestamp TIMESTAMP,
        pst_date DATE,
        recs_clicked_users VARCHAR(48),
        adding_users VARCHAR(48),
        ordering_users VARCHAR(48),
        product_summary_selected_views VARCHAR(48),
        add_to_bag_items VARCHAR(48),
        order_items VARCHAR(48),
        orders VARCHAR(48),
        demand VARCHAR(48),
        dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(pst_date)
;

SET QUERY_BAND = NONE FOR SESSION;