SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_sales_person_video_pdp_view_11521_ACE_ENG;
     Task_Name=ddl_pdp_view;'
     FOR SESSION VOLATILE;

/*

Table: spv.pdp_view
Owner: Elianna Wang
Edit Date: 2022-11-08

*/
CALL SYS_MGMT.DROP_IF_EXISTS_SP('{spv_t2_schema}', 'pdp_view', OUT_RETURN_MSG);

create multiset table {spv_t2_schema}.pdp_view
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    shopper_id                   VARCHAR(100) CHARACTER SET UNICODE NOT NULL
    , web_style_num              VARCHAR(10) CHARACTER SET UNICODE NOT NULL
    , view_page_country          VARCHAR(2) CHARACTER SET UNICODE NOT NULL
    , view_page_channel          VARCHAR(10) CHARACTER SET UNICODE NOT NULL
    , view_page_platform         VARCHAR(7) CHARACTER SET UNICODE NOT NULL
    , view_page_year             INTEGER
    , view_page_month            INTEGER
    , view_page_day              INTEGER
    , view_page_date             DATE FORMAT 'YYYY-MM-DD' NOT NULL
    , view_pdp_count             INTEGER
    , min_view_pdp_time          TIMESTAMP(0) WITH TIME ZONE
    , max_view_pdp_time          TIMESTAMP(0) WITH TIME ZONE
    , dw_sys_load_tmstp          TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(shopper_id, web_style_num, view_page_country, view_page_channel, view_page_platform, view_page_year, view_page_month, view_page_day)
partition by range_n(view_page_date BETWEEN DATE'2022-02-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;
SET QUERY_BAND = NONE FOR SESSION;