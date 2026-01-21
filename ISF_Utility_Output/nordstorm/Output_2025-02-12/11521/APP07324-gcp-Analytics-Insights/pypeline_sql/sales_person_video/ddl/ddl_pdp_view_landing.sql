SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sales_person_video_pdp_view_11521_ACE_ENG;
     Task_Name=ddl_pdp_view_landing;'
     FOR SESSION VOLATILE;

/*

Table: spv.pdp_view_landing
Owner: Elianna Wang
Creation Date: 2022-11-04

This table is created as part of the spv_pdp_view job that loads data from S3 to teradata.  The staging table is the job completes

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP('{spv_t2_schema}', 'pdp_view_landing', OUT_RETURN_MSG);

create multiset table {spv_t2_schema}.pdp_view_landing
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
    )
primary index(shopper_id, web_style_num, view_page_country, view_page_channel, view_page_platform, view_page_year, view_page_month, view_page_day)
partition by range_n(view_page_date BETWEEN DATE'2022-02-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

SET QUERY_BAND = NONE FOR SESSION;