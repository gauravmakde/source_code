/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08907;
     DAG_ID=ddl_speed_content_asset_summary_tpt_11521_ACE_ENG;
     Task_Name=ddl_speed_content_asset_summary_tpt;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_DIGENG.SPEED_CONTENT_ASSET_SUMMARY
Team/Owner: Digital Optimization/Soren Stime
Date Created/Modified: 07/26/2023,TBD

Note:
SPEED Content Asset Summary aggregates the data in content session analytical 
to view data on the asset level. Due to this, if further aggregations are done 
it is recommended to only use event counts as your measure of choice. 
*/
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{deg_t2_schema}', 'speed_content_asset_summary_ldg', OUT_RETURN_MSG);

create multiset table {deg_t2_schema}.speed_content_asset_summary_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    fiscal_year_num int, 
	fiscal_quarter_num int, 
	fiscal_month_num int, 
	fiscal_week_num int,
	channel varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	experience varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	channelcountry varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	loyalty_flag int,
	context_pagetype varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	page_level varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	page_detail varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
	anniversary_flag int,
	landing_page_flag int,
	exit_page_flag int,
	sub_component varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	element_value varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
	canvas_id varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	component_name varchar(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
	mrkt_type varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	finance_rollup varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	finance_detail varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	event_imp_count bigint,
	event_eng_count bigint,
	session_imp_count bigint,
    session_eng_count bigint,
	page_imp_count bigint,
    page_eng_count bigint,
    next_products_viewed bigint,
	next_products_selected bigint,
	next_page_sessions_selected_product bigint,
	next_page_sessions_veiwed_product bigint,
	next_page_sessions_click_occurred bigint,
	activity_date_partition date
    )
primary index(activity_date_partition)
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;

SET QUERY_BAND = NONE FOR SESSION;