SET QUERY_BAND = 'App_ID=APP08907;
     DAG_ID=ddl_speed_content_asset_summary_tpt_11521_ACE_ENG;
     Task_Name=ddl_speed_content_asset_summary_tpt;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_DIGENG.SPEED_CONTENT_ASSET_SUMMARY
Team/Owner: Digital Optimization/Soren Stime
Date Created/Modified: 08/18/2023,TBD

Note:
SPEED Content Asset Summary aggregates the data in content session analytical 
to view data on the asset level. Due to this, if further aggregations are done 
it is recommended to only use event counts as your measure of choice. 
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'SPEED_CONTENT_ASSET_SUMMARY', OUT_RETURN_MSG);

create multiset table {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    fiscal_year_num integer, 
	fiscal_quarter_num integer, 
	fiscal_month_num integer, 
	fiscal_week_num integer,
	channel varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	experience varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	channelcountry varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	loyalty_flag integer,
	context_pagetype varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	page_level varchar(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
	page_detail varchar(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
	anniversary_flag integer,
	landing_page_flag integer,
	exit_page_flag integer,
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
	activity_date_partition date,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(activity_date_partition, channel, experience, page_detail, canvas_id)
partition by range_n(activity_date_partition BETWEEN DATE'2021-01-01' AND DATE'2031-12-31' EACH INTERVAL '1' DAY, NO RANGE)
;


-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY IS 'Aggregated table built off of content_session_analytical with a focus on an asset view of content';
-- Column comments (OPTIONAL)
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.fiscal_year_num IS 'The fiscal year that the date coincides with, based on the 454 retail calendar';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.fiscal_quarter_num IS 'The fiscal quarter that the date coincides with, based on the 454 retail calendar';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.fiscal_month_num IS 'The fiscal month that the date coincides with, based on the 454 retail calendar';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.fiscal_week_num IS 'The fiscal week that the date coincides with, based on the 454 retail calendar';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.channel IS 'Banner for the session, either NORDSTROM or NORDSTROM_RACK';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.experience IS 'Platform for the session DESKTOP_WEB, MOBILE_WEB, IOS_APP, or ANDROID_APP';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.channelcountry IS 'Country where session was initiated, US';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.loyalty_flag IS 'A flag if the session is enrolled in loyalty or not';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.context_pagetype IS 'The page type of where the impression or engagement occurred';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.page_level IS 'The page level of where the impression or engagement occurred focusing on browse pages';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.page_detail IS 'The page detail of where the impression or engagement occurred focusing on the page location of true browse pages';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.anniversary_flag IS 'A flag if the page location is anniversary related';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.landing_page_flag IS 'A flag for the first page view of the session, aka entry page';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.exit_page_flag IS 'A flag for the last page view of the session, aka exit page';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.sub_component IS 'Describes the type of content displayed or clicked on';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.element_value IS 'The value displayed in the content that is clickable';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.canvas_id IS 'The ID of the specific content in view or clicked';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.component_name IS 'The component name of the specific content in view or clicked';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.mrkt_type IS 'Highest view of marketing hierarchy, whether the session was BASE (non marketing driven), or driven by PAID or UNPAID marketing. Derived from referrer and utm_channel in the url';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.finance_rollup IS 'More granular view of which marketing channel drove a customer to their digital experience with Nordstrom';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.finance_detail IS 'Most granular view of which marketing channel drove a customer to their digital experience with Nordstrom';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.event_imp_count IS 'Count of unique impression events for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.event_eng_count IS 'Count of unique engagement events for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.session_imp_count IS 'Count of unique impressed sessions for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.session_eng_count IS 'Count of unique engaged sessions for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.page_imp_count IS 'Count of unique engaged pageIDs for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.page_eng_count IS 'Count of unique engaged pageIDs for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.next_products_viewed IS 'Count of unique products viewed on the next page after a content engagement for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.next_products_selected IS 'Count of unique products selected on the next page after a content engagement for the given dimensions in the record, includes quick view engagement';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.next_page_sessions_selected_product IS 'Count of unique sessions that viewed and clicked a product on the next page after a content engagement for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.next_page_sessions_veiwed_product IS 'Count of unique sessions that only viewed a product on the next page after a content engagement for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.next_page_sessions_click_occurred IS 'Count of unique sessions that clicked anywhere on the next page after a content engagement for the given dimensions in the record';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.activity_date_partition IS 'Partition field and date of occurance';
COMMENT ON  {deg_t2_schema}.SPEED_CONTENT_ASSET_SUMMARY.dw_sys_load_tmstp IS 'Timestamp that the record was added to the table';
SET QUERY_BAND = NONE FOR SESSION;