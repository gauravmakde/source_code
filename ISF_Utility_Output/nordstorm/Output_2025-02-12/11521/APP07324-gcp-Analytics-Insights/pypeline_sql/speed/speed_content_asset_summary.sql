/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08907;
     DAG_ID=speed_content_asset_summary_tpt_11521_ACE_ENG;
     Task_Name=speed_content_asset_summary_tpt;'
     FOR SESSION VOLATILE; 

/*
T2/Table Name: T2DL_DAS_DIGENG.SPEED_CONTENT_ASSET_SUMMARY
Team/Owner: Digital Optimization/Soren Stime
Date Created/Modified: 08/18/2023,TBD

Note: 
-- This table is used to populate the content summary dashboard 
-- It will have a 4 day lookback window and updae daily (2 days at a time)

Teradata SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition, channel, experience, page_detail, canvas_id), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on {deg_t2_schema}.speed_content_asset_summary
;

delete 
from    {deg_t2_schema}.speed_content_asset_summary
where   activity_date_partition between {start_date} and {end_date}

;
insert into {deg_t2_schema}.speed_content_asset_summary
select  
	fiscal_year_num, 
	fiscal_quarter_num, 
	fiscal_month_num, 
	fiscal_week_num,
	channel,
	experience,
	channelcountry,
	loyalty_flag,
	context_pagetype,
	page_level,
	page_detail,
	anniversary_flag,
	landing_page_flag,
	exit_page_flag,
	sub_component,
	element_value,
	canvas_id,
	component_name,
	mrkt_type,
	finance_rollup,
	finance_detail,
	event_imp_count,
	event_eng_count,
	session_imp_count,
    session_eng_count,
	page_imp_count,
    page_eng_count,
    next_products_viewed,
	next_products_selected,
	next_page_sessions_selected_product,
	next_page_sessions_veiwed_product,
	next_page_sessions_click_occurred,
	activity_date_partition,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp 
    from    {deg_t2_schema}.speed_content_asset_summary_ldg
where   activity_date_partition between {start_date} and {end_date}
;
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (activity_date_partition, channel, experience, page_detail, canvas_id), -- column names used for primary index
                    COLUMN (activity_date_partition)  -- column names used for partition
on {deg_t2_schema}.speed_content_asset_summary
;

SET QUERY_BAND = NONE FOR SESSION; 
;