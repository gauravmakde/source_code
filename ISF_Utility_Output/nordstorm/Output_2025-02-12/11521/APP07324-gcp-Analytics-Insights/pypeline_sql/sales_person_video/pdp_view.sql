SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sales_person_video_pdp_view_11521_ACE_ENG;
     Task_Name=pdp_view;'
     FOR SESSION VOLATILE;

/*

Table: spv.pdp_view
Owner: Elianna Wang
Modification Date: 2023-01-12

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    {spv_t2_schema}.pdp_view
where   view_page_date between {start_date} and {end_date}
;

insert into {spv_t2_schema}.pdp_view
select  
    shopper_id,
	web_style_num,
	view_page_country,
	view_page_channel,
	view_page_platform,
	view_page_year,
	view_page_month,
	view_page_day,
	view_page_date,
	view_pdp_count,
	min_view_pdp_time,
	max_view_pdp_time,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp
from {spv_t2_schema}.pdp_view_landing
where view_page_date between {start_date} and {end_date}
;

collect statistics column (shopper_id)
		, column (web_style_num)
		, column (view_page_country)
		, column (view_page_channel)
		, column (view_page_platform)
		, column (view_page_year)
		, column (view_page_month)
		, column (view_page_day)
on {spv_t2_schema}.pdp_view
;

-- Drop staging table
drop table {spv_t2_schema}.pdp_view_landing
;

SET QUERY_BAND = NONE FOR SESSION;