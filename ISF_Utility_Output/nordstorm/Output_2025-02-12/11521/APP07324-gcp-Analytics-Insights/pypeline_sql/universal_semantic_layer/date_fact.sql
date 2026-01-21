/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=date_fact_11521_ACE_ENG;
     Task_Name=date_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {usl_t2_schema}.date_fact
Prior Table Layer: {usl_t2_schema}.usl_rolling_52wk_calendar
Team/Owner: Customer Analytics
Date Created/Modified: June 11 2024

Note:
-- Purpose of the table: Table to directly get up to date calendar information from normal fiscal calendar and realigned fiscal calendar.
-- Update Cadence: Daily

*/

-- STEP ONE: GATHER CALENDAR DATA FROM FISCAL CALENDAR AND REALIGNED FISCAL CALENDAR    
    
CREATE MULTISET VOLATILE TABLE date_fact_prep AS (
-- 
	SELECT
		DC.day_date AS date_id
		, DC.day_num 
		, RC.day_num AS realigned_day_num
		, DC.day_desc 
		, DC.week_num
		, DC.week_desc
		, RC.week_num AS realigned_week_num
		, RC.week_desc AS realigned_week_desc
		, DC.week_454_num 
		, DC.month_num
		, DC.month_desc
		, RC.month_num AS realigned_month_num
		, RC.month_short_desc AS realigned_month_desc
		, DC.quarter_num 
		, DC.quarter_desc
		, RC.quarter_num AS realigned_quarter_num
		, DC.halfyear_num 
		, RC.halfyear_num AS realigned_halfyear_num
		, DC.year_num 
		, RC.year_num AS realigned_year_num
		, CURRENT_TIME AS dw_sys_load_tmstp
	FROM PRD_NAP_USR_VWS.DAY_CAL AS DC
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC
		ON DC.day_date = RC.day_date
	WHERE 1 = 1
--
) WITH DATA PRIMARY INDEX(date_id) ON COMMIT PRESERVE ROWS;

-- STEP TWO: INSERT DATA INTO THE T2 TABLE FOR UPDATED VALUES

DELETE FROM {usl_t2_schema}.date_fact;
INSERT INTO {usl_t2_schema}.date_fact
	SELECT 
		date_id
		, day_num 
		, realigned_day_num
		, day_desc 
		, week_num
		, week_desc
		, realigned_week_num
		, realigned_week_desc
		, week_454_num 
		, month_num
		, month_desc
		, realigned_month_num
		, realigned_month_desc
		, quarter_num 
		, quarter_desc
		, realigned_quarter_num
		, halfyear_num 
		, realigned_halfyear_num
		, year_num 
		, realigned_year_num
		, dw_sys_load_tmstp
	FROM date_fact_prep
;

COLLECT STATISTICS COLUMN (date_id) on {usl_t2_schema}.date_fact;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;