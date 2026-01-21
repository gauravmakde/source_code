/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=trips_sandbox_weekly_cust_11521_ACE_ENG;
     Task_Name=trips_sandbox_cust_single_attribute;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {usl_t2_schema}.trips_sandbox_cust_single_attribute
Prior Table Layer: {usl_t2_schema}.trips_sandbox_weekly_cust
Team/Owner: Customer Analytics
Date Created/Modified: August 8th 2024

Note:
-- Purpose of the table: Table to get a single value for each customer attribute which will be useful for creating aggregatable customer counts
-- Update Cadence: Weekly

*/

-- GET CUSTOMERS AND THEIR ATTRIBUTES BY TIME GRANULARITY
CREATE VOLATILE MULTISET TABLE customer_lookup as (
--
	SELECT DISTINCT
		acp_id
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		, ROW_NUMBER() OVER (PARTITION BY acp_id, week_num_realigned ORDER BY week_num_realigned DESC) AS week_order
		, ROW_NUMBER() OVER (PARTITION BY acp_id, month_num_realigned ORDER BY week_num_realigned DESC) AS month_week_order
		, ROW_NUMBER() OVER (PARTITION BY acp_id, quarter_num_realigned ORDER BY week_num_realigned DESC) AS quarter_week_order
		, ROW_NUMBER() OVER (PARTITION BY acp_id, year_num_realigned ORDER BY week_num_realigned DESC) AS year_week_order
	FROM {usl_t2_schema}.trips_sandbox_weekly_cust tswc
	WHERE 1 = 1
--
)WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (acp_id, week_num_realigned) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id, region) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id, dma) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id, aec) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id, predicted_segment) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id, loyalty_level) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id, loyalty_type) ON customer_lookup;
COLLECT STATISTICS COLUMN (acp_id, new_to_jwn) ON customer_lookup;

-- GET A WEEKLY SETUP FOR THIS TABLE
/*
 * 
 * Step 2: Built the Customer Tables
 * 
 */

/*
 * 
 * Step 2.1: Built the Customer Tables (WEEKLY)
 * 
 */

 CREATE VOLATILE MULTISET TABLE all_times AS (
--
	SELECT
		'WEEK    ' AS time_granularity
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
		AND week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
		AND week_num_realigned <= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE)
	GROUP BY 1,2,3,4,5,6
--
) WITH DATA PRIMARY INDEX(acp_id, time_granularity, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned) ON COMMIT PRESERVE ROWS;

INSERT INTO all_times
--
	SELECT
		'WEEK    ' AS time_granularity
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
		AND week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
		AND week_num_realigned < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
	GROUP BY 1,2,3,4,5,6
--
;

INSERT INTO all_times
--
	SELECT
		'WEEK    ' AS time_granularity
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
		AND week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -24))
		AND week_num_realigned < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
	GROUP BY 1,2,3,4,5,6
--
;

COLLECT STATISTICS COLUMN (acp_id, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
COLLECT STATISTICS COLUMN (acp_id) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, region) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, dma) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, aec) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, predicted_segment) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, loyalty_level) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, loyalty_type) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, new_to_jwn) ON all_times;

/*
 * 
 * Step 2.2: Built the Customer Tables (MONTHLY)
 * 
 */

INSERT INTO all_times  -- Insert new data
	SELECT
		'MONTH' AS time_granularity
		, 000000 AS week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN month_week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN month_week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN month_week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN month_week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
		AND month_num_realigned >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
		AND month_num_realigned <= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE)
	GROUP BY 1,2,3,4,5,6
;

INSERT INTO all_times  -- Insert new data
	SELECT
		'MONTH' AS time_granularity
		, 000000 AS week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN month_week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN month_week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN month_week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN month_week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
		AND month_num_realigned >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
		AND month_num_realigned < (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
	GROUP BY 1,2,3,4,5,6
;

INSERT INTO all_times  -- Insert new data
	SELECT
		'MONTH' AS time_granularity
		, 000000 AS week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN month_week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN month_week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN month_week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN month_week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
		AND month_num_realigned >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -24))
		AND month_num_realigned < (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
	GROUP BY 1,2,3,4,5,6
;

COLLECT STATISTICS COLUMN (acp_id, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
COLLECT STATISTICS COLUMN (acp_id) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, region) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, dma) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, aec) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, predicted_segment) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, loyalty_level) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, loyalty_type) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, new_to_jwn) ON all_times;

/*
 * 
 * Step 2.3: Built the Customer Tables (QUARTER AND YEAR)
 * 
 */

 INSERT INTO all_times  -- Insert new data
	SELECT
		'QUARTER' AS time_granularity
		, 000000 AS week_num_realigned
		, 000000 AS month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN month_week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN month_week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN month_week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN month_week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
	GROUP BY 1,2,3,4,5,6
	--
	UNION
	--
	SELECT
		'YEAR' AS time_granularity
		, 000000 AS week_num_realigned
		, 000000 AS month_num_realigned
		, 00000 AS quarter_num_realigned
		, year_num_realigned
		, acp_id
		, MAX(CASE WHEN month_week_order = 1 THEN region END) AS region
		, MAX(CASE WHEN month_week_order = 1 THEN dma END) AS dma
		, MAX(CASE WHEN month_week_order = 1 THEN aec END) AS aec
		, MAX(CASE WHEN month_week_order = 1 THEN predicted_segment END) AS predicted_segment
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_level END) AS loyalty_level
		, MAX(CASE WHEN month_week_order = 1 THEN loyalty_type END) AS loyalty_type
		, MAX(new_to_jwn) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	WHERE 1 = 1
	GROUP BY 1,2,3,4,5,6
;

COLLECT STATISTICS COLUMN (acp_id, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
COLLECT STATISTICS COLUMN (acp_id) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, region) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, dma) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, aec) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, predicted_segment) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, loyalty_level) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, loyalty_type) ON all_times;
COLLECT STATISTICS COLUMN (acp_id, new_to_jwn) ON all_times;

-- COMBINE ALL THE ATTRIBUTES INTO ONE CUSTOMER TABLE

DELETE FROM {usl_t2_schema}.trips_sandbox_cust_single_attribute ALL; -- Remove all historic data to prevent overlaps
----
INSERT INTO {usl_t2_schema}.trips_sandbox_cust_single_attribute  -- Insert new data
--
	SELECT
		TRIM(time_granularity) AS time_granularity
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, COALESCE(region, 'Z_UNKNOWN') AS region
		, COALESCE(dma, 'Z_UNKNOWN') AS dma
		, COALESCE(aec, 'UNDEFINED') AS aec
		, COALESCE(predicted_segment, 'Z_UNKNOWN') AS predicted_segment
		, COALESCE(loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, COALESCE(new_to_jwn, 0) AS new_to_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM all_times
--
;

COLLECT STATISTICS COLUMN (acp_id, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id, region) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id, dma) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id, aec) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id, predicted_segment) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id, loyalty_level) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id, loyalty_type) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;
COLLECT STATISTICS COLUMN (acp_id, new_to_jwn) ON {usl_t2_schema}.trips_sandbox_cust_single_attribute;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;