/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=trips_sandbox_weekly_cust_11521_ACE_ENG;
     Task_Name=trips_sandbox_cust_count;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {usl_t2_schema}.trips_sandbox_yoy
Prior Table Layer: {usl_t2_schema}.trips_sandbox_weekly_cust
Team/Owner: Customer Analytics
Date Created/Modified: September 17 2024

Note:
-- Purpose of the table: Table to directly setup the highest level of the sandbox.
-- Update Cadence: Weekly

*/

/*
 * 
 * Step 1: Get the latest values for each customer in terms of a weekly value. This will be used to group customers by their latest attribute later. Additionally, create identifiers for which week is the last week and what channels the customer shopped at.
 * 
 */

CREATE VOLATILE MULTISET TABLE customer_lookup as (
--
	SELECT
		tswc.week_num_realigned
		, tswc.month_num_realigned
		, tswc.quarter_num_realigned
		, tswc.year_num_realigned
		, tswc.acp_id
		, MAX(CASE WHEN channel = '1) Nordstrom Stores' THEN 1 ELSE 0 END) AS fls_shopper
		, MAX(CASE WHEN channel = '2) Nordstrom.com' THEN 1 ELSE 0 END) AS ncom_shopper
		, MAX(CASE WHEN channel = '3) Rack Stores' THEN 1 ELSE 0 END) AS rs_shopper
		, MAX(CASE WHEN channel = '4) Rack.com' THEN 1 ELSE 0 END) AS rcom_shopper
		-- -- BY DIGITAL VS STORE		
		, MAX(CASE WHEN channel IN ('1) Nordstrom Stores','3) Rack Stores') THEN  1 ELSE 0 END) AS store_shopper
		, MAX(CASE WHEN channel IN ('2) Nordstrom.com','4) Rack.com') THEN  1 ELSE 0 END) AS digital_shopper
		-- -- BY BANNER		
		, MAX(CASE WHEN banner = '1) Nordstrom Banner' THEN 1 ELSE 0 END) AS nord_shopper
		, MAX(CASE WHEN banner = '2) Rack Banner' THEN 1 ELSE 0 END) AS rack_shopper
	FROM {usl_t2_schema}.trips_sandbox_weekly_cust tswc
	WHERE 1 = 1
	GROUP BY 1,2,3,4,5
--
)WITH DATA PRIMARY INDEX(week_num_realigned) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (acp_id, week_num_realigned) ON customer_lookup;

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
		, CL.week_num_realigned
		, CL.month_num_realigned
		, CL.quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = CL.week_num_realigned
			AND TSCA.month_num_realigned = CL.month_num_realigned
			AND TSCA.quarter_num_realigned = CL.quarter_num_realigned
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = TRIM('WEEK    ')
	WHERE 1 = 1
		AND CL.week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
		AND CL.week_num_realigned <= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
--
) WITH DATA PRIMARY INDEX(time_granularity, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned) ON COMMIT PRESERVE ROWS;

INSERT INTO all_times
--
	SELECT
		'WEEK    ' AS time_granularity
		, CL.week_num_realigned
		, CL.month_num_realigned
		, CL.quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = CL.week_num_realigned
			AND TSCA.month_num_realigned = CL.month_num_realigned
			AND TSCA.quarter_num_realigned = CL.quarter_num_realigned
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = TRIM('WEEK    ')
	WHERE 1 = 1
		AND CL.week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
		AND CL.week_num_realigned < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
--
;

INSERT INTO all_times
--
	SELECT
		'WEEK    ' AS time_granularity
		, CL.week_num_realigned
		, CL.month_num_realigned
		, CL.quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = CL.week_num_realigned
			AND TSCA.month_num_realigned = CL.month_num_realigned
			AND TSCA.quarter_num_realigned = CL.quarter_num_realigned
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = TRIM('WEEK    ')
	WHERE 1 = 1
		AND CL.week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -24))
		AND CL.week_num_realigned < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
--
;

COLLECT STATISTICS COLUMN (week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
COLLECT STATISTICS COLUMN (region) ON all_times;
COLLECT STATISTICS COLUMN (dma) ON all_times;
COLLECT STATISTICS COLUMN (aec) ON all_times;
COLLECT STATISTICS COLUMN (predicted_segment) ON all_times;
COLLECT STATISTICS COLUMN (loyalty_level) ON all_times;
COLLECT STATISTICS COLUMN (loyalty_type) ON all_times;
COLLECT STATISTICS COLUMN (new_to_jwn) ON all_times;

/*
 * 
 * Step 2.2: Built the Customer Tables (MONTHLY)
 * 
 */

INSERT INTO all_times  -- Insert new data
	SELECT
		'MONTH' AS time_granularity
		, 000000 AS week_num_realigned
		, CL.month_num_realigned
		, CL.quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = 000000
			AND TSCA.month_num_realigned = CL.month_num_realigned
			AND TSCA.quarter_num_realigned = CL.quarter_num_realigned
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = 'MONTH'
	WHERE 1 = 1
		AND CL.month_num_realigned >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
		AND CL.month_num_realigned <= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

INSERT INTO all_times  -- Insert new data
	SELECT
		'MONTH' AS time_granularity
		, 000000 AS week_num_realigned
		, CL.month_num_realigned
		, CL.quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = 000000
			AND TSCA.month_num_realigned = CL.month_num_realigned
			AND TSCA.quarter_num_realigned = CL.quarter_num_realigned
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = 'MONTH'
	WHERE 1 = 1
		AND CL.month_num_realigned >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
		AND CL.month_num_realigned < (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -8))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

INSERT INTO all_times  -- Insert new data
	SELECT
		'MONTH' AS time_granularity
		, 000000 AS week_num_realigned
		, CL.month_num_realigned
		, CL.quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = 000000
			AND TSCA.month_num_realigned = CL.month_num_realigned
			AND TSCA.quarter_num_realigned = CL.quarter_num_realigned
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = 'MONTH'
	WHERE 1 = 1
		AND CL.month_num_realigned >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -24))
		AND CL.month_num_realigned < (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -16))
		-- ENSURE MATCHING TO WEEKLY TABLE IN TERMS OF FEATURED TIME SPAN
		AND CL.week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -24))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

COLLECT STATISTICS COLUMN (week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
COLLECT STATISTICS COLUMN (region) ON all_times;
COLLECT STATISTICS COLUMN (dma) ON all_times;
COLLECT STATISTICS COLUMN (aec) ON all_times;
COLLECT STATISTICS COLUMN (predicted_segment) ON all_times;
COLLECT STATISTICS COLUMN (loyalty_level) ON all_times;
COLLECT STATISTICS COLUMN (loyalty_type) ON all_times;
COLLECT STATISTICS COLUMN (new_to_jwn) ON all_times;

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
		, CL.quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = 000000
			AND TSCA.month_num_realigned = 000000
			AND TSCA.quarter_num_realigned = CL.quarter_num_realigned
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = 'QUARTER'
	WHERE 1 = 1
		-- ENSURE MATCHING TO WEEKLY TABLE IN TERMS OF FEATURED TIME SPAN
		AND CL.week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -24))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
	--
	UNION
	--
	SELECT
		'YEAR' AS time_granularity
		, 000000 AS week_num_realigned
		, 000000 AS month_num_realigned
		, 00000 AS quarter_num_realigned
		, CL.year_num_realigned
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- COUNTS
		, COUNT(DISTINCT CASE WHEN fls_shopper = 1 THEN CL.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN ncom_shopper = 1 THEN CL.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN rs_shopper = 1 THEN CL.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN rcom_shopper = 1 THEN CL.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN store_shopper = 1 THEN CL.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN digital_shopper = 1 THEN CL.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN nord_shopper = 1 THEN CL.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN rack_shopper = 1 THEN CL.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT CL.acp_id) AS cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM customer_lookup AS CL
	LEFT JOIN {usl_t2_schema}.trips_sandbox_cust_single_attribute AS TSCA
		ON TSCA.acp_id = CL.acp_id
			AND TSCA.week_num_realigned = 000000
			AND TSCA.month_num_realigned = 000000
			AND TSCA.quarter_num_realigned = 00000
			AND TSCA.year_num_realigned = CL.year_num_realigned
			AND TSCA.time_granularity = 'YEAR'
	WHERE 1 = 1
		-- ENSURE MATCHING TO WEEKLY TABLE IN TERMS OF FEATURED TIME SPAN
		AND CL.week_num_realigned >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(CURRENT_DATE, -24))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

COLLECT STATISTICS COLUMN (week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
COLLECT STATISTICS COLUMN (region) ON all_times;
COLLECT STATISTICS COLUMN (dma) ON all_times;
COLLECT STATISTICS COLUMN (aec) ON all_times;
COLLECT STATISTICS COLUMN (predicted_segment) ON all_times;
COLLECT STATISTICS COLUMN (loyalty_level) ON all_times;
COLLECT STATISTICS COLUMN (loyalty_type) ON all_times;
COLLECT STATISTICS COLUMN (new_to_jwn) ON all_times;

-- COMBINE ALL THE ATTRIBUTES INTO ONE CUSTOMER TABLE

DELETE FROM {usl_t2_schema}.trips_sandbox_cust_count ALL; -- Remove all historic data to prevent overlaps
----
INSERT INTO {usl_t2_schema}.trips_sandbox_cust_count  -- Insert new data
--
	SELECT
		TRIM(time_granularity) AS time_granularity
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, COALESCE(region, 'Z_UNKNOWN') AS region
		, COALESCE(dma, 'Z_UNKNOWN') AS dma
		, COALESCE(aec, 'UNDEFINED') AS aec
		, COALESCE(predicted_segment, 'Z_UNKNOWN') AS predicted_segment
		, COALESCE(loyalty_level, '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(loyalty_type, 'c) Non-Loyalty') AS loyalty_type
		, COALESCE(new_to_jwn, 0) AS new_to_jwn
		-- COUNTS
		, cust_count_fls
		, cust_count_ncom
		, cust_count_rs
		, cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, cust_count_stores
		, cust_count_digital
		-- -- BY BANNER		
		, cust_count_nord
		, cust_count_rack
		-- BY JWN
		, cust_count_jwn
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM all_times
--
;

COLLECT STATISTICS COLUMN (week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON {usl_t2_schema}.trips_sandbox_cust_count;
COLLECT STATISTICS COLUMN (region) ON {usl_t2_schema}.trips_sandbox_cust_count;
COLLECT STATISTICS COLUMN (dma) ON {usl_t2_schema}.trips_sandbox_cust_count;
COLLECT STATISTICS COLUMN (aec) ON {usl_t2_schema}.trips_sandbox_cust_count;
COLLECT STATISTICS COLUMN (predicted_segment) ON {usl_t2_schema}.trips_sandbox_cust_count;
COLLECT STATISTICS COLUMN (loyalty_level) ON {usl_t2_schema}.trips_sandbox_cust_count;
COLLECT STATISTICS COLUMN (loyalty_type) ON {usl_t2_schema}.trips_sandbox_cust_count;
COLLECT STATISTICS COLUMN (new_to_jwn) ON {usl_t2_schema}.trips_sandbox_cust_count;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;