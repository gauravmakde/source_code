/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=trips_sandbox_yoy_11521_ACE_ENG;
     Task_Name=trips_sandbox_yoy;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {usl_t2_schema}.trips_sandbox_yoy
Prior Table Layer: {usl_t2_schema}.trips_sandbox_weekly_cust
Team/Owner: Customer Analytics
Date Created/Modified: May 8 2024

Note:
-- Purpose of the table: Table to directly setup the highest level of the sandbox.
-- Update Cadence: Weekly

*/

    
/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage One: Create a date filter so I can correct year_id values for a monthly level.
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE date_lookup AS (
	SELECT 
		DC.month_num
		, DC.quarter_num
		, DC.year_num
		, (CASE
			WHEN DC.month_num >= WN.month_num - 100 and DC.month_num < WN.month_num THEN 'TY'
			WHEN  DC.month_num >= WN.month_num - 200 and DC.month_num < WN.month_num - 100 THEN 'LY'
		END) AS year_id -- Create current year, last year, and prior year index
		, MIN(day_date) AS ty_start_dt
		, MAX(day_date) AS ty_end_dt
	FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC
	LEFT JOIN (
		SELECT DISTINCT
			month_num
		FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC
		WHERE 1 = 1
			AND day_date = CURRENT_DATE
	) AS WN
		ON 1 = 1
	WHERE 1 = 1
		AND DC.month_num >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE) - 200 
		and DC.month_num < (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE)
--		AND DC.month_num IN (202403,202303)
	GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX(month_num) ON COMMIT PRESERVE ROWS;

-- GET CUSTOMERS AND THEIR ATTRIBUTES BY TIME GRANULARITY
CREATE VOLATILE MULTISET TABLE customer_lookup as (
--
	SELECT DISTINCT
		acp_id
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, DC.year_id
		, region
		, dma
		, aec
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		, ROW_NUMBER() OVER (PARTITION BY acp_id, DC.year_id ORDER BY week_num_realigned DESC) AS year_id_week_order
	FROM {usl_t2_schema}.trips_sandbox_weekly_cust tswc
	INNER JOIN date_lookup AS DC 
		ON DC.month_num = tswc.month_num_realigned
	WHERE 1 = 1
--
)WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

CREATE VOLATILE MULTISET TABLE customer_single_attribute as (
--
	SELECT
		acp_id
		, year_id
		, COALESCE(MAX(CASE WHEN year_id_week_order = 1 THEN region END), 'Z_UNKNOWN') AS region
		, COALESCE(MAX(CASE WHEN year_id_week_order = 1 THEN dma END), 'Z_UNKNOWN') AS dma
		, COALESCE(MAX(CASE WHEN year_id_week_order = 1 THEN aec END), 'UNDEFINED') AS aec
		, COALESCE(MAX(CASE WHEN year_id_week_order = 1 THEN predicted_segment END), 'Z_UNKNOWN') AS predicted_segment
		, COALESCE(MAX(CASE WHEN year_id_week_order = 1 THEN loyalty_level END), '0) NOT A MEMBER') AS loyalty_level
		, COALESCE(MAX(CASE WHEN year_id_week_order = 1 THEN loyalty_type END), 'c) Non-Loyalty') AS loyalty_type
		, COALESCE(MAX(new_to_jwn),0) AS new_to_jwn
	FROM customer_lookup
	GROUP BY 1,2
--
)WITH DATA PRIMARY INDEX(acp_id, year_id) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Break out the weekly customer data into a TY table to simplify later joining.
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE TY_DATA AS ( 
--
	SELECT
		DC.year_id
	--	, year_num_realigned
		, CSA.region
		, CSA.dma
		, CSA.AEC
		, CSA.predicted_segment
		, CSA.loyalty_level
		, CSA.loyalty_type
		, CSA.new_to_jwn
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COUNT(DISTINCT CASE WHEN channel = '1) Nordstrom Stores' THEN SCF.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN channel = '2) Nordstrom.com' THEN SCF.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN channel = '3) Rack Stores' THEN SCF.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN channel = '4) Rack.com' THEN SCF.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN channel IN ('1) Nordstrom Stores','3) Rack Stores') THEN SCF.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN channel IN ('2) Nordstrom.com','4) Rack.com') THEN SCF.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN banner = '1) Nordstrom Banner' THEN SCF.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN banner = '2) Rack Banner' THEN SCF.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT SCF.acp_id) AS cust_count_jwn
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips END) AS trips_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips END) AS trips_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips END) AS trips_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips END) AS trips_rcom
		, SUM(trips) AS trips_JWN
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend END) AS net_spend_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend END) AS net_spend_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend END) AS net_spend_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend END) AS net_spend_rcom
		, SUM(net_spend) AS net_spend_JWN
		--
		-- GROSS SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN gross_spend END) AS gross_spend_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN gross_spend END) AS gross_spend_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN gross_spend END) AS gross_spend_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN gross_spend END) AS gross_spend_rcom
		, SUM(gross_spend) AS gross_spend_JWN
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units END) AS net_units_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units END) AS net_units_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units END) AS net_units_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units END) AS net_units_rcom
		, SUM(net_units) AS net_units_JWN
		--
		-- GROSS UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN gross_units END) AS gross_units_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN gross_units END) AS gross_units_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN gross_units END) AS gross_units_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN gross_units END) AS gross_units_rcom
		, SUM(gross_units) AS gross_units_JWN
	FROM {usl_t2_schema}.trips_sandbox_weekly_cust AS SCF
	INNER JOIN date_lookup AS DC 
		ON DC.month_num = SCF.month_num_realigned
			AND DC.year_id = 'TY'
	LEFT JOIN customer_single_attribute AS CSA 
		ON CSA.acp_id = SCF.acp_id
			AND CSA.year_id = DC.year_id
	GROUP BY 1,2,3,4,5,6,7,8
--	ORDER BY DC.year_id DESC
--
) WITH DATA PRIMARY INDEX(year_id) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Break out the weekly customer data into a LY table to simplify later joining.
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE LY_DATA AS ( 
--
	SELECT
		DC.year_id
	--	, year_num_realigned
		, CSA.region
		, CSA.dma
		, CSA.AEC
		, CSA.predicted_segment
		, CSA.loyalty_level
		, CSA.loyalty_type
		, CSA.new_to_jwn
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COUNT(DISTINCT CASE WHEN channel = '1) Nordstrom Stores' THEN SCF.acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN channel = '2) Nordstrom.com' THEN SCF.acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN channel = '3) Rack Stores' THEN SCF.acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN channel = '4) Rack.com' THEN SCF.acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN channel IN ('1) Nordstrom Stores','3) Rack Stores') THEN SCF.acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN channel IN ('2) Nordstrom.com','4) Rack.com') THEN SCF.acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN banner = '1) Nordstrom Banner' THEN SCF.acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN banner = '2) Rack Banner' THEN SCF.acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT SCF.acp_id) AS cust_count_jwn
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips END) AS trips_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips END) AS trips_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips END) AS trips_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips END) AS trips_rcom
		, SUM(trips) AS trips_JWN
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend END) AS net_spend_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend END) AS net_spend_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend END) AS net_spend_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend END) AS net_spend_rcom
		, SUM(net_spend) AS net_spend_JWN
		--
		-- GROSS SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN gross_spend END) AS gross_spend_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN gross_spend END) AS gross_spend_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN gross_spend END) AS gross_spend_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN gross_spend END) AS gross_spend_rcom
		, SUM(gross_spend) AS gross_spend_JWN
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units END) AS net_units_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units END) AS net_units_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units END) AS net_units_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units END) AS net_units_rcom
		, SUM(net_units) AS net_units_JWN
		--
		-- GROSS UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN gross_units END) AS gross_units_fls
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN gross_units END) AS gross_units_ncom
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN gross_units END) AS gross_units_rs
		, SUM(CASE WHEN channel = '4) Rack.com' THEN gross_units END) AS gross_units_rcom
		, SUM(gross_units) AS gross_units_JWN
	FROM {usl_t2_schema}.trips_sandbox_weekly_cust AS SCF
	INNER JOIN date_lookup AS DC 
		ON DC.month_num = SCF.month_num_realigned
			AND DC.year_id = 'LY'
	LEFT JOIN customer_single_attribute AS CSA 
		ON CSA.acp_id = SCF.acp_id
			AND CSA.year_id = DC.year_id
	GROUP BY 1,2,3,4,5,6,7,8
--	ORDER BY DC.year_id DESC
--
) WITH DATA PRIMARY INDEX(year_id) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Create the final table
 * 
----------------------------------------------------------------------------------------------------------------*/

--CREATE MULTISET VOLATILE TABLE usl_trips_sandbox_yoy AS (
--DROP TABLE T3DL_ACE_CORP.so1p_usl_trips_sandbox_year_yoy_V8;
--
--CREATE MULTISET TABLE T3DL_ACE_CORP.so1p_usl_trips_sandbox_year_yoy_V8 AS (
DELETE FROM {usl_t2_schema}.trips_sandbox_yoy ALL; -- Remove all historic data to prevent overlaps
--
INSERT INTO {usl_t2_schema}.trips_sandbox_yoy  -- Insert new data
--
	SELECT
		'TY' AS year_id
		, COALESCE(TSO.region, TSO2.region) AS region
		, COALESCE(TSO.dma, TSO2.dma) AS dma
		, COALESCE(TSO.AEC, TSO2.AEC) AS AEC
		, COALESCE(TSO.predicted_segment, TSO2.predicted_segment) AS predicted_segment
		, COALESCE(TSO.loyalty_level, TSO2.loyalty_level) AS loyalty_level
		, COALESCE(TSO.loyalty_type, TSO2.loyalty_type) AS loyalty_type
		, COALESCE(TSO.new_to_jwn, TSO2.new_to_jwn) AS new_to_jwn
		-- Calculated Metrics THIS YEAR
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COALESCE(TSO.cust_count_fls,0) AS cust_count_fls_ty
		, COALESCE(TSO.cust_count_ncom,0) AS cust_count_ncom_ty
		, COALESCE(TSO.cust_count_rs,0) AS cust_count_rs_ty
		, COALESCE(TSO.cust_count_rcom,0) AS cust_count_rcom_ty
		-- -- BY DIGITAL VS STORE		
		, COALESCE(TSO.cust_count_stores,0) AS cust_count_stores_ty
		, COALESCE(TSO.cust_count_digital,0) AS cust_count_digital_ty
		-- -- BY BANNER		
		, COALESCE(TSO.cust_count_nord,0) AS cust_count_nord_ty
		, COALESCE(TSO.cust_count_rack,0) AS cust_count_rack_ty
		-- BY JWN
		, COALESCE(TSO.cust_count_jwn,0) AS cust_count_jwn_ty
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.trips_fls,0) AS trips_fls_ty
		, COALESCE(TSO.trips_ncom,0) AS trips_ncom_ty
		, COALESCE(TSO.trips_rs,0) AS trips_rs_ty
		, COALESCE(TSO.trips_rcom,0) AS trips_rcom_ty
		, COALESCE(TSO.trips_JWN,0) AS trips_JWN_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.net_spend_fls,0) AS net_spend_fls_ty
		, COALESCE(TSO.net_spend_ncom,0) AS net_spend_ncom_ty
		, COALESCE(TSO.net_spend_rs,0) AS net_spend_rs_ty
		, COALESCE(TSO.net_spend_rcom,0) AS net_spend_rcom_ty
		, COALESCE(TSO.net_spend_JWN,0) AS net_spend_JWN_ty
		--
		-- GROSS SALES BY CHANNEL
		--
		, COALESCE(TSO.gross_spend_fls,0) AS gross_spend_fls_ty
		, COALESCE(TSO.gross_spend_ncom,0) AS gross_spend_ncom_ty
		, COALESCE(TSO.gross_spend_rs,0) AS gross_spend_rs_ty
		, COALESCE(TSO.gross_spend_rcom,0) AS gross_spend_rcom_ty
		, COALESCE(TSO.gross_spend_JWN,0) AS gross_spend_JWN_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.net_units_fls,0) AS net_units_fls_ty
		, COALESCE(TSO.net_units_ncom,0) AS net_units_ncom_ty
		, COALESCE(TSO.net_units_rs,0) AS net_units_rs_ty
		, COALESCE(TSO.net_units_rcom,0) AS net_units_rcom_ty
		, COALESCE(TSO.net_units_JWN,0) AS net_units_JWN_ty
		--
		-- GROSS UNITS BY CHANNEL
		--
		, COALESCE(TSO.gross_units_fls,0) AS gross_units_fls_ty
		, COALESCE(TSO.gross_units_ncom,0) AS gross_units_ncom_ty
		, COALESCE(TSO.gross_units_rs,0) AS gross_units_rs_ty
		, COALESCE(TSO.gross_units_rcom,0) AS gross_units_rcom_ty
		, COALESCE(TSO.gross_units_JWN,0) AS gross_units_JWN_ty
		--
		--
		--
		-- LAST YEAR
		--
		--
		--
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COALESCE(TSO2.cust_count_fls,0) AS cust_count_fls_ly
		, COALESCE(TSO2.cust_count_ncom,0) AS cust_count_ncom_ly
		, COALESCE(TSO2.cust_count_rs,0) AS cust_count_rs_ly
		, COALESCE(TSO2.cust_count_rcom,0) AS cust_count_rcom_ly
		-- -- BY DIGITAL VS STORE		
		, COALESCE(TSO2.cust_count_stores,0) AS cust_count_stores_ly
		, COALESCE(TSO2.cust_count_digital,0) AS cust_count_digital_ly
		-- -- BY BANNER		
		, COALESCE(TSO2.cust_count_nord,0) AS cust_count_nord_ly
		, COALESCE(TSO2.cust_count_rack,0) AS cust_count_rack_ly
		-- BY JWN
		, COALESCE(TSO2.cust_count_jwn,0) AS cust_count_jwn_ly
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.trips_fls,0) AS trips_fls_ly
		, COALESCE(TSO2.trips_ncom,0) AS trips_ncom_ly
		, COALESCE(TSO2.trips_rs,0) AS trips_rs_ly
		, COALESCE(TSO2.trips_rcom,0) AS trips_rcom_ly
		, COALESCE(TSO2.trips_JWN,0) AS trips_JWN_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.net_spend_fls,0) AS net_spend_fls_ly
		, COALESCE(TSO2.net_spend_ncom,0) AS net_spend_ncom_ly
		, COALESCE(TSO2.net_spend_rs,0) AS net_spend_rs_ly
		, COALESCE(TSO2.net_spend_rcom,0) AS net_spend_rcom_ly
		, COALESCE(TSO2.net_spend_JWN,0) AS net_spend_JWN_ly
		--
		-- GROSS SALES BY CHANNEL
		--
		, COALESCE(TSO2.gross_spend_fls,0) AS gross_spend_fls_ly
		, COALESCE(TSO2.gross_spend_ncom,0) AS gross_spend_ncom_ly
		, COALESCE(TSO2.gross_spend_rs,0) AS gross_spend_rs_ly
		, COALESCE(TSO2.gross_spend_rcom,0) AS gross_spend_rcom_ly
		, COALESCE(TSO2.gross_spend_JWN,0) AS gross_spend_JWN_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.net_units_fls,0) AS net_units_fls_ly
		, COALESCE(TSO2.net_units_ncom,0) AS net_units_ncom_ly
		, COALESCE(TSO2.net_units_rs,0) AS net_units_rs_ly
		, COALESCE(TSO2.net_units_rcom,0) AS net_units_rcom_ly
		, COALESCE(TSO2.net_units_JWN,0) AS net_units_JWN_ly
		--
		-- GROSS UNITS BY CHANNEL
		--
		, COALESCE(TSO2.gross_units_fls,0) AS gross_units_fls_ly
		, COALESCE(TSO2.gross_units_ncom,0) AS gross_units_ncom_ly
		, COALESCE(TSO2.gross_units_rs,0) AS gross_units_rs_ly
		, COALESCE(TSO2.gross_units_rcom,0) AS gross_units_rcom_ly
		, COALESCE(TSO2.gross_units_JWN,0) AS gross_units_JWN_ly
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM TY_DATA AS TSO
	FULL OUTER JOIN LY_DATA AS TSO2
		ON TSO.region = TSO2.region
			AND TSO.dma = TSO2.dma
			AND TSO.AEC = TSO2.AEC
			AND TSO.predicted_segment = TSO2.predicted_segment
			AND TSO.loyalty_level = TSO2.loyalty_level
			AND TSO.loyalty_type = TSO2.loyalty_type
			AND TSO.new_to_jwn = TSO2.new_to_jwn
	-- SPACER
	UNION
	-- SPACER
	SELECT
		'LY' AS year_id
		, region
		, dma
		, AEC
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		-- Calculated Metrics THIS YEAR
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COALESCE(TSO.cust_count_fls,0) AS cust_count_fls_ty
		, COALESCE(TSO.cust_count_ncom,0) AS cust_count_ncom_ty
		, COALESCE(TSO.cust_count_rs,0) AS cust_count_rs_ty
		, COALESCE(TSO.cust_count_rcom,0) AS cust_count_rcom_ty
		-- -- BY DIGITAL VS STORE		
		, COALESCE(TSO.cust_count_stores,0) AS cust_count_stores_ty
		, COALESCE(TSO.cust_count_digital,0) AS cust_count_digital_ty
		-- -- BY BANNER		
		, COALESCE(TSO.cust_count_nord,0) AS cust_count_nord_ty
		, COALESCE(TSO.cust_count_rack,0) AS cust_count_rack_ty
		-- BY JWN
		, COALESCE(TSO.cust_count_jwn,0) AS cust_count_jwn_ty
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.trips_fls,0) AS trips_fls_ty
		, COALESCE(TSO.trips_ncom,0) AS trips_ncom_ty
		, COALESCE(TSO.trips_rs,0) AS trips_rs_ty
		, COALESCE(TSO.trips_rcom,0) AS trips_rcom_ty
		, COALESCE(TSO.trips_JWN,0) AS trips_JWN_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.net_spend_fls,0) AS net_spend_fls_ty
		, COALESCE(TSO.net_spend_ncom,0) AS net_spend_ncom_ty
		, COALESCE(TSO.net_spend_rs,0) AS net_spend_rs_ty
		, COALESCE(TSO.net_spend_rcom,0) AS net_spend_rcom_ty
		, COALESCE(TSO.net_spend_JWN,0) AS net_spend_JWN_ty
		--
		-- GROSS SALES BY CHANNEL
		--
		, COALESCE(TSO.gross_spend_fls,0) AS gross_spend_fls_ty
		, COALESCE(TSO.gross_spend_ncom,0) AS gross_spend_ncom_ty
		, COALESCE(TSO.gross_spend_rs,0) AS gross_spend_rs_ty
		, COALESCE(TSO.gross_spend_rcom,0) AS gross_spend_rcom_ty
		, COALESCE(TSO.gross_spend_JWN,0) AS gross_spend_JWN_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.net_units_fls,0) AS net_units_fls_ty
		, COALESCE(TSO.net_units_ncom,0) AS net_units_ncom_ty
		, COALESCE(TSO.net_units_rs,0) AS net_units_rs_ty
		, COALESCE(TSO.net_units_rcom,0) AS net_units_rcom_ty
		, COALESCE(TSO.net_units_JWN,0) AS net_units_JWN_ty
		--
		-- GROSS UNITS BY CHANNEL
		--
		, COALESCE(TSO.gross_units_fls,0) AS gross_units_fls_ty
		, COALESCE(TSO.gross_units_ncom,0) AS gross_units_ncom_ty
		, COALESCE(TSO.gross_units_rs,0) AS gross_units_rs_ty
		, COALESCE(TSO.gross_units_rcom,0) AS gross_units_rcom_ty
		, COALESCE(TSO.gross_units_JWN,0) AS gross_units_JWN_ty
		--
		--
		--
		-- LAST YEAR
		--
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, 0 AS cust_count_fls_ly
		, 0 AS cust_count_ncom_ly
		, 0 AS cust_count_rs_ly
		, 0 AS cust_count_rcom_ly
		-- -- BY DIGITAL VS STORE		
		, 0 AS cust_count_stores_ly
		, 0 AS cust_count_digital_ly
		-- -- BY BANNER		
		, 0 AS cust_count_nord_ly
		, 0 AS cust_count_rack_ly
		-- BY JWN
		, 0 AS cust_count_jwn_ly
		--
		-- TRIP BY CHANNEL
		--
		, 0 AS trips_fls_ly
		, 0 AS trips_ncom_ly
		, 0 AS trips_rs_ly
		, 0 AS trips_rcom_ly
		, 0 AS trips_JWN_ly
		--
		-- NET SALES BY CHANNEL
		--
		, 0 AS net_spend_fls_ly
		, 0 AS net_spend_ncom_ly
		, 0 AS net_spend_rs_ly
		, 0 AS net_spend_rcom_ly
		, 0 AS net_spend_JWN_ly
		--
		-- GROSS SALES BY CHANNEL
		--
		, 0 AS gross_spend_fls_ly
		, 0 AS gross_spend_ncom_ly
		, 0 AS gross_spend_rs_ly
		, 0 AS gross_spend_rcom_ly
		, 0 AS gross_spend_JWN_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, 0 AS net_units_fls_ly
		, 0 AS net_units_ncom_ly
		, 0 AS net_units_rs_ly
		, 0 AS net_units_rcom_ly
		, 0 AS net_units_JWN_ly
		--
		-- GROSS UNITS BY CHANNEL
		--
		, 0 AS gross_units_fls_ly
		, 0 AS gross_units_ncom_ly
		, 0 AS gross_units_rs_ly
		, 0 AS gross_units_rcom_ly
		, 0 AS gross_units_JWN_ly
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM LY_DATA AS TSO
--
--)WITH DATA PRIMARY INDEX(year_id, loyalty_type, loyalty_level, AEC) ON COMMIT PRESERVE ROWS;
-- )WITH DATA PRIMARY INDEX(year_id, loyalty_type, loyalty_level, AEC);
;

--select
--    year_id
--    , SUM(cust_count_jwn_ty) as total_customers_ty
--    , sum(gross_spend_JWN_ty) as total_gross_spend_ty
--    , sum(net_spend_JWN_ty) as total_net_spend_ty
--    , sum(trips_JWN_ty) as total_trips_ty
--    , sum(gross_units_JWN_ty) as total_items_ty
--    , sum(net_units_JWN_ty) as net_items_ty
--    , SUM(cust_count_jwn_ly) as total_customers_ly
--    , sum(gross_spend_JWN_ly) as total_gross_spend_ty
--    , sum(net_spend_JWN_ly) as total_net_spend_ty
--    , sum(trips_JWN_ly) as total_trips_ty
--    , sum(gross_units_JWN_ly) as total_items_ty
--    , sum(net_units_JWN_ly) as net_items_ty
--  from usl_trips_sandbox_yoy
--  WHERE 1 = 1
-- GROUP BY 1
-- ORDER BY year_id DESC;

COLLECT STATISTICS 
	COLUMN (year_id)
    , COLUMN (region)
    , COLUMN (dma)
    , COLUMN (AEC)
    , COLUMN (predicted_segment)
    , COLUMN (loyalty_level)
    , COLUMN (loyalty_type)
on {usl_t2_schema}.trips_sandbox_yoy;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;