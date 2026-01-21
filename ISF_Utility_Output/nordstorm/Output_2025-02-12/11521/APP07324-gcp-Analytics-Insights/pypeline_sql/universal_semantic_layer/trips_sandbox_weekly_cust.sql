/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=trips_sandbox_weekly_cust_11521_ACE_ENG;
     Task_Name=trips_sandbox_weekly_cust;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {usl_t2_schema}.trips_sandbox_weekly_cust
Prior Table Layer: {usl_t2_schema}.sales_cust_fact
Team/Owner: Customer Analytics
Date Created/Modified: Mar 27 2024

Note:
-- Purpose of the table: Table to directly setup the deepest level for sandbox information.
-- Update Cadence: Daily

*/
    
/*
 * I will be adding several metrics to the sales_fact table. This metrics will be focused towards divisions and simple financial calculations to keep the table in line with buyerflow information.
 */

/*set date range as full or incremental
based on day of week (which is variable to allow
manual full loads any day of the week)*/

/*
 * Written: Ian Rasquinha
 * Date Created: 3/21/2024
 * Date Last Edited: 3/27/2024
 * 
 * Goal: The goal of this code is to create a table level before joining on any of the customer attributes to correct the trips first. 
 */
/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION Fill 1



*******************************************************************************************************************/

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage One: Create a date filter so we only get either 2 to 3 years (min 2 full fy)
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE date_lookup AS (
	SELECT 
		DC.week_num
		, DC.month_num
		, DC.quarter_num
		, DC.year_num
		, (CASE
			WHEN DC.week_num >= WN.week_num - 100 and DC.week_num <= WN.week_num THEN 'TY'
			WHEN  DC.week_num >= WN.week_num - 200 and DC.week_num < WN.week_num - 100 THEN 'LY'
			ELSE 'NA'
		END) AS year_id -- Create current year, last year, and prior year index
		, MIN(day_date) AS ty_start_dt
		, MAX(day_date) AS ty_end_dt
	FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC
	LEFT JOIN (
		SELECT DISTINCT
			week_num
		FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC
		WHERE 1 = 1
			AND day_date = CURRENT_DATE
	) AS WN
		ON 1 = 1
	WHERE 1 = 1
--		AND year_num between EXTRACT(YEAR FROM CURRENT_DATE)-2 and EXTRACT(YEAR FROM CURRENT_DATE)
--		AND day_date <= CURRENT_DATE
--		AND quarter_num IN (20234)
		-- AND day_date between (select start_date from sf_start_date) and current_date() -1
		AND DC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE) - 300 
		AND DC.week_num <= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE)
	GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX(week_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage 1.5: Create a UPC lookup information table
 * 
----------------------------------------------------------------------------------------------------------------*/

create multiset volatile table upc_lookup_table as (
	select distinct 
		ltrim(upc.upc_num,'0') upc_num
		, case when cast(div_num as int) in(310,340,345,351,360,365,600,700,800,900) then div_num else -1 end as div_num
		, case when div_num in(310,340,345,351,360,365,600,700,800,900) then div_desc else 'OTHER' end as div_desc
		, cast(grp_num as int) as subdiv_num
		, grp_desc subdiv_name
		, cast(dept_num as int) as dept_num
		, dept_desc dept_name
		, class_num
		, sbclass_num
		, brand_name
	from prd_nap_usr_vws.product_sku_dim_vw sku
	join prd_nap_usr_vws.product_upc_dim upc 
		on sku.rms_sku_num=upc.rms_sku_num 
			and sku.channel_country = upc.channel_country
	where 1 = 1
		AND sku.channel_country = 'US'
		AND sku.div_num in (310, 345, 360, 340, 365, 351,700)
) with data primary index(upc_num) on commit preserve rows;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num AS week_num_realigned
		, RC.month_num AS month_num_realigned
		, RC.quarter_num AS quarter_num_realigned
		, RC.year_num AS year_num_realigned
		, SCF.week_num 
		, SCF.month_num
		, SCF.quarter_num
		, SCF.year_num
		, DL.year_id
		, SCF.global_tran_id
		, SCF.line_item_seq_num
		, SCF.store_num
		, SCF.acp_id
		, SCF.sku_num
		, SCF.upc_num
		, COALESCE(DIV.div_desc, 'OTHER') AS div_desc
		, SCF.trip_id
		, SCF.employee_discount_flag
		, SCF.transaction_type_id
		, SCF.device_id
		, SCF.ship_method_id
		, SCF.price_type_id
		, SCF.line_net_usd_amt
		, SCF.giftcard_flag
		, SCF.items
		, SCF.returned_sales
		, SCF.returned_items
		, SCF.non_gc_amt
		, SCF.region
		, SCF.dma
		-- , SCF.store_segment
		-- , SCF.trade_area_type
		, SCF.engagement_cohort
		, SCF.predicted_segment
		, SCF.loyalty_level
		, SCF.loyalty_type
		, SCF.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
--		, SCF.cust_age
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	WHERE 1 = 1
		AND RC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -4)) 
		AND RC.week_num <= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE())
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS gross_spend
        , SUM(non_gc_amt) AS non_gc_spend
        , COUNT(DISTINCT trip_id) AS trips
        , SUM(items) AS items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt > 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS return_spend
        , SUM(items) AS return_items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt <= 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE usl_trips_sandbox_weekly_cust AS (
	SELECT
		-- Grouping Columns
		COALESCE(a.week_num, b.week_num) AS week_num
		, COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
		, COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned
		, COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned
		, COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned
		, COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned
		, COALESCE(a.year_id, b.year_id) AS year_id
		, COALESCE(a.acp_id, b.acp_id) AS acp_id
		, COALESCE(a.channel, b.channel) AS channel
		, COALESCE(a.banner, b.banner) AS banner
		, COALESCE(a.region, b.region) AS region
		, COALESCE(a.dma, b.dma) AS dma
		-- , COALESCE(a.store_segment, b.store_segment) AS store_segment
		-- , COALESCE(a.trade_area_type, b.trade_area_type) AS trade_area_type
		, COALESCE(a.engagement_cohort, b.engagement_cohort) AS AEC
		, COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment
		, COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level
		, COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type
--		, COALESCE(a.cust_age, b.cust_age) AS cust_age_bucket
		-- New to JWN metrics adjustment
		, CAST(CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS INTEGER) AS new_to_jwn
		-- Calculated Metrics
		, COALESCE(a.gross_spend, 0) AS gross_spend
		, COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend,0) AS net_spend
		, COALESCE(a.trips, 0) AS trips
		, COALESCE(a.items, 0) AS gross_units
		, COALESCE(a.items, 0) - COALESCE(b.return_items,0) AS net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , CASE WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1 THEN 1 ELSE 0 END AS div_accessories_flag
        -- APPAREL
        , CASE WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1 THEN 1 ELSE 0 END AS div_apparel_flag
        -- BEAUTY
        , CASE WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1 THEN 1 ELSE 0 END AS div_beauty_flag
        -- DESIGNER
        , CASE WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1 THEN 1 ELSE 0 END AS div_designer_flag
        -- HOME
        , CASE WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1 THEN 1 ELSE 0 END AS div_home_flag
        -- MERCH
        , CASE WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1 THEN 1 ELSE 0 END AS div_merch_flag
        -- SHOES
        , CASE WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1 THEN 1 ELSE 0 END AS div_shoes_flag
        -- OTHER
        , CASE WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1 THEN 1 ELSE 0 END AS div_other_flag
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.week_num = b.week_num
			AND a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
			AND a.week_num_realigned = b.week_num_realigned
			AND a.month_num_realigned = b.month_num_realigned 
			AND a.quarter_num_realigned = b.quarter_num_realigned 
			AND a.year_num_realigned = b.year_num_realigned
			AND a.year_id = b.year_id 
			AND a.acp_id = b.acp_id 
			AND a.channel = b.channel
			AND a.banner = b.banner 
			AND a.region = b.region
			AND a.dma = b.dma 
			-- AND a.store_segment = b.store_segment
			-- AND a.trade_area_type = b.trade_area_type 
			AND a.engagement_cohort = b.engagement_cohort 
			AND a.predicted_segment = b.predicted_segment
			AND a.loyalty_level = b.loyalty_level 
			AND a.loyalty_type = b.loyalty_type
--			AND a.cust_age = b.cust_age
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION Fill 2



*******************************************************************************************************************/

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/
drop table sales_information;

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num AS week_num_realigned
		, RC.month_num AS month_num_realigned
		, RC.quarter_num AS quarter_num_realigned
		, RC.year_num AS year_num_realigned
		, SCF.week_num 
		, SCF.month_num
		, SCF.quarter_num
		, SCF.year_num
		, DL.year_id
		, SCF.global_tran_id
		, SCF.line_item_seq_num
		, SCF.store_num
		, SCF.acp_id
		, SCF.sku_num
		, SCF.upc_num
		, COALESCE(DIV.div_desc, 'OTHER') AS div_desc
		, SCF.trip_id
		, SCF.employee_discount_flag
		, SCF.transaction_type_id
		, SCF.device_id
		, SCF.ship_method_id
		, SCF.price_type_id
		, SCF.line_net_usd_amt
		, SCF.giftcard_flag
		, SCF.items
		, SCF.returned_sales
		, SCF.returned_items
		, SCF.non_gc_amt
		, SCF.region
		, SCF.dma
		-- , SCF.store_segment
		-- , SCF.trade_area_type
		, SCF.engagement_cohort
		, SCF.predicted_segment
		, SCF.loyalty_level
		, SCF.loyalty_type
		, SCF.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
--		, SCF.cust_age
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	WHERE 1 = 1
		AND RC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -8)) 
		AND RC.week_num < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -4))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
drop table ty_positive;
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS gross_spend
        , SUM(non_gc_amt) AS non_gc_spend
        , COUNT(DISTINCT trip_id) AS trips
        , SUM(items) AS items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt > 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
drop table ty_negative;
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS return_spend
        , SUM(items) AS return_items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt <= 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO usl_trips_sandbox_weekly_cust  -- Insert new data
	SELECT
		-- Grouping Columns
		COALESCE(a.week_num, b.week_num) AS week_num
		, COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
		, COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned
		, COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned
		, COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned
		, COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned
		, COALESCE(a.year_id, b.year_id) AS year_id
		, COALESCE(a.acp_id, b.acp_id) AS acp_id
		, COALESCE(a.channel, b.channel) AS channel
		, COALESCE(a.banner, b.banner) AS banner
		, COALESCE(a.region, b.region) AS region
		, COALESCE(a.dma, b.dma) AS dma
		-- , COALESCE(a.store_segment, b.store_segment) AS store_segment
		-- , COALESCE(a.trade_area_type, b.trade_area_type) AS trade_area_type
		, COALESCE(a.engagement_cohort, b.engagement_cohort) AS AEC
		, COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment
		, COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level
		, COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type
--		, COALESCE(a.cust_age, b.cust_age) AS cust_age_bucket
		-- New to JWN metrics adjustment
		, CAST(CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS INTEGER) AS new_to_jwn
		-- Calculated Metrics
		, COALESCE(a.gross_spend, 0) AS gross_spend
		, COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend,0) AS net_spend
		, COALESCE(a.trips, 0) AS trips
		, COALESCE(a.items, 0) AS gross_units
		, COALESCE(a.items, 0) - COALESCE(b.return_items,0) AS net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , CASE WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1 THEN 1 ELSE 0 END AS div_accessories_flag
        -- APPAREL
        , CASE WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1 THEN 1 ELSE 0 END AS div_apparel_flag
        -- BEAUTY
        , CASE WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1 THEN 1 ELSE 0 END AS div_beauty_flag
        -- DESIGNER
        , CASE WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1 THEN 1 ELSE 0 END AS div_designer_flag
        -- HOME
        , CASE WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1 THEN 1 ELSE 0 END AS div_home_flag
        -- MERCH
        , CASE WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1 THEN 1 ELSE 0 END AS div_merch_flag
        -- SHOES
        , CASE WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1 THEN 1 ELSE 0 END AS div_shoes_flag
        -- OTHER
        , CASE WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1 THEN 1 ELSE 0 END AS div_other_flag
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.week_num = b.week_num
			AND a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
			AND a.week_num_realigned = b.week_num_realigned
			AND a.month_num_realigned = b.month_num_realigned 
			AND a.quarter_num_realigned = b.quarter_num_realigned 
			AND a.year_num_realigned = b.year_num_realigned
			AND a.year_id = b.year_id 
			AND a.acp_id = b.acp_id 
			AND a.channel = b.channel
			AND a.banner = b.banner 
			AND a.region = b.region
			AND a.dma = b.dma 
			-- AND a.store_segment = b.store_segment
			-- AND a.trade_area_type = b.trade_area_type 
			AND a.engagement_cohort = b.engagement_cohort 
			AND a.predicted_segment = b.predicted_segment
			AND a.loyalty_level = b.loyalty_level 
			AND a.loyalty_type = b.loyalty_type
--			AND a.cust_age = b.cust_age
;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION Fill 3



*******************************************************************************************************************/

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/
drop table sales_information;

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num AS week_num_realigned
		, RC.month_num AS month_num_realigned
		, RC.quarter_num AS quarter_num_realigned
		, RC.year_num AS year_num_realigned
		, SCF.week_num 
		, SCF.month_num
		, SCF.quarter_num
		, SCF.year_num
		, DL.year_id
		, SCF.global_tran_id
		, SCF.line_item_seq_num
		, SCF.store_num
		, SCF.acp_id
		, SCF.sku_num
		, SCF.upc_num
		, COALESCE(DIV.div_desc, 'OTHER') AS div_desc
		, SCF.trip_id
		, SCF.employee_discount_flag
		, SCF.transaction_type_id
		, SCF.device_id
		, SCF.ship_method_id
		, SCF.price_type_id
		, SCF.line_net_usd_amt
		, SCF.giftcard_flag
		, SCF.items
		, SCF.returned_sales
		, SCF.returned_items
		, SCF.non_gc_amt
		, SCF.region
		, SCF.dma
		-- , SCF.store_segment
		-- , SCF.trade_area_type
		, SCF.engagement_cohort
		, SCF.predicted_segment
		, SCF.loyalty_level
		, SCF.loyalty_type
		, SCF.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
--		, SCF.cust_age
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	WHERE 1 = 1
		AND RC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -12)) 
		AND RC.week_num < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -8))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
drop table ty_positive;
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS gross_spend
        , SUM(non_gc_amt) AS non_gc_spend
        , COUNT(DISTINCT trip_id) AS trips
        , SUM(items) AS items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt > 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
drop table ty_negative;
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS return_spend
        , SUM(items) AS return_items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt <= 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO usl_trips_sandbox_weekly_cust  -- Insert new data
	SELECT
		-- Grouping Columns
		COALESCE(a.week_num, b.week_num) AS week_num
		, COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
		, COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned
		, COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned
		, COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned
		, COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned
		, COALESCE(a.year_id, b.year_id) AS year_id
		, COALESCE(a.acp_id, b.acp_id) AS acp_id
		, COALESCE(a.channel, b.channel) AS channel
		, COALESCE(a.banner, b.banner) AS banner
		, COALESCE(a.region, b.region) AS region
		, COALESCE(a.dma, b.dma) AS dma
		-- , COALESCE(a.store_segment, b.store_segment) AS store_segment
		-- , COALESCE(a.trade_area_type, b.trade_area_type) AS trade_area_type
		, COALESCE(a.engagement_cohort, b.engagement_cohort) AS AEC
		, COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment
		, COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level
		, COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type
--		, COALESCE(a.cust_age, b.cust_age) AS cust_age_bucket
		-- New to JWN metrics adjustment
		, CAST(CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS INTEGER) AS new_to_jwn
		-- Calculated Metrics
		, COALESCE(a.gross_spend, 0) AS gross_spend
		, COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend,0) AS net_spend
		, COALESCE(a.trips, 0) AS trips
		, COALESCE(a.items, 0) AS gross_units
		, COALESCE(a.items, 0) - COALESCE(b.return_items,0) AS net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , CASE WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1 THEN 1 ELSE 0 END AS div_accessories_flag
        -- APPAREL
        , CASE WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1 THEN 1 ELSE 0 END AS div_apparel_flag
        -- BEAUTY
        , CASE WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1 THEN 1 ELSE 0 END AS div_beauty_flag
        -- DESIGNER
        , CASE WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1 THEN 1 ELSE 0 END AS div_designer_flag
        -- HOME
        , CASE WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1 THEN 1 ELSE 0 END AS div_home_flag
        -- MERCH
        , CASE WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1 THEN 1 ELSE 0 END AS div_merch_flag
        -- SHOES
        , CASE WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1 THEN 1 ELSE 0 END AS div_shoes_flag
        -- OTHER
        , CASE WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1 THEN 1 ELSE 0 END AS div_other_flag
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.week_num = b.week_num
			AND a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
			AND a.week_num_realigned = b.week_num_realigned
			AND a.month_num_realigned = b.month_num_realigned 
			AND a.quarter_num_realigned = b.quarter_num_realigned 
			AND a.year_num_realigned = b.year_num_realigned
			AND a.year_id = b.year_id 
			AND a.acp_id = b.acp_id 
			AND a.channel = b.channel
			AND a.banner = b.banner 
			AND a.region = b.region
			AND a.dma = b.dma 
			-- AND a.store_segment = b.store_segment
			-- AND a.trade_area_type = b.trade_area_type 
			AND a.engagement_cohort = b.engagement_cohort 
			AND a.predicted_segment = b.predicted_segment
			AND a.loyalty_level = b.loyalty_level 
			AND a.loyalty_type = b.loyalty_type
--			AND a.cust_age = b.cust_age
;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION Fill 4



*******************************************************************************************************************/

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/
drop table sales_information;

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num AS week_num_realigned
		, RC.month_num AS month_num_realigned
		, RC.quarter_num AS quarter_num_realigned
		, RC.year_num AS year_num_realigned
		, SCF.week_num 
		, SCF.month_num
		, SCF.quarter_num
		, SCF.year_num
		, DL.year_id
		, SCF.global_tran_id
		, SCF.line_item_seq_num
		, SCF.store_num
		, SCF.acp_id
		, SCF.sku_num
		, SCF.upc_num
		, COALESCE(DIV.div_desc, 'OTHER') AS div_desc
		, SCF.trip_id
		, SCF.employee_discount_flag
		, SCF.transaction_type_id
		, SCF.device_id
		, SCF.ship_method_id
		, SCF.price_type_id
		, SCF.line_net_usd_amt
		, SCF.giftcard_flag
		, SCF.items
		, SCF.returned_sales
		, SCF.returned_items
		, SCF.non_gc_amt
		, SCF.region
		, SCF.dma
		-- , SCF.store_segment
		-- , SCF.trade_area_type
		, SCF.engagement_cohort
		, SCF.predicted_segment
		, SCF.loyalty_level
		, SCF.loyalty_type
		, SCF.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
--		, SCF.cust_age
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	WHERE 1 = 1
		AND RC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -16)) 
		AND RC.week_num < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -12))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
drop table ty_positive;
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS gross_spend
        , SUM(non_gc_amt) AS non_gc_spend
        , COUNT(DISTINCT trip_id) AS trips
        , SUM(items) AS items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt > 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
drop table ty_negative;
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS return_spend
        , SUM(items) AS return_items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt <= 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO usl_trips_sandbox_weekly_cust  -- Insert new data
	SELECT
		-- Grouping Columns
		COALESCE(a.week_num, b.week_num) AS week_num
		, COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
		, COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned
		, COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned
		, COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned
		, COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned
		, COALESCE(a.year_id, b.year_id) AS year_id
		, COALESCE(a.acp_id, b.acp_id) AS acp_id
		, COALESCE(a.channel, b.channel) AS channel
		, COALESCE(a.banner, b.banner) AS banner
		, COALESCE(a.region, b.region) AS region
		, COALESCE(a.dma, b.dma) AS dma
		-- , COALESCE(a.store_segment, b.store_segment) AS store_segment
		-- , COALESCE(a.trade_area_type, b.trade_area_type) AS trade_area_type
		, COALESCE(a.engagement_cohort, b.engagement_cohort) AS AEC
		, COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment
		, COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level
		, COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type
--		, COALESCE(a.cust_age, b.cust_age) AS cust_age_bucket
		-- New to JWN metrics adjustment
		, CAST(CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS INTEGER) AS new_to_jwn
		-- Calculated Metrics
		, COALESCE(a.gross_spend, 0) AS gross_spend
		, COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend,0) AS net_spend
		, COALESCE(a.trips, 0) AS trips
		, COALESCE(a.items, 0) AS gross_units
		, COALESCE(a.items, 0) - COALESCE(b.return_items,0) AS net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , CASE WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1 THEN 1 ELSE 0 END AS div_accessories_flag
        -- APPAREL
        , CASE WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1 THEN 1 ELSE 0 END AS div_apparel_flag
        -- BEAUTY
        , CASE WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1 THEN 1 ELSE 0 END AS div_beauty_flag
        -- DESIGNER
        , CASE WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1 THEN 1 ELSE 0 END AS div_designer_flag
        -- HOME
        , CASE WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1 THEN 1 ELSE 0 END AS div_home_flag
        -- MERCH
        , CASE WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1 THEN 1 ELSE 0 END AS div_merch_flag
        -- SHOES
        , CASE WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1 THEN 1 ELSE 0 END AS div_shoes_flag
        -- OTHER
        , CASE WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1 THEN 1 ELSE 0 END AS div_other_flag
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.week_num = b.week_num
			AND a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
			AND a.week_num_realigned = b.week_num_realigned
			AND a.month_num_realigned = b.month_num_realigned 
			AND a.quarter_num_realigned = b.quarter_num_realigned 
			AND a.year_num_realigned = b.year_num_realigned
			AND a.year_id = b.year_id 
			AND a.acp_id = b.acp_id 
			AND a.channel = b.channel
			AND a.banner = b.banner 
			AND a.region = b.region
			AND a.dma = b.dma 
			-- AND a.store_segment = b.store_segment
			-- AND a.trade_area_type = b.trade_area_type 
			AND a.engagement_cohort = b.engagement_cohort 
			AND a.predicted_segment = b.predicted_segment
			AND a.loyalty_level = b.loyalty_level 
			AND a.loyalty_type = b.loyalty_type
--			AND a.cust_age = b.cust_age
;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION Fill 5



*******************************************************************************************************************/

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/
drop table sales_information;

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num AS week_num_realigned
		, RC.month_num AS month_num_realigned
		, RC.quarter_num AS quarter_num_realigned
		, RC.year_num AS year_num_realigned
		, SCF.week_num 
		, SCF.month_num
		, SCF.quarter_num
		, SCF.year_num
		, DL.year_id
		, SCF.global_tran_id
		, SCF.line_item_seq_num
		, SCF.store_num
		, SCF.acp_id
		, SCF.sku_num
		, SCF.upc_num
		, COALESCE(DIV.div_desc, 'OTHER') AS div_desc
		, SCF.trip_id
		, SCF.employee_discount_flag
		, SCF.transaction_type_id
		, SCF.device_id
		, SCF.ship_method_id
		, SCF.price_type_id
		, SCF.line_net_usd_amt
		, SCF.giftcard_flag
		, SCF.items
		, SCF.returned_sales
		, SCF.returned_items
		, SCF.non_gc_amt
		, SCF.region
		, SCF.dma
		-- , SCF.store_segment
		-- , SCF.trade_area_type
		, SCF.engagement_cohort
		, SCF.predicted_segment
		, SCF.loyalty_level
		, SCF.loyalty_type
		, SCF.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
--		, SCF.cust_age
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	WHERE 1 = 1
		AND RC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -20)) 
		AND RC.week_num < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -16))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
drop table ty_positive;
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS gross_spend
        , SUM(non_gc_amt) AS non_gc_spend
        , COUNT(DISTINCT trip_id) AS trips
        , SUM(items) AS items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt > 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
drop table ty_negative;
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS return_spend
        , SUM(items) AS return_items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt <= 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO usl_trips_sandbox_weekly_cust  -- Insert new data
	SELECT
		-- Grouping Columns
		COALESCE(a.week_num, b.week_num) AS week_num
		, COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
		, COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned
		, COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned
		, COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned
		, COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned
		, COALESCE(a.year_id, b.year_id) AS year_id
		, COALESCE(a.acp_id, b.acp_id) AS acp_id
		, COALESCE(a.channel, b.channel) AS channel
		, COALESCE(a.banner, b.banner) AS banner
		, COALESCE(a.region, b.region) AS region
		, COALESCE(a.dma, b.dma) AS dma
		-- , COALESCE(a.store_segment, b.store_segment) AS store_segment
		-- , COALESCE(a.trade_area_type, b.trade_area_type) AS trade_area_type
		, COALESCE(a.engagement_cohort, b.engagement_cohort) AS AEC
		, COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment
		, COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level
		, COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type
--		, COALESCE(a.cust_age, b.cust_age) AS cust_age_bucket
		-- New to JWN metrics adjustment
		, CAST(CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS INTEGER) AS new_to_jwn
		-- Calculated Metrics
		, COALESCE(a.gross_spend, 0) AS gross_spend
		, COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend,0) AS net_spend
		, COALESCE(a.trips, 0) AS trips
		, COALESCE(a.items, 0) AS gross_units
		, COALESCE(a.items, 0) - COALESCE(b.return_items,0) AS net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , CASE WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1 THEN 1 ELSE 0 END AS div_accessories_flag
        -- APPAREL
        , CASE WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1 THEN 1 ELSE 0 END AS div_apparel_flag
        -- BEAUTY
        , CASE WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1 THEN 1 ELSE 0 END AS div_beauty_flag
        -- DESIGNER
        , CASE WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1 THEN 1 ELSE 0 END AS div_designer_flag
        -- HOME
        , CASE WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1 THEN 1 ELSE 0 END AS div_home_flag
        -- MERCH
        , CASE WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1 THEN 1 ELSE 0 END AS div_merch_flag
        -- SHOES
        , CASE WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1 THEN 1 ELSE 0 END AS div_shoes_flag
        -- OTHER
        , CASE WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1 THEN 1 ELSE 0 END AS div_other_flag
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.week_num = b.week_num
			AND a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
			AND a.week_num_realigned = b.week_num_realigned
			AND a.month_num_realigned = b.month_num_realigned 
			AND a.quarter_num_realigned = b.quarter_num_realigned 
			AND a.year_num_realigned = b.year_num_realigned
			AND a.year_id = b.year_id 
			AND a.acp_id = b.acp_id 
			AND a.channel = b.channel
			AND a.banner = b.banner 
			AND a.region = b.region
			AND a.dma = b.dma 
			-- AND a.store_segment = b.store_segment
			-- AND a.trade_area_type = b.trade_area_type 
			AND a.engagement_cohort = b.engagement_cohort 
			AND a.predicted_segment = b.predicted_segment
			AND a.loyalty_level = b.loyalty_level 
			AND a.loyalty_type = b.loyalty_type
--			AND a.cust_age = b.cust_age
;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION Fill 6



*******************************************************************************************************************/

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/
drop table sales_information;

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num AS week_num_realigned
		, RC.month_num AS month_num_realigned
		, RC.quarter_num AS quarter_num_realigned
		, RC.year_num AS year_num_realigned
		, SCF.week_num 
		, SCF.month_num
		, SCF.quarter_num
		, SCF.year_num
		, DL.year_id
		, SCF.global_tran_id
		, SCF.line_item_seq_num
		, SCF.store_num
		, SCF.acp_id
		, SCF.sku_num
		, SCF.upc_num
		, COALESCE(DIV.div_desc, 'OTHER') AS div_desc
		, SCF.trip_id
		, SCF.employee_discount_flag
		, SCF.transaction_type_id
		, SCF.device_id
		, SCF.ship_method_id
		, SCF.price_type_id
		, SCF.line_net_usd_amt
		, SCF.giftcard_flag
		, SCF.items
		, SCF.returned_sales
		, SCF.returned_items
		, SCF.non_gc_amt
		, SCF.region
		, SCF.dma
		-- , SCF.store_segment
		-- , SCF.trade_area_type
		, SCF.engagement_cohort
		, SCF.predicted_segment
		, SCF.loyalty_level
		, SCF.loyalty_type
		, SCF.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
--		, SCF.cust_age
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	WHERE 1 = 1
		AND RC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -24)) 
		AND RC.week_num < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -20))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
drop table ty_positive;
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS gross_spend
        , SUM(non_gc_amt) AS non_gc_spend
        , COUNT(DISTINCT trip_id) AS trips
        , SUM(items) AS items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt > 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
drop table ty_negative;
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS return_spend
        , SUM(items) AS return_items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt <= 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO usl_trips_sandbox_weekly_cust  -- Insert new data
	SELECT
		-- Grouping Columns
		COALESCE(a.week_num, b.week_num) AS week_num
		, COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
		, COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned
		, COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned
		, COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned
		, COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned
		, COALESCE(a.year_id, b.year_id) AS year_id
		, COALESCE(a.acp_id, b.acp_id) AS acp_id
		, COALESCE(a.channel, b.channel) AS channel
		, COALESCE(a.banner, b.banner) AS banner
		, COALESCE(a.region, b.region) AS region
		, COALESCE(a.dma, b.dma) AS dma
		-- , COALESCE(a.store_segment, b.store_segment) AS store_segment
		-- , COALESCE(a.trade_area_type, b.trade_area_type) AS trade_area_type
		, COALESCE(a.engagement_cohort, b.engagement_cohort) AS AEC
		, COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment
		, COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level
		, COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type
--		, COALESCE(a.cust_age, b.cust_age) AS cust_age_bucket
		-- New to JWN metrics adjustment
		, CAST(CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS INTEGER) AS new_to_jwn
		-- Calculated Metrics
		, COALESCE(a.gross_spend, 0) AS gross_spend
		, COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend,0) AS net_spend
		, COALESCE(a.trips, 0) AS trips
		, COALESCE(a.items, 0) AS gross_units
		, COALESCE(a.items, 0) - COALESCE(b.return_items,0) AS net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , CASE WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1 THEN 1 ELSE 0 END AS div_accessories_flag
        -- APPAREL
        , CASE WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1 THEN 1 ELSE 0 END AS div_apparel_flag
        -- BEAUTY
        , CASE WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1 THEN 1 ELSE 0 END AS div_beauty_flag
        -- DESIGNER
        , CASE WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1 THEN 1 ELSE 0 END AS div_designer_flag
        -- HOME
        , CASE WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1 THEN 1 ELSE 0 END AS div_home_flag
        -- MERCH
        , CASE WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1 THEN 1 ELSE 0 END AS div_merch_flag
        -- SHOES
        , CASE WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1 THEN 1 ELSE 0 END AS div_shoes_flag
        -- OTHER
        , CASE WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1 THEN 1 ELSE 0 END AS div_other_flag
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.week_num = b.week_num
			AND a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
			AND a.week_num_realigned = b.week_num_realigned
			AND a.month_num_realigned = b.month_num_realigned 
			AND a.quarter_num_realigned = b.quarter_num_realigned 
			AND a.year_num_realigned = b.year_num_realigned
			AND a.year_id = b.year_id 
			AND a.acp_id = b.acp_id 
			AND a.channel = b.channel
			AND a.banner = b.banner 
			AND a.region = b.region
			AND a.dma = b.dma 
			-- AND a.store_segment = b.store_segment
			-- AND a.trade_area_type = b.trade_area_type 
			AND a.engagement_cohort = b.engagement_cohort 
			AND a.predicted_segment = b.predicted_segment
			AND a.loyalty_level = b.loyalty_level 
			AND a.loyalty_type = b.loyalty_type
--			AND a.cust_age = b.cust_age
;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION Fill 7 (ADDITIONAL MONTH FOR DOWNSTREAM TABLES)



*******************************************************************************************************************/

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/
drop table sales_information;

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num AS week_num_realigned
		, RC.month_num AS month_num_realigned
		, RC.quarter_num AS quarter_num_realigned
		, RC.year_num AS year_num_realigned
		, SCF.week_num 
		, SCF.month_num
		, SCF.quarter_num
		, SCF.year_num
		, DL.year_id
		, SCF.global_tran_id
		, SCF.line_item_seq_num
		, SCF.store_num
		, SCF.acp_id
		, SCF.sku_num
		, SCF.upc_num
		, COALESCE(DIV.div_desc, 'OTHER') AS div_desc
		, SCF.trip_id
		, SCF.employee_discount_flag
		, SCF.transaction_type_id
		, SCF.device_id
		, SCF.ship_method_id
		, SCF.price_type_id
		, SCF.line_net_usd_amt
		, SCF.giftcard_flag
		, SCF.items
		, SCF.returned_sales
		, SCF.returned_items
		, SCF.non_gc_amt
		, SCF.region
		, SCF.dma
		-- , SCF.store_segment
		-- , SCF.trade_area_type
		, SCF.engagement_cohort
		, SCF.predicted_segment
		, SCF.loyalty_level
		, SCF.loyalty_type
		, SCF.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
--		, SCF.cust_age
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	WHERE 1 = 1
		AND RC.week_num >= (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -25)) 
		AND RC.week_num < (SELECT DISTINCT week_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -24))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
drop table ty_positive;
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS gross_spend
        , SUM(non_gc_amt) AS non_gc_spend
        , COUNT(DISTINCT trip_id) AS trips
        , SUM(items) AS items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt > 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
drop table ty_negative;
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, channel
        , banner
        , region
        , dma
        -- , store_segment
        -- , trade_area_type
        , engagement_cohort
        , predicted_segment
        , loyalty_level
        , loyalty_type
--        , cust_age
        , acp_id
        , MAX(new_to_jwn) AS new_to_jwn
        -- Overall
        , SUM(line_net_usd_amt) AS return_spend
        , SUM(items) AS return_items
        --
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , MAX(CASE WHEN div_desc = 'ACCESSORIES' THEN 1 ELSE 0 END) AS div_accessories_flag
        -- APPAREL
        , MAX(CASE WHEN div_desc = 'APPAREL' THEN 1 ELSE 0 END) AS div_apparel_flag
        -- BEAUTY
        , MAX(CASE WHEN div_desc = 'BEAUTY' THEN 1 ELSE 0 END) AS div_beauty_flag
        -- DESIGNER
        , MAX(CASE WHEN div_desc = 'DESIGNER' THEN 1 ELSE 0 END) AS div_designer_flag
        -- HOME
        , MAX(CASE WHEN div_desc = 'HOME' THEN 1 ELSE 0 END) AS div_home_flag
        -- MERCH
        , MAX(CASE WHEN div_desc = 'MERCH PROJECTS' THEN 1 ELSE 0 END) AS div_merch_flag
        -- SHOES
        , MAX(CASE WHEN div_desc = 'SHOES' THEN 1 ELSE 0 END) AS div_shoes_flag
        -- OTHER
        , MAX(CASE WHEN div_desc = 'OTHER' THEN 1 ELSE 0 END) AS div_other_flag
   	FROM sales_information AS SF
	WHERE 1 = 1
		AND SF.sale_date >= (SELECT MIN(ty_start_dt) FROM date_lookup)
		AND SF.sale_date <= (SELECT MAX(ty_end_dt) FROM date_lookup)
		AND NOT SF.acp_id IS NULL
		AND line_net_usd_amt <= 0
    	AND business_unit_desc in (
		         'FULL LINE',
		         'FULL LINE CANADA',
		         'N.CA',
		         'N.COM',
		         'OFFPRICE ONLINE',
		         'RACK',
		         'RACK CANADA',
		         'TRUNK CLUB')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO usl_trips_sandbox_weekly_cust  -- Insert new data
	SELECT
		-- Grouping Columns
		COALESCE(a.week_num, b.week_num) AS week_num
		, COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
		, COALESCE(a.week_num_realigned, b.week_num_realigned) AS week_num_realigned
		, COALESCE(a.month_num_realigned, b.month_num_realigned) AS month_num_realigned
		, COALESCE(a.quarter_num_realigned, b.quarter_num_realigned) AS quarter_num_realigned
		, COALESCE(a.year_num_realigned, b.year_num_realigned) AS year_num_realigned
		, COALESCE(a.year_id, b.year_id) AS year_id
		, COALESCE(a.acp_id, b.acp_id) AS acp_id
		, COALESCE(a.channel, b.channel) AS channel
		, COALESCE(a.banner, b.banner) AS banner
		, COALESCE(a.region, b.region) AS region
		, COALESCE(a.dma, b.dma) AS dma
		-- , COALESCE(a.store_segment, b.store_segment) AS store_segment
		-- , COALESCE(a.trade_area_type, b.trade_area_type) AS trade_area_type
		, COALESCE(a.engagement_cohort, b.engagement_cohort) AS AEC
		, COALESCE(a.predicted_segment, b.predicted_segment) AS predicted_segment
		, COALESCE(a.loyalty_level, b.loyalty_level) AS loyalty_level
		, COALESCE(a.loyalty_type, b.loyalty_type) AS loyalty_type
--		, COALESCE(a.cust_age, b.cust_age) AS cust_age_bucket
		-- New to JWN metrics adjustment
		, CAST(CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS INTEGER) AS new_to_jwn
		-- Calculated Metrics
		, COALESCE(a.gross_spend, 0) AS gross_spend
		, COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend,0) AS net_spend
		, COALESCE(a.trips, 0) AS trips
		, COALESCE(a.items, 0) AS gross_units
		, COALESCE(a.items, 0) - COALESCE(b.return_items,0) AS net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , CASE WHEN a.div_accessories_flag >= 1 OR b.div_accessories_flag >= 1 THEN 1 ELSE 0 END AS div_accessories_flag
        -- APPAREL
        , CASE WHEN a.div_apparel_flag >= 1 OR b.div_apparel_flag >= 1 THEN 1 ELSE 0 END AS div_apparel_flag
        -- BEAUTY
        , CASE WHEN a.div_beauty_flag >= 1 OR b.div_beauty_flag >= 1 THEN 1 ELSE 0 END AS div_beauty_flag
        -- DESIGNER
        , CASE WHEN a.div_designer_flag >= 1 OR b.div_designer_flag >= 1 THEN 1 ELSE 0 END AS div_designer_flag
        -- HOME
        , CASE WHEN a.div_home_flag >= 1 OR b.div_home_flag >= 1 THEN 1 ELSE 0 END AS div_home_flag
        -- MERCH
        , CASE WHEN a.div_merch_flag >= 1 OR b.div_merch_flag >= 1 THEN 1 ELSE 0 END AS div_merch_flag
        -- SHOES
        , CASE WHEN a.div_shoes_flag >= 1 OR b.div_shoes_flag >= 1 THEN 1 ELSE 0 END AS div_shoes_flag
        -- OTHER
        , CASE WHEN a.div_other_flag >= 1 OR b.div_other_flag >= 1 THEN 1 ELSE 0 END AS div_other_flag
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.week_num = b.week_num
			AND a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
			AND a.week_num_realigned = b.week_num_realigned
			AND a.month_num_realigned = b.month_num_realigned 
			AND a.quarter_num_realigned = b.quarter_num_realigned 
			AND a.year_num_realigned = b.year_num_realigned
			AND a.year_id = b.year_id 
			AND a.acp_id = b.acp_id 
			AND a.channel = b.channel
			AND a.banner = b.banner 
			AND a.region = b.region
			AND a.dma = b.dma 
			-- AND a.store_segment = b.store_segment
			-- AND a.trade_area_type = b.trade_area_type 
			AND a.engagement_cohort = b.engagement_cohort 
			AND a.predicted_segment = b.predicted_segment
			AND a.loyalty_level = b.loyalty_level 
			AND a.loyalty_type = b.loyalty_type
--			AND a.cust_age = b.cust_age
;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Five: Create the Final Table
 * 
----------------------------------------------------------------------------------------------------------------*/

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------

*/

DELETE FROM {usl_t2_schema}.trips_sandbox_weekly_cust ALL; -- Remove all historic data to prevent overlaps

--
INSERT INTO {usl_t2_schema}.trips_sandbox_weekly_cust  -- Insert new data
	SELECT
		-- Grouping Columns
		week_num
		, month_num
		, quarter_num
		, year_num
		, week_num_realigned
		, month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, year_id
		, acp_id
		, channel
		, banner
		, region
		, dma
		-- , store_segment
		-- , trade_area_type
		, COALESCE(AEC, 'UNDEFINED') AS AEC
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
		, NULL AS cust_age_bucket
		-- New to JWN metrics adjustment
		-- Calculated Metrics
		, gross_spend
		, net_spend
		, trips
		, gross_units
		, net_units
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECTS', 'SHOES')
        --
        -- ACCESSORIES
        , div_accessories_flag
        -- APPAREL
        , div_apparel_flag
        -- BEAUTY
        , div_beauty_flag
        -- DESIGNER
        , div_designer_flag
        -- HOME
        , div_home_flag
        -- MERCH
        , div_merch_flag
        -- SHOES
        , div_shoes_flag
        -- OTHER
        , div_other_flag
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp  
	FROM usl_trips_sandbox_weekly_cust
;

COLLECT STATISTICS COLUMN (acp_id, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id, region) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id, dma) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id, aec) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id, predicted_segment) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id, loyalty_level) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id, loyalty_type) ON {usl_t2_schema}.trips_sandbox_weekly_cust;
COLLECT STATISTICS COLUMN (acp_id, new_to_jwn) ON {usl_t2_schema}.trips_sandbox_weekly_cust;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;