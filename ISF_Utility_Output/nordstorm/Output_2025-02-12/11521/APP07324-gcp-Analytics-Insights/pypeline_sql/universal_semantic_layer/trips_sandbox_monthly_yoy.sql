/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=trips_sandbox_weekly_cust_11521_ACE_ENG;
     Task_Name=trips_sandbox_monthly_yoy;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: {usl_t2_schema}.trips_sandbox_monthly_yoy
Team/Owner: Customer Analytics
Date Created/Modified: Mar 26 2024

Note:
-- Purpose of the table: Table to get monthly trips metrics by customer attributes.
-- Update Cadence: Monthly

*/

/*
 * Written: Ian Rasquinha
 * Date Created: 3/21/2024
 * Date Last Edited: 3/26/2024
 * 
 * Goal: The goal of this code is to create a table level before joining on any of the customer attributes to correct the trips first. 
 */
/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION START 1



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
			WHEN DC.month_num >= WN.month_num - 100 and DC.month_num <= WN.month_num THEN 'TY'
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
		and DC.month_num <= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE)
--		AND quarter_num IN (20241,20231)
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
 * Stage 1.8: Customer Attribute Lookup
 * 
----------------------------------------------------------------------------------------------------------------*/

 CREATE VOLATILE MULTISET TABLE customer_single_attribute AS (
--
	SELECT
		month_num_realigned
		, quarter_num_realigned
		, year_num_realigned
		, acp_id
		, region
		, dma
		, aec AS engagement_cohort
		, predicted_segment
		, loyalty_level
		, loyalty_type
		, new_to_jwn
	FROM {usl_t2_schema}.trips_sandbox_cust_single_attribute 
	WHERE 1 = 1 
		AND time_granularity = 'MONTH'
--
) WITH DATA PRIMARY INDEX(acp_id, month_num_realigned, quarter_num_realigned, year_num_realigned) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Two: Connect the sales_cust_fact table to division description and filter based on date of interests
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE sales_information AS (
	SELECT
		SCF.sale_date
		, RC.week_num 
		, RC.month_num
		, RC.quarter_num
		, RC.year_num
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
		, TSA.region
		, TSA.dma
		, TSA.engagement_cohort
		, TSA.predicted_segment
		, TSA.loyalty_level
		, TSA.loyalty_type
		, TSA.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	LEFT JOIN customer_single_attribute AS TSA
		ON TSA.acp_id = SCF.acp_id
			AND RC.month_num = TSA.month_num_realigned
			AND RC.quarter_num = TSA.quarter_num_realigned
			AND RC.year_num = TSA.year_num_realigned
	WHERE 1 = 1
		AND RC.month_num >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -8)) 
		AND RC.month_num <= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = CURRENT_DATE())
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

-- POSITIVE
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		month_num
		, quarter_num
		, year_num
		, NULL AS year_id
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
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN line_net_usd_amt END) AS gross_spend_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN non_gc_amt END) AS non_gc_spend_accessories
        , COUNT(DISTINCT CASE WHEN div_desc = 'ACCESSORIES' THEN trip_id END) AS trips_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN items END) AS items_accessories
        -- APPAREL
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN line_net_usd_amt END) AS gross_spend_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN non_gc_amt END) AS non_gc_spend_apparel
        , COUNT(DISTINCT CASE WHEN div_desc = 'APPAREL' THEN trip_id END) AS trips_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN items END) AS items_apparel
        -- BEAUTY
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN line_net_usd_amt END) AS gross_spend_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN non_gc_amt END) AS non_gc_spend_beauty
        , COUNT(DISTINCT CASE WHEN div_desc = 'BEAUTY' THEN trip_id END) AS trips_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN items END) AS items_beauty
        -- DESIGNER
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN line_net_usd_amt END) AS gross_spend_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN non_gc_amt END) AS non_gc_spend_designer
        , COUNT(DISTINCT CASE WHEN div_desc = 'DESIGNER' THEN trip_id END) AS trips_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN items END) AS items_designer
        -- HOME
        , SUM(CASE WHEN div_desc = 'HOME' THEN line_net_usd_amt END) AS gross_spend_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN non_gc_amt END) AS non_gc_spend_home
        , COUNT(DISTINCT CASE WHEN div_desc = 'HOME' THEN trip_id END) AS trips_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN items END) AS items_home
        -- MERCH
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN line_net_usd_amt END) AS gross_spend_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN non_gc_amt END) AS non_gc_spend_merch
        , COUNT(DISTINCT CASE WHEN div_desc = 'MERCH PROJECTS' THEN trip_id END) AS trips_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN items END) AS items_merch
        -- SHOES
        , SUM(CASE WHEN div_desc = 'SHOES' THEN line_net_usd_amt END) AS gross_spend_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN non_gc_amt END) AS non_gc_spend_shoes
        , COUNT(DISTINCT CASE WHEN div_desc = 'SHOES' THEN trip_id END) AS trips_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN items END) AS items_shoes
        -- OTHER
        , SUM(CASE WHEN div_desc = 'OTHER' THEN line_net_usd_amt END) AS gross_spend_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN non_gc_amt END) AS non_gc_spend_other
        , COUNT(DISTINCT CASE WHEN div_desc = 'OTHER' THEN trip_id END) AS trips_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN items END) AS items_other
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
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

-- Negative
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		month_num
		, quarter_num
		, year_num
		, NULL AS year_id
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
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN line_net_usd_amt END) AS return_spend_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN items END) AS return_items_accessories
        -- APPAREL
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN line_net_usd_amt END) AS return_spend_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN items END) AS return_items_apparel
        -- BEAUTY
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN line_net_usd_amt END) AS return_spend_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN items END) AS return_items_beauty
        -- DESIGNER
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN line_net_usd_amt END) AS return_spend_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN items END) AS return_items_designer
        -- HOME
        , SUM(CASE WHEN div_desc = 'HOME' THEN line_net_usd_amt END) AS return_spend_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN items END) AS return_items_home
        -- MERCH
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN line_net_usd_amt END) AS return_spend_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN items END) AS return_items_merch
        -- SHOES
        , SUM(CASE WHEN div_desc = 'SHOES' THEN line_net_usd_amt END) AS return_spend_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN items END) AS return_items_shoes
        -- OTHER
        , SUM(CASE WHEN div_desc = 'OTHER' THEN line_net_usd_amt END) AS return_spend_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN items END) AS return_items_other
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
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE VOLATILE MULTISET TABLE TY AS (
	SELECT
		-- Grouping Columns
		COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
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
		, CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS new_to_jwn
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
		, COALESCE(a.gross_spend_accessories, 0) AS gross_spend_accessories
		, COALESCE(a.non_gc_spend_accessories, 0) + COALESCE(b.return_spend_accessories,0) AS net_spend_accessories
		, COALESCE(a.trips_accessories, 0) AS trips_accessories
		, COALESCE(a.items_accessories, 0) AS gross_units_accessories
		, COALESCE(a.items_accessories, 0) - COALESCE(b.return_items_accessories,0) AS net_units_accessories
        -- APPAREL
		, COALESCE(a.gross_spend_apparel, 0) AS gross_spend_apparel
		, COALESCE(a.non_gc_spend_apparel, 0) + COALESCE(b.return_spend_apparel,0) AS net_spend_apparel
		, COALESCE(a.trips_apparel, 0) AS trips_apparel
		, COALESCE(a.items_apparel, 0) AS gross_units_apparel
		, COALESCE(a.items_apparel, 0) - COALESCE(b.return_items_apparel,0) AS net_units_apparel
        -- BEAUTY
		, COALESCE(a.gross_spend_beauty, 0) AS gross_spend_beauty
		, COALESCE(a.non_gc_spend_beauty, 0) + COALESCE(b.return_spend_beauty,0) AS net_spend_beauty
		, COALESCE(a.trips_beauty, 0) AS trips_beauty
		, COALESCE(a.items_beauty, 0) AS gross_units_beauty
		, COALESCE(a.items_beauty, 0) - COALESCE(b.return_items_beauty,0) AS net_units_beauty
        -- DESIGNER
		, COALESCE(a.gross_spend_designer, 0) AS gross_spend_designer
		, COALESCE(a.non_gc_spend_designer, 0) + COALESCE(b.return_spend_designer,0) AS net_spend_designer
		, COALESCE(a.trips_designer, 0) AS trips_designer
		, COALESCE(a.items_designer, 0) AS gross_units_designer
		, COALESCE(a.items_designer, 0) - COALESCE(b.return_items_designer,0) AS net_units_designer
        -- HOME
		, COALESCE(a.gross_spend_home, 0) AS gross_spend_home
		, COALESCE(a.non_gc_spend_home, 0) + COALESCE(b.return_spend_home,0) AS net_spend_home
		, COALESCE(a.trips_home, 0) AS trips_home
		, COALESCE(a.items_home, 0) AS gross_units_home
		, COALESCE(a.items_home, 0) - COALESCE(b.return_items_home,0) AS net_units_home
        -- MERCH
		, COALESCE(a.gross_spend_merch, 0) AS gross_spend_merch
		, COALESCE(a.non_gc_spend_merch, 0) + COALESCE(b.return_spend_merch,0) AS net_spend_merch
		, COALESCE(a.trips_merch, 0) AS trips_merch
		, COALESCE(a.items_merch, 0) AS gross_units_merch
		, COALESCE(a.items_merch, 0) - COALESCE(b.return_items_merch,0) AS net_units_merch
        -- SHOES
		, COALESCE(a.gross_spend_shoes, 0) AS gross_spend_shoes
		, COALESCE(a.non_gc_spend_shoes, 0) + COALESCE(b.return_spend_shoes,0) AS net_spend_shoes
		, COALESCE(a.trips_shoes, 0) AS trips_shoes
		, COALESCE(a.items_shoes, 0) AS gross_units_shoes
		, COALESCE(a.items_shoes, 0) - COALESCE(b.return_items_shoes,0) AS net_units_shoes
        -- OTHER
		, COALESCE(a.gross_spend_other, 0) AS gross_spend_other
		, COALESCE(a.non_gc_spend_other, 0) + COALESCE(b.return_spend_other,0) AS net_spend_other
		, COALESCE(a.trips_other, 0) AS trips_other
		, COALESCE(a.items_other, 0) AS gross_units_other
		, COALESCE(a.items_other, 0) - COALESCE(b.return_items_other,0) AS net_units_other
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
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
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Five: Aggregate to a weekly basis but split by overall and divisions
 * 
----------------------------------------------------------------------------------------------------------------*/

CREATE MULTISET VOLATILE TABLE trip_summary_overall AS (
	SELECT
		-- Grouping Columns
		month_num
		, quarter_num
		, year_num
		, year_id
		, region
		, dma
		-- , store_segment
		-- , trade_area_type
		, AEC
		, predicted_segment
		, loyalty_level
		, loyalty_type
--		, cust_age_bucket
		, new_to_jwn
		--
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COUNT(DISTINCT CASE WHEN channel = '1) Nordstrom Stores' THEN acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN channel = '2) Nordstrom.com' THEN acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN channel = '3) Rack Stores' THEN acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN channel = '4) Rack.com' THEN acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN channel IN ('1) Nordstrom Stores','3) Rack Stores') THEN acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN channel IN ('2) Nordstrom.com','4) Rack.com') THEN acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN banner = '1) Nordstrom Banner' THEN acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN banner = '2) Rack Banner' THEN acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT acp_id) AS cust_count_jwn
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
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_accessories END) AS NS_accessories_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_accessories END) AS NCOM_accessories_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_accessories END) AS RS_accessories_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_accessories END) AS RCOM_accessories_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_accessories END) AS NS_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_accessories END) AS NCOM_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_accessories END) AS RS_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_accessories END) AS RCOM_accessories_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_accessories END) AS NS_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com'THEN net_units_accessories END) AS NCOM_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_accessories END) AS RS_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_accessories END) AS RCOM_accessories_weekly_net_units
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_apparel END) AS NS_apparel_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_apparel END) AS NCOM_apparel_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_apparel END) AS RS_apparel_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_apparel END) AS RCOM_apparel_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_apparel END) AS NS_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_apparel END) AS NCOM_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_apparel END) AS RS_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_apparel END) AS RCOM_apparel_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_apparel END) AS NS_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_apparel END) AS NCOM_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_apparel END) AS RS_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_apparel END) AS RCOM_apparel_weekly_net_units
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_beauty END) AS NS_beauty_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_beauty END) AS NCOM_beauty_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_beauty END) AS RS_beauty_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_beauty END) AS RCOM_beauty_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_beauty END) AS NS_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_beauty END) AS NCOM_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_beauty END) AS RS_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_beauty END) AS RCOM_beauty_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_beauty END) AS NS_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_beauty END) AS NCOM_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_beauty END) AS RS_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_beauty END) AS RCOM_beauty_weekly_net_units
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_designer END) AS NS_designer_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_designer END) AS NCOM_designer_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_designer END) AS RS_designer_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_designer END) AS RCOM_designer_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_designer END) AS NS_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_designer END) AS NCOM_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_designer END) AS RS_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_designer END) AS RCOM_designer_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_designer END) AS NS_designer_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_designer END) AS NCOM_designer_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_designer END) AS RS_designer_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_designer END) AS RCOM_designer_weekly_net_units
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_home END) AS NS_home_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_home END) AS NCOM_home_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_home END) AS RS_home_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_home END) AS RCOM_home_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_home END) AS NS_home_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_home END) AS NCOM_home_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_home END) AS RS_home_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_home END) AS RCOM_home_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_home END) AS NS_home_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_home END) AS NCOM_home_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_home END) AS RS_home_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_home END) AS RCOM_home_weekly_net_units
		--
		-- MERCH PROJECTS
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_merch END) AS NS_merch_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_merch END) AS NCOM_merch_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_merch END) AS RS_merch_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_merch END) AS RCOM_merch_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_merch END) AS NS_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_merch END) AS NCOM_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_merch END) AS RS_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_merch END) AS RCOM_merch_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_merch END) AS NS_merch_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_merch END) AS NCOM_merch_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_merch END) AS RS_merch_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_merch END) AS RCOM_merch_weekly_net_units
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_shoes END) AS NS_shoes_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_shoes END) AS NCOM_shoes_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_shoes END) AS RS_shoes_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_shoes END) AS RCOM_shoes_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_shoes END) AS NS_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_shoes END) AS NCOM_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_shoes END) AS RS_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_shoes END) AS RCOM_shoes_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_shoes END) AS NS_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_shoes END) AS NCOM_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_shoes END) AS RS_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_shoes END) AS RCOM_shoes_weekly_net_units
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_other END) AS NS_other_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_other END) AS NCOM_other_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_other END) AS RS_other_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_other END) AS RCOM_other_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_other END) AS NS_other_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_other END) AS NCOM_other_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_other END) AS RS_other_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_other END) AS RCOM_other_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_other END) AS NS_other_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_other END) AS NCOM_other_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_other END) AS RS_other_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_other END) AS RCOM_other_weekly_net_units
	FROM TY
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)WITH DATA PRIMARY INDEX(quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (region) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (dma) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (aec) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (predicted_segment) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (loyalty_level) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (loyalty_type) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (new_to_jwn) ON trip_summary_overall;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION START 2



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
		, RC.week_num 
		, RC.month_num
		, RC.quarter_num
		, RC.year_num
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
		, TSA.region
		, TSA.dma
		, TSA.engagement_cohort
		, TSA.predicted_segment
		, TSA.loyalty_level
		, TSA.loyalty_type
		, TSA.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	LEFT JOIN customer_single_attribute AS TSA
		ON TSA.acp_id = SCF.acp_id
			AND RC.month_num = TSA.month_num_realigned
			AND RC.quarter_num = TSA.quarter_num_realigned
			AND RC.year_num = TSA.year_num_realigned
	WHERE 1 = 1
		AND RC.month_num >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -16)) 
		AND RC.month_num < (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -8))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

drop table ty_positive;

-- POSITIVE
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		month_num
		, quarter_num
		, year_num
		, NULL AS year_id
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
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN line_net_usd_amt END) AS gross_spend_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN non_gc_amt END) AS non_gc_spend_accessories
        , COUNT(DISTINCT CASE WHEN div_desc = 'ACCESSORIES' THEN trip_id END) AS trips_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN items END) AS items_accessories
        -- APPAREL
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN line_net_usd_amt END) AS gross_spend_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN non_gc_amt END) AS non_gc_spend_apparel
        , COUNT(DISTINCT CASE WHEN div_desc = 'APPAREL' THEN trip_id END) AS trips_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN items END) AS items_apparel
        -- BEAUTY
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN line_net_usd_amt END) AS gross_spend_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN non_gc_amt END) AS non_gc_spend_beauty
        , COUNT(DISTINCT CASE WHEN div_desc = 'BEAUTY' THEN trip_id END) AS trips_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN items END) AS items_beauty
        -- DESIGNER
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN line_net_usd_amt END) AS gross_spend_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN non_gc_amt END) AS non_gc_spend_designer
        , COUNT(DISTINCT CASE WHEN div_desc = 'DESIGNER' THEN trip_id END) AS trips_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN items END) AS items_designer
        -- HOME
        , SUM(CASE WHEN div_desc = 'HOME' THEN line_net_usd_amt END) AS gross_spend_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN non_gc_amt END) AS non_gc_spend_home
        , COUNT(DISTINCT CASE WHEN div_desc = 'HOME' THEN trip_id END) AS trips_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN items END) AS items_home
        -- MERCH
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN line_net_usd_amt END) AS gross_spend_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN non_gc_amt END) AS non_gc_spend_merch
        , COUNT(DISTINCT CASE WHEN div_desc = 'MERCH PROJECTS' THEN trip_id END) AS trips_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN items END) AS items_merch
        -- SHOES
        , SUM(CASE WHEN div_desc = 'SHOES' THEN line_net_usd_amt END) AS gross_spend_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN non_gc_amt END) AS non_gc_spend_shoes
        , COUNT(DISTINCT CASE WHEN div_desc = 'SHOES' THEN trip_id END) AS trips_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN items END) AS items_shoes
        -- OTHER
        , SUM(CASE WHEN div_desc = 'OTHER' THEN line_net_usd_amt END) AS gross_spend_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN non_gc_amt END) AS non_gc_spend_other
        , COUNT(DISTINCT CASE WHEN div_desc = 'OTHER' THEN trip_id END) AS trips_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN items END) AS items_other
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
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

drop table ty_negative;

-- Negative
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		month_num
		, quarter_num
		, year_num
		, NULL AS year_id
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
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN line_net_usd_amt END) AS return_spend_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN items END) AS return_items_accessories
        -- APPAREL
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN line_net_usd_amt END) AS return_spend_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN items END) AS return_items_apparel
        -- BEAUTY
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN line_net_usd_amt END) AS return_spend_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN items END) AS return_items_beauty
        -- DESIGNER
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN line_net_usd_amt END) AS return_spend_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN items END) AS return_items_designer
        -- HOME
        , SUM(CASE WHEN div_desc = 'HOME' THEN line_net_usd_amt END) AS return_spend_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN items END) AS return_items_home
        -- MERCH
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN line_net_usd_amt END) AS return_spend_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN items END) AS return_items_merch
        -- SHOES
        , SUM(CASE WHEN div_desc = 'SHOES' THEN line_net_usd_amt END) AS return_spend_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN items END) AS return_items_shoes
        -- OTHER
        , SUM(CASE WHEN div_desc = 'OTHER' THEN line_net_usd_amt END) AS return_spend_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN items END) AS return_items_other
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
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

drop table TY;

CREATE VOLATILE MULTISET TABLE TY AS (
	SELECT
		-- Grouping Columns
		COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
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
		, CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS new_to_jwn
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
		, COALESCE(a.gross_spend_accessories, 0) AS gross_spend_accessories
		, COALESCE(a.non_gc_spend_accessories, 0) + COALESCE(b.return_spend_accessories,0) AS net_spend_accessories
		, COALESCE(a.trips_accessories, 0) AS trips_accessories
		, COALESCE(a.items_accessories, 0) AS gross_units_accessories
		, COALESCE(a.items_accessories, 0) - COALESCE(b.return_items_accessories,0) AS net_units_accessories
        -- APPAREL
		, COALESCE(a.gross_spend_apparel, 0) AS gross_spend_apparel
		, COALESCE(a.non_gc_spend_apparel, 0) + COALESCE(b.return_spend_apparel,0) AS net_spend_apparel
		, COALESCE(a.trips_apparel, 0) AS trips_apparel
		, COALESCE(a.items_apparel, 0) AS gross_units_apparel
		, COALESCE(a.items_apparel, 0) - COALESCE(b.return_items_apparel,0) AS net_units_apparel
        -- BEAUTY
		, COALESCE(a.gross_spend_beauty, 0) AS gross_spend_beauty
		, COALESCE(a.non_gc_spend_beauty, 0) + COALESCE(b.return_spend_beauty,0) AS net_spend_beauty
		, COALESCE(a.trips_beauty, 0) AS trips_beauty
		, COALESCE(a.items_beauty, 0) AS gross_units_beauty
		, COALESCE(a.items_beauty, 0) - COALESCE(b.return_items_beauty,0) AS net_units_beauty
        -- DESIGNER
		, COALESCE(a.gross_spend_designer, 0) AS gross_spend_designer
		, COALESCE(a.non_gc_spend_designer, 0) + COALESCE(b.return_spend_designer,0) AS net_spend_designer
		, COALESCE(a.trips_designer, 0) AS trips_designer
		, COALESCE(a.items_designer, 0) AS gross_units_designer
		, COALESCE(a.items_designer, 0) - COALESCE(b.return_items_designer,0) AS net_units_designer
        -- HOME
		, COALESCE(a.gross_spend_home, 0) AS gross_spend_home
		, COALESCE(a.non_gc_spend_home, 0) + COALESCE(b.return_spend_home,0) AS net_spend_home
		, COALESCE(a.trips_home, 0) AS trips_home
		, COALESCE(a.items_home, 0) AS gross_units_home
		, COALESCE(a.items_home, 0) - COALESCE(b.return_items_home,0) AS net_units_home
        -- MERCH
		, COALESCE(a.gross_spend_merch, 0) AS gross_spend_merch
		, COALESCE(a.non_gc_spend_merch, 0) + COALESCE(b.return_spend_merch,0) AS net_spend_merch
		, COALESCE(a.trips_merch, 0) AS trips_merch
		, COALESCE(a.items_merch, 0) AS gross_units_merch
		, COALESCE(a.items_merch, 0) - COALESCE(b.return_items_merch,0) AS net_units_merch
        -- SHOES
		, COALESCE(a.gross_spend_shoes, 0) AS gross_spend_shoes
		, COALESCE(a.non_gc_spend_shoes, 0) + COALESCE(b.return_spend_shoes,0) AS net_spend_shoes
		, COALESCE(a.trips_shoes, 0) AS trips_shoes
		, COALESCE(a.items_shoes, 0) AS gross_units_shoes
		, COALESCE(a.items_shoes, 0) - COALESCE(b.return_items_shoes,0) AS net_units_shoes
        -- OTHER
		, COALESCE(a.gross_spend_other, 0) AS gross_spend_other
		, COALESCE(a.non_gc_spend_other, 0) + COALESCE(b.return_spend_other,0) AS net_spend_other
		, COALESCE(a.trips_other, 0) AS trips_other
		, COALESCE(a.items_other, 0) AS gross_units_other
		, COALESCE(a.items_other, 0) - COALESCE(b.return_items_other,0) AS net_units_other
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
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
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Five: Aggregate to a weekly basis but split by overall and divisions
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO trip_summary_overall
	SELECT
		-- Grouping Columns
		month_num
		, quarter_num
		, year_num
		, year_id
		, region
		, dma
		-- , store_segment
		-- , trade_area_type
		, AEC
		, predicted_segment
		, loyalty_level
		, loyalty_type
--		, cust_age_bucket
		, new_to_jwn
		--
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COUNT(DISTINCT CASE WHEN channel = '1) Nordstrom Stores' THEN acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN channel = '2) Nordstrom.com' THEN acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN channel = '3) Rack Stores' THEN acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN channel = '4) Rack.com' THEN acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN channel IN ('1) Nordstrom Stores','3) Rack Stores') THEN acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN channel IN ('2) Nordstrom.com','4) Rack.com') THEN acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN banner = '1) Nordstrom Banner' THEN acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN banner = '2) Rack Banner' THEN acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT acp_id) AS cust_count_jwn
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
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_accessories END) AS NS_accessories_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_accessories END) AS NCOM_accessories_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_accessories END) AS RS_accessories_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_accessories END) AS RCOM_accessories_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_accessories END) AS NS_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_accessories END) AS NCOM_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_accessories END) AS RS_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_accessories END) AS RCOM_accessories_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_accessories END) AS NS_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com'THEN net_units_accessories END) AS NCOM_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_accessories END) AS RS_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_accessories END) AS RCOM_accessories_weekly_net_units
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_apparel END) AS NS_apparel_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_apparel END) AS NCOM_apparel_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_apparel END) AS RS_apparel_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_apparel END) AS RCOM_apparel_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_apparel END) AS NS_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_apparel END) AS NCOM_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_apparel END) AS RS_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_apparel END) AS RCOM_apparel_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_apparel END) AS NS_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_apparel END) AS NCOM_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_apparel END) AS RS_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_apparel END) AS RCOM_apparel_weekly_net_units
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_beauty END) AS NS_beauty_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_beauty END) AS NCOM_beauty_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_beauty END) AS RS_beauty_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_beauty END) AS RCOM_beauty_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_beauty END) AS NS_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_beauty END) AS NCOM_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_beauty END) AS RS_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_beauty END) AS RCOM_beauty_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_beauty END) AS NS_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_beauty END) AS NCOM_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_beauty END) AS RS_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_beauty END) AS RCOM_beauty_weekly_net_units
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_designer END) AS NS_designer_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_designer END) AS NCOM_designer_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_designer END) AS RS_designer_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_designer END) AS RCOM_designer_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_designer END) AS NS_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_designer END) AS NCOM_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_designer END) AS RS_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_designer END) AS RCOM_designer_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_designer END) AS NS_designer_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_designer END) AS NCOM_designer_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_designer END) AS RS_designer_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_designer END) AS RCOM_designer_weekly_net_units
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_home END) AS NS_home_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_home END) AS NCOM_home_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_home END) AS RS_home_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_home END) AS RCOM_home_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_home END) AS NS_home_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_home END) AS NCOM_home_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_home END) AS RS_home_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_home END) AS RCOM_home_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_home END) AS NS_home_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_home END) AS NCOM_home_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_home END) AS RS_home_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_home END) AS RCOM_home_weekly_net_units
		--
		-- MERCH PROJECTS
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_merch END) AS NS_merch_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_merch END) AS NCOM_merch_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_merch END) AS RS_merch_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_merch END) AS RCOM_merch_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_merch END) AS NS_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_merch END) AS NCOM_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_merch END) AS RS_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_merch END) AS RCOM_merch_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_merch END) AS NS_merch_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_merch END) AS NCOM_merch_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_merch END) AS RS_merch_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_merch END) AS RCOM_merch_weekly_net_units
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_shoes END) AS NS_shoes_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_shoes END) AS NCOM_shoes_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_shoes END) AS RS_shoes_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_shoes END) AS RCOM_shoes_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_shoes END) AS NS_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_shoes END) AS NCOM_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_shoes END) AS RS_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_shoes END) AS RCOM_shoes_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_shoes END) AS NS_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_shoes END) AS NCOM_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_shoes END) AS RS_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_shoes END) AS RCOM_shoes_weekly_net_units
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_other END) AS NS_other_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_other END) AS NCOM_other_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_other END) AS RS_other_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_other END) AS RCOM_other_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_other END) AS NS_other_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_other END) AS NCOM_other_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_other END) AS RS_other_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_other END) AS RCOM_other_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_other END) AS NS_other_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_other END) AS NCOM_other_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_other END) AS RS_other_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_other END) AS RCOM_other_weekly_net_units
	FROM TY
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
--)WITH DATA PRIMARY INDEX(quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;
	;

COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (region) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (dma) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (aec) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (predicted_segment) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (loyalty_level) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (loyalty_type) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (new_to_jwn) ON trip_summary_overall;

/******************************************************************************************************************



SECTION ONE: CODE ACQUISITION START 3



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
		, RC.week_num 
		, RC.month_num
		, RC.quarter_num
		, RC.year_num
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
		, TSA.region
		, TSA.dma
		, TSA.engagement_cohort
		, TSA.predicted_segment
		, TSA.loyalty_level
		, TSA.loyalty_type
		, TSA.new_to_jwn
		, SCF.channel
		, SCF.banner
		, SCF.business_unit_desc
	FROM {usl_t2_schema}.sales_cust_fact AS SCF
	LEFT JOIN upc_lookup_table AS DIV
		ON DIV.upc_num = SCF.upc_num
	INNER JOIN {usl_t2_schema}.usl_rolling_52wk_calendar AS RC 
		ON RC.day_date = SCF.sale_date
	INNER JOIN date_lookup AS DL 
		ON DL.week_num = RC.week_num
	LEFT JOIN customer_single_attribute AS TSA
		ON TSA.acp_id = SCF.acp_id
			AND RC.month_num = TSA.month_num_realigned
			AND RC.quarter_num = TSA.quarter_num_realigned
			AND RC.year_num = TSA.year_num_realigned
	WHERE 1 = 1
		AND RC.month_num >= (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -24)) 
		AND RC.month_num < (SELECT DISTINCT month_num FROM {usl_t2_schema}.usl_rolling_52wk_calendar AS DC WHERE day_date = ADD_MONTHS(current_date(), -16))
)WITH DATA PRIMARY INDEX(acp_id, week_num, quarter_num, month_num, year_num, upc_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Three: Dissect sales information into ty_positive and ty_negative datasets
 * 
----------------------------------------------------------------------------------------------------------------*/

drop table ty_positive;

-- POSITIVE
CREATE MULTISET VOLATILE TABLE ty_positive AS (
	SELECT
		month_num
		, quarter_num
		, year_num
		, NULL AS year_id
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
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN line_net_usd_amt END) AS gross_spend_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN non_gc_amt END) AS non_gc_spend_accessories
        , COUNT(DISTINCT CASE WHEN div_desc = 'ACCESSORIES' THEN trip_id END) AS trips_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN items END) AS items_accessories
        -- APPAREL
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN line_net_usd_amt END) AS gross_spend_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN non_gc_amt END) AS non_gc_spend_apparel
        , COUNT(DISTINCT CASE WHEN div_desc = 'APPAREL' THEN trip_id END) AS trips_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN items END) AS items_apparel
        -- BEAUTY
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN line_net_usd_amt END) AS gross_spend_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN non_gc_amt END) AS non_gc_spend_beauty
        , COUNT(DISTINCT CASE WHEN div_desc = 'BEAUTY' THEN trip_id END) AS trips_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN items END) AS items_beauty
        -- DESIGNER
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN line_net_usd_amt END) AS gross_spend_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN non_gc_amt END) AS non_gc_spend_designer
        , COUNT(DISTINCT CASE WHEN div_desc = 'DESIGNER' THEN trip_id END) AS trips_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN items END) AS items_designer
        -- HOME
        , SUM(CASE WHEN div_desc = 'HOME' THEN line_net_usd_amt END) AS gross_spend_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN non_gc_amt END) AS non_gc_spend_home
        , COUNT(DISTINCT CASE WHEN div_desc = 'HOME' THEN trip_id END) AS trips_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN items END) AS items_home
        -- MERCH
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN line_net_usd_amt END) AS gross_spend_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN non_gc_amt END) AS non_gc_spend_merch
        , COUNT(DISTINCT CASE WHEN div_desc = 'MERCH PROJECTS' THEN trip_id END) AS trips_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN items END) AS items_merch
        -- SHOES
        , SUM(CASE WHEN div_desc = 'SHOES' THEN line_net_usd_amt END) AS gross_spend_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN non_gc_amt END) AS non_gc_spend_shoes
        , COUNT(DISTINCT CASE WHEN div_desc = 'SHOES' THEN trip_id END) AS trips_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN items END) AS items_shoes
        -- OTHER
        , SUM(CASE WHEN div_desc = 'OTHER' THEN line_net_usd_amt END) AS gross_spend_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN non_gc_amt END) AS non_gc_spend_other
        , COUNT(DISTINCT CASE WHEN div_desc = 'OTHER' THEN trip_id END) AS trips_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN items END) AS items_other
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
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

drop table ty_negative;

-- Negative
CREATE MULTISET VOLATILE TABLE ty_negative AS (
	SELECT
		month_num
		, quarter_num
		, year_num
		, NULL AS year_id
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
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN line_net_usd_amt END) AS return_spend_accessories
        , SUM(CASE WHEN div_desc = 'ACCESSORIES' THEN items END) AS return_items_accessories
        -- APPAREL
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN line_net_usd_amt END) AS return_spend_apparel
        , SUM(CASE WHEN div_desc = 'APPAREL' THEN items END) AS return_items_apparel
        -- BEAUTY
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN line_net_usd_amt END) AS return_spend_beauty
        , SUM(CASE WHEN div_desc = 'BEAUTY' THEN items END) AS return_items_beauty
        -- DESIGNER
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN line_net_usd_amt END) AS return_spend_designer
        , SUM(CASE WHEN div_desc = 'DESIGNER' THEN items END) AS return_items_designer
        -- HOME
        , SUM(CASE WHEN div_desc = 'HOME' THEN line_net_usd_amt END) AS return_spend_home
        , SUM(CASE WHEN div_desc = 'HOME' THEN items END) AS return_items_home
        -- MERCH
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN line_net_usd_amt END) AS return_spend_merch
        , SUM(CASE WHEN div_desc = 'MERCH PROJECTS' THEN items END) AS return_items_merch
        -- SHOES
        , SUM(CASE WHEN div_desc = 'SHOES' THEN line_net_usd_amt END) AS return_spend_shoes
        , SUM(CASE WHEN div_desc = 'SHOES' THEN items END) AS return_items_shoes
        -- OTHER
        , SUM(CASE WHEN div_desc = 'OTHER' THEN line_net_usd_amt END) AS return_spend_other
        , SUM(CASE WHEN div_desc = 'OTHER' THEN items END) AS return_items_other
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
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Four: Combine TY Data
 * 
----------------------------------------------------------------------------------------------------------------*/

drop table TY;

CREATE VOLATILE MULTISET TABLE TY AS (
	SELECT
		-- Grouping Columns
		COALESCE(a.month_num, b.month_num) AS month_num
		, COALESCE(a.quarter_num, b.quarter_num) AS quarter_num
		, COALESCE(a.year_num, b.year_num) AS year_num
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
		, CASE WHEN a.new_to_jwn >= 1 OR b.new_to_jwn >= 1 THEN 1 ELSE 0 END AS new_to_jwn
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
		, COALESCE(a.gross_spend_accessories, 0) AS gross_spend_accessories
		, COALESCE(a.non_gc_spend_accessories, 0) + COALESCE(b.return_spend_accessories,0) AS net_spend_accessories
		, COALESCE(a.trips_accessories, 0) AS trips_accessories
		, COALESCE(a.items_accessories, 0) AS gross_units_accessories
		, COALESCE(a.items_accessories, 0) - COALESCE(b.return_items_accessories,0) AS net_units_accessories
        -- APPAREL
		, COALESCE(a.gross_spend_apparel, 0) AS gross_spend_apparel
		, COALESCE(a.non_gc_spend_apparel, 0) + COALESCE(b.return_spend_apparel,0) AS net_spend_apparel
		, COALESCE(a.trips_apparel, 0) AS trips_apparel
		, COALESCE(a.items_apparel, 0) AS gross_units_apparel
		, COALESCE(a.items_apparel, 0) - COALESCE(b.return_items_apparel,0) AS net_units_apparel
        -- BEAUTY
		, COALESCE(a.gross_spend_beauty, 0) AS gross_spend_beauty
		, COALESCE(a.non_gc_spend_beauty, 0) + COALESCE(b.return_spend_beauty,0) AS net_spend_beauty
		, COALESCE(a.trips_beauty, 0) AS trips_beauty
		, COALESCE(a.items_beauty, 0) AS gross_units_beauty
		, COALESCE(a.items_beauty, 0) - COALESCE(b.return_items_beauty,0) AS net_units_beauty
        -- DESIGNER
		, COALESCE(a.gross_spend_designer, 0) AS gross_spend_designer
		, COALESCE(a.non_gc_spend_designer, 0) + COALESCE(b.return_spend_designer,0) AS net_spend_designer
		, COALESCE(a.trips_designer, 0) AS trips_designer
		, COALESCE(a.items_designer, 0) AS gross_units_designer
		, COALESCE(a.items_designer, 0) - COALESCE(b.return_items_designer,0) AS net_units_designer
        -- HOME
		, COALESCE(a.gross_spend_home, 0) AS gross_spend_home
		, COALESCE(a.non_gc_spend_home, 0) + COALESCE(b.return_spend_home,0) AS net_spend_home
		, COALESCE(a.trips_home, 0) AS trips_home
		, COALESCE(a.items_home, 0) AS gross_units_home
		, COALESCE(a.items_home, 0) - COALESCE(b.return_items_home,0) AS net_units_home
        -- MERCH
		, COALESCE(a.gross_spend_merch, 0) AS gross_spend_merch
		, COALESCE(a.non_gc_spend_merch, 0) + COALESCE(b.return_spend_merch,0) AS net_spend_merch
		, COALESCE(a.trips_merch, 0) AS trips_merch
		, COALESCE(a.items_merch, 0) AS gross_units_merch
		, COALESCE(a.items_merch, 0) - COALESCE(b.return_items_merch,0) AS net_units_merch
        -- SHOES
		, COALESCE(a.gross_spend_shoes, 0) AS gross_spend_shoes
		, COALESCE(a.non_gc_spend_shoes, 0) + COALESCE(b.return_spend_shoes,0) AS net_spend_shoes
		, COALESCE(a.trips_shoes, 0) AS trips_shoes
		, COALESCE(a.items_shoes, 0) AS gross_units_shoes
		, COALESCE(a.items_shoes, 0) - COALESCE(b.return_items_shoes,0) AS net_units_shoes
        -- OTHER
		, COALESCE(a.gross_spend_other, 0) AS gross_spend_other
		, COALESCE(a.non_gc_spend_other, 0) + COALESCE(b.return_spend_other,0) AS net_spend_other
		, COALESCE(a.trips_other, 0) AS trips_other
		, COALESCE(a.items_other, 0) AS gross_units_other
		, COALESCE(a.items_other, 0) - COALESCE(b.return_items_other,0) AS net_units_other
	FROM ty_positive AS a
	FULL JOIN ty_negative AS b
		ON a.month_num = b.month_num 
			AND a.quarter_num = b.quarter_num 
			AND a.year_num = b.year_num 
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
)WITH DATA PRIMARY INDEX(acp_id, quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Five: Aggregate to a weekly basis but split by overall and divisions
 * 
----------------------------------------------------------------------------------------------------------------*/

INSERT INTO trip_summary_overall
	SELECT
		-- Grouping Columns
		month_num
		, quarter_num
		, year_num
		, year_id
		, region
		, dma
		-- , store_segment
		-- , trade_area_type
		, AEC
		, predicted_segment
		, loyalty_level
		, loyalty_type
--		, cust_age_bucket
		, new_to_jwn
		--
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, COUNT(DISTINCT CASE WHEN channel = '1) Nordstrom Stores' THEN acp_id END) AS cust_count_fls
		, COUNT(DISTINCT CASE WHEN channel = '2) Nordstrom.com' THEN acp_id END) AS cust_count_ncom
		, COUNT(DISTINCT CASE WHEN channel = '3) Rack Stores' THEN acp_id END) AS cust_count_rs
		, COUNT(DISTINCT CASE WHEN channel = '4) Rack.com' THEN acp_id END) AS cust_count_rcom
		-- -- BY DIGITAL VS STORE		
		, COUNT(DISTINCT CASE WHEN channel IN ('1) Nordstrom Stores','3) Rack Stores') THEN acp_id END) AS cust_count_stores
		, COUNT(DISTINCT CASE WHEN channel IN ('2) Nordstrom.com','4) Rack.com') THEN acp_id END) AS cust_count_digital
		-- -- BY BANNER		
		, COUNT(DISTINCT CASE WHEN banner = '1) Nordstrom Banner' THEN acp_id END) AS cust_count_nord
		, COUNT(DISTINCT CASE WHEN banner = '2) Rack Banner' THEN acp_id END) AS cust_count_rack
		-- BY JWN
		, COUNT(DISTINCT acp_id) AS cust_count_jwn
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
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_accessories END) AS NS_accessories_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_accessories END) AS NCOM_accessories_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_accessories END) AS RS_accessories_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_accessories END) AS RCOM_accessories_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_accessories END) AS NS_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_accessories END) AS NCOM_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_accessories END) AS RS_accessories_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_accessories END) AS RCOM_accessories_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_accessories END) AS NS_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com'THEN net_units_accessories END) AS NCOM_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_accessories END) AS RS_accessories_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_accessories END) AS RCOM_accessories_weekly_net_units
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_apparel END) AS NS_apparel_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_apparel END) AS NCOM_apparel_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_apparel END) AS RS_apparel_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_apparel END) AS RCOM_apparel_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_apparel END) AS NS_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_apparel END) AS NCOM_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_apparel END) AS RS_apparel_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_apparel END) AS RCOM_apparel_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_apparel END) AS NS_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_apparel END) AS NCOM_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_apparel END) AS RS_apparel_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_apparel END) AS RCOM_apparel_weekly_net_units
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_beauty END) AS NS_beauty_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_beauty END) AS NCOM_beauty_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_beauty END) AS RS_beauty_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_beauty END) AS RCOM_beauty_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_beauty END) AS NS_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_beauty END) AS NCOM_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_beauty END) AS RS_beauty_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_beauty END) AS RCOM_beauty_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_beauty END) AS NS_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_beauty END) AS NCOM_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_beauty END) AS RS_beauty_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_beauty END) AS RCOM_beauty_weekly_net_units
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_designer END) AS NS_designer_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_designer END) AS NCOM_designer_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_designer END) AS RS_designer_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_designer END) AS RCOM_designer_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_designer END) AS NS_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_designer END) AS NCOM_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_designer END) AS RS_designer_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_designer END) AS RCOM_designer_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_designer END) AS NS_designer_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_designer END) AS NCOM_designer_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_designer END) AS RS_designer_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_designer END) AS RCOM_designer_weekly_net_units
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_home END) AS NS_home_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_home END) AS NCOM_home_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_home END) AS RS_home_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_home END) AS RCOM_home_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_home END) AS NS_home_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_home END) AS NCOM_home_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_home END) AS RS_home_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_home END) AS RCOM_home_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_home END) AS NS_home_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_home END) AS NCOM_home_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_home END) AS RS_home_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_home END) AS RCOM_home_weekly_net_units
		--
		-- MERCH PROJECTS
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_merch END) AS NS_merch_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_merch END) AS NCOM_merch_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_merch END) AS RS_merch_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_merch END) AS RCOM_merch_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_merch END) AS NS_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_merch END) AS NCOM_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_merch END) AS RS_merch_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_merch END) AS RCOM_merch_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_merch END) AS NS_merch_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_merch END) AS NCOM_merch_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_merch END) AS RS_merch_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_merch END) AS RCOM_merch_weekly_net_units
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_shoes END) AS NS_shoes_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_shoes END) AS NCOM_shoes_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_shoes END) AS RS_shoes_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_shoes END) AS RCOM_shoes_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_shoes END) AS NS_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_shoes END) AS NCOM_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_shoes END) AS RS_shoes_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_shoes END) AS RCOM_shoes_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_shoes END) AS NS_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_shoes END) AS NCOM_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_shoes END) AS RS_shoes_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_shoes END) AS RCOM_shoes_weekly_net_units
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN trips_other END) AS NS_other_weekly_trips
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN trips_other END) AS NCOM_other_weekly_trips
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN trips_other END) AS RS_other_weekly_trips
		, SUM(CASE WHEN channel = '4) Rack.com' THEN trips_other END) AS RCOM_other_weekly_trips
		--
		-- NET SALES BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_spend_other END) AS NS_other_weekly_net_spend
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_spend_other END) AS NCOM_other_weekly_net_spend
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_spend_other END) AS RS_other_weekly_net_spend
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_spend_other END) AS RCOM_other_weekly_net_spend
		--
		-- NET UNITS BY CHANNEL
		--
		, SUM(CASE WHEN channel = '1) Nordstrom Stores' THEN net_units_other END) AS NS_other_weekly_net_units
		, SUM(CASE WHEN channel = '2) Nordstrom.com' THEN net_units_other END) AS NCOM_other_weekly_net_units
		, SUM(CASE WHEN channel = '3) Rack Stores' THEN net_units_other END) AS RS_other_weekly_net_units
		, SUM(CASE WHEN channel = '4) Rack.com' THEN net_units_other END) AS RCOM_other_weekly_net_units
	FROM TY
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
--)WITH DATA PRIMARY INDEX(quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;
	;

COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (region) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (dma) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (aec) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (predicted_segment) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (loyalty_level) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (loyalty_type) ON trip_summary_overall;
COLLECT STATISTICS COLUMN (new_to_jwn) ON trip_summary_overall;

/* -------------------------------------------------------------------------------------------------------------
 * 
 * Stage Six: Create the final summary table
 * 
----------------------------------------------------------------------------------------------------------------*/
-- Drops the old iteration of the table.
-- DROP TABLE T3DL_ACE_CORP.so1p_trips_sandbox_metrics;

-- Creates a newer version of the table with up to date information.
DELETE FROM {usl_t2_schema}.trips_sandbox_monthly_yoy ALL; -- Remove all historic data to prevent overlaps
--
INSERT INTO {usl_t2_schema}.trips_sandbox_monthly_yoy -- Insert new data
--CREATE MULTISET TABLE T3DL_ACE_CORP.so1p_trips_sandbox_metrics AS (
--drop table usl_trips_sandbox_monthly_yoy;
--CREATE MULTISET VOLATILE TABLE usl_trips_sandbox_monthly_yoy AS (
	SELECT
		-- Grouping Columns
		COALESCE(TSO.month_num, TSO2.month_num + 100) AS month_num
		, COALESCE(TSO.quarter_num, TSO2.quarter_num+10) AS quarter_num
		, COALESCE(TSO.year_num, TSO2.year_num + 1) AS year_num
		, COALESCE(TSO.region, TSO2.region) AS region
		, COALESCE(TSO.dma, TSO2.dma) AS dma
		-- , COALESCE(TSO.store_segment, TSO2.store_segment) AS store_segment
		-- , COALESCE(TSO.trade_area_type, TSO2.trade_area_type) AS trade_area_type
		, COALESCE(TSO.AEC, TSO2.AEC) AS AEC
		, COALESCE(TSO.predicted_segment, TSO2.predicted_segment) AS predicted_segment
		, COALESCE(TSO.loyalty_level, TSO2.loyalty_level) AS loyalty_level
		, COALESCE(TSO.loyalty_type, TSO2.loyalty_type) AS loyalty_type
--		, COALESCE(TSO.cust_age_bucket, TSO2.cust_age_bucket) AS cust_age_bucket
		, COALESCE(TSO.new_to_jwn, TSO2.new_to_jwn) AS new_to_jwn
		--
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
		-- Calculated Metrics
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_accessories_weekly_trips,0) AS NS_accessories_trips_ty
		, COALESCE(TSO.NCOM_accessories_weekly_trips,0) AS NCOM_accessories_trips_ty
		, COALESCE(TSO.RS_accessories_weekly_trips,0) AS RS_accessories_trips_ty
		, COALESCE(TSO.RCOM_accessories_weekly_trips,0) AS RCOM_accessories_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_accessories_weekly_net_spend,0) AS NS_accessories_net_spend_ty
		, COALESCE(TSO.NCOM_accessories_weekly_net_spend,0) AS NCOM_accessories_net_spend_ty
		, COALESCE(TSO.RS_accessories_weekly_net_spend,0) AS RS_accessories_net_spend_ty
		, COALESCE(TSO.RCOM_accessories_weekly_net_spend,0) AS RCOM_accessories_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_accessories_weekly_net_units,0) AS NS_accessories_net_units_ty
		, COALESCE(TSO.NCOM_accessories_weekly_net_units,0) AS NCOM_accessories_net_units_ty
		, COALESCE(TSO.RS_accessories_weekly_net_units,0) AS RS_accessories_net_units_ty
		, COALESCE(TSO.RCOM_accessories_weekly_net_units,0) AS RCOM_accessories_net_units_ty
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_apparel_weekly_trips,0) AS NS_apparel_trips_ty
		, COALESCE(TSO.NCOM_apparel_weekly_trips,0) AS NCOM_apparel_trips_ty
		, COALESCE(TSO.RS_apparel_weekly_trips,0) AS RS_apparel_trips_ty
		, COALESCE(TSO.RCOM_apparel_weekly_trips,0) AS RCOM_apparel_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_apparel_weekly_net_spend,0) AS NS_apparel_net_spend_ty
		, COALESCE(TSO.NCOM_apparel_weekly_net_spend,0) AS NCOM_apparel_net_spend_ty
		, COALESCE(TSO.RS_apparel_weekly_net_spend,0) AS RS_apparel_net_spend_ty
		, COALESCE(TSO.RCOM_apparel_weekly_net_spend,0) AS RCOM_apparel_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_apparel_weekly_net_units,0) AS NS_apparel_net_units_ty
		, COALESCE(TSO.NCOM_apparel_weekly_net_units,0) AS NCOM_apparel_net_units_ty
		, COALESCE(TSO.RS_apparel_weekly_net_units,0) AS RS_apparel_net_units_ty
		, COALESCE(TSO.RCOM_apparel_weekly_net_units,0) AS RCOM_apparel_net_units_ty
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_beauty_weekly_trips,0) AS NS_beauty_trips_ty
		, COALESCE(TSO.NCOM_beauty_weekly_trips,0) AS NCOM_beauty_trips_ty
		, COALESCE(TSO.RS_beauty_weekly_trips,0) AS RS_beauty_trips_ty
		, COALESCE(TSO.RCOM_beauty_weekly_trips,0) AS RCOM_beauty_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_beauty_weekly_net_spend,0) AS NS_beauty_net_spend_ty
		, COALESCE(TSO.NCOM_beauty_weekly_net_spend,0) AS NCOM_beauty_net_spend_ty
		, COALESCE(TSO.RS_beauty_weekly_net_spend,0) AS RS_beauty_net_spend_ty
		, COALESCE(TSO.RCOM_beauty_weekly_net_spend,0) AS RCOM_beauty_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_beauty_weekly_net_units,0) AS NS_beauty_net_units_ty
		, COALESCE(TSO.NCOM_beauty_weekly_net_units,0) AS NCOM_beauty_net_units_ty
		, COALESCE(TSO.RS_beauty_weekly_net_units,0) AS RS_beauty_net_units_ty
		, COALESCE(TSO.RCOM_beauty_weekly_net_units,0) AS RCOM_beauty_net_units_ty
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_designer_weekly_trips,0) AS NS_designer_trips_ty
		, COALESCE(TSO.NCOM_designer_weekly_trips,0) AS NCOM_designer_trips_ty
		, COALESCE(TSO.RS_designer_weekly_trips,0) AS RS_designer_trips_ty
		, COALESCE(TSO.RCOM_designer_weekly_trips,0) AS RCOM_designer_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_designer_weekly_net_spend,0) AS NS_designer_net_spend_ty
		, COALESCE(TSO.NCOM_designer_weekly_net_spend,0) AS NCOM_designer_net_spend_ty
		, COALESCE(TSO.RS_designer_weekly_net_spend,0) AS RS_designer_net_spend_ty
		, COALESCE(TSO.RCOM_designer_weekly_net_spend,0) AS RCOM_designer_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_designer_weekly_net_units,0) AS NS_designer_net_units_ty
		, COALESCE(TSO.NCOM_designer_weekly_net_units,0) AS NCOM_designer_net_units_ty
		, COALESCE(TSO.RS_designer_weekly_net_units,0) AS RS_designer_net_units_ty
		, COALESCE(TSO.RCOM_designer_weekly_net_units,0) AS RCOM_designer_net_units_ty
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_home_weekly_trips,0) AS NS_home_trips_ty
		, COALESCE(TSO.NCOM_home_weekly_trips,0) AS NCOM_home_trips_ty
		, COALESCE(TSO.RS_home_weekly_trips,0) AS RS_home_trips_ty
		, COALESCE(TSO.RCOM_home_weekly_trips,0) AS RCOM_home_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_home_weekly_net_spend,0) AS NS_home_net_spend_ty
		, COALESCE(TSO.NCOM_home_weekly_net_spend,0) AS NCOM_home_net_spend_ty
		, COALESCE(TSO.RS_home_weekly_net_spend,0) AS RS_home_net_spend_ty
		, COALESCE(TSO.RCOM_home_weekly_net_spend,0) AS RCOM_home_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_home_weekly_net_units,0) AS NS_home_net_units_ty
		, COALESCE(TSO.NCOM_home_weekly_net_units,0) AS NCOM_home_net_units_ty
		, COALESCE(TSO.RS_home_weekly_net_units,0) AS RS_home_net_units_ty
		, COALESCE(TSO.RCOM_home_weekly_net_units,0) AS RCOM_home_net_units_ty
		--
		-- MERCH
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_merch_weekly_trips,0) AS NS_merch_trips_ty
		, COALESCE(TSO.NCOM_merch_weekly_trips,0) AS NCOM_merch_trips_ty
		, COALESCE(TSO.RS_merch_weekly_trips,0) AS RS_merch_trips_ty
		, COALESCE(TSO.RCOM_merch_weekly_trips,0) AS RCOM_merch_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_merch_weekly_net_spend,0) AS NS_merch_net_spend_ty
		, COALESCE(TSO.NCOM_merch_weekly_net_spend,0) AS NCOM_merch_net_spend_ty
		, COALESCE(TSO.RS_merch_weekly_net_spend,0) AS RS_merch_net_spend_ty
		, COALESCE(TSO.RCOM_merch_weekly_net_spend,0) AS RCOM_merch_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_merch_weekly_net_units,0) AS NS_merch_net_units_ty
		, COALESCE(TSO.NCOM_merch_weekly_net_units,0) AS NCOM_merch_net_units_ty
		, COALESCE(TSO.RS_merch_weekly_net_units,0) AS RS_merch_net_units_ty
		, COALESCE(TSO.RCOM_merch_weekly_net_units,0) AS RCOM_merch_net_units_ty
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_shoes_weekly_trips,0) AS NS_shoes_trips_ty
		, COALESCE(TSO.NCOM_shoes_weekly_trips,0) AS NCOM_shoes_trips_ty
		, COALESCE(TSO.RS_shoes_weekly_trips,0) AS RS_shoes_trips_ty
		, COALESCE(TSO.RCOM_shoes_weekly_trips,0) AS RCOM_shoes_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_shoes_weekly_net_spend,0) AS NS_shoes_net_spend_ty
		, COALESCE(TSO.NCOM_shoes_weekly_net_spend,0) AS NCOM_shoes_net_spend_ty
		, COALESCE(TSO.RS_shoes_weekly_net_spend,0) AS RS_shoes_net_spend_ty
		, COALESCE(TSO.RCOM_shoes_weekly_net_spend,0) AS RCOM_shoes_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_shoes_weekly_net_units,0) AS NS_shoes_net_units_ty
		, COALESCE(TSO.NCOM_shoes_weekly_net_units,0) AS NCOM_shoes_net_units_ty
		, COALESCE(TSO.RS_shoes_weekly_net_units,0) AS RS_shoes_net_units_ty
		, COALESCE(TSO.RCOM_shoes_weekly_net_units,0) AS RCOM_shoes_net_units_ty
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO.NS_other_weekly_trips,0) AS NS_other_trips_ty
		, COALESCE(TSO.NCOM_other_weekly_trips,0) AS NCOM_other_trips_ty
		, COALESCE(TSO.RS_other_weekly_trips,0) AS RS_other_trips_ty
		, COALESCE(TSO.RCOM_other_weekly_trips,0) AS RCOM_other_trips_ty
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO.NS_other_weekly_net_spend,0) AS NS_other_net_spend_ty
		, COALESCE(TSO.NCOM_other_weekly_net_spend,0) AS NCOM_other_net_spend_ty
		, COALESCE(TSO.RS_other_weekly_net_spend,0) AS RS_other_net_spend_ty
		, COALESCE(TSO.RCOM_other_weekly_net_spend,0) AS RCOM_other_net_spend_ty
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO.NS_other_weekly_net_units,0) AS NS_other_net_units_ty
		, COALESCE(TSO.NCOM_other_weekly_net_units,0) AS NCOM_other_net_units_ty
		, COALESCE(TSO.RS_other_weekly_net_units,0) AS RS_other_net_units_ty
		, COALESCE(TSO.RCOM_other_weekly_net_units,0) AS RCOM_other_net_units_ty
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
		-- Calculated Metrics
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_accessories_weekly_trips,0) AS NS_accessories_trips_ly
		, COALESCE(TSO2.NCOM_accessories_weekly_trips,0) AS NCOM_accessories_trips_ly
		, COALESCE(TSO2.RS_accessories_weekly_trips,0) AS RS_accessories_trips_ly
		, COALESCE(TSO2.RCOM_accessories_weekly_trips,0) AS RCOM_accessories_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_accessories_weekly_net_spend,0) AS NS_accessories_net_spend_ly
		, COALESCE(TSO2.NCOM_accessories_weekly_net_spend,0) AS NCOM_accessories_net_spend_ly
		, COALESCE(TSO2.RS_accessories_weekly_net_spend,0) AS RS_accessories_net_spend_ly
		, COALESCE(TSO2.RCOM_accessories_weekly_net_spend,0) AS RCOM_accessories_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_accessories_weekly_net_units,0) AS NS_accessories_net_units_ly
		, COALESCE(TSO2.NCOM_accessories_weekly_net_units,0) AS NCOM_accessories_net_units_ly
		, COALESCE(TSO2.RS_accessories_weekly_net_units,0) AS RS_accessories_net_units_ly
		, COALESCE(TSO2.RCOM_accessories_weekly_net_units,0) AS RCOM_accessories_net_units_ly
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_apparel_weekly_trips,0) AS NS_apparel_trips_ly
		, COALESCE(TSO2.NCOM_apparel_weekly_trips,0) AS NCOM_apparel_trips_ly
		, COALESCE(TSO2.RS_apparel_weekly_trips,0) AS RS_apparel_trips_ly
		, COALESCE(TSO2.RCOM_apparel_weekly_trips,0) AS RCOM_apparel_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_apparel_weekly_net_spend,0) AS NS_apparel_net_spend_ly
		, COALESCE(TSO2.NCOM_apparel_weekly_net_spend,0) AS NCOM_apparel_net_spend_ly
		, COALESCE(TSO2.RS_apparel_weekly_net_spend,0) AS RS_apparel_net_spend_ly
		, COALESCE(TSO2.RCOM_apparel_weekly_net_spend,0) AS RCOM_apparel_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_apparel_weekly_net_units,0) AS NS_apparel_net_units_ly
		, COALESCE(TSO2.NCOM_apparel_weekly_net_units,0) AS NCOM_apparel_net_units_ly
		, COALESCE(TSO2.RS_apparel_weekly_net_units,0) AS RS_apparel_net_units_ly
		, COALESCE(TSO2.RCOM_apparel_weekly_net_units,0) AS RCOM_apparel_net_units_ly
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_beauty_weekly_trips,0) AS NS_beauty_trips_ly
		, COALESCE(TSO2.NCOM_beauty_weekly_trips,0) AS NCOM_beauty_trips_ly
		, COALESCE(TSO2.RS_beauty_weekly_trips,0) AS RS_beauty_trips_ly
		, COALESCE(TSO2.RCOM_beauty_weekly_trips,0) AS RCOM_beauty_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_beauty_weekly_net_spend,0) AS NS_beauty_net_spend_ly
		, COALESCE(TSO2.NCOM_beauty_weekly_net_spend,0) AS NCOM_beauty_net_spend_ly
		, COALESCE(TSO2.RS_beauty_weekly_net_spend,0) AS RS_beauty_net_spend_ly
		, COALESCE(TSO2.RCOM_beauty_weekly_net_spend,0) AS RCOM_beauty_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_beauty_weekly_net_units,0) AS NS_beauty_net_units_ly
		, COALESCE(TSO2.NCOM_beauty_weekly_net_units,0) AS NCOM_beauty_net_units_ly
		, COALESCE(TSO2.RS_beauty_weekly_net_units,0) AS RS_beauty_net_units_ly
		, COALESCE(TSO2.RCOM_beauty_weekly_net_units,0) AS RCOM_beauty_net_units_ly
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_designer_weekly_trips,0) AS NS_designer_trips_ly
		, COALESCE(TSO2.NCOM_designer_weekly_trips,0) AS NCOM_designer_trips_ly
		, COALESCE(TSO2.RS_designer_weekly_trips,0) AS RS_designer_trips_ly
		, COALESCE(TSO2.RCOM_designer_weekly_trips,0) AS RCOM_designer_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_designer_weekly_net_spend,0) AS NS_designer_net_spend_ly
		, COALESCE(TSO2.NCOM_designer_weekly_net_spend,0) AS NCOM_designer_net_spend_ly
		, COALESCE(TSO2.RS_designer_weekly_net_spend,0) AS RS_designer_net_spend_ly
		, COALESCE(TSO2.RCOM_designer_weekly_net_spend,0) AS RCOM_designer_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_designer_weekly_net_units,0) AS NS_designer_net_units_ly
		, COALESCE(TSO2.NCOM_designer_weekly_net_units,0) AS NCOM_designer_net_units_ly
		, COALESCE(TSO2.RS_designer_weekly_net_units,0) AS RS_designer_net_units_ly
		, COALESCE(TSO2.RCOM_designer_weekly_net_units,0) AS RCOM_designer_net_units_ly
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_home_weekly_trips,0) AS NS_home_trips_ly
		, COALESCE(TSO2.NCOM_home_weekly_trips,0) AS NCOM_home_trips_ly
		, COALESCE(TSO2.RS_home_weekly_trips,0) AS RS_home_trips_ly
		, COALESCE(TSO2.RCOM_home_weekly_trips,0) AS RCOM_home_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_home_weekly_net_spend,0) AS NS_home_net_spend_ly
		, COALESCE(TSO2.NCOM_home_weekly_net_spend,0) AS NCOM_home_net_spend_ly
		, COALESCE(TSO2.RS_home_weekly_net_spend,0) AS RS_home_net_spend_ly
		, COALESCE(TSO2.RCOM_home_weekly_net_spend,0) AS RCOM_home_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_home_weekly_net_units,0) AS NS_home_net_units_ly
		, COALESCE(TSO2.NCOM_home_weekly_net_units,0) AS NCOM_home_net_units_ly
		, COALESCE(TSO2.RS_home_weekly_net_units,0) AS RS_home_net_units_ly
		, COALESCE(TSO2.RCOM_home_weekly_net_units,0) AS RCOM_home_net_units_ly
		--
		-- MERCH
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_merch_weekly_trips,0) AS NS_merch_trips_ly
		, COALESCE(TSO2.NCOM_merch_weekly_trips,0) AS NCOM_merch_trips_ly
		, COALESCE(TSO2.RS_merch_weekly_trips,0) AS RS_merch_trips_ly
		, COALESCE(TSO2.RCOM_merch_weekly_trips,0) AS RCOM_merch_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_merch_weekly_net_spend,0) AS NS_merch_net_spend_ly
		, COALESCE(TSO2.NCOM_merch_weekly_net_spend,0) AS NCOM_merch_net_spend_ly
		, COALESCE(TSO2.RS_merch_weekly_net_spend,0) AS RS_merch_net_spend_ly
		, COALESCE(TSO2.RCOM_merch_weekly_net_spend,0) AS RCOM_merch_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_merch_weekly_net_units,0) AS NS_merch_net_units_ly
		, COALESCE(TSO2.NCOM_merch_weekly_net_units,0) AS NCOM_merch_net_units_ly
		, COALESCE(TSO2.RS_merch_weekly_net_units,0) AS RS_merch_net_units_ly
		, COALESCE(TSO2.RCOM_merch_weekly_net_units,0) AS RCOM_merch_net_units_ly
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_shoes_weekly_trips,0) AS NS_shoes_trips_ly
		, COALESCE(TSO2.NCOM_shoes_weekly_trips,0) AS NCOM_shoes_trips_ly
		, COALESCE(TSO2.RS_shoes_weekly_trips,0) AS RS_shoes_trips_ly
		, COALESCE(TSO2.RCOM_shoes_weekly_trips,0) AS RCOM_shoes_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_shoes_weekly_net_spend,0) AS NS_shoes_net_spend_ly
		, COALESCE(TSO2.NCOM_shoes_weekly_net_spend,0) AS NCOM_shoes_net_spend_ly
		, COALESCE(TSO2.RS_shoes_weekly_net_spend,0) AS RS_shoes_net_spend_ly
		, COALESCE(TSO2.RCOM_shoes_weekly_net_spend,0) AS RCOM_shoes_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_shoes_weekly_net_units,0) AS NS_shoes_net_units_ly
		, COALESCE(TSO2.NCOM_shoes_weekly_net_units,0) AS NCOM_shoes_net_units_ly
		, COALESCE(TSO2.RS_shoes_weekly_net_units,0) AS RS_shoes_net_units_ly
		, COALESCE(TSO2.RCOM_shoes_weekly_net_units,0) AS RCOM_shoes_net_units_ly
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, COALESCE(TSO2.NS_other_weekly_trips,0) AS NS_other_trips_ly
		, COALESCE(TSO2.NCOM_other_weekly_trips,0) AS NCOM_other_trips_ly
		, COALESCE(TSO2.RS_other_weekly_trips,0) AS RS_other_trips_ly
		, COALESCE(TSO2.RCOM_other_weekly_trips,0) AS RCOM_other_trips_ly
		--
		-- NET SALES BY CHANNEL
		--
		, COALESCE(TSO2.NS_other_weekly_net_spend,0) AS NS_other_net_spend_ly
		, COALESCE(TSO2.NCOM_other_weekly_net_spend,0) AS NCOM_other_net_spend_ly
		, COALESCE(TSO2.RS_other_weekly_net_spend,0) AS RS_other_net_spend_ly
		, COALESCE(TSO2.RCOM_other_weekly_net_spend,0) AS RCOM_other_net_spend_ly
		--
		-- NET UNITS BY CHANNEL
		--
		, COALESCE(TSO2.NS_other_weekly_net_units,0) AS NS_other_net_units_ly
		, COALESCE(TSO2.NCOM_other_weekly_net_units,0) AS NCOM_other_net_units_ly
		, COALESCE(TSO2.RS_other_weekly_net_units,0) AS RS_other_net_units_ly
		, COALESCE(TSO2.RCOM_other_weekly_net_units,0) AS RCOM_other_net_units_ly
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp
	FROM trip_summary_overall AS TSO
	FULL OUTER JOIN trip_summary_overall AS TSO2
		ON MOD(TSO.month_num,100) = MOD(TSO2.month_num, 100)
			AND TSO.quarter_num =  TSO2.quarter_num + 10
			AND TSO.year_num = TSO2.year_num+1
			AND TSO.year_id = TSO2.year_id
			AND TSO.region = TSO2.region
			AND TSO.dma = TSO2.dma
			-- AND TSO.store_segment = TSO2.store_segment
			-- AND TSO.trade_area_type = TSO2.trade_area_type
			AND TSO.AEC = TSO2.AEC
			AND TSO.predicted_segment = TSO2.predicted_segment
			AND TSO.loyalty_level = TSO2.loyalty_level
			AND TSO.loyalty_type = TSO2.loyalty_type
--			AND TSO.cust_age_bucket = cust_age_bucket
			AND TSO.new_to_jwn = TSO2.new_to_jwn
	WHERE 1 = 1
		AND COALESCE(TSO.month_num, TSO2.month_num + 100) IN (SELECT month_num FROM date_lookup)
-- )WITH DATA PRIMARY INDEX(quarter_num, month_num, year_num, loyalty_type, loyalty_level, AEC) ON COMMIT PRESERVE ROWS;
--)WITH DATA PRIMARY INDEX(quarter_num, month_num, year_num) ON COMMIT PRESERVE ROWS;
;

COLLECT STATISTICS COLUMN (month_num, quarter_num, year_num) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;
COLLECT STATISTICS COLUMN (region) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;
COLLECT STATISTICS COLUMN (dma) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;
COLLECT STATISTICS COLUMN (aec) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;
COLLECT STATISTICS COLUMN (predicted_segment) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;
COLLECT STATISTICS COLUMN (loyalty_level) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;
COLLECT STATISTICS COLUMN (loyalty_type) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;
COLLECT STATISTICS COLUMN (new_to_jwn) ON {usl_t2_schema}.trips_sandbox_monthly_yoy;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;