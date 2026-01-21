/*
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=cust_pop_initiative_exposal_11521_ACE_ENG;
     Task_Name=cust_pop_initiative_exposal;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_USL.cust_pop_initiative_exposal
Team/Owner: Customer Analytics
Date Created/Modified: Mar 12 2024

Note:
-- Purpose of the table: Table to get the first time that a customer was exposed to the F2DD and/or Beauty5X initiative
-- Update Cadence: Daily

*/

/*
 *
 * STEP ONE: Identify the NMS markets where F2DD is available
 *
 */

CREATE MULTISET VOLATILE TABLE f2dd_locations AS (
	CURRENT VALIDTIME SELECT DISTINCT
		LMPD.coarse_postal_code AS zip_code
		, LMPD.local_market
	FROM prd_nap_usr_vws.local_market_postal_dim AS LMPD
	WHERE 1 = 1
		AND LMPD.local_market NOT IN ('TORONTO')
) WITH DATA PRIMARY INDEX(zip_code) ON COMMIT PRESERVE ROWS;

/*
 *
 * STEP TWO: Account for lines where the acp_id is missing in item_delivery_method_funnel_daily table
 *
 */

-- Identify order lines with missing acp_ids
CREATE MULTISET VOLATILE TABLE order_missing_acp AS (
	SELECT DISTINCT
		IDMF.order_line_id
	FROM t2dl_das_item_delivery.Item_delivery_method_funnel_daily AS IDMF
	WHERE IDMF.acp_id IS NULL
) WITH DATA PRIMARY INDEX(order_line_id) ON COMMIT PRESERVE ROWS;

-- Find enterprise customer id for missing acp_id but having enterprise id
CREATE MULTISET VOLATILE TABLE order_missing_acp_ent AS (
	SELECT DISTINCT
		OLDF.order_line_id
		, OLDF.ent_cust_id
	FROM PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT AS OLDF
	WHERE 1 = 1
		AND OLDF.order_line_id IN (SELECT order_line_id FROM order_missing_acp)
		AND OLDF.ent_cust_id IS NOT NULL
) WITH DATA PRIMARY INDEX(order_line_id) ON COMMIT PRESERVE ROWS;

-- Identify acp id based on enterprise id
CREATE MULTISET VOLATILE TABLE acp_enterprise AS (
	SELECT
		AACX.acp_id
		, AACX.cust_id
	FROM PRD_NAP_USR_VWS.ACP_ANALYTICAL_CUST_XREF AS AACX
	WHERE 1 = 1
		AND AACX.cust_source = 'ICON'
		AND AACX.cust_id IN (SELECT DISTINCT ent_cust_id FROM order_missing_acp_ent)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AACX.cust_id
								ORDER BY AACX.dw_batch_date DESC) = 1
) WITH DATA PRIMARY INDEX(cust_id) ON COMMIT PRESERVE ROWS;

-- Find shopper id for missing acp_id but have shopper id
CREATE MULTISET VOLATILE TABLE order_missing_acp_shopper AS (
	SELECT DISTINCT
		OLDF.order_line_id
		, OLDF.shopper_id
	FROM PRD_NAP_USR_VWS.ORDER_LINE_DETAIL_FACT AS OLDF
	WHERE 1 = 1
		AND OLDF.order_line_id IN (SELECT order_line_id FROM order_missing_acp)
		AND OLDF.ent_cust_id IS NULL
		AND OLDF.shopper_id IS NOT NULL
) WITH DATA PRIMARY INDEX(order_line_id) ON COMMIT PRESERVE ROWS;

-- Find acp_id based on shopper_id
CREATE MULTISET VOLATILE TABLE acp_shopper AS (
	SELECT DISTINCT
		AAPX.program_index_id
		, AAPX.acp_id
	FROM PRD_NAP_USR_VWS.ACP_ANALYTICAL_PROGRAM_XREF AS AAPX
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AAPX.acp_id
								ORDER BY AAPX.program_index_id DESC) = 1
    	AND ROW_NUMBER() OVER (PARTITION BY AAPX.program_index_id
    							ORDER BY AAPX.acp_id DESC) = 1
    WHERE 1 = 1
    	AND AAPX.acp_id IS NOT NULL
    	AND AAPX.program_index_id IS NOT NULL
    	AND AAPX.program_name IN ('WEB')
) WITH DATA PRIMARY INDEX(program_index_id) ON COMMIT PRESERVE ROWS;

/*
 *
 * STEP THREE: Identify all customers who have made an F2DD purchase and fill in missing acp_id where possible
 *
 */

CREATE MULTISET VOLATILE TABLE f2dd_customers AS (
	SELECT
		COALESCE(IDMF.acp_id, AE.acp_id, ACS.acp_id) AS acp_id
		, 'F2DD' AS initiative_name
		, MIN(order_date_pacific) AS first_exposed
		, MAX(order_date_pacific) AS most_recent_initiative_interaction
	--	, IDMF.*
	FROM t2dl_das_item_delivery.Item_delivery_method_funnel_daily AS IDMF
	LEFT JOIN f2dd_locations AS MKT
		ON COALESCE(IDMF.bill_zip_code, IDMF.destination_zip_code) = MKT.zip_code
	-- Fill in missing acp_ids to have more filled in core data set
	LEFT JOIN order_missing_acp_ent AS MAE
		ON MAE.order_line_id = IDMF.order_line_id
	LEFT JOIN acp_enterprise AS AE
		ON AE.cust_id = MAE.ent_cust_id
	LEFT JOIN order_missing_acp_shopper AS MAS
		ON MAS.order_line_id = IDMF.order_line_id
	LEFT JOIN acp_shopper AS ACS
		ON ACS.program_index_id = MAS.shopper_id
	WHERE 1 = 1
		AND MKT.zip_code IS NOT NULL
		AND IDMF.item_delivery_method IN ('FREE_2DAY_DELIVERY', 'FREE_2DAY_SHIP_TO_STORE')
	GROUP BY 1,2
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

/*
 *
 * STEP THREE: Identify all customers who have interacted with the beauty 5x initiative
 *
 */

CREATE MULTISET VOLATILE TABLE beauty5x_customers AS (
	SELECT
		SRF.acp_id
		, 'BEAUTY_5X' AS initiative_name
		, MIN(SRF.business_day_date) AS first_exposed
		, MAX(SRF.business_day_date) AS most_recent_initiative_interaction
	--	, SRF.*
	--	, PSD.*
	FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact AS SRF
	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW AS PSD
		ON SRF.sku_num = PSD.rms_sku_num
	WHERE 1 = 1
		AND SRF.sku_num IS NOT NULL
		AND PSD.rms_sku_num IS NOT NULL
		AND (PSD.dept_num IN (812, 813, 814, 815, 816, 817, 818, 819, 925)
			OR PSD.div_desc IN ('BEAUTY'))
	GROUP BY 1,2
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

/*
 *
 * STEP FOUR: Merge initiative datasets into one table
 *
 */

CREATE MULTISET VOLATILE TABLE initiative_customer AS (
	SELECT
		BC.acp_id
		, BC.initiative_name
		, BC.first_exposed
		, BC.most_recent_initiative_interaction
	FROM beauty5x_customers AS BC
	-- Spacer
	UNION
	-- Spacer
	SELECT
		FC.acp_id
		, FC.initiative_name
		, FC.first_exposed
		, FC.most_recent_initiative_interaction
	FROM f2dd_customers AS FC
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

/*
 *
 * STEP Five: Remove null value rows
 *
 */

CREATE MULTISET VOLATILE TABLE usl_cust_pop_initiative_exposal AS (
	SELECT
		IC.acp_id
		, IC.initiative_name
		, IC.first_exposed
		, IC.most_recent_initiative_interaction
	FROM initiative_customer AS IC
	WHERE 1 = 1
		AND acp_id IS NOT NULL
		AND initiative_name IS NOT NULL
		AND first_exposed IS NOT NULL
		AND most_recent_initiative_interaction IS NOT NULL
) WITH DATA PRIMARY INDEX(acp_id) ON COMMIT PRESERVE ROWS;

DELETE FROM T2DL_DAS_USL.cust_pop_initiative_exposal ALL; -- Remove all historic data to prevent overlaps

INSERT INTO T2DL_DAS_USL.cust_pop_initiative_exposal  -- Insert new data
SELECT
	IC.acp_id
	, IC.initiative_name
	, IC.first_exposed
	, IC.most_recent_initiative_interaction
	, CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM usl_cust_pop_initiative_exposal AS IC
;
/*
SQL script must end with statement to turn off QUERY_BAND
*/

COLLECT STATISTICS COLUMN(acp_id)
	ON T2DL_DAS_USL.cust_pop_initiative_exposal;

SET QUERY_BAND = NONE FOR SESSION;