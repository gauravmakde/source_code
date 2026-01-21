/*
Name: Anniversary Ownership Report
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the backend table that powers the Scaled Events Anniversary Ownership Report
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
DAG: merch_pra_se_an_ownership
Author(s): Alli Moore
Date Created: 04-19-2023
Date Last Updated: 04-24-2023
*/


/******************************************************************************
** START: VOLATILE TABLES
******************************************************************************/
-- Identify 2023 AN SKUs
CREATE MULTISET VOLATILE TABLE AN_SKU
AS (
	SELECT DISTINCT
	    	sku_idnt
	    	, country AS store_country_code
	    FROM T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU
	    WHERE ty_ly_ind = 'TY'
)
WITH DATA
PRIMARY INDEX ( sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    PRIMARY INDEX (sku_idnt)
    ,COLUMN (sku_idnt)
        ON AN_SKU;


-- Inventory Staging Table
CREATE MULTISET VOLATILE TABLE INV_STG
AS (
	SELECT DISTINCT
		rms_sku_id AS sku_idnt
		, location_id AS loc_idnt
		, stock_on_hand_qty AS soh_qty
		, in_transit_qty AS in_transit_qty
	FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT inv
	INNER JOIN PRD_NAP_USR_VWS.STORE_DIM loc
       ON inv.location_id = loc.store_num
   	INNER JOIN AN_SKU sku
            ON inv.rms_sku_id = sku.sku_idnt
	WHERE snapshot_date = CURRENT_DATE - 1
		AND CHANNEL_NUM in (110, 120)
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    PRIMARY INDEX (sku_idnt, loc_idnt)
    ,COLUMN (sku_idnt, loc_idnt)
        ON INV_STG;


-- Pull Future OO and union with Inventory
CREATE MULTISET VOLATILE TABLE OWNERSHIP_STG
AS (
    SELECT
        sub.sku_idnt
        , sub.loc_idnt
        , SUM(COALESCE(sub.on_order_dollars, 0)) AS on_order_dollars
        , SUM(COALESCE(sub.on_order_cost, 0)) AS on_order_cost
        , SUM(COALESCE(sub.on_order_units, 0)) AS on_order_units
        , SUM(COALESCE(sub.oo_ordered_dollars, 0)) AS total_ordered_oo_dollars
        , SUM(COALESCE(sub.oo_ordered_cost, 0)) AS total_ordered_oo_cost
        , SUM(COALESCE(sub.oo_ordered, 0)) AS total_ordered_oo_units
        , SUM(COALESCE(sub.oo_received_dollars, 0)) AS total_received_oo_dollars
        , SUM(COALESCE(sub.oo_received_cost, 0)) AS total_received_oo_cost
        , SUM(COALESCE(sub.oo_received, 0)) AS total_received_oo_units
        , SUM(COALESCE(sub.oo_canceled, 0)) AS total_canceled_oo_units
        , SUM(COALESCE(sub.in_transit_dollars, 0)) AS in_transit_dollars
        , SUM(COALESCE(sub.in_transit_cost, 0)) AS in_transit_cost
        , SUM(COALESCE(sub.in_transit_units, 0)) AS in_transit_units
        , SUM(COALESCE(sub.eoh_dollars, 0)) AS eoh_dollars
        , SUM(COALESCE(sub.eoh_cost, 0)) AS eoh_cost
        , SUM(COALESCE(sub.eoh_units, 0)) AS eoh_units
        , CURRENT_TIMESTAMP AS table_update_date
	FROM (
		SELECT
	            oo.rms_sku_num AS sku_idnt
	            , CAST(oo.store_num AS VARCHAR(20)) AS loc_idnt
	            , SUM(oo.ANTICIPATED_RETAIL_AMT * oo.QUANTITY_OPEN) AS on_order_dollars
	            , SUM(oo.UNIT_ESTIMATED_LANDING_COST  * oo.QUANTITY_OPEN) AS on_order_cost
	            , SUM(oo.quantity_open) AS on_order_units
	            , SUM(oo.ANTICIPATED_RETAIL_AMT * oo.QUANTITY_ORDERED) AS oo_ordered_dollars
	            , SUM(oo.UNIT_ESTIMATED_LANDING_COST  * oo.QUANTITY_ORDERED) AS oo_ordered_cost
	            , SUM(oo.quantity_ordered) AS oo_ordered
	            , SUM(oo.ANTICIPATED_RETAIL_AMT * oo.QUANTITY_RECEIVED) AS oo_received_dollars
	            , SUM(oo.UNIT_ESTIMATED_LANDING_COST  * oo.QUANTITY_RECEIVED) AS oo_received_cost
	            , SUM(oo.quantity_received) AS oo_received
	            , SUM(oo.quantity_canceled) AS oo_canceled
	            -- Inventory
	            , CAST(0 AS DECIMAL(38,2)) AS eoh_dollars
	            , CAST(0 AS DECIMAL(38,2)) AS eoh_cost
	            , CAST(0 AS INTEGER) AS eoh_units
	            , CAST(0 AS DECIMAL(38,2)) AS in_transit_dollars
	            , CAST(0 AS DECIMAL(38,2)) AS in_transit_cost
	            , CAST(0 AS INTEGER) AS in_transit_units
	        FROM
	            PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW oo
	        INNER JOIN PRD_NAP_USR_VWS.STORE_DIM loc
	            ON oo.store_num = loc.store_num
	        INNER JOIN AN_SKU sku
	            ON oo.rms_sku_num = sku.sku_idnt
	        WHERE week_num <= 202325 --Week of 7/17/23 Public Sale Day 1 --202329 -- 1 week post-event
	            AND week_num >= 202314 --May Week 1 --(SELECT MAX(week_idnt) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM dc WHERE week_end_day_date <= CURRENT_DATE)
	            AND channel_num in (110, 120)
	        GROUP BY 1, 2

	        UNION ALL

	        --Inventory
	        SELECT
	            inv.sku_idnt
	            , inv.loc_idnt
	            , CAST(0 AS DECIMAL(38,9)) AS on_order_dollars
	            , CAST(0 AS DECIMAL(38,9)) AS on_order_cost
	            , CAST(0 AS INTEGER) AS on_order_units
	            , CAST(0 AS DECIMAL(38,9)) AS oo_ordered_dollars
	            , CAST(0 AS DECIMAL(38,9)) AS oo_ordered_cost
	            , CAST(0 AS INTEGER) AS oo_ordered
	            , CAST(0 AS INTEGER) AS oo_received
	            , CAST(0 AS DECIMAL(38,9)) AS oo_received_dollars
	            , CAST(0 AS DECIMAL(38,9)) AS oo_received_cost
	            , CAST(0 AS INTEGER) AS oo_canceled
	            -- Inventory
	            , SUM(inv.soh_qty * ownership_retail_price_amt) AS eoh_dollars
	            , SUM(inv.soh_qty * mf.weighted_average_cost) AS eoh_cost
	            , SUM(inv.soh_qty) AS eoh_units
	            , SUM(inv.in_transit_qty * ownership_retail_price_amt) AS in_transit_dollars
	            , SUM(inv.in_transit_qty * mf.weighted_average_cost) AS in_transit_cost
	            , SUM(inv.in_transit_qty) AS in_transit_units
	        FROM INV_STG inv
	        INNER JOIN PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW loc
	            ON inv.loc_idnt = loc.store_num
	        INNER JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM pr
	            ON pr.store_num = loc.price_store_num
	                AND pr.rms_sku_num = inv.sku_idnt
	                AND PERIOD(pr.eff_begin_tmstp, pr.eff_end_tmstp) CONTAINS CURRENT_TIMESTAMP AT TIME ZONE 'GMT'
    		LEFT JOIN T2DL_DAS_ACE_MFP.SKU_LOC_PRICETYPE_DAY mf
		    	ON mf.SKU_IDNT = inv.sku_idnt
		    		AND mf.LOC_IDNT = inv.loc_idnt
		    		AND mf.DAY_DT = CURRENT_DATE - 1
		    		AND mf.PRICE_TYPE = LEFT(pr.ownership_retail_price_type_code, 1)
	        GROUP BY 1, 2
    ) sub
    GROUP BY 1, 2
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    PRIMARY INDEX (sku_idnt, loc_idnt)
    ,COLUMN (sku_idnt, loc_idnt)
        ON OWNERSHIP_STG;

/******************************************************************************
** END: VOLATILE TABLES
******************************************************************************/



/******************************************************************************
** START: FINAL INSERT
******************************************************************************/

/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'ANNIVERSARY_OWNERSHIP', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.ANNIVERSARY_OWNERSHIP
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
(
	table_update_date 		TIMESTAMP(6) WITH TIME ZONE
    , data_source			VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('NAP')
    , country				VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('US')
    , channel				VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('110, FULL LINE STORES', '310, RESERVE STOCK', '120, N.COM')
    , channel_idnt			INTEGER NOT NULL COMPRESS(110, 310, 120)
    , region				VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC
    , location				VARCHAR(46) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , division				VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subdivision			VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC
    , department			VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC
    , "class"				VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subclass				VARCHAR(73) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_group_num		VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_group_desc		VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , style_num 			BIGINT
    , style_desc			VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC
    , vpn					VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , supp_color			VARCHAR(80) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , nrf_colr				VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
    , brand_tier 			VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , brand 				VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
    , supplier_num 			VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , supplier 				VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC
    , udig					VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , npg_ind				VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , rp_ind				VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Y', 'N')
    , on_order_dollars		DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , on_order_cost 		DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , on_order_units		INTEGER NOT NULL
    , ttl_ordered_dollars   DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , ttl_ordered_cost      DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , ttl_ordered_units 	INTEGER NOT NULL
    , ttl_received_dollars  DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , ttl_received_cost     DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , ttl_received_units	INTEGER NOT NULL
    , ttl_canceled_units	INTEGER NOT NULL
    , in_transit_dollars	DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , in_transit_cost   	DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , in_transit_units		INTEGER NOT NULL
    , eoh_dollars			DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_cost  			DECIMAL(20,2) NOT NULL DEFAULT 0.00 COMPRESS 0.00
    , eoh_units				INTEGER NOT NULL
)
PRIMARY INDEX (vpn, supp_color, location);
GRANT SELECT ON {environment_schema}.ANNIVERSARY_OWNERSHIP TO PUBLIC;
*/


DELETE FROM {environment_schema}.ANNIVERSARY_OWNERSHIP ALL;
INSERT INTO {environment_schema}.ANNIVERSARY_OWNERSHIP

SELECT apo.table_update_date
    , 'NAP' AS data_source
    , loc.country
    , TRIM(loc.chnl_label) AS channel
    , loc.chnl_idnt AS channel_idnt
    , TRIM(loc.regn_label) AS region
    , TRIM(loc.loc_label) AS location
    , TRIM(sku.div_num|| ', '||sku.div_desc) AS division
    , TRIM(sku.grp_num|| ', '||sku.grp_desc) AS subdivision
    , TRIM(sku.dept_num|| ', '||sku.dept_desc) AS department
    , TRIM(sku.class_num|| ', '||sku.class_desc) AS "class"
    , TRIM(sku.sbclass_num|| ', '||sku.sbclass_desc) AS subclass
    , sku.style_group_num AS style_group_num
    , sku.style_group_desc
    , sku.web_style_num AS style_num
    , sku.style_desc
    , sku.supp_part_num AS vpn
    , COALESCE(sku.supp_color, 'None') as supp_color
    , sku.color_desc AS nrf_colr
    , spdpt.preferred_partner_desc AS brand_tier
    , vb.vendor_brand_name AS brand
    , sku.prmy_supp_num AS supplier_num
    , ven.vendor_name AS supplier
    , udig.udig
    , coalesce(SKU.npg_ind, 'N') AS npg_ind
    , max(CASE WHEN rp.rp_ind = 1 THEN 'Y' ELSE 'N' END) AS rp_ind
    , sum(apo.on_order_dollars) AS on_order_dollars
    , SUM(apo.on_order_cost) AS on_order_cost
    , sum(apo.on_order_units) AS on_order_units
    , SUM(apo.total_ordered_oo_dollars) AS ttl_ordered_dollars
    , SUM(apo.total_ordered_oo_cost) AS ttl_ordered_cost
    , SUM(apo.total_ordered_oo_units) AS ttl_ordered_units
    , SUM(apo.total_received_oo_dollars) AS ttl_received_dollars
    , SUM(apo.total_received_oo_cost) AS ttl_received_cost
    , SUM(apo.total_received_oo_units) AS ttl_received_units
    , SUM(apo.total_canceled_oo_units) AS ttl_canceled_units
    , sum(apo.in_transit_dollars) AS in_transit_dollars
    , SUM(apo.in_transit_cost) AS in_transit_cost
    , sum(apo.in_transit_units) AS in_transit_units
    , sum(apo.eoh_dollars) AS eoh_dollars
    , sum(apo.eoh_cost) AS eoh_cost
    , sum(apo.eoh_units) AS eoh_units

FROM OWNERSHIP_STG apo
INNER JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
    ON apo.sku_idnt = sku.rms_sku_num
    	AND sku.channel_country = 'US'
INNER JOIN (
    SELECT
    	channel_num||', '||channel_desc AS chnl_label
        , channel_num AS chnl_idnt
        , region_num||', '||region_desc AS regn_label
        , CAST(store_num AS VARCHAR(4))||', '||store_short_name AS loc_label
        , store_num AS loc_idnt
        , channel_desc AS chnl_desc
        , store_country_code AS country
    FROM PRD_NAP_USR_VWS.STORE_DIM
    WHERE store_country_code = 'US'
        AND store_close_date IS NULL
)loc
    ON apo.loc_idnt = loc.loc_idnt
LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM ven
	ON ven.vendor_num = SKU.prmy_supp_num
LEFT JOIN PRD_NAP_USR_VWS.VENDOR_BRAND_XREF vb
	ON vb.vendor_num = SKU.prmy_supp_num
        AND vb.vendor_brand_name = SKU.brand_name
LEFT JOIN PRD_NAP_USR_VWS.SUPP_DEPT_MAP_DIM spdpt
	ON spdpt.supplier_num = SKU.prmy_supp_num
		AND spdpt.dept_num = SKU.dept_num
		AND spdpt.banner = 'FP'
		AND CURRENT_DATE BETWEEN CAST(spdpt.eff_begin_tmstp AS DATE FORMAT 'YYYY-MM-DD') AND CAST(spdpt.eff_end_tmstp AS DATE FORMAT 'YYYY-MM-DD')
LEFT JOIN
(
   SELECT
        rms_sku_idnt AS sku_idnt
        , loc_idnt
        , MAX(CASE WHEN aip_rp_fl = 'Y' THEN 1 END) AS rp_ind
    FROM prd_usr_vws.aip_rp_sku_ld_lkup
    WHERE day_idnt = (SELECT DISTINCT day_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = (CURRENT_DATE - INTERVAL '1' DAY))
    GROUP BY 1, 2
) rp
    ON apo.sku_idnt = rp.sku_idnt
    AND apo.loc_idnt = rp.loc_idnt
LEFT JOIN
(
  SELECT DISTINCT udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS udig,
                'US' AS country,
                sku_idnt
         FROM PRD_USR_VWS.UDIG_ITM_GRP_SKU_LKUP
         WHERE UDIG_COLCTN_IDNT IN ('50002')
) udig
    ON apo.sku_idnt = udig.sku_idnt
    AND loc.country = udig.country
WHERE sku.partner_relationship_type_code <> 'ECONCESSION'
    AND ven.vendor_name not like '%MKTPL'		

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
;


COLLECT STATISTICS COLUMN(vpn, supp_color, location) ON {environment_schema}.ANNIVERSARY_OWNERSHIP;
--GRANT SELECT ON {environment_schema}.ANNIVERSARY_OWNERSHIP TO PUBLIC;
/******************************************************************************
** END: FINAL INSERT
******************************************************************************/
