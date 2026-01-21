/******************************************************************************
Name: Scaled Events Anniversary Scaled Event Base Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the Scaled Events Base table for the evening FULL DAG refressh
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
                {start_date} - beginning date of refresh date range
                {end_date} - ending date of refresh date range
                {start_date_partition} - static start date of DS Volatile Table partition
                {end_date_partition} - static end date of DS Volatile Table partition
                {date_event_type} - Anniversary/Cyber data indicator
DAG: merch_se_all_daily_full
TABLE NAME: T2DL_DAS_SCALED_EVENTS.scaled_event_base (T3: T3DL_ACE_PRA.scaled_event_base)
Author(s): Manuela Hurtado Gonzalez, Alli Moore
Date Last Updated: 06-10-2024
******************************************************************************/

------------------------------------------------------------- START TEMPORARY TABLES -----------------------------------------------------------------------

-- LOCS
CREATE MULTISET VOLATILE TABLE locs
AS (
    SELECT
        loc_idnt
        , chnl_idnt
        , CASE
	        WHEN chnl_idnt IN (110, 120, 310, 920) THEN 'NORDSTROM'
        	WHEN chnl_idnt IN (210, 250) THEN 'NORDSTROM_RACK'
        	END AS channel_brand
        , CASE
            WHEN chnl_idnt IN (110, 210) THEN 'STORE'
            WHEN chnl_idnt IN (120, 250) THEN 'ONLINE'
            WHEN chnl_idnt IN (310, 920) THEN 'ONLINE'  -- RS, DC match to online in price_dim
            ELSE NULL
            END as EVENT_SELLING_CHANNEL
        , CASE
            WHEN chnl_idnt = 110 THEN 'FL'
            WHEN chnl_idnt IN (210, 250) THEN 'RK'
            WHEN chnl_idnt = 120 THEN 'FC'
            WHEN chnl_idnt IN (310, 920) THEN 'FC'  -- RS, DC match to 'FC' in price_dim
            --WHEN chnl_idnt IN (250) THEN 'HL' -- store 828 = "HL" IN NTN BUT "RK" IN price_dim
            ELSE NULL
            END as event_store_type_code
        , CASE
            WHEN cyber_loc_ind = 1 THEN 'Cyber'
            ELSE NULL
        END AS cyber_loc_ind_type
        , CASE
            WHEN anniv_loc_ind = 1 THEN 'Anniversary'
            ELSE NULL
        END AS anniv_loc_ind_type
    FROM {environment_schema}.SCALED_EVENT_LOCS_VW
    WHERE anniv_loc_ind_type = '{date_event_type}'
        OR cyber_loc_ind_type = '{date_event_type}'

) WITH DATA
PRIMARY INDEX(loc_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( LOC_IDNT )
    ,COLUMN ( loc_idnt )
        ON locs;


-- DATES
CREATE MULTISET VOLATILE TABLE dates
AS (
    SELECT
        tdl.day_idnt
        , tdl.day_dt
        , tdl.wk_idnt
        , tdl.anniv_ind
        , tdl.cyber_ind
        , CASE
            WHEN anniv_ind = 1 THEN 'Anniversary'
            WHEN cyber_ind = 1 THEN 'Cyber'
        END AS date_event_type
        , MIN(CASE WHEN anniv_ind = '1' THEN tdl.day_dt ELSE NULL END) OVER() AS min_dt_anniv
        , MAX(CASE WHEN anniv_ind = '1' THEN tdl.day_dt ELSE NULL END) OVER() AS max_dt_anniv
        , MIN(CASE WHEN cyber_ind = '1' THEN tdl.day_dt ELSE NULL END) OVER() AS min_dt_cyber
        , MAX(CASE WHEN cyber_ind = '1' THEN tdl.day_dt ELSE NULL END) OVER() AS max_dt_cyber
        , CASE
            WHEN anniv_ind = 1 THEN min_dt_anniv
            WHEN cyber_ind = 1 THEN min_dt_cyber
        END AS min_date
        ,  CASE
            WHEN anniv_ind = 1 THEN max_dt_anniv
            WHEN cyber_ind = 1 THEN max_dt_cyber
        END AS max_date
    FROM {environment_schema}.SCALED_EVENT_DATES tdl
    WHERE day_dt BETWEEN {start_date} AND {end_date}
        AND date_event_type = '{date_event_type}'
) WITH DATA
PRIMARY INDEX(day_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( DAY_IDNT )
    ,COLUMN ( DAY_IDNT )
    ,COLUMN ( DAY_DT )
    ,COLUMN ( DAY_IDNT, WK_IDNT )
        ON dates;


-- RP
CREATE MULTISET VOLATILE TABLE rp (sku_idnt, day_idnt, rp_date_event_type, loc_idnt, rp_ind)
AS (
    SELECT DISTINCT
        rp.rms_sku_num AS sku_idnt
        , dt.day_idnt
        , CASE
            WHEN dt.anniv_ind = 1 THEN 'Anniversary'
            WHEN dt.cyber_ind = 1 THEN 'Cyber'
        END AS rp_date_event_type
        , loc.loc_idnt
        , 1 AS rp_ind
    FROM prd_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
    INNER JOIN locs loc
        ON loc.loc_idnt = rp.location_num
    INNER JOIN {environment_schema}.SCALED_EVENT_DATES dt
        ON rp.rp_period CONTAINS dt.day_dt
    WHERE dt.day_dt <= {end_date}
        AND rp_date_event_type = '{date_event_type}'
) WITH DATA
PRIMARY INDEX(sku_idnt, loc_idnt, day_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( SKU_IDNT, LOC_IDNT, DAY_IDNT )
        ON rp;

-- Dropship
CREATE MULTISET VOLATILE TABLE dropship(sku_idnt,day_dt)
AS (
    SELECT DISTINCT
        rms_sku_id AS sku_idnt
        ,snapshot_date AS day_dt
    FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT
    WHERE location_type IN ('DS')
        AND snapshot_date BETWEEN {start_date} AND {end_date}
) WITH DATA
PRIMARY INDEX(sku_idnt)
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '{start_date_partition}' AND DATE '{end_date_partition}' EACH INTERVAL '1' DAY, NO RANGE)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( SKU_IDNT )
    ,COLUMN ( sku_idnt )
    ,COLUMN ( DAY_DT )
        ON dropship;

-- NTN
CREATE MULTISET VOLATILE TABLE scaled_event_customer_ntn
AS (
    SELECT a.sku_num AS sku_idnt
        ,c.loc_idnt AS loc_idnt_ntn
        ,b.day_idnt
        ,a.business_day_date AS day_dt
        ,b.date_event_type
        ,CASE WHEN d.selling_retail_price_type_code = 'PROMOTION' THEN 'P'
        	WHEN d.selling_retail_price_type_code = 'REGULAR' THEN 'R'
        	WHEN d.selling_retail_price_type_code = 'CLEARANCE' THEN 'C'
        	END AS price_type -- Updated
        ,COUNT(DISTINCT a.ACP_ID) AS ntn
    FROM PRD_NAP_USR_VWS.CUSTOMER_NTN_FACT a
        INNER JOIN LOCS c
            ON a.store_num = c.loc_idnt
        INNER JOIN DATES b
            ON a.business_day_date = b.day_dt

        INNER JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM d
			ON d.RMS_SKU_NUM = a.sku_num
      AND d.CHANNEL_COUNTRY = 'US'
			AND d.SELLING_CHANNEL = c.event_selling_channel
			AND d.channel_brand = c.channel_brand
			AND a.TRANS_UTC_TMSTP BETWEEN d.EFF_BEGIN_TMSTP AND d.EFF_END_TMSTP
    WHERE a.merch_ind = 1
        AND a.sku_num IS NOT NULL
    GROUP BY  1,2,3,4,5,6
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt_ntn, day_idnt, price_type )
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( SKU_IDNT, LOC_IDNT_NTN, DAY_IDNT, PRICE_TYPE )
    ,COLUMN ( SKU_IDNT, LOC_IDNT_NTN, DAY_IDNT, PRICE_TYPE )
        ON SCALED_EVENT_CUSTOMER_NTN;




------------------------------------------------------------- END TEMPORARY TABLES -----------------------------------------------------------------------





---------------------------------------------------------------- START MAIN QUERY -------------------------------------------------------------------------

DELETE FROM {environment_schema}.scaled_event_base WHERE date_event_type = '{date_event_type}' AND day_dt BETWEEN {start_date} AND {end_date}; -- Delete only rows that will be refreshed and inserted below

INSERT INTO {environment_schema}.scaled_event_base
SELECT
      ap.sku_idnt
    , ap.loc_idnt
    , ap.day_idnt
    , ap.day_dt
    , COALESCE(ap.price_type, 'NA') AS price_type
    , ap.date_event_type
    , locs.chnl_idnt
    , MAX(CASE WHEN anniv.anniv_item_ind = 1 THEN 'Y' ELSE 'N' END) AS anniv_ind
    , MAX(CASE WHEN rp.rp_ind = 1 THEN 'Y' ELSE 'N' END) AS rp_ind
    , SUM(ap.sales_units) AS sales_units
    , SUM(ap.sales_dollars) AS sales_dollars
    , SUM(ap.return_units) AS return_units
    , SUM(ap.return_dollars) AS return_dollars
    , SUM(ap.ntn) AS ntn
    , SUM(ap.demand_units) AS demand_units
    , SUM(ap.demand_dollars) AS demand_dollars
    , SUM(ap.shipped_units) AS shipped_units
    , SUM(ap.shipped_dollars) AS shipped_dollars
    , SUM(ap.demand_cancel_units) AS demand_cancel_units
    , SUM(ap.demand_cancel_dollars) AS demand_cancel_dollars
    , SUM(ap.demand_dropship_units) AS demand_dropship_units
    , SUM(ap.demand_dropship_dollars) AS demand_dropship_dollars
    , SUM(ap.store_fulfill_units) AS store_fulfill_units
    , SUM(ap.store_fulfill_dollars) AS store_fulfill_dollars
    , SUM(ap.dropship_units) AS dropship_units
    , SUM(ap.dropship_dollars) AS dropship_dollars
    , SUM(ap.eoh_units) AS eoh_units
    , SUM(ap.eoh_dollars) AS eoh_dollars
    , SUM(ap.boh_units) AS boh_units
    , SUM(ap.boh_dollars) AS boh_dollars
    , SUM(ap.nonsellable_units) AS nonsellable_units
    , SUM(ap.cogs) as cogs
    , SUM(ap.receipt_units) AS receipt_units
    , SUM(ap.receipt_dollars) AS receipt_dollars
    , SUM(ap.receipt_dropship_units) AS receipt_dropship_units
    , SUM(ap.receipt_dropship_dollars) AS receipt_dropship_dollars
    , CASE WHEN SUM(ap.sales_units) = 0 THEN 0
        ELSE SUM(ap.sales_dollars)/SUM(ap.sales_units)
        END AS sales_aur
    , CASE WHEN SUM(ap.demand_units) = 0 THEN 0
        ELSE SUM(ap.demand_dollars)/SUM(ap.demand_units)
        END AS demand_aur
    , CASE WHEN SUM(ap.eoh_units) = 0 THEN 0
        ELSE SUM(ap.eoh_dollars)/SUM(ap.eoh_units)
        END AS eoh_aur
    , CASE WHEN SUM(ap.receipt_units) = 0 THEN 0
        ELSE SUM(ap.receipt_dollars)/SUM(ap.receipt_units)
        END AS receipt_aur
    , COALESCE(MAX(anniv.reg_price_amt),0) AS anniv_retail_original
    , COALESCE(MAX(anniv.spcl_price_amt),0) AS anniv_retail_special
    , current_timestamp as process_tmstp
    -- NEW FIELDS - Added Anniv 2023
    , SUM(ap.sales_cost) AS sales_cost -- NEW
    , SUM(ap.return_cost) AS return_cost -- NEW
	, SUM(ap.shipped_cost) AS shipped_cost -- NEW
	, SUM(ap.store_fulfill_cost) AS store_fulfill_cost -- NEW
	, SUM(ap.dropship_cost) AS dropship_cost -- NEW
	, SUM(ap.eoh_cost) AS eoh_cost -- NEW
	, SUM(ap.boh_cost) AS boh_cost -- NEW
	, SUM(ap.receipt_cost) AS receipt_cost -- NEW
	, SUM(ap.receipt_dropship_cost) AS receipt_dropship_cost -- NEW
	, SUM(ap.sales_pm) AS sales_pm -- NEW
FROM
    (
    SELECT
        COALESCE(sls_dmd_inv.sku_idnt, rcpt.sku_idnt) AS sku_idnt
        , TRIM(COALESCE(sls_dmd_inv.loc_idnt, rcpt.loc_idnt)) AS loc_idnt
        , COALESCE(sls_dmd_inv.day_idnt, rcpt.day_idnt) AS day_idnt
        , COALESCE(sls_dmd_inv.day_dt, rcpt.day_dt) AS day_dt
        , COALESCE(sls_dmd_inv.price_type, rcpt.price_type) AS price_type
        , COALESCE(sls_dmd_inv.date_event_type, rcpt.date_event_type) AS date_event_type
        , SUM(COALESCE(sls_dmd_inv.sales_units, 0)) AS sales_units
        , SUM(COALESCE(sls_dmd_inv.sales_dollars, 0)) AS sales_dollars
        , SUM(COALESCE(sls_dmd_inv.sales_cost, 0)) AS sales_cost -- NEW
        , SUM(COALESCE(sls_dmd_inv.return_units, 0)) AS return_units
        , SUM(COALESCE(sls_dmd_inv.return_dollars, 0)) AS return_dollars
        , SUM(COALESCE(sls_dmd_inv.return_cost, 0)) AS return_cost -- NEW
        , SUM(COALESCE(sls_dmd_inv.ntn,0)) AS ntn
        , SUM(COALESCE(sls_dmd_inv.demand_units, 0)) AS demand_units
        , SUM(COALESCE(sls_dmd_inv.demand_dollars, 0)) AS demand_dollars
        , SUM(COALESCE(sls_dmd_inv.shipped_units, 0)) AS shipped_units
        , SUM(COALESCE(sls_dmd_inv.shipped_dollars, 0)) AS shipped_dollars
        , SUM(COALESCE(sls_dmd_inv.shipped_cost, 0)) AS shipped_cost -- NEW
        , SUM(COALESCE(sls_dmd_inv.demand_cancel_units, 0)) AS demand_cancel_units
        , SUM(COALESCE(sls_dmd_inv.demand_cancel_dollars, 0)) AS demand_cancel_dollars
        , SUM(COALESCE(sls_dmd_inv.demand_dropship_units, 0)) AS demand_dropship_units
        , SUM(COALESCE(sls_dmd_inv.demand_dropship_dollars, 0)) AS demand_dropship_dollars
        , SUM(COALESCE(sls_dmd_inv.store_fulfill_units, 0)) AS store_fulfill_units
        , SUM(COALESCE(sls_dmd_inv.store_fulfill_dollars, 0)) AS store_fulfill_dollars
        , SUM(COALESCE(sls_dmd_inv.store_fulfill_cost, 0)) AS store_fulfill_cost -- NEW
        , SUM(COALESCE(sls_dmd_inv.dropship_dollars, 0)) AS dropship_dollars
        , SUM(COALESCE(sls_dmd_inv.dropship_units, 0)) AS dropship_units
        , SUM(COALESCE(sls_dmd_inv.dropship_cost, 0)) AS dropship_cost -- NEW
        , SUM(COALESCE(sls_dmd_inv.eoh_units, 0)) AS eoh_units
        , SUM(COALESCE(sls_dmd_inv.eoh_dollars, 0)) AS eoh_dollars
        , SUM(COALESCE(sls_dmd_inv.eoh_cost, 0)) AS eoh_cost -- NEW
        , SUM(COALESCE(sls_dmd_inv.boh_units, 0)) AS boh_units
        , SUM(COALESCE(sls_dmd_inv.boh_dollars, 0)) AS boh_dollars
        , SUM(COALESCE(sls_dmd_inv.boh_cost, 0)) AS boh_cost -- NEW
        , SUM(COALESCE(sls_dmd_inv.nonsellable_units, 0)) AS nonsellable_units
        , SUM(COALESCE(sls_dmd_inv.cogs,0)) as cogs
        , SUM(COALESCE(sls_dmd_inv.sales_pm, 0)) AS sales_pm -- NEW
        , SUM(COALESCE(rcpt.receipt_units, 0)) AS receipt_units
        , SUM(COALESCE(rcpt.receipt_dollars, 0)) AS receipt_dollars
        , SUM(COALESCE(rcpt.receipt_cost, 0)) AS receipt_cost --NEW
        , SUM(COALESCE(rcpt.receipt_dropship_units,0)) AS receipt_dropship_units
        , SUM(COALESCE(rcpt.receipt_dropship_dollars,0)) AS receipt_dropship_dollars
        , SUM(COALESCE(rcpt.receipt_dropship_cost, 0)) AS receipt_dropship_cost --NEW
    FROM
    (
        SELECT
            COALESCE(sls_dmnd_inv.sku_idnt, ntn.sku_idnt) AS sku_idnt
            , COALESCE(sls_dmnd_inv.loc_idnt, ntn.loc_idnt) AS loc_idnt
            , COALESCE(sls_dmnd_inv.day_idnt, ntn.day_idnt) AS day_idnt
            , COALESCE(sls_dmnd_inv.day_dt, ntn.day_dt) AS day_dt
            , COALESCE(sls_dmnd_inv.price_type, ntn.price_type) AS price_type
            , COALESCE(sls_dmnd_inv.date_event_type, ntn.date_event_type) AS date_event_type
            , SUM(COALESCE(sls_dmnd_inv.sales_units, 0)) AS sales_units
            , SUM(COALESCE(sls_dmnd_inv.sales_dollars, 0)) AS sales_dollars
            , SUM(COALESCE(sls_dmnd_inv.sales_cost, 0)) AS sales_cost -- NEW
            , SUM(COALESCE(sls_dmnd_inv.return_units, 0)) AS return_units
            , SUM(COALESCE(sls_dmnd_inv.return_dollars, 0)) AS return_dollars
            , SUM(COALESCE(sls_dmnd_inv.return_cost, 0)) AS return_cost -- NEW
            , SUM(COALESCE(ntn.ntn, 0)) AS ntn
            , SUM(COALESCE(sls_dmnd_inv.demand_units, 0)) AS demand_units
            , SUM(COALESCE(sls_dmnd_inv.demand_dollars, 0)) AS demand_dollars
            , SUM(COALESCE(sls_dmnd_inv.shipped_units, 0)) AS shipped_units
            , SUM(COALESCE(sls_dmnd_inv.shipped_dollars, 0)) AS shipped_dollars
            , SUM(COALESCE(sls_dmnd_inv.shipped_cost, 0)) AS shipped_cost -- NEW
            , SUM(COALESCE(sls_dmnd_inv.demand_cancel_units, 0)) AS demand_cancel_units
            , SUM(COALESCE(sls_dmnd_inv.demand_cancel_dollars, 0)) AS demand_cancel_dollars
            , SUM(COALESCE(sls_dmnd_inv.demand_dropship_units, 0)) AS demand_dropship_units
            , SUM(COALESCE(sls_dmnd_inv.demand_dropship_dollars, 0)) AS demand_dropship_dollars
            , SUM(COALESCE(sls_dmnd_inv.store_fulfill_units, 0)) AS store_fulfill_units
            , SUM(COALESCE(sls_dmnd_inv.store_fulfill_dollars, 0)) AS store_fulfill_dollars
            , SUM(COALESCE(sls_dmnd_inv.store_fulfill_cost, 0)) AS store_fulfill_cost -- NEW
            , SUM(COALESCE(sls_dmnd_inv.dropship_dollars, 0)) AS dropship_dollars
            , SUM(COALESCE(sls_dmnd_inv.dropship_units, 0)) AS dropship_units
            , SUM(COALESCE(sls_dmnd_inv.dropship_cost, 0)) AS dropship_cost -- NEW
            , SUM(COALESCE(sls_dmnd_inv.eoh_units, 0)) AS eoh_units
            , SUM(COALESCE(sls_dmnd_inv.eoh_dollars, 0)) AS eoh_dollars
            , SUM(COALESCE(sls_dmnd_inv.eoh_cost, 0)) AS eoh_cost -- NEW
            , SUM(COALESCE(sls_dmnd_inv.boh_units, 0)) AS boh_units
            , SUM(COALESCE(sls_dmnd_inv.boh_dollars, 0)) AS boh_dollars
            , SUM(COALESCE(sls_dmnd_inv.boh_cost, 0)) AS boh_cost -- NEW
            , SUM(COALESCE(sls_dmnd_inv.nonsellable_units, 0)) AS nonsellable_units
            , SUM(COALESCE(CAST(sls_dmnd_inv.cogs AS DECIMAL(20,2)),0)) as cogs
            , SUM(COALESCE(sls_dmnd_inv.sales_pm,0)) as sales_pm -- NEW
        FROM
        (
            SELECT
                base.sku_idnt
                , base.loc_idnt
                , dates.day_idnt
                , base.day_dt
                , base.price_type
                , dates.date_event_type
        ------- SALES -------
                , SUM(base.SALES_UNITS) AS sales_units
                , SUM(base.SALES_DOLLARS) AS sales_dollars
                , SUM(base.cost_of_goods_sold) AS sales_cost -- NEW
                , SUM(base.RETURN_UNITS) AS return_units
                , SUM(base.RETURN_DOLLARS) AS return_dollars
                , SUM(base.RETURN_UNITS * WEIGHTED_AVERAGE_COST) AS return_cost -- NEW
        ------- DEMAND -------
                , SUM(DEMAND_UNITS) AS demand_units
                , SUM(DEMAND_DOLLARS) AS demand_dollars
                , SUM(SHIPPED_UNITS) AS shipped_units
                , SUM(SHIPPED_DOLLARS) AS shipped_dollars
                , SUM(SHIPPED_UNITS * WEIGHTED_AVERAGE_COST) AS shipped_cost -- NEW - incorrect + not used in reporting
                , SUM(DEMAND_CANCEL_UNITS) AS demand_cancel_units
                , SUM(DEMAND_CANCEL_DOLLARS) AS demand_cancel_dollars
                , SUM(DEMAND_DROPSHIP_UNITS) AS demand_dropship_units
                , SUM(DEMAND_DROPSHIP_DOLLARS) AS demand_dropship_dollars
                , SUM(STORE_FULFILL_UNITS) AS store_fulfill_units
                , SUM(STORE_FULFILL_DOLLARS) AS store_fulfill_dollars
                , SUM(STORE_FULFILL_UNITS * WEIGHTED_AVERAGE_COST) AS store_fulfill_cost -- NEW - incorrect + not used in reporting
                , SUM(CASE WHEN dropship.sku_idnt IS NOT NULL THEN base.SALES_DOLLARS ELSE NULL END) AS dropship_dollars
                , SUM(CASE WHEN dropship.sku_idnt IS NOT NULL THEN base.SALES_UNITS ELSE NULL END) AS dropship_units
                , SUM(CASE WHEN dropship.sku_idnt IS NOT NULL THEN base.cost_of_goods_sold ELSE NULL END) AS dropship_cost -- NEW
        ------- INVENTORY -------
                , SUM(EOH_UNITS) AS eoh_units
                , SUM(EOH_DOLLARS) AS eoh_dollars
                , SUM(EOH_UNITS * WEIGHTED_AVERAGE_COST) AS eoh_cost -- NEW
        ------- Non-Sellable Fix -------  bring non-sellable from base for AN2025
                , SUM(COALESCE(ns.nonsellable_qty,0)) AS nonsellable_units
                , SUM(BOH_UNITS) AS boh_units
                , SUM(BOH_DOLLARS) AS boh_dollars
                , SUM(BOH_UNITS * WEIGHTED_AVERAGE_COST) AS boh_cost -- NEW
        ------- COST -------
                , SUM(cost_of_goods_sold) AS cogs
                , SUM(CASE WHEN base.cost_of_goods_sold IS NOT NULL THEN base.sales_dollars ELSE NULL END) AS sales_pm -- NEW
            FROM t2dl_das_ace_mfp.sku_loc_pricetype_day_vw base
            INNER JOIN locs
                ON base.loc_idnt = locs.loc_idnt
            INNER JOIN dates
                ON base.day_dt = dates.day_dt
            LEFT JOIN dropship
                ON base.sku_idnt = dropship.sku_idnt
                AND base.day_dt  = dropship.day_dt
        ------- Non-Sellable Fix -------  delete join below for AN2025
            LEFT JOIN {environment_schema}.ANNIVERSARY_NONSELL ns
                ON base.loc_idnt = ns.location_id
                AND base.day_dt = ns.snapshot_date
                AND base.sku_idnt = ns.rms_sku_id
                AND base.price_type = ns.current_price_type
            GROUP BY 1, 2, 3, 4, 5, 6
        ) sls_dmnd_inv
        FULL OUTER JOIN
        (
            SELECT
                sku_idnt
                , CAST(loc_idnt_ntn AS INTEGER) AS loc_idnt
                , day_idnt
                , day_dt
                , price_type
                , date_event_type
                , ntn
            FROM Scaled_Event_customer_ntn
        )ntn
        ON sls_dmnd_inv.sku_idnt = ntn.sku_idnt
            AND sls_dmnd_inv.loc_idnt = ntn.loc_idnt
            AND sls_dmnd_inv.day_idnt = ntn.day_idnt
            AND sls_dmnd_inv.price_type = ntn.price_type
        GROUP BY 1,2,3,4,5,6
    ) sls_dmd_inv
    FULL OUTER JOIN
    (
        SELECT
        	base.SKU_NUM AS sku_idnt
        	, base.store_num AS loc_idnt
        	, dates.day_idnt
        	, base.TRAN_DATE AS day_dt
        	, CASE WHEN pt.ownership_retail_price_type_code = 'PROMOTION' THEN 'P'
    		WHEN pt.ownership_retail_price_type_code = 'CLEARANCE' THEN 'C'
    	 	ELSE 'R' END AS price_type
        	, dates.date_event_type
        	, SUM(RECEIPTS_UNITS + RECEIPTS_CROSSDOCK_UNITS) AS receipt_units
        	, SUM(RECEIPTS_RETAIL + RECEIPTS_CROSSDOCK_RETAIL) AS receipt_dollars
        	, SUM(RECEIPTS_COST + RECEIPTS_CROSSDOCK_COST) AS receipt_cost -- NEW
        	, SUM(CASE WHEN base.DROPSHIP_IND = 'Y' THEN RECEIPTS_UNITS + RECEIPTS_CROSSDOCK_UNITS ELSE 0 END) AS receipt_dropship_units
        	, SUM(CASE WHEN base.DROPSHIP_IND = 'Y' THEN RECEIPTS_RETAIL + RECEIPTS_CROSSDOCK_RETAIL ELSE 0 END) AS receipt_dropship_dollars
        	, SUM(CASE WHEN base.DROPSHIP_IND = 'Y' THEN RECEIPTS_COST + RECEIPTS_CROSSDOCK_COST ELSE 0 END) AS receipt_dropship_cost -- NEW
       	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_FACT_VW base
       	INNER JOIN locs
            ON base.store_num = locs.loc_idnt
        INNER JOIN dates
            ON base.TRAN_DATE = dates.day_dt
        LEFT JOIN PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW ps
			ON ps.store_num = base.store_num
   	 	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM pt
    		ON pt.rms_sku_num = base.sku_num
    		AND pt.store_num = ps.price_store_num
    		AND PERIOD(pt.eff_begin_tmstp, pt.eff_end_tmstp) CONTAINS base.EVENT_TIME AT TIME ZONE 'GMT'
        GROUP BY 1, 2, 3, 4, 5, 6
    ) rcpt
    ON sls_dmd_inv.sku_idnt = rcpt.sku_idnt
        AND sls_dmd_inv.loc_idnt = rcpt.loc_idnt
        AND sls_dmd_inv.day_idnt = rcpt.day_idnt
        AND sls_dmd_inv.price_type = rcpt.price_type
    GROUP BY 1, 2, 3, 4, 5, 6
) ap
LEFT JOIN rp
    ON ap.sku_idnt = rp.sku_idnt
    AND ap.loc_idnt = rp.loc_idnt
    AND ap.day_idnt = rp.day_idnt
LEFT JOIN locs
    ON ap.loc_idnt = locs.loc_idnt
LEFT JOIN {environment_schema}.anniversary_sku_chnl_date anniv
    ON ap.sku_idnt = anniv.sku_idnt
	AND anniv.channel_country = 'US'
	AND locs.EVENT_SELLING_CHANNEL = anniv.selling_channel
	AND locs.event_store_type_code = anniv.store_type_code
    AND ap.day_idnt = anniv.day_idnt
    AND 'Anniversary' = '{date_event_type}'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 43
;
COLLECT STATS
    PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt, price_type )
    ,COLUMN ( sku_idnt, loc_idnt, day_idnt, price_type )
    ,COLUMN(sku_idnt)
    ,COLUMN(loc_idnt)
    ,COLUMN(day_idnt)
    ,COLUMN(price_type)
        ON {environment_schema}.scaled_event_base;

-------------------------------------------------------------------- END MAIN QUERY ----------------------------------------------------------------------------
