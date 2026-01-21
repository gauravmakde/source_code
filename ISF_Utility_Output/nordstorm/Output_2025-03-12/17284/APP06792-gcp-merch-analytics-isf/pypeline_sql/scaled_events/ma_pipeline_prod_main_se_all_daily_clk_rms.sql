/******************************************************************************
Name: Scaled Events - Anniversary Scaled Event SKU CLK RMS Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the Scaled Events SKU CLK RMS table for the evening FULL DAG refressh
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
                {start_date} - beginning date of refresh date range
                {end_date} - ending date of refresh date range
                {start_date_partition} - static start date of DS Volatile Table partition
                {end_date_partition} - static end date of DS Volatile Table partition
                {date_event_type} - Anniversary/Cyber data indicator
DAG: merch_se_all_daily_delta
TABLE NAME: T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms (T3: T3DL_ACE_MCH.scaled_event_sku_clk_rms)
Author(s): Manuela Hurtado, Alli Moore
Date Last Updated: 06-10-2024
******************************************************************************/



-- locs
CREATE MULTISET VOLATILE TABLE locs
AS (
    SELECT
        locs.*
        , CASE
            WHEN cyber_loc_ind = 1 THEN 'Cyber'
            ELSE NULL
        END AS cyber_loc_ind_type
        , CASE
            WHEN anniv_loc_ind = 1 THEN 'Anniversary'
            ELSE NULL
        END AS anniv_loc_ind_type
        , CASE
           WHEN locs.chnl_idnt IN (110,120,130,140,310,920) THEN 'NORDSTROM'
           ELSE 'RACK'
       END AS banner
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
            WHEN chnl_idnt IN (110) THEN 'FL'
            WHEN chnl_idnt IN (210, 250) THEN 'RK'
            WHEN chnl_idnt IN (120) THEN 'FC'
            WHEN chnl_idnt IN (310, 920) THEN 'FC'  -- RS, DC match to 'FC' in price_dim
            ELSE NULL
            END as event_store_type_code
        , COALESCE(clu.peer_group_desc, 'NA') AS CLUSTER_CLIMATE
        , 'NA' AS NR_CLUSTER_PRICE -- Remove from DDL for Anniversary
    FROM {environment_schema}.SCALED_EVENT_LOCS_VW locs
    LEFT JOIN PRD_NAP_USR_VWS.STORE_PEER_GROUP_DIM clu
        ON clu.store_num = locs.loc_idnt
        AND PEER_GROUP_TYPE_CODE IN ('OPC', 'FPC') --  'OPC' for Rack or ‘FPC’ for Nordstrom
    WHERE anniv_loc_ind_type = '{date_event_type}'
        OR cyber_loc_ind_type = '{date_event_type}'

)
WITH DATA
PRIMARY INDEX ( loc_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( loc_idnt )
    ,COLUMN ( loc_idnt )
        ON locs;



-- dropship
CREATE MULTISET VOLATILE TABLE dropship(day_dt,sku_idnt)
AS (
    SELECT DISTINCT
        snapshot_date AS day_dt
        , rms_sku_id AS sku_idnt
    FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT
    WHERE location_type IN ('DS')
        AND snapshot_date BETWEEN {start_date} AND {end_date}
)
WITH DATA
PRIMARY INDEX ( sku_idnt )
PARTITION BY RANGE_N(DAY_DT BETWEEN DATE '{start_date_partition}' AND DATE '{end_date_partition}' EACH INTERVAL '1' DAY, NO RANGE)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
	PRIMARY INDEX ( sku_idnt )
	,COLUMN ( sku_idnt )
	,COLUMN ( day_dt )
		ON dropship;



-- rp
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
        INNER JOIN T2DL_DAS_SCALED_EVENTS.SCALED_EVENT_DATES dt
            ON rp.rp_period CONTAINS dt.day_dt
    WHERE dt.day_dt <= {end_date}
        AND rp_date_event_type = '{date_event_type}'
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
	PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )
		ON rp;


-- PFD
CREATE MULTISET VOLATILE TABLE pfd
AS (
    SELECT
        sku_idnt
        , loc_idnt
        , day_idnt
        , event_date_pacific AS day_dt
        , date_event
        , price_type
        , dma_code
        , SUM(product_views) AS product_views
        , SUM(cart_adds) AS cart_adds
        , SUM(order_units) AS order_units
        , SUM(instock_views) AS instock_views
        , SUM(scored_views) AS scored_views -- NEW
        , SUM(demand) AS demand
        , SUM(wishlist_adds) AS wishlist_adds
    FROM (
        SELECT
            pfd.rms_sku_num AS sku_idnt
            , pfd.event_date_pacific
            , dt.day_idnt
            , CASE
                WHEN anniv_ind = 1 THEN 'Anniversary'
                WHEN cyber_ind = 1 THEN 'Cyber'
            END AS date_event
            , CASE
                WHEN pfd.channel = 'FULL_LINE' THEN '808' --check previous code to check if CA was included
                WHEN pfd.channel = 'RACK' THEN '828'
            END AS loc_idnt
            , CASE WHEN pfd.current_price_type = 'R' THEN 'REGULAR'
            WHEN pfd.current_price_type = 'C' THEN 'CLEARANCE'
            WHEN pfd.current_price_type = 'P' THEN 'PROMOTION'
            END AS price_type
            , '0' as dma_code
            , SUM(pfd.product_views) AS product_views
            , SUM(pfd.add_to_bag_quantity) AS cart_adds
            , SUM(pfd.order_quantity) AS order_units
            , COALESCE(SUM(pfd.product_views*pfd.pct_instock),0) AS instock_views
            , SUM(CASE WHEN pfd.pct_instock IS NOT NULL THEN pfd.product_views END) AS scored_views -- NEW
            , SUM(order_demand) AS demand
            , SUM(0) AS wishlist_adds
        FROM T2DL_DAS_PRODUCT_FUNNEL.product_price_funnel_daily pfd
        INNER JOIN {environment_schema}.SCALED_EVENT_DATES dt
            ON pfd.event_date_pacific = dt.day_dt
                AND pfd.channelcountry = 'US'
        WHERE date_event = '{date_event_type}'
            AND dt.day_dt BETWEEN {start_date} AND {end_date}
        GROUP BY 1,2,3,4,5,6,7
        UNION ALL
        SELECT
            w.rms_sku_num
            , event_date_pacific
            , dt.day_idnt
            , CASE
                WHEN anniv_ind = 1 THEN 'Anniversary'
                WHEN cyber_ind = 1 THEN 'Cyber'
            END AS date_event
            , CASE
                WHEN channelbrand = 'NORDSTROM' THEN '808'
                WHEN channelbrand = 'NORDSTROM_RACK' THEN '828'
            END AS loc_idnt
            , selling_retail_price_type_code AS price_type
            , '0' as dma_code
            , SUM(0) AS product_views
            , SUM(0) AS cart_adds
            , SUM(0) AS order_units
            , SUM(0) AS instock_views
            , SUM(0) AS scored_views -- NEW
            , SUM(0) AS demand
            , SUM(quantity) AS wishlist_adds
        FROM T2DL_DAS_SCALED_EVENTS.sku_item_added w
        INNER JOIN {environment_schema}.SCALED_EVENT_DATES dt
            ON w.event_date_pacific = dt.day_dt
                AND w.channelcountry = 'US'
        LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM price
			ON price.RMS_SKU_NUM = w.rms_sku_num
				AND price.CHANNEL_COUNTRY = 'US'
				AND price.SELLING_CHANNEL = 'ONLINE'
				AND price.channel_brand = w.channelbrand
				AND w.event_date_pacific BETWEEN CAST((price.EFF_BEGIN_TMSTP AT TIME ZONE 'GMT-7') AS DATE FORMAT 'YYYY-MM-DD')
					AND (CAST((price.EFF_END_TMSTP AT TIME ZONE 'GMT-7') AS DATE FORMAT 'YYYY-MM-DD') - INTERVAL '1' DAY)
        WHERE date_event = '{date_event_type}'
            AND dt.day_dt BETWEEN {start_date} AND {end_date}

        GROUP BY 1,2,3,4,5,6,7
    ) a
    GROUP BY 1,2,3,4,5,6,7
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )
    ,COLUMN ( loc_idnt )
        ON pfd;

--AOR Lookup
CREATE MULTISET VOLATILE TABLE aor AS (
SELECT DISTINCT
    REGEXP_REPLACE(channel_brand,'_',' ') AS banner
    ,dept_num
    ,general_merch_manager_executive_vice_president
    ,div_merch_manager_senior_vice_president
    ,div_merch_manager_vice_president
    ,merch_director
    ,buyer
    ,merch_planning_executive_vice_president
    ,merch_planning_senior_vice_president
    ,merch_planning_vice_president
    ,merch_planning_director_manager
    ,assortment_planner
FROM prd_nap_usr_vws.area_of_responsibility_dim
QUALIFY ROW_NUMBER() OVER (PARTITION BY banner, dept_num ORDER BY 3,4,5,6,7,8,9,10,11,12) = 1
WHERE banner = 'NORDSTROM'
) WITH DATA
    PRIMARY INDEX (dept_num)
    ON COMMIT PRESERVE ROWS;
COLLECT STATS
    PRIMARY INDEX (dept_num)
        ON aor;

-- prod hier
CREATE VOLATILE MULTISET TABLE prod_hier AS (
    SELECT DISTINCT
        sku.rms_sku_num AS sku_idnt
        , sku.rms_style_num AS style_idnt
        , sku.style_desc
        , sku.supp_part_num AS supp_prt_nbr
        , sku.supp_color
        , sku.color_num AS colr_idnt
        , sku.style_group_num AS style_group_idnt
        , TRIM(sku.div_num)||', '||TRIM(sku.div_desc) AS div_label
        , TRIM(sku.grp_num||', '||sku.grp_desc) AS sdiv_label
        , TRIM(sku.dept_num||', '||sku.dept_desc) AS dept_label
        , TRIM(sku.class_num||', '||sku.class_desc) AS class_label
        , TRIM(sku.sbclass_num||', '||sku.sbclass_desc) AS sbclass_label
        , supp.vendor_name AS supp_name -- UPDATED
        , COALESCE(sup.vendor_brand_name, sku.brand_name) AS brand_name  -- UPDATED
        , supp.npg_flag AS npg_ind
        , th.merch_themes AS merch_theme
        , th.anniversary_theme --UPDATED
        , th.NORD_ROLE_DESC
        , th.RACK_ROLE_DESC
        , th.CCS_CATEGORY
        , th.CCS_SUBCATEGORY
        , th.QUANTRIX_CATEGORY
        , th.ASSORTMENT_GROUPING
        , th.HOLIDAY_21 AS HOLIDAY_THEME_TY
        , th.GIFT_IND
        , th.STOCKING_STUFFER
        , last_receipt_date
        , CASE WHEN dib.brand_name IS NOT NULL THEN 'Y' ELSE 'N' END AS bipoc_ind
        , COALESCE(general_merch_manager_executive_vice_president, 'NA') AS  general_merch_manager_executive_vice_president -- NEW FIELD AN 2024
        , COALESCE(div_merch_manager_senior_vice_president, 'NA') AS div_merch_manager_senior_vice_president -- NEW FIELD AN 2024
        , COALESCE(div_merch_manager_vice_president, 'NA') AS div_merch_manager_vice_president -- NEW FIELD AN 2024
        , COALESCE(merch_director, 'NA') AS merch_director -- NEW FIELD AN 2024
        , COALESCE(buyer, 'NA') AS buyer -- NEW FIELD AN 2024
        , COALESCE(merch_planning_executive_vice_president, 'NA') AS merch_planning_executive_vice_president -- NEW FIELD AN 2024
        , COALESCE(merch_planning_senior_vice_president, 'NA') AS merch_planning_senior_vice_president -- NEW FIELD AN 2024
        , COALESCE(merch_planning_vice_president, 'NA') AS merch_planning_vice_president -- NEW FIELD AN 2024
        , COALESCE(merch_planning_director_manager, 'NA') AS merch_planning_director_manager -- NEW FIELD AN 2024
        , COALESCE(assortment_planner, 'NA') AS assortment_planner -- NEW FIELD AN 2024
        , MAX(CASE WHEN anchor.anchor_brands_ind = 1 THEN 'Y' ELSE 'N' END) AS anchor_brands_ind -- NEW FIELD AN 2024

    FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
    -- Brand Name
    LEFT JOIN PRD_NAP_USR_VWS.VENDOR_BRAND_XREF sup
    	ON sku.prmy_supp_num = sup.vendor_num
    		AND sku.brand_name = sup.vendor_brand_name
    		AND sup.association_status = 'A'
    LEFT JOIN T2DL_DAS_CCS_CATEGORIES.CCS_MERCH_THEMES th
        ON sku.dept_num = th.dept_idnt
            AND sku.class_num = th.class_idnt
            AND sku.sbclass_num = th.sbclass_idnt
    LEFT JOIN {environment_schema}.SCALED_EVENT_SKU_LAST_RECEIPT_VW lr
           ON sku.rms_sku_num = lr.sku_idnt
    -- Supplier Name
    LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM supp
    	ON supp.vendor_num = sku.prmy_supp_num
    -- BIPOC Ind
    LEFT JOIN t2dl_das_in_season_management_reporting.diverse_brands dib
        ON dib.brand_name = COALESCE(sup.vendor_brand_name, sku.brand_name)
    LEFT JOIN aor
        ON sku.dept_num = aor.dept_num
    LEFT JOIN {environment_schema}.AN_STRATEGIC_BRANDS_2024 anchor
       ON sku.dept_num = anchor.dept_num
       AND sku.prmy_supp_num = anchor.supplier_idnt
    -- exclude GWP
    WHERE sku.gwp_ind <> 'Y'
    AND sku.channel_country = 'US' -- NEW
    -- exclude Marketplace items
    AND sku.partner_relationship_type_code <> 'ECONCESSION'
    AND supp_name not like '%MKTPL'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38
)
WITH DATA
PRIMARY INDEX ( sku_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
  PRIMARY INDEX ( sku_idnt )
  ,COLUMN ( sku_idnt )
    ON prod_hier;

-- UDIG main
CREATE MULTISET VOLATILE TABLE udig_main AS (
    SELECT DISTINCT catalog_name AS udig
    , rms_sku_num as sku_idnt
    , case when catalog_num = 'dIk4ii41TzaGo1CKgVOy8Q' then 'NORDSTROM RACK'
      when catalog_num = 'UTvl_wJWQMqCPEuMCPiCsg' then 'NORDSTROM'
      else 'NA'
      end as banner
    FROM prd_nap_usr_vws.PRODUCT_CATALOG_DIM
    WHERE catalog_num in ('UTvl_wJWQMqCPEuMCPiCsg', 'dIk4ii41TzaGo1CKgVOy8Q')
  )
  WITH DATA
  PRIMARY INDEX ( sku_idnt, banner )
  ON COMMIT PRESERVE ROWS;
  COLLECT STATISTICS
      PRIMARY INDEX ( sku_idnt, banner )
          ON udig_main;

-- (BELOW COMMENTED QUERY IS FOR ANNIVERSARY 2024 ONLY)-- MODIFY AND UNCOMMENT for AN 2025

--  SELECT
--  DISTINCT udig_item_grp_idnt || ', ' || udig_item_grp_name AS udig
--  , sku_idnt
--  , 2024 as udig_year
--  FROM {environment_schema}.AN_UDIGS_2024
--  WHERE UDIG_COLLECTION_IDNT IN ('50011')
--  UNION ALL
--  SELECT
--  DISTINCT udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS udig
--  , sku_idnt
--  , 2023 as udig_year
--  FROM PRD_USR_VWS.UDIG_ITM_GRP_SKU_LKUP
--  WHERE UDIG_COLCTN_IDNT IN ('50002')
--  AND udig_itm_grp_idnt = '1'


    CREATE MULTISET VOLATILE TABLE dma
    AS (
        SELECT
            DISTINCT dma_code
            , dma_short
            , dma_proxy_zip
        FROM {environment_schema}.SCALED_EVENT_DMA_LKP_VW
    )
    WITH DATA
    PRIMARY INDEX ( dma_code )
    ON COMMIT PRESERVE ROWS;

    COLLECT STATS
    	PRIMARY INDEX ( dma_code )
    	,COLUMN ( dma_code )
    		ON dma;

-- Price Match (USED IN CYBER ONLY)
CREATE VOLATILE MULTISET TABLE price_match AS (
  SELECT
    DISTINCT pptd.rms_sku_num
    , dt.day_date AS day_dt
    , store.channel_num
    , CASE
        WHEN pptd.event_id = 877 OR pptd.enticement_tags LIKE '%LIMITED_TIME_SAVINGS%' THEN 'Limited Time Savings'
        WHEN pptd.event_id = 1234 THEN 'Event Matching'
        WHEN pptd.event_id = 856 THEN 'Brand Matching'
      END AS promo_grouping
    FROM PRD_NAP_USR_VWS.PRODUCT_PROMOTION_TIMELINE_DIM pptd
    INNER JOIN prd_nap_usr_vws.price_store_dim_vw store
      ON pptd.store_num = store.price_store_num
    INNER JOIN PRD_NAP_USR_VWS.DAY_CAL dt
      ON dt.day_date BETWEEN CAST(enticement_start_tmstp AS date) AND CAST(enticement_end_tmstp AS date)
    WHERE dt.day_date BETWEEN {start_date} AND {end_date}
    AND dt.day_date IN (SELECT DISTINCT day_dt FROM {environment_schema}.SCALED_EVENT_DATES sed  WHERE ty_ly_lly = 'TY' AND cyber_ind = '1')
    AND 'Cyber' = '{date_event_type}'
    AND pptd.channel_country = 'US'
    AND (pptd.event_id IN (877, 1234, 856) OR pptd.enticement_tags LIKE '%LIMITED_TIME_SAVINGS%')
    AND store.channel_num is not null
)
WITH DATA
PRIMARY INDEX ( rms_sku_num, day_dt, channel_num, promo_grouping)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS
PRIMARY INDEX ( rms_sku_num, day_dt, channel_num, promo_grouping)
  ,column ( rms_sku_num )
  ,column ( day_dt )
  ,column ( channel_num )
  ,column ( promo_grouping )
    ON price_match;
------------------------------------------------------------

-- merch clk

CREATE MULTISET VOLATILE TABLE merch_clk
AS (
    WITH date_filter AS (
        SELECT
                day_idnt
                , day_dt
                , anniv_ind
                , CASE
                    WHEN anniv_ind = 1 THEN 'Anniversary'
                    WHEN cyber_ind = 1 THEN 'Cyber'
                END AS date_event_type
                , cyber_ind
                , wk_idnt
            FROM {environment_schema}.SCALED_EVENT_DATES
            WHERE day_dt BETWEEN {start_date} AND {end_date}
                AND date_event_type = '{date_event_type}'
    )
    SELECT
        base.sku_idnt
        , base.loc_idnt
        , base.day_idnt
        , base.date_event AS date_event_type
        , CASE WHEN base.price_type = 'REGULAR' THEN 'R'
        WHEN base.price_type = 'CLEARANCE' THEN 'C'
        WHEN base.price_type = 'PROMOTION' THEN 'P'
        ELSE 'NA'
        END AS price_type
        , base.dma_code
        , MAX(ZEROIFNULL(COALESCE(base.anniv_retail_original,price.regular_price_amt))) AS retail_original_mch
        , MAX(ZEROIFNULL(base.anniv_retail_special)) as retail_special_mch
        , SUM(base.product_views) AS product_views
        , SUM(base.cart_adds) AS cart_adds
        , SUM(base.order_units) AS order_units
        , SUM(base.instock_views) AS instock_views
        , SUM(base.scored_views) AS scored_views -- NEW
        , SUM(base.demand) AS demand
        , SUM(base.wishlist_adds) AS wishlist_adds
        , SUM(base.sales_units) AS sales_units
        , SUM(base.sales_dollars) AS sales_dollars
        , SUM(base.return_units) AS return_units
        , SUM(base.return_dollars) AS return_dollars
        , SUM(base.ntn) as ntn
        , SUM(base.demand_units) AS demand_units
        , SUM(base.demand_dollars) AS demand_dollars
        , SUM(base.demand_cancel_units) AS demand_cancel_units
        , SUM(base.demand_cancel_dollars) AS demand_cancel_dollars
        , SUM(base.shipped_units) AS shipped_units
        , SUM(base.shipped_dollars) AS shipped_dollars
        , SUM(base.eoh_units) AS eoh_units
        , SUM(base.eoh_dollars) AS eoh_dollars
        , SUM(base.boh_units) AS boh_units
        , SUM(base.boh_dollars) AS boh_dollars
        , SUM(base.nonsellable_units) AS nonsellable_units
        , SUM(base.cogs) AS cogs
        , SUM(base.receipt_units) AS receipt_units
        , SUM(base.receipt_dollars) AS receipt_dollars
        , MAX(base.sales_aur) AS sales_aur
        , MAX(base.demand_aur) AS demand_aur
        , MAX(base.eoh_aur) AS eoh_aur
        , MAX(base.receipt_aur) AS receipt_aur
        , ZEROIFNULL(ABS(MAX(COALESCE(NULLIFZERO(base.sales_aur), NULLIFZERO(base.demand_aur), NULLIFZERO(base.eoh_aur), NULLIFZERO(base.receipt_aur), NULLIFZERO(base.on_order_aur), NULLIFZERO(base.com_demand_aur))))) price_band_aur
        , SUM(base.store_fulfill_units) as store_fulfill_units
        , SUM(base.store_fulfill_dollars) as store_fulfill_dollars
        , SUM(base.dropship_units) AS dropship_units
        , SUM(base.dropship_dollars) as dropship_dollars
        , SUM(base.demand_dropship_units) as demand_dropship_units
        , SUM(base.demand_dropship_dollars) as demand_dropship_dollars
        , SUM(base.receipt_dropship_units) as receipt_dropship_units
        , SUM(base.receipt_dropship_dollars) as receipt_dropship_dollars
        --- INSERT NEW 2023  FIELDS ---
        , SUM(sales_cost) AS sales_cost -- NEW
	    , SUM(return_cost) AS return_cost -- NEW
		, SUM(shipped_cost) AS shipped_cost -- NEW
		, SUM(store_fulfill_cost) AS store_fulfill_cost -- NEW
		, SUM(dropship_cost) AS dropship_cost -- NEW
		, SUM(eoh_cost) AS eoh_cost -- NEW
		, SUM(boh_cost) AS boh_cost -- NEW
		, SUM(receipt_cost) AS receipt_cost -- NEW
		, SUM(receipt_dropship_cost) AS receipt_dropship_cost -- NEW
		, SUM(sales_pm) AS sales_pm -- NEW
        ----------------------------------
        , SUM(on_order_units) AS on_order_units
        , SUM(on_order_retail_dollars) AS on_order_retail_dollars
        , SUM(on_order_cost_dollars) AS on_order_cost_dollars -- NEW
    FROM(
        --merch data
        SELECT
            base.sku_idnt
            , base.loc_idnt
            , base.day_idnt
            , df.day_dt
            , base.date_event_type as date_event
            , CASE WHEN price_type = 'R' THEN 'REGULAR'
        	WHEN price_type = 'P' THEN 'PROMOTION'
        	WHEN price_type = 'C' THEN 'CLEARANCE'
        	ELSE NULL
       		END AS price_type
            , NULLIFZERO(base.anniv_retail_original) AS anniv_retail_original
            , NULLIFZERO(base.anniv_retail_special) AS anniv_retail_special
            , COALESCE(dma.dma_code, '0') AS dma_code
            , CAST(0 AS DECIMAL(12,2)) AS product_views
            , CAST(0 AS DECIMAL(12,2)) AS cart_adds
            , CAST(0 AS DECIMAL(12,2)) AS order_units
            , CAST(0 AS DECIMAL(12,2)) AS instock_views
            , CAST(0 AS DECIMAL(12,2)) AS scored_views -- NEW
            , CAST(0 AS DECIMAL(12,2)) AS demand
            , CAST(0 AS DECIMAL(12,2)) AS wishlist_adds
            , sales_units
            , sales_dollars
            , return_units
            , return_dollars
            , ntn
            , demand_units
            , demand_dollars
            , demand_cancel_units
            , demand_cancel_dollars
            , demand_dropship_units
            , demand_dropship_dollars
            , store_fulfill_units
            , store_fulfill_dollars
            , dropship_units
            , dropship_dollars
            , shipped_units
            , shipped_dollars
            , eoh_units
            , eoh_dollars
            , boh_units
            , boh_dollars
            , nonsellable_units
            , cogs
            , receipt_units
            , receipt_dollars
            , receipt_dropship_units
            , receipt_dropship_dollars
            --- INSERT NEW 2023  FIELDS ---
            , sales_cost -- NEW
		    , return_cost -- NEW
			, shipped_cost -- NEW
			, store_fulfill_cost -- NEW
			, dropship_cost -- NEW
			, eoh_cost -- NEW
			, boh_cost -- NEW
			, receipt_cost -- NEW
			, receipt_dropship_cost -- NEW
			, sales_pm -- NEW
            ----------------------------------
            , sales_aur
            , demand_aur
            , eoh_aur
            , receipt_aur
            , CAST(0 AS DECIMAL(12,2)) AS com_demand_aur
            , CAST(0 AS DECIMAL(12,2)) AS on_order_aur
            , CAST(0 AS DECIMAL(10,0)) AS on_order_units
            , CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
            , CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars -- NEW
        FROM {environment_schema}.SCALED_EVENT_BASE base
        INNER JOIN locs loc
            ON base.loc_idnt = loc.loc_idnt
        INNER JOIN date_filter df
            ON base.day_idnt = df.day_idnt
        LEFT JOIN {environment_schema}.SCALED_EVENT_DMA_LKP_VW dma
            ON base.loc_idnt = dma.stor_num
        UNION ALL
         --clickstream data
        SELECT
            clk.sku_idnt
            , clk.loc_idnt
            , clk.day_idnt
            , clk.day_dt
            , date_event
            , clk.price_type
            , NULL AS anniv_retail_original
            , NULL AS anniv_retail_special
            , dma_code
            , product_views
            , cart_adds
            , order_units
            , instock_views
            , scored_views -- NEW
            , demand
            , wishlist_adds
            , CAST(0 AS DECIMAL(10,0)) AS sales_units
            , CAST(0 AS DECIMAL(12,2)) AS sales_dollars
            , CAST(0 AS DECIMAL(10,0)) AS return_units
            , CAST(0 AS DECIMAL(12,2)) AS return_dollars
            , CAST(0 AS INTEGER) AS ntn
            , CAST(0 AS DECIMAL(10,0)) AS demand_units
            , CAST(0 AS DECIMAL(12,2)) AS demand_dollars
            , CAST(0 AS DECIMAL(10,0)) AS demand_cancel_units
            , CAST(0 AS DECIMAL(12,2)) AS demand_cancel_dollars
            , CAST(0 AS DECIMAL(10,0)) AS demand_dropship_units
            , CAST(0 AS DECIMAL(12,2)) AS demand_dropship_dollars
            , CAST(0 AS DECIMAL(10,0)) AS store_fulfill_units
            , CAST(0 AS DECIMAL(12,2)) AS store_fulfill_dollars
            , CAST(0 AS DECIMAL(10,0)) AS dropship_units
            , CAST(0 AS DECIMAL(12,2)) AS dropship_dollars
            , CAST(0 AS DECIMAL(10,0)) AS shipped_units
            , CAST(0 AS DECIMAL(12,2)) AS shipped_dollars
            , CAST(0 AS DECIMAL(10,0)) AS eoh_units
            , CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
            , CAST(0 AS DECIMAL(10,0)) AS boh_units
            , CAST(0 AS DECIMAL(12,2)) AS boh_dollars
            , CAST(0 AS DECIMAL(12,2)) AS nonsellable_units
            , CAST(0 AS DECIMAL(12,2)) AS cogs
            , CAST(0 AS DECIMAL(10,0)) AS receipt_units
            , CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
            , CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
            , CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
            --- INSERT NEW 2023  FIELDS ---
            , CAST(0 AS DECIMAL(12,2)) AS sales_cost -- NEW
		    , CAST(0 AS DECIMAL(12,2)) AS return_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS shipped_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS store_fulfill_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS dropship_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS eoh_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS boh_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS receipt_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS sales_pm -- NEW
            ----------------------------------
            , CAST(0 AS DECIMAL(12,2)) AS sales_aur
            , CAST(0 AS DECIMAL(12,2)) AS demand_aur
            , CAST(0 AS DECIMAL(12,2)) AS eoh_aur
            , CAST(0 AS DECIMAL(12,2)) AS receipt_aur
            , CASE WHEN order_units = 0 THEN 0
                ELSE demand/order_units
                END AS com_demand_aur
            , CAST(0 AS DECIMAL(12,2)) AS on_order_aur
            , CAST(0 AS DECIMAL(10,0)) AS on_order_units
            , CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
            , CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars -- NEW
        FROM pfd clk
        INNER JOIN locs loc
            ON clk.loc_idnt = loc.loc_idnt
        UNION ALL
        --OO
        SELECT
            base.rms_sku_num AS sku_idnt
            , CAST(base.store_num AS VARCHAR(4)) AS loc_idnt
            , df.day_idnt
            , df.day_dt
            , CASE
                WHEN anniv_ind = 1 THEN 'Anniversary'
                WHEN cyber_ind = 1 THEN 'Cyber'
            END AS date_event
            , CAST('NA' AS VARCHAR(2)) AS price_type
            , NULL AS anniv_retail_original
            , NULL AS anniv_retail_special
            , COALESCE(dma.dma_code, '0') AS dma_code
            , CAST(0 AS DECIMAL(12,2)) AS product_views
            , CAST(0 AS DECIMAL(12,2)) AS cart_adds
            , CAST(0 AS DECIMAL(12,2)) AS order_units
            , CAST(0 AS DECIMAL(12,2)) AS instock_views
            , CAST(0 AS DECIMAL(12,2)) AS scored_views -- NEW
            , CAST(0 AS DECIMAL(12,2)) AS demand
            , CAST(0 AS DECIMAL(12,2)) AS wishlist_adds
            , CAST(0 AS DECIMAL(10,0)) AS sales_units
            , CAST(0 AS DECIMAL(12,2)) AS sales_dollars
            , CAST(0 AS DECIMAL(10,0)) AS return_units
            , CAST(0 AS DECIMAL(12,2)) AS return_dollars
            , CAST(0 AS INTEGER) AS ntn
            , CAST(0 AS DECIMAL(10,0)) AS demand_units
            , CAST(0 AS DECIMAL(12,2)) AS demand_dollars
            , CAST(0 AS DECIMAL(10,0)) AS demand_cancel_units
            , CAST(0 AS DECIMAL(12,2)) AS demand_cancel_dollars
            , CAST(0 AS DECIMAL(10,0)) AS demand_dropship_units
            , CAST(0 AS DECIMAL(12,2)) AS demand_dropship_dollars
            , CAST(0 AS DECIMAL(10,0)) AS store_fulfill_units
            , CAST(0 AS DECIMAL(12,2)) AS store_fulfill_dollars
            , CAST(0 AS DECIMAL(10,0)) AS dropship_units
            , CAST(0 AS DECIMAL(12,2)) AS dropship_dollars
            , CAST(0 AS DECIMAL(10,0)) AS shipped_units
            , CAST(0 AS DECIMAL(12,2)) AS shipped_dollars
            , CAST(0 AS DECIMAL(10,0)) AS eoh_units
            , CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
            , CAST(0 AS DECIMAL(10,0)) AS boh_units
            , CAST(0 AS DECIMAL(12,2)) AS boh_dollars
            , CAST(0 AS DECIMAL(12,2)) AS nonsellable_units
            , CAST(0 AS DECIMAL(12,2)) AS cogs
            , CAST(0 AS DECIMAL(10,0)) AS receipt_units
            , CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
            , CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
            , CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
            --- INSERT NEW 2023  FIELDS ---
            , CAST(0 AS DECIMAL(12,2)) AS sales_cost -- NEW
		    , CAST(0 AS DECIMAL(12,2)) AS return_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS shipped_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS store_fulfill_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS dropship_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS eoh_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS boh_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS receipt_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost -- NEW
			, CAST(0 AS DECIMAL(12,2)) AS sales_pm -- NEW
            ----------------------------------
            , CAST(0 AS DECIMAL(12,2)) AS sales_aur
            , CAST(0 AS DECIMAL(12,2)) AS demand_aur
            , CAST(0 AS DECIMAL(12,2)) AS eoh_aur
            , CAST(0 AS DECIMAL(12,2)) AS receipt_aur
            , CAST(0 AS DECIMAL(12,2)) AS com_demand_aur
            ---  NEW 2023 OO FIELDS ---
            , CASE WHEN base.quantity_open = 0 THEN 0
                ELSE base.ANTICIPATED_RETAIL_AMT
                END AS on_order_aur
            , base.quantity_open AS on_order_units
            , (base.ANTICIPATED_RETAIL_AMT * base.QUANTITY_OPEN) AS on_order_retail_dollars
            , (base.UNIT_ESTIMATED_LANDING_COST * base.QUANTITY_OPEN) AS on_order_cost_dollars -- NEW

        FROM PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW base
        INNER JOIN locs loc
            ON base.store_num = loc.loc_idnt
        INNER JOIN date_filter df
            ON df.wk_idnt = base.week_num -- Will product JOIN across ALL days IN week AS same OO amount then LOD in tableau
        LEFT JOIN {environment_schema}.SCALED_EVENT_DMA_LKP_VW dma
            ON base.store_num = dma.stor_num
        WHERE base.quantity_open > 0
        AND ((STATUS = 'CLOSED' AND END_SHIP_DATE >= (SELECT MAX(WEEK_END_DAY_DATE) FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE WEEK_END_DAY_DATE < CURRENT_DATE) - 45)
        OR STATUS IN ('APPROVED','WORKSHEET'))
    ) base
    LEFT JOIN PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW ps
        ON ps.store_num = base.loc_idnt
    LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM price
		ON price.RMS_SKU_NUM = base.sku_idnt
		AND price.store_num = ps.price_store_num
		AND price.selling_retail_price_type_code = base.price_type
		AND base.day_dt BETWEEN CAST((price.EFF_BEGIN_TMSTP AT TIME ZONE 'GMT-7') AS DATE FORMAT 'YYYY-MM-DD')
			AND (CAST((price.EFF_END_TMSTP AT TIME ZONE 'GMT-7') AS DATE FORMAT 'YYYY-MM-DD') - INTERVAL '1' DAY)
    GROUP BY 1,2,3,4,5,6
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
	PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )
    ,column ( day_idnt )
		ON merch_clk;

------------------------------------------------------------- END TEMPORARY TABLES -----------------------------------------------------------------------


---------------------------------------------------------------- START MAIN QUERY -------------------------------------------------------------------------

-- Delete only rows that will be refreshed and inserted below
DELETE
FROM {environment_schema}.scaled_event_sku_clk_rms
WHERE date_event_type = '{date_event_type}'
    AND day_dt BETWEEN {start_date} AND {end_date};

--final insert with sku, loc, date joins
INSERT INTO {environment_schema}.scaled_event_sku_clk_rms
SELECT
    ap.sku_idnt
    , ap.day_idnt
    , dt.day_dt
    , dt.day_dt_aligned
    , ap.date_event_type
    , dt.event_type AS event_phase_orig
    , dt.event_day AS event_day_orig
    , dt.event_type_mod AS event_phase
    , dt.event_day_mod AS event_day
    , dt.month_idnt
    , dt.month_label
    , CASE WHEN an.anniv_item_ind = '1' THEN 'Y' ELSE 'N' END AS anniv_ind
    , CASE ap.price_type
        WHEN 'R' THEN 'Reg'
        WHEN 'P' THEN 'Pro'
        WHEN 'C' THEN 'Clear'
        ELSE COALESCE(ap.price_type,'NA') END AS price_type
    , CASE WHEN rp.rp_ind = 1 THEN 'Y' ELSE 'N' END AS rp_ind
    , dt.ty_ly_lly AS ty_ly_ind
    , COALESCE(loc.chnl_label,'0, UNKNOWN')  AS channel
    , COALESCE(CAST(loc.chnl_idnt AS VARCHAR(10)), '0') AS channel_idnt
    , loc.loc_label AS location
    , loc.banner
    , loc.CLUSTER_CLIMATE AS climate_cluster   -- NEW TO ANNIV 23
    , loc.NR_CLUSTER_PRICE AS NR_price_cluster
    , loc.country
    , COALESCE(dma.dma_short, 'OTHER') AS dma_short
    , COALESCE(dma.dma_proxy_zip, 'OTHER') AS dma_proxy_zip
    , sku.style_idnt as style_num
    , sku.style_desc
    , sku.supp_prt_nbr AS vpn
    , sku.supp_color
    , sku.colr_idnt
    , sku.style_group_idnt
    , CASE WHEN ds.sku_idnt IS NOT NULL THEN 'Y' ELSE 'N' END AS dropship_ind--
    , TRIM(sku.div_label) AS division
    , TRIM(sku.sdiv_label) AS subdivision
    , TRIM(sku.dept_label) AS department
    , TRIM(sku.class_label) AS "class"
    , TRIM(sku.sbclass_label) AS subclass
    , sku.supp_name AS supplier
    , sku.brand_name AS brand
    , sku.npg_ind
    , sku.assortment_grouping
    , sku.quantrix_category
    , sku.ccs_category
    , sku.ccs_subcategory
    , sku.nord_role_desc
    , sku.rack_role_desc
    , sku.merch_theme
    , sku.anniversary_theme as anniversary_theme
    , sku.holiday_theme_ty AS holiday_theme
    , CASE WHEN sku.gift_ind = 'GIFT' THEN 'GIFT' ELSE 'NON-GIFT' END AS gift_ind
    , CASE WHEN sku.stocking_stuffer = 'Y' THEN 'Y' ELSE 'N' END AS stocking_stuffer
    , CASE WHEN pm.rms_sku_num IS NOT NULL THEN 'Y' ELSE 'N' END AS price_match_ind --'NA' AS price_match_ind
    , pm.promo_grouping -- 'NA' AS promo_grouping
    , sku.bipoc_ind
    , um.udig
    , CASE
        --Anniv Event
        WHEN an.anniv_item_ind = '1' AND retail_special = 0.00       THEN 'No Retail Special'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 10.00     THEN '< $10'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 25.00     THEN '$10 - $25'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 50.00     THEN '$25 - $50'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 100.00    THEN '$50 - $100'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 150.00    THEN '$100 - $150'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 200.00    THEN '$150 - $200'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 300.00    THEN '$200 - $300'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 500.00    THEN '$300 - $500'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 1000.00   THEN '$500 - $1000'
        WHEN an.anniv_item_ind = '1' AND retail_special >  1000.00   THEN '> $1000'
        --Anniv Non-Event
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original = 0.00       THEN 'No Retail Original'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 10.00     THEN '< $10'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 25.00     THEN '$10 - $25'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 50.00     THEN '$25 - $50'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 100.00    THEN '$50 - $100'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 150.00    THEN '$100 - $150'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 200.00    THEN '$150 - $200'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 300.00    THEN '$200 - $300'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 500.00    THEN '$300 - $500'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 1000.00   THEN '$500 - $1000'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original >  1000.00   THEN '> $1000'
        --Cyber
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur = 0.00      THEN 'UNKNOWN'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 10.00    THEN '< $10'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 25.00    THEN '$10 - $25'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 50.00    THEN '$25 - $50'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 100.00   THEN '$50 - $100'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 150.00   THEN '$100 - $150'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 200.00   THEN '$150 - $200'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 300.00   THEN '$200 - $300'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 500.00   THEN '$300 - $500'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 1000.00  THEN '$500 - $1000'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur >  1000.00  THEN '> $1000'
   END AS price_band_one
   , CASE
        --Anniv Event
        WHEN an.anniv_item_ind = '1' AND retail_special = 0.00       THEN 'No Retail Special'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 10.00     THEN '< $10'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 15.00     THEN '$10 - $15'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 20.00     THEN '$15 - $20'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 25.00     THEN '$20 - $25'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 30.00     THEN '$25 - $30'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 40.00     THEN '$30 - $40'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 50.00     THEN '$40 - $50'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 60.00     THEN '$50 - $60'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 80.00     THEN '$60 - $80'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 100.00    THEN '$80 - $100'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 125.00    THEN '$100 - $125'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 150.00    THEN '$125 - $150'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 175.00    THEN '$150 - $175'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 200.00    THEN '$175 - $200'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 250.00    THEN '$200 - $250'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 300.00    THEN '$250 - $300'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 400.00    THEN '$300 - $400'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 500.00    THEN '$400 - $500'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 700.00    THEN '$500 - $700'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 900.00    THEN '$700 - $900'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 1000.00   THEN '$900 - $1000'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 1200.00   THEN '$1000 - $1200'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 1500.00   THEN '$1200 - $1500'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 1800.00   THEN '$1500 - $1800'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 2000.00   THEN '$1800 - $2000'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 3000.00   THEN '$2000 - $3000'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 4000.00   THEN '$3000 - $4000'
        WHEN an.anniv_item_ind = '1' AND retail_special <= 5000.00   THEN '$4000 - $5000'
        WHEN an.anniv_item_ind = '1' AND retail_special >  5000.00   THEN '> $5000'
        --Anniv Non-Event
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original = 0.00       THEN 'No Retail Original'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 10.00     THEN '< $10'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 15.00     THEN '$10 - $15'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 20.00     THEN '$15 - $20'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 25.00     THEN '$20 - $25'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 30.00     THEN '$25 - $30'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 40.00     THEN '$30 - $40'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 50.00     THEN '$40 - $50'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 60.00     THEN '$50 - $60'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 80.00     THEN '$60 - $80'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 100.00    THEN '$80 - $100'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 125.00    THEN '$100 - $125'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 150.00    THEN '$125 - $150'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 175.00    THEN '$150 - $175'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 200.00    THEN '$175 - $200'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 250.00    THEN '$200 - $250'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 300.00    THEN '$250 - $300'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 400.00    THEN '$300 - $400'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 500.00    THEN '$400 - $500'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 700.00    THEN '$500 - $700'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 900.00    THEN '$700 - $900'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 1000.00   THEN '$900 - $1000'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 1200.00   THEN '$1000 - $1200'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 1500.00   THEN '$1200 - $1500'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 1800.00   THEN '$1500 - $1800'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 2000.00   THEN '$1800 - $2000'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 3000.00   THEN '$2000 - $3000'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 4000.00   THEN '$3000 - $4000'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original <= 5000.00   THEN '$4000 - $5000'
        WHEN ap.date_event_type = 'Anniversary' AND ZEROIFNULL(an.anniv_item_ind) = '0' AND retail_original >  5000.00   THEN '> $5000'
        --Cyber
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur = 0.00       THEN 'UNKNOWN'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 10.00     THEN '< $10'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 15.00     THEN '$10 - $15'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 20.00     THEN '$15 - $20'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 25.00     THEN '$20 - $25'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 30.00     THEN '$25 - $30'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 40.00     THEN '$30 - $40'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 50.00     THEN '$40 - $50'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 60.00     THEN '$50 - $60'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 80.00     THEN '$60 - $80'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 100.00    THEN '$80 - $100'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 125.00    THEN '$100 - $125'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 150.00    THEN '$125 - $150'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 175.00    THEN '$150 - $175'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 200.00    THEN '$175 - $200'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 250.00    THEN '$200 - $250'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 300.00    THEN '$250 - $300'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 400.00    THEN '$300 - $400'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 500.00    THEN '$400 - $500'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 700.00    THEN '$500 - $700'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 900.00    THEN '$700 - $900'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 1000.00   THEN '$900 - $1000'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 1200.00   THEN '$1000 - $1200'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 1500.00   THEN '$1200 - $1500'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 1800.00   THEN '$1500 - $1800'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 2000.00   THEN '$1800 - $2000'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 3000.00   THEN '$2000 - $3000'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 4000.00   THEN '$3000 - $4000'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur <= 5000.00   THEN '$4000 - $5000'
        WHEN ap.date_event_type = 'Cyber' AND ap.price_band_aur >  5000.00   THEN '> $5000'
     END AS price_band_two
    , ap.sales_units
    , ap.sales_dollars
    , ap.return_units
    , ap.return_dollars
    , ap.ntn
    , ap.demand_units
    , ap.demand_dollars
    , ap.demand_cancel_units
    , ap.demand_cancel_dollars
    , ap.shipped_units
    , ap.shipped_dollars
    , ap.eoh_units
    , ap.eoh_dollars
    , ap.boh_units
    , ap.boh_dollars
    , ap.nonsellable_units
    , ap.cogs
    , ap.receipt_units
    , ap.receipt_dollars
    , ap.sales_aur
    , ap.demand_aur
    , ap.eoh_aur
    , ap.store_fulfill_units
    , ap.store_fulfill_dollars
    , ap.dropship_units
    , ap.dropship_dollars
    , ap.demand_dropship_units
    , ap.demand_dropship_dollars
    , ap.receipt_dropship_units
    , ap.receipt_dropship_dollars
    , ap.on_order_units
    , ap.on_order_retail_dollars
--    , ap.on_order_4wk_units  -- REMOVED Anniv 23
--    , ap.on_order_4wk_retail_dollars -- REMOVED Anniv 23
    , ap.product_views
    , ap.cart_adds
    , ap.order_units AS order_units
    , ap.instock_views
    , ap.demand AS demand
    , ap.wishlist_adds
    , ZEROIFNULL(COALESCE(ap.retail_original_mch,an.reg_price_amt)) AS retail_original
    , ZEROIFNULL(COALESCE(NULLIFZERO(ap.retail_special_mch)
        , NULLIFZERO(an.spcl_price_amt)
        , (CASE WHEN an.anniv_item_ind = '1' AND sku.div_label = '340, BEAUTY' THEN ap.retail_original_mch END))
    ) AS retail_special
    , CASE WHEN an.anniv_item_ind = '1' AND retail_special > 0 AND retail_original > 0 THEN ZEROIFNULL(
        COALESCE(NULLIFZERO(
            ap.eoh_units
            + ap.sales_units
            + ap.on_order_units
            + ap.receipt_dropship_units
            + ap.return_units)
        , NULLIFZERO(ap.demand_units)
        , NULLIFZERO(ap.receipt_units)
        , NULLIFZERO(ap.return_units)
        , NULLIFZERO(ap.boh_units)))
            ELSE 0 END AS units
    , sku.last_receipt_date
    ,current_timestamp as process_tmstp
    --- INSERT NEW 2023  FIELDS ---
    , ap.sales_cost -- NEW
    , ap.return_cost -- NEW
	, ap.shipped_cost -- NEW
	, ap.store_fulfill_cost -- NEW
	, ap.dropship_cost -- NEW
	, ap.eoh_cost -- NEW
	, ap.boh_cost -- NEW
	, ap.receipt_cost -- NEW
	, ap.receipt_dropship_cost -- NEW
	, ap.sales_pm -- NEW
	, ZEROIFNULL(ap.on_order_cost_dollars) -- NEW
  , ZEROIFNULL(ap.scored_views) -- NEW
  , general_merch_manager_executive_vice_president -- NEW FIELD AN 2024
  , div_merch_manager_senior_vice_president -- NEW FIELD AN 2024
  , div_merch_manager_vice_president -- NEW FIELD AN 2024
  , merch_director -- NEW FIELD AN 2024
  , buyer -- NEW FIELD AN 2024
  , merch_planning_executive_vice_president -- NEW FIELD AN 2024
  , merch_planning_senior_vice_president -- NEW FIELD AN 2024
  , merch_planning_vice_president -- NEW FIELD AN 2024
  , merch_planning_director_manager -- NEW FIELD AN 2024
  , assortment_planner -- NEW FIELD AN 2024
  , anchor_brands_ind -- NEW FIELD AN 2024
    ----------------------------------
FROM merch_clk ap
--loc info
INNER JOIN locs loc
    ON ap.loc_idnt = loc.loc_idnt
--dates
LEFT JOIN {environment_schema}.SCALED_EVENT_DATES dt
    ON ap.day_idnt = dt.day_idnt
-- sku_prod
INNER JOIN prod_hier sku
    ON ap.sku_idnt = sku.sku_idnt
-- anniversary indicator
LEFT JOIN {environment_schema}.ANNIVERSARY_SKU_CHNL_DATE an
    ON ap.date_event_type = 'Anniversary'
    AND ap.sku_idnt = an.sku_idnt
	AND an.channel_country = 'US'
	AND loc.EVENT_SELLING_CHANNEL = an.selling_channel
	AND loc.event_store_type_code = an.store_type_code
    AND ap.day_idnt = an.day_idnt
--rp indicator
LEFT JOIN rp
    ON ap.sku_idnt = rp.sku_idnt
    AND ap.loc_idnt = rp.loc_idnt
    AND ap.day_idnt = rp.day_idnt
--dropship indicator
LEFT JOIN dropship ds
    ON ap.sku_idnt = ds.sku_idnt
    AND dt.day_dt = ds.day_dt
--dma info
LEFT JOIN dma
        ON ap.dma_code = dma.dma_code
--UDIG
LEFT JOIN udig_main um
    ON ap.sku_idnt = um.sku_idnt
    --AND dt.yr_idnt = um.udig_year
    AND loc.banner = um.banner
-- price match
LEFT JOIN price_match pm
    ON ap.sku_idnt = pm.rms_sku_num
    AND loc.chnl_idnt = pm.channel_num
    AND pm.day_dt = dt.day_dt
   	AND ap.date_event_type = 'Cyber'
;

COLLECT STATISTICS
    PRIMARY INDEX ( sku_idnt, day_idnt, location )
    ,COLUMN ( day_dt )
    ,COLUMN ( date_event_type )
        ON {environment_schema}.scaled_event_sku_clk_rms;

------------------------------------------------------------- END MAIN QUERY -----------------------------------------------------------------------
