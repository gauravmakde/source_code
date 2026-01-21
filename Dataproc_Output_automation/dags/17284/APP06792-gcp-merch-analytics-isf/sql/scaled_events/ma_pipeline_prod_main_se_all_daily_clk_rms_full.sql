/******************************************************************************
Name: Scaled Events - Anniversary Scaled Event SKU CLK RMS Table
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Populate the Scaled Events SKU CLK RMS table for the evening FULL DAG refressh
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
                {start_date} - beginning date of full refresh date range and pt.1 start date
                {end_date_middle} - ending date of pt.1 refresh date range
                {start_date_middle} - beginning date of pt.2 refresh date range
                {end_date} - ending date of full refresh date range and pt.2 end date
                {start_date_partition} - static start date of DS Volatile Table partition
                {end_date_partition} - static end date of DS Volatile Table partition
                {date_event_type} - Anniversary/Cyber data indicator
DAG: merch_se_all_daily_full
TABLE NAME: T2DL_DAS_SCALED_EVENTS.scaled_event_sku_clk_rms (T3: T3DL_ACE_MCH.scaled_event_sku_clk_rms)
Author(s): Manuela Hurtado, Alli Moore
Date Last Updated: 06-10-2024
******************************************************************************/

-- locs



CREATE TEMPORARY TABLE IF NOT EXISTS locs

AS
SELECT locs.loc_idnt,
 locs.chnl_label,
 locs.chnl_idnt,
 locs.loc_label,
 locs.country,
 locs.cyber_loc_ind,
 locs.anniv_loc_ind,
  CASE
  WHEN locs.cyber_loc_ind = 1
  THEN 'Cyber'
  ELSE NULL
  END AS cyber_loc_ind_type,
  CASE
  WHEN locs.anniv_loc_ind = 1
  THEN 'Anniversary'
  ELSE NULL
  END AS anniv_loc_ind_type,
  CASE
  WHEN locs.chnl_idnt IN (110, 120, 130, 140, 310, 920)
  THEN 'NORDSTROM'
  ELSE 'RACK'
  END AS banner,
  CASE
  WHEN locs.chnl_idnt IN (110, 120, 310, 920)
  THEN 'NORDSTROM'
  WHEN locs.chnl_idnt IN (210, 250)
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
  CASE
  WHEN locs.chnl_idnt IN (110, 210)
  THEN 'STORE'
  WHEN locs.chnl_idnt IN (120, 250) OR locs.chnl_idnt IN (310, 920)
  THEN 'ONLINE'
  ELSE NULL
  END AS event_selling_channel,
  CASE
  WHEN locs.chnl_idnt IN (110)
  THEN 'FL'
  WHEN locs.chnl_idnt IN (210, 250)
  THEN 'RK'
  WHEN locs.chnl_idnt IN (120) OR locs.chnl_idnt IN (310, 920)
  THEN 'FC'
  ELSE NULL
  END AS event_store_type_code,
 COALESCE(clu.peer_group_desc, 'NA') AS cluster_climate,
 'NA' AS nr_cluster_price
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_locs_vw AS locs
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_peer_group_dim AS clu ON locs.loc_idnt = clu.store_num AND LOWER(clu.peer_group_type_code
    ) IN (LOWER('OPC'), LOWER('FPC'))
WHERE LOWER(CASE
    WHEN locs.anniv_loc_ind = 1
    THEN 'Anniversary'
    ELSE NULL
    END) = LOWER('{{params.date_event_type}}')
 OR LOWER(CASE
    WHEN locs.cyber_loc_ind = 1
    THEN 'Cyber'
    ELSE NULL
    END) = LOWER('{{params.date_event_type}}');


--COLLECT STATISTICS     PRIMARY INDEX ( loc_idnt )     ,COLUMN ( loc_idnt )         ON locs


CREATE TEMPORARY TABLE IF NOT EXISTS dropship (
day_dt DATE,
sku_idnt STRING
) 
AS
SELECT DISTINCT snapshot_date AS day_dt,
 rms_sku_id AS sku_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_by_day_physical_fact
WHERE LOWER(location_type) IN (LOWER('DS'))
 AND snapshot_date BETWEEN {{params.start_date}} AND {{params.end_date}};


--COLLECT STATISTICS 	PRIMARY INDEX ( sku_idnt ) 	,COLUMN ( sku_idnt ) 	,COLUMN ( day_dt ) 		ON dropship


CREATE TEMPORARY TABLE IF NOT EXISTS rp

AS 
    SELECT sku_idnt,
          day_idnt,
          rp_date_event_type,
          loc_idnt,
          rp_ind,
          day_dt
    FROM(
     SELECT DISTINCT
        rp.rms_sku_num AS sku_idnt
        , dt.day_idnt
        , CASE
            WHEN dt.anniv_ind = 1 THEN 'Anniversary'
            WHEN dt.cyber_ind = 1 THEN 'Cyber'
            ELSE NULL
        END AS rp_date_event_type
        , loc.loc_idnt
        , 1 AS rp_ind,
        dt.day_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
    INNER JOIN locs loc
        ON loc.loc_idnt = rp.location_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.SCALED_EVENT_DATES dt
        ON RANGE_CONTAINS(rp.rp_period, dt.day_dt))
    WHERE day_dt <= {{params.end_date}}
        AND rp_date_event_type = '{{params.date_event_type}}';


--COLLECT STATISTICS 	PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt ) 		ON rp


CREATE TEMPORARY TABLE IF NOT EXISTS pfd

AS
SELECT sku_idnt,
 loc_idnt,
 day_idnt,
 event_date_pacific AS day_dt,
 date_event,
 price_type,
 dma_code,
 SUM(product_views) AS product_views,
 SUM(cart_adds) AS cart_adds,
 SUM(order_units) AS order_units,
 SUM(instock_views) AS instock_views,
 SUM(scored_views) AS scored_views,
 SUM(demand) AS demand,
 SUM(wishlist_adds) AS wishlist_adds
FROM (SELECT pfd.rms_sku_num AS sku_idnt,
    pfd.event_date_pacific,
    dt.day_idnt,
     CASE
     WHEN dt.anniv_ind = 1
     THEN 'Anniversary'
     WHEN dt.cyber_ind = 1
     THEN 'Cyber'
     ELSE NULL
     END AS date_event,
     CASE
     WHEN LOWER(pfd.channel) = LOWER('FULL_LINE')
     THEN '808'
     WHEN LOWER(pfd.channel) = LOWER('RACK')
     THEN '828'
     ELSE NULL
     END AS loc_idnt,
     CASE
     WHEN LOWER(pfd.current_price_type) = LOWER('R')
     THEN 'REGULAR'
     WHEN LOWER(pfd.current_price_type) = LOWER('C')
     THEN 'CLEARANCE'
     WHEN LOWER(pfd.current_price_type) = LOWER('P')
     THEN 'PROMOTION'
     ELSE NULL
     END AS price_type,
    '0' AS dma_code,
    SUM(pfd.product_views) AS product_views,
    SUM(pfd.add_to_bag_quantity) AS cart_adds,
    SUM(pfd.order_quantity) AS order_units,
    COALESCE(SUM(pfd.product_views * pfd.pct_instock), 0) AS instock_views,
    SUM(CASE
      WHEN pfd.pct_instock IS NOT NULL
      THEN pfd.product_views
      ELSE NULL
      END) AS scored_views,
    SUM(pfd.order_demand) AS demand,
    SUM(0) AS wishlist_adds
   FROM t2dl_das_product_funnel.product_price_funnel_daily AS pfd
    INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates AS dt ON pfd.event_date_pacific = dt.day_dt AND LOWER(pfd.channelcountry
       ) = LOWER('US')
   WHERE LOWER(CASE
       WHEN dt.anniv_ind = 1
       THEN 'Anniversary'
       WHEN dt.cyber_ind = 1
       THEN 'Cyber'
       ELSE NULL
       END) = LOWER('{{params.date_event_type}}')
    AND dt.day_dt BETWEEN {{params.start_date}} AND {{params.end_date}}
   GROUP BY sku_idnt,
    pfd.event_date_pacific,
    dt.day_idnt,
    date_event,
    loc_idnt,
    price_type,
    dma_code
   UNION ALL
   SELECT w.rms_sku_num,
    w.event_date_pacific,
    dt0.day_idnt,
     CASE
     WHEN dt0.anniv_ind = 1
     THEN 'Anniversary'
     WHEN dt0.cyber_ind = 1
     THEN 'Cyber'
     ELSE NULL
     END AS date_event,
     CASE
     WHEN LOWER(w.channelbrand) = LOWER('NORDSTROM')
     THEN '808'
     WHEN LOWER(w.channelbrand) = LOWER('NORDSTROM_RACK')
     THEN '828'
     ELSE NULL
     END AS loc_idnt,
    price.selling_retail_price_type_code AS price_type,
    '0' AS dma_code,
    SUM(0) AS product_views,
    SUM(0) AS cart_adds,
    SUM(0) AS order_units,
    SUM(0) AS instock_views,
    SUM(0) AS scored_views,
    SUM(0) AS demand,
    SUM(w.quantity) AS wishlist_adds
   FROM t2dl_das_scaled_events.sku_item_added AS w
    INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates AS dt0 ON w.event_date_pacific = dt0.day_dt AND LOWER(w.channelcountry
       ) = LOWER('US')
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS price ON LOWER(price.rms_sku_num) = LOWER(w.rms_sku_num) AND
        LOWER(price.channel_country) = LOWER('US') AND LOWER(price.selling_channel) = LOWER('ONLINE') AND LOWER(price.channel_brand
       ) = LOWER(w.channelbrand)
   WHERE LOWER(CASE
       WHEN dt0.anniv_ind = 1
       THEN 'Anniversary'
       WHEN dt0.cyber_ind = 1
       THEN 'Cyber'
       ELSE NULL
       END) = LOWER('{{params.date_event_type}}')
	   AND w.event_date_pacific BETWEEN cast(FORMAT_DATE('%Y-%m-%d',(datetime(price.EFF_BEGIN_TMSTP))) as date)
	   AND date_sub(cast(FORMAT_DATE('%Y-%m-%d',(price.EFF_END_TMSTP)) as date),INTERVAL 1 DAY)
    AND dt0.day_dt BETWEEN {{params.start_date}} AND {{params.end_date}}
   GROUP BY w.rms_sku_num,
    w.event_date_pacific,
    dt0.day_idnt,
    date_event,
    loc_idnt,
    price_type,
    dma_code) AS a
GROUP BY sku_idnt,
 loc_idnt,
 day_idnt,
 day_dt,
 date_event,
 price_type,
 dma_code;


--COLLECT STATISTICS     PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )     ,COLUMN ( loc_idnt )         ON pfd


CREATE TEMPORARY TABLE IF NOT EXISTS aor

AS
SELECT DISTINCT REGEXP_REPLACE(channel_brand, '_', ' ') AS banner,
 dept_num,
 general_merch_manager_executive_vice_president,
 div_merch_manager_senior_vice_president,
 div_merch_manager_vice_president,
 merch_director,
 buyer,
 merch_planning_executive_vice_president,
 merch_planning_senior_vice_president,
 merch_planning_vice_president,
 merch_planning_director_manager,
 assortment_planner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.area_of_responsibility_dim
WHERE LOWER(REGEXP_REPLACE(channel_brand, '_', ' ')) = LOWER('NORDSTROM')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY banner, dept_num ORDER BY 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) = 1;


--COLLECT STATS     PRIMARY INDEX (dept_num)         ON aor


CREATE TEMPORARY TABLE IF NOT EXISTS prod_hier

AS
SELECT sku.rms_sku_num AS sku_idnt,
 sku.rms_style_num AS style_idnt,
 sku.style_desc,
 sku.supp_part_num AS supp_prt_nbr,
 sku.supp_color,
 sku.color_num AS colr_idnt,
 sku.style_group_num AS style_group_idnt,
   TRIM(FORMAT('%11d', sku.div_num)) || ', ' || TRIM(sku.div_desc) AS div_label,
 TRIM(FORMAT('%11d', sku.grp_num) || ', ' || sku.grp_desc) AS sdiv_label,
 TRIM(FORMAT('%11d', sku.dept_num) || ', ' || sku.dept_desc) AS dept_label,
 TRIM(FORMAT('%11d', sku.class_num) || ', ' || sku.class_desc) AS class_label,
 TRIM(FORMAT('%11d', sku.sbclass_num) || ', ' || sku.sbclass_desc) AS sbclass_label,
 supp.vendor_name AS supp_name,
 COALESCE(sup.vendor_brand_name, sku.brand_name) AS brand_name,
 supp.npg_flag AS npg_ind,
 th.merch_themes AS merch_theme,
 th.anniversary_theme,
 th.nord_role_desc,
 th.rack_role_desc,
 th.ccs_category,
 th.ccs_subcategory,
 th.quantrix_category,
 th.assortment_grouping,
 th.holiday_theme_ty,
 th.gift_ind,
 th.stocking_stuffer,
 lr.last_receipt_date,
  CASE
  WHEN dib.brand_name IS NOT NULL
  THEN 'Y'
  ELSE 'N'
  END AS bipoc_ind,
 COALESCE(aor.general_merch_manager_executive_vice_president, 'NA') AS general_merch_manager_executive_vice_president,
 COALESCE(aor.div_merch_manager_senior_vice_president, 'NA') AS div_merch_manager_senior_vice_president,
 COALESCE(aor.div_merch_manager_vice_president, 'NA') AS div_merch_manager_vice_president,
 COALESCE(aor.merch_director, 'NA') AS merch_director,
 COALESCE(aor.buyer, 'NA') AS buyer,
 COALESCE(aor.merch_planning_executive_vice_president, 'NA') AS merch_planning_executive_vice_president,
 COALESCE(aor.merch_planning_senior_vice_president, 'NA') AS merch_planning_senior_vice_president,
 COALESCE(aor.merch_planning_vice_president, 'NA') AS merch_planning_vice_president,
 COALESCE(aor.merch_planning_director_manager, 'NA') AS merch_planning_director_manager,
 COALESCE(aor.assortment_planner, 'NA') AS assortment_planner,
 MAX(CASE
   WHEN anchor.anchor_brands_ind = 1
   THEN 'Y'
   ELSE 'N'
   END) AS anchor_brands_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_brand_xref AS sup ON LOWER(sku.prmy_supp_num) = LOWER(sup.vendor_num) AND LOWER(sku.brand_name
     ) = LOWER(sup.vendor_brand_name) AND LOWER(sup.association_status) = LOWER('A')
 LEFT JOIN t2dl_das_ccs_categories.ccs_merch_themes AS th ON sku.dept_num = th.dept_idnt AND sku.class_num = th.class_idnt
     AND sku.sbclass_num = th.sbclass_idnt
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_sku_last_receipt_vw AS lr ON LOWER(sku.rms_sku_num) = LOWER(lr.sku_idnt)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(supp.vendor_num) = LOWER(sku.prmy_supp_num)
 LEFT JOIN t2dl_das_in_season_management_reporting.diverse_brands AS dib ON LOWER(dib.brand_name) = LOWER(COALESCE(sup.vendor_brand_name
    , sku.brand_name))
 LEFT JOIN aor ON sku.dept_num = aor.dept_num
 LEFT JOIN t2dl_das_scaled_events.an_strategic_brands_2024 AS anchor ON sku.dept_num = anchor.dept_num AND CAST(sku.prmy_supp_num AS FLOAT64)
   = anchor.supplier_idnt
WHERE LOWER(sku.gwp_ind) <> LOWER('Y')
 AND LOWER(sku.channel_country) = LOWER('US')
 AND LOWER(sku.partner_relationship_type_code) <> LOWER('ECONCESSION')
 AND LOWER(supp.vendor_name) NOT LIKE LOWER('%MKTPL')
GROUP BY sku_idnt,
 style_idnt,
 sku.style_desc,
 supp_prt_nbr,
 sku.supp_color,
 colr_idnt,
 style_group_idnt,
 div_label,
 sdiv_label,
 dept_label,
 class_label,
 sbclass_label,
 supp_name,
 brand_name,
 npg_ind,
 merch_theme,
 th.anniversary_theme,
 th.nord_role_desc,
 th.rack_role_desc,
 th.ccs_category,
 th.ccs_subcategory,
 th.quantrix_category,
 th.assortment_grouping,
 th.holiday_theme_ty,
 th.gift_ind,
 th.stocking_stuffer,
 lr.last_receipt_date,
 bipoc_ind,
 general_merch_manager_executive_vice_president,
 div_merch_manager_senior_vice_president,
 div_merch_manager_vice_president,
 merch_director,
 buyer,
 merch_planning_executive_vice_president,
 merch_planning_senior_vice_president,
 merch_planning_vice_president,
 merch_planning_director_manager,
 assortment_planner;


--COLLECT STATISTICS     PRIMARY INDEX ( sku_idnt )     ,COLUMN ( sku_idnt )         ON prod_hier


CREATE TEMPORARY TABLE IF NOT EXISTS udig_main

AS
SELECT DISTINCT udig_item_grp_idnt || ', ' || udig_item_grp_name AS udig,
 sku_idnt,
 2024 AS udig_year
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.an_udigs_2024
WHERE LOWER(udig_collection_idnt) IN (LOWER('50011'))
UNION ALL
SELECT DISTINCT udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS udig,
 sku_idnt,
 2023 AS udig_year
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_usr_vws.udig_itm_grp_sku_lkup
WHERE LOWER(udig_colctn_idnt) IN (LOWER('50002'))
 AND LOWER(udig_itm_grp_idnt) = LOWER('1');


--COLLECT STATISTICS     PRIMARY INDEX ( sku_idnt )         ON udig_main


CREATE TEMPORARY TABLE IF NOT EXISTS dma

AS
SELECT DISTINCT dma_code,
 dma_short,
 dma_proxy_zip
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dma_lkp_vw;


--COLLECT STATS 	PRIMARY INDEX ( dma_code ) 	,COLUMN ( dma_code ) 		ON dma


CREATE TABLE IF NOT EXISTS t2dl_das_scaled_events.merch_clk AS WITH date_filter AS (SELECT day_idnt,
   day_dt,
   anniv_ind,
    CASE
    WHEN anniv_ind = 1
    THEN 'Anniversary'
    WHEN cyber_ind = 1
    THEN 'Cyber'
    ELSE NULL
    END AS date_event_type,
   cyber_ind,
   wk_idnt
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates
  WHERE day_dt BETWEEN {{params.start_date}} AND {{params.end_date_middle}}
   AND LOWER(CASE
      WHEN anniv_ind = 1
      THEN 'Anniversary'
      WHEN cyber_ind = 1
      THEN 'Cyber'
      ELSE NULL
      END) = LOWER('{{params.date_event_type}}')) (SELECT base.sku_idnt,
   base.loc_idnt,
   base.day_idnt,
   base.date_event AS date_event_type,
    CASE
    WHEN LOWER(base.price_type) = LOWER('REGULAR')
    THEN 'R'
    WHEN LOWER(base.price_type) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(base.price_type) = LOWER('PROMOTION')
    THEN 'P'
    ELSE 'NA'
    END AS price_type,
   base.dma_code,
   MAX(IFNULL(COALESCE(base.anniv_retail_original, price.regular_price_amt), 0)) AS retail_original_mch,
   MAX(IFNULL(base.anniv_retail_special, 0)) AS retail_special_mch,
   SUM(base.product_views) AS product_views,
   SUM(base.cart_adds) AS cart_adds,
   SUM(base.order_units) AS order_units,
   SUM(base.instock_views) AS instock_views,
   SUM(base.scored_views) AS scored_views,
   SUM(base.demand) AS demand,
   SUM(base.wishlist_adds) AS wishlist_adds,
   SUM(base.sales_units) AS sales_units,
   SUM(base.sales_dollars) AS sales_dollars,
   SUM(base.return_units) AS return_units,
   SUM(base.return_dollars) AS return_dollars,
   SUM(base.ntn) AS ntn,
   SUM(base.demand_units) AS demand_units,
   SUM(base.demand_dollars) AS demand_dollars,
   SUM(base.demand_cancel_units) AS demand_cancel_units,
   SUM(base.demand_cancel_dollars) AS demand_cancel_dollars,
   SUM(base.shipped_units) AS shipped_units,
   SUM(base.shipped_dollars) AS shipped_dollars,
   SUM(base.eoh_units) AS eoh_units,
   SUM(base.eoh_dollars) AS eoh_dollars,
   SUM(base.boh_units) AS boh_units,
   SUM(base.boh_dollars) AS boh_dollars,
   SUM(base.nonsellable_units) AS nonsellable_units,
   SUM(base.cogs) AS cogs,
   SUM(base.receipt_units) AS receipt_units,
   SUM(base.receipt_dollars) AS receipt_dollars,
   MAX(base.sales_aur) AS sales_aur,
   MAX(base.demand_aur) AS demand_aur,
   MAX(base.eoh_aur) AS eoh_aur,
   MAX(base.receipt_aur) AS receipt_aur,
   IFNULL(ABS(MAX(COALESCE(IF(base.sales_aur = 0, NULL, base.sales_aur), IF(base.demand_aur = 0, NULL, base.demand_aur)
       , IF(base.eoh_aur = 0, NULL, base.eoh_aur), IF(base.receipt_aur = 0, NULL, base.receipt_aur), IF(base.on_order_aur
          = 0, NULL, base.on_order_aur), IF(base.com_demand_aur = 0, NULL, base.com_demand_aur)))), 0) AS price_band_aur
   ,
   SUM(base.store_fulfill_units) AS store_fulfill_units,
   SUM(base.store_fulfill_dollars) AS store_fulfill_dollars,
   SUM(base.dropship_units) AS dropship_units,
   SUM(base.dropship_dollars) AS dropship_dollars,
   SUM(base.demand_dropship_units) AS demand_dropship_units,
   SUM(base.demand_dropship_dollars) AS demand_dropship_dollars,
   SUM(base.receipt_dropship_units) AS receipt_dropship_units,
   SUM(base.receipt_dropship_dollars) AS receipt_dropship_dollars,
   SUM(base.sales_cost) AS sales_cost,
   SUM(base.return_cost) AS return_cost,
   SUM(base.shipped_cost) AS shipped_cost,
   SUM(base.store_fulfill_cost) AS store_fulfill_cost,
   SUM(base.dropship_cost) AS dropship_cost,
   SUM(base.eoh_cost) AS eoh_cost,
   SUM(base.boh_cost) AS boh_cost,
   SUM(base.receipt_cost) AS receipt_cost,
   SUM(base.receipt_dropship_cost) AS receipt_dropship_cost,
   SUM(base.sales_pm) AS sales_pm,
   SUM(base.on_order_units) AS on_order_units,
   SUM(base.on_order_retail_dollars) AS on_order_retail_dollars,
   SUM(base.on_order_cost_dollars) AS on_order_cost_dollars
  FROM (SELECT base.sku_idnt,
      base.loc_idnt,
      base.day_idnt,
      df.day_dt,
      base.date_event_type AS date_event,
       CASE
       WHEN LOWER(base.price_type) = LOWER('R')
       THEN 'REGULAR'
       WHEN LOWER(base.price_type) = LOWER('P')
       THEN 'PROMOTION'
       WHEN LOWER(base.price_type) = LOWER('C')
       THEN 'CLEARANCE'
       ELSE NULL
       END AS price_type,
      IF(base.anniv_retail_original = 0, NULL, base.anniv_retail_original) AS anniv_retail_original,
      IF(base.anniv_retail_special = 0, NULL, base.anniv_retail_special) AS anniv_retail_special,
      COALESCE(dma.dma_code, '0') AS dma_code,
      0 AS product_views,
      0 AS cart_adds,
      0 AS order_units,
      0 AS instock_views,
      0 AS scored_views,
      0 AS demand,
      0 AS wishlist_adds,
      base.sales_units,
      base.sales_dollars,
      base.return_units,
      base.return_dollars,
      base.ntn,
      base.demand_units,
      base.demand_dollars,
      base.demand_cancel_units,
      base.demand_cancel_dollars,
      base.demand_dropship_units,
      base.demand_dropship_dollars,
      base.store_fulfill_units,
      base.store_fulfill_dollars,
      base.dropship_units,
      base.dropship_dollars,
      base.shipped_units,
      base.shipped_dollars,
      base.eoh_units,
      base.eoh_dollars,
      base.boh_units,
      base.boh_dollars,
      base.nonsellable_units,
      base.cogs,
      base.receipt_units,
      base.receipt_dollars,
      base.receipt_dropship_units,
      base.receipt_dropship_dollars,
      base.sales_cost,
      base.return_cost,
      base.shipped_cost,
      base.store_fulfill_cost,
      base.dropship_cost,
      base.eoh_cost,
      base.boh_cost,
      base.receipt_cost,
      base.receipt_dropship_cost,
      base.sales_pm,
      base.sales_aur,
      base.demand_aur,
      base.eoh_aur,
      base.receipt_aur,
      0 AS com_demand_aur,
      0 AS on_order_aur,
      0 AS on_order_units,
      0 AS on_order_retail_dollars,
      0 AS on_order_cost_dollars
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_base AS base
      INNER JOIN locs AS loc ON CAST(base.loc_idnt AS FLOAT64) = loc.loc_idnt
      INNER JOIN date_filter AS df ON base.day_idnt = df.day_idnt
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dma_lkp_vw AS dma ON CAST(base.loc_idnt AS FLOAT64) = dma.stor_num
     UNION ALL
     SELECT clk.sku_idnt,
      clk.loc_idnt,
      clk.day_idnt,
      clk.day_dt,
      clk.date_event,
      clk.price_type,
      CAST(NULL AS NUMERIC) AS anniv_retail_original,
      CAST(NULL AS NUMERIC) AS anniv_retail_special,
      clk.dma_code,
      clk.product_views,
      clk.cart_adds,
      clk.order_units,
      clk.instock_views,
      clk.scored_views,
      clk.demand,
      clk.wishlist_adds,
      0 AS sales_units,
      0 AS sales_dollars,
      0 AS return_units,
      0 AS return_dollars,
      0 AS ntn,
      0 AS demand_units,
      0 AS demand_dollars,
      0 AS demand_cancel_units,
      0 AS demand_cancel_dollars,
      0 AS demand_dropship_units,
      0 AS demand_dropship_dollars,
      0 AS store_fulfill_units,
      0 AS store_fulfill_dollars,
      0 AS dropship_units,
      0 AS dropship_dollars,
      0 AS shipped_units,
      0 AS shipped_dollars,
      0 AS eoh_units,
      0 AS eoh_dollars,
      0 AS boh_units,
      0 AS boh_dollars,
      0 AS nonsellable_units,
      0 AS cogs,
      0 AS receipt_units,
      0 AS receipt_dollars,
      0 AS receipt_dropship_units,
      0 AS receipt_dropship_dollars,
      0 AS sales_cost,
      0 AS return_cost,
      0 AS shipped_cost,
      0 AS store_fulfill_cost,
      0 AS dropship_cost,
      0 AS eoh_cost,
      0 AS boh_cost,
      0 AS receipt_cost,
      0 AS receipt_dropship_cost,
      0 AS sales_pm,
      0 AS sales_aur,
      0 AS demand_aur,
      0 AS eoh_aur,
      0 AS receipt_aur,
       CASE
       WHEN clk.order_units = 0
       THEN 0
       ELSE clk.demand / clk.order_units
       END AS com_demand_aur,
      0 AS on_order_aur,
      0 AS on_order_units,
      0 AS on_order_retail_dollars,
      0 AS on_order_cost_dollars
     FROM pfd AS clk
      INNER JOIN locs AS loc0 ON CAST(clk.loc_idnt AS FLOAT64) = loc0.loc_idnt
      INNER JOIN date_filter AS df ON clk.day_idnt = df.day_idnt
     WHERE clk.day_dt BETWEEN {{params.start_date}} AND {{params.end_date_middle}}
     UNION ALL
     SELECT base0.rms_sku_num AS sku_idnt,
      SUBSTR(CAST(base0.store_num AS STRING), 1, 4) AS loc_idnt,
      df.day_idnt,
      df.day_dt,
       CASE
       WHEN df.anniv_ind = 1
       THEN 'Anniversary'
       WHEN df.cyber_ind = 1
       THEN 'Cyber'
       ELSE NULL
       END AS date_event,
      SUBSTR('NA', 1, 2) AS price_type,
      CAST(NULL AS NUMERIC) AS anniv_retail_original,
      CAST(NULL AS NUMERIC) AS anniv_retail_special,
      COALESCE(dma0.dma_code, '0') AS dma_code,
      0 AS product_views,
      0 AS cart_adds,
      0 AS order_units,
      0 AS instock_views,
      0 AS scored_views,
      0 AS demand,
      0 AS wishlist_adds,
      0 AS sales_units,
      0 AS sales_dollars,
      0 AS return_units,
      0 AS return_dollars,
      0 AS ntn,
      0 AS demand_units,
      0 AS demand_dollars,
      0 AS demand_cancel_units,
      0 AS demand_cancel_dollars,
      0 AS demand_dropship_units,
      0 AS demand_dropship_dollars,
      0 AS store_fulfill_units,
      0 AS store_fulfill_dollars,
      0 AS dropship_units,
      0 AS dropship_dollars,
      0 AS shipped_units,
      0 AS shipped_dollars,
      0 AS eoh_units,
      0 AS eoh_dollars,
      0 AS boh_units,
      0 AS boh_dollars,
      0 AS nonsellable_units,
      0 AS cogs,
      0 AS receipt_units,
      0 AS receipt_dollars,
      0 AS receipt_dropship_units,
      0 AS receipt_dropship_dollars,
      0 AS sales_cost,
      0 AS return_cost,
      0 AS shipped_cost,
      0 AS store_fulfill_cost,
      0 AS dropship_cost,
      0 AS eoh_cost,
      0 AS boh_cost,
      0 AS receipt_cost,
      0 AS receipt_dropship_cost,
      0 AS sales_pm,
      0 AS sales_aur,
      0 AS demand_aur,
      0 AS eoh_aur,
      0 AS receipt_aur,
      0 AS com_demand_aur,
       CASE
       WHEN base0.quantity_open = 0
       THEN 0
       ELSE base0.anticipated_retail_amt
       END AS on_order_aur,
      base0.quantity_open AS on_order_units,
       base0.anticipated_retail_amt * base0.quantity_open AS on_order_retail_dollars,
       base0.unit_estimated_landing_cost * base0.quantity_open AS on_order_cost_dollars
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS base0
      INNER JOIN locs AS loc1 ON base0.store_num = loc1.loc_idnt
      INNER JOIN date_filter AS df ON base0.week_num = df.wk_idnt
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dma_lkp_vw AS dma0 ON base0.store_num = dma0.stor_num
     WHERE base0.quantity_open > 0
      AND (LOWER(base0.status) = LOWER('CLOSED') AND base0.end_ship_date >= DATE_SUB((SELECT MAX(week_end_day_date)
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
            WHERE week_end_day_date < CURRENT_DATE), INTERVAL 45 DAY) OR LOWER(base0.status) IN (LOWER('APPROVED'),
          LOWER('WORKSHEET')))) AS base
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS ps ON ps.store_num = CAST(base.loc_idnt AS FLOAT64)
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS price ON LOWER(price.rms_sku_num) = LOWER(base.sku_idnt) AND CAST(price.store_num AS FLOAT64)
      = ps.price_store_num AND LOWER(price.selling_retail_price_type_code) = LOWER(base.price_type)
	 AND base.day_dt BETWEEN cast(FORMAT_DATE('%Y-%m-%d',(datetime(price.EFF_BEGIN_TMSTP))) as date) 
	 AND date_sub(cast(FORMAT_DATE('%Y-%m-%d',(price.EFF_END_TMSTP)) as date),INTERVAL 1 DAY) 
  GROUP BY base.sku_idnt,
   base.loc_idnt,
   base.day_idnt,
   date_event_type,
   price_type,
   base.dma_code);


--COLLECT STATISTICS 	PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )     ,column ( day_idnt ) 		ON merch_clk


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_sku_clk_rms
WHERE LOWER(date_event_type) = LOWER('{{params.date_event_type}}') AND day_dt BETWEEN {{params.start_date}} AND {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_sku_clk_rms
(SELECT ap.sku_idnt,
  ap.day_idnt,
  dt.day_dt,
  dt.day_dt_aligned,
  ap.date_event_type,
  dt.event_type AS event_phase_orig,
  dt.event_day AS event_day_orig,
  dt.event_type_mod AS event_phase,
  dt.event_day_mod AS event_day,
  dt.month_idnt,
  dt.month_label,
   CASE
   WHEN an.anniv_item_ind = 1
   THEN 'Y'
   ELSE 'N'
   END AS anniv_ind,
   CASE
   WHEN LOWER(ap.price_type) = LOWER('R')
   THEN 'Reg'
   WHEN LOWER(ap.price_type) = LOWER('P')
   THEN 'Pro'
   WHEN LOWER(ap.price_type) = LOWER('C')
   THEN 'Clear'
   ELSE COALESCE(ap.price_type, 'NA')
   END AS price_type,
   CASE
   WHEN rp.rp_ind = 1
   THEN 'Y'
   ELSE 'N'
   END AS rp_ind,
  dt.ty_ly_lly AS ty_ly_ind,
  COALESCE(loc.chnl_label, '0, UNKNOWN') AS channel,
  COALESCE(SUBSTR(CAST(loc.chnl_idnt AS STRING), 1, 10), '0') AS channel_idnt,
  loc.loc_label AS location,
  loc.banner,
  loc.cluster_climate AS climate_cluster,
  loc.nr_cluster_price AS nr_price_cluster,
  loc.country,
  COALESCE(dma.dma_short, 'OTHER') AS dma_short,
  COALESCE(dma.dma_proxy_zip, 'OTHER') AS dma_proxy_zip,
  sku.style_idnt AS style_num,
  sku.style_desc,
  sku.supp_prt_nbr AS vpn,
  sku.supp_color,
  sku.colr_idnt,
  sku.style_group_idnt,
   CASE
   WHEN ds.sku_idnt IS NOT NULL
   THEN 'Y'
   ELSE 'N'
   END AS dropship_ind,
  TRIM(sku.div_label) AS division,
  TRIM(sku.sdiv_label) AS subdivision,
  TRIM(sku.dept_label) AS department,
  TRIM(sku.class_label) AS class,
  TRIM(sku.sbclass_label) AS subclass,
  sku.supp_name AS supplier,
  sku.brand_name AS brand,
  sku.npg_ind,
  sku.assortment_grouping,
  sku.quantrix_category,
  sku.ccs_category,
  sku.ccs_subcategory,
  sku.nord_role_desc,
  sku.rack_role_desc,
  sku.merch_theme,
  sku.anniversary_theme,
  sku.holiday_theme_ty AS holiday_theme,
   CASE
   WHEN LOWER(sku.gift_ind) = LOWER('GIFT')
   THEN 'GIFT'
   ELSE 'NON-GIFT'
   END AS gift_ind,
   CASE
   WHEN LOWER(sku.stocking_stuffer) = LOWER('Y')
   THEN 'Y'
   ELSE 'N'
   END AS stocking_stuffer,
  'NA' AS price_match_ind,
  'NA' AS promo_grouping,
  sku.bipoc_ind,
  um.udig,
   CASE
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) = 0.00
   THEN 'No Retail Special'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 10.00
   THEN '< $10'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 25.00
   THEN '$10 - $25'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 50.00
   THEN '$25 - $50'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 100.00
   THEN '$50 - $100'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 150.00
   THEN '$100 - $150'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 200.00
   THEN '$150 - $200'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 300.00
   THEN '$200 - $300'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 500.00
   THEN '$300 - $500'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1000.00
   THEN '$500 - $1000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) > 1000.00
   THEN '> $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) = 0.00
   THEN 'No Retail Original'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 25.00
   THEN '$10 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 50.00
   THEN '$25 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 100.00
   THEN '$50 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 150.00
   THEN '$100 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 200.00
   THEN '$150 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 300.00
   THEN '$200 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 500.00
   THEN '$300 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1000.00
   THEN '$500 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) > 1000.00
   THEN '> $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur = 0.00
   THEN 'UNKNOWN'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 25.00
   THEN '$10 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 50.00
   THEN '$25 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 100.00
   THEN '$50 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 150.00
   THEN '$100 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 200.00
   THEN '$150 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 300.00
   THEN '$200 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 500.00
   THEN '$300 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1000.00
   THEN '$500 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur > 1000.00
   THEN '> $1000'
   ELSE NULL
   END AS price_band_one,
   CASE
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) = 0.00
   THEN 'No Retail Special'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 10.00
   THEN '< $10'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 15.00
   THEN '$10 - $15'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 20.00
   THEN '$15 - $20'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 25.00
   THEN '$20 - $25'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 30.00
   THEN '$25 - $30'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 40.00
   THEN '$30 - $40'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 50.00
   THEN '$40 - $50'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 60.00
   THEN '$50 - $60'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 80.00
   THEN '$60 - $80'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 100.00
   THEN '$80 - $100'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 125.00
   THEN '$100 - $125'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 150.00
   THEN '$125 - $150'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 175.00
   THEN '$150 - $175'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 200.00
   THEN '$175 - $200'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 250.00
   THEN '$200 - $250'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 300.00
   THEN '$250 - $300'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 400.00
   THEN '$300 - $400'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 500.00
   THEN '$400 - $500'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 700.00
   THEN '$500 - $700'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 900.00
   THEN '$700 - $900'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1000.00
   THEN '$900 - $1000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1200.00
   THEN '$1000 - $1200'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1500.00
   THEN '$1200 - $1500'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1800.00
   THEN '$1500 - $1800'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 2000.00
   THEN '$1800 - $2000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 3000.00
   THEN '$2000 - $3000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 4000.00
   THEN '$3000 - $4000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 5000.00
   THEN '$4000 - $5000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) > 5000.00
   THEN '> $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) = 0.00
   THEN 'No Retail Original'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 15.00
   THEN '$10 - $15'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 20.00
   THEN '$15 - $20'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 25.00
   THEN '$20 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 30.00
   THEN '$25 - $30'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 40.00
   THEN '$30 - $40'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 50.00
   THEN '$40 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 60.00
   THEN '$50 - $60'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 80.00
   THEN '$60 - $80'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 100.00
   THEN '$80 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 125.00
   THEN '$100 - $125'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 150.00
   THEN '$125 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 175.00
   THEN '$150 - $175'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 200.00
   THEN '$175 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 250.00
   THEN '$200 - $250'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 300.00
   THEN '$250 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 400.00
   THEN '$300 - $400'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 500.00
   THEN '$400 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 700.00
   THEN '$500 - $700'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 900.00
   THEN '$700 - $900'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1000.00
   THEN '$900 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1200.00
   THEN '$1000 - $1200'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1500.00
   THEN '$1200 - $1500'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1800.00
   THEN '$1500 - $1800'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 2000.00
   THEN '$1800 - $2000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 3000.00
   THEN '$2000 - $3000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 4000.00
   THEN '$3000 - $4000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 5000.00
   THEN '$4000 - $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) > 5000.00
   THEN '> $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur = 0.00
   THEN 'UNKNOWN'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 15.00
   THEN '$10 - $15'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 20.00
   THEN '$15 - $20'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 25.00
   THEN '$20 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 30.00
   THEN '$25 - $30'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 40.00
   THEN '$30 - $40'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 50.00
   THEN '$40 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 60.00
   THEN '$50 - $60'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 80.00
   THEN '$60 - $80'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 100.00
   THEN '$80 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 125.00
   THEN '$100 - $125'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 150.00
   THEN '$125 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 175.00
   THEN '$150 - $175'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 200.00
   THEN '$175 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 250.00
   THEN '$200 - $250'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 300.00
   THEN '$250 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 400.00
   THEN '$300 - $400'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 500.00
   THEN '$400 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 700.00
   THEN '$500 - $700'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 900.00
   THEN '$700 - $900'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1000.00
   THEN '$900 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1200.00
   THEN '$1000 - $1200'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1500.00
   THEN '$1200 - $1500'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1800.00
   THEN '$1500 - $1800'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 2000.00
   THEN '$1800 - $2000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 3000.00
   THEN '$2000 - $3000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 4000.00
   THEN '$3000 - $4000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 5000.00
   THEN '$4000 - $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur > 5000.00
   THEN '> $5000'
   ELSE NULL
   END AS price_band_two,
  ap.sales_units,
  ap.sales_dollars,
  ap.return_units,
  ap.return_dollars,
  ap.ntn,
  ap.demand_units,
  ap.demand_dollars,
  ap.demand_cancel_units,
  ap.demand_cancel_dollars,
  ap.shipped_units,
  ap.shipped_dollars,
  ap.eoh_units,
  ap.eoh_dollars,
  ap.boh_units,
  ap.boh_dollars,
  ap.nonsellable_units,
  ap.cogs,
  ap.receipt_units,
  ap.receipt_dollars,
  ap.sales_aur,
  ap.demand_aur,
  ap.eoh_aur,
  ap.store_fulfill_units,
  ap.store_fulfill_dollars,
  ap.dropship_units,
  ap.dropship_dollars,
  ap.demand_dropship_units,
  ap.demand_dropship_dollars,
  ap.receipt_dropship_units,
  ap.receipt_dropship_dollars,
  ap.on_order_units,
  ROUND(CAST(ap.on_order_retail_dollars AS NUMERIC), 2) AS on_order_retail_dollars,
  ap.product_views,
  ap.cart_adds,
  ap.order_units,
  ROUND(CAST(ap.instock_views AS NUMERIC), 5) AS instock_views,
  ap.demand,
  ap.wishlist_adds,
  IFNULL(ap.retail_original_mch, 0) AS retail_original,
  CAST(IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt = 0, NULL, an.spcl_price_amt), CASE
      WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
      THEN ap.retail_original_mch
      ELSE NULL
      END), 0) AS NUMERIC) AS retail_special,
  CAST(CASE
    WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt = 0, NULL, an.spcl_price_amt), CASE
          WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
          THEN ap.retail_original_mch
          ELSE NULL
          END), 0) > 0 AND IFNULL(ap.retail_original_mch, 0) > 0
    THEN IFNULL(COALESCE(IF(ap.eoh_units + ap.sales_units + ap.on_order_units + ap.receipt_dropship_units + ap.return_units = 0, NULL, ap.eoh_units + ap.sales_units + ap.on_order_units + ap.receipt_dropship_units + ap.return_units), IF(ap.demand_units = 0, NULL, ap.demand_units), IF(ap.receipt_units
         = 0, NULL, ap.receipt_units), IF(ap.return_units = 0, NULL, ap.return_units), IF(ap.boh_units = 0, NULL, ap.boh_units)), 0)
    ELSE 0
    END AS NUMERIC) AS units,
  sku.last_receipt_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  process_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(CURRENT_DATETIME('PST8PDT') as string)) as process_tmstp_tz,
  ap.sales_cost,
  ap.return_cost,
  ap.shipped_cost,
  ap.store_fulfill_cost,
  ap.dropship_cost,
  ap.eoh_cost,
  ap.boh_cost,
  ap.receipt_cost,
  ap.receipt_dropship_cost,
  ap.sales_pm,
  ROUND(CAST(IFNULL(ap.on_order_cost_dollars, 0) AS NUMERIC), 2) AS ap_on_order_cost_dollars,
  CAST(IFNULL(ap.scored_views, 0) AS NUMERIC) AS ap_scored_views,
  sku.general_merch_manager_executive_vice_president,
  sku.div_merch_manager_senior_vice_president,
  sku.div_merch_manager_vice_president,
  sku.merch_director,
  sku.buyer,
  sku.merch_planning_executive_vice_president,
  sku.merch_planning_senior_vice_president,
  sku.merch_planning_vice_president,
  sku.merch_planning_director_manager,
  sku.assortment_planner,
  sku.anchor_brands_ind
 FROM t2dl_das_scaled_events.merch_clk AS ap
  INNER JOIN locs AS loc ON CAST(ap.loc_idnt AS FLOAT64) = loc.loc_idnt
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates AS dt ON ap.day_idnt = dt.day_idnt
  INNER JOIN prod_hier AS sku ON LOWER(ap.sku_idnt) = LOWER(sku.sku_idnt)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_sku_chnl_date AS an ON LOWER(ap.date_event_type) = LOWER('Anniversary')
       AND LOWER(ap.sku_idnt) = LOWER(an.sku_idnt) AND LOWER(an.channel_country) = LOWER('US') AND LOWER(loc.event_selling_channel
       ) = LOWER(an.selling_channel) AND LOWER(loc.event_store_type_code) = LOWER(an.store_type_code) AND ap.day_idnt =
    an.day_idnt
  LEFT JOIN rp ON LOWER(ap.sku_idnt) = LOWER(rp.sku_idnt) AND CAST(ap.loc_idnt AS FLOAT64) = rp.loc_idnt AND ap.day_idnt
     = rp.day_idnt
  LEFT JOIN dropship AS ds ON LOWER(ap.sku_idnt) = LOWER(ds.sku_idnt) AND dt.day_dt = ds.day_dt
  LEFT JOIN dma ON LOWER(ap.dma_code) = LOWER(dma.dma_code)
  LEFT JOIN udig_main AS um ON LOWER(ap.sku_idnt) = LOWER(um.sku_idnt) AND dt.yr_idnt = um.udig_year);


--TRUNCATE TABLE temp_schema.merch_clk;


--DROP TABLE IF EXISTS temp_schema.merch_clk;


CREATE TEMPORARY TABLE IF NOT EXISTS merch_clk AS WITH date_filter AS (SELECT day_idnt,
   day_dt,
   anniv_ind,
    CASE
    WHEN anniv_ind = 1
    THEN 'Anniversary'
    WHEN cyber_ind = 1
    THEN 'Cyber'
    ELSE NULL
    END AS date_event_type,
   cyber_ind,
   wk_idnt
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates
  WHERE day_dt BETWEEN {{params.start_date_middle}} AND {{params.end_date}}
   AND LOWER(CASE
      WHEN anniv_ind = 1
      THEN 'Anniversary'
      WHEN cyber_ind = 1
      THEN 'Cyber'
      ELSE NULL
      END) = LOWER('{{params.date_event_type}}')) (SELECT base.sku_idnt,
   base.loc_idnt,
   base.day_idnt,
   base.date_event AS date_event_type,
    CASE
    WHEN LOWER(base.price_type) = LOWER('REGULAR')
    THEN 'R'
    WHEN LOWER(base.price_type) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(base.price_type) = LOWER('PROMOTION')
    THEN 'P'
    ELSE 'NA'
    END AS price_type,
   base.dma_code,
   MAX(IFNULL(COALESCE(base.anniv_retail_original, price.regular_price_amt), 0)) AS retail_original_mch,
   MAX(IFNULL(base.anniv_retail_special, 0)) AS retail_special_mch,
   SUM(base.product_views) AS product_views,
   SUM(base.cart_adds) AS cart_adds,
   SUM(base.order_units) AS order_units,
   SUM(base.instock_views) AS instock_views,
   SUM(base.scored_views) AS scored_views,
   SUM(base.demand) AS demand,
   SUM(base.wishlist_adds) AS wishlist_adds,
   SUM(base.sales_units) AS sales_units,
   SUM(base.sales_dollars) AS sales_dollars,
   SUM(base.return_units) AS return_units,
   SUM(base.return_dollars) AS return_dollars,
   SUM(base.ntn) AS ntn,
   SUM(base.demand_units) AS demand_units,
   SUM(base.demand_dollars) AS demand_dollars,
   SUM(base.demand_cancel_units) AS demand_cancel_units,
   SUM(base.demand_cancel_dollars) AS demand_cancel_dollars,
   SUM(base.shipped_units) AS shipped_units,
   SUM(base.shipped_dollars) AS shipped_dollars,
   SUM(base.eoh_units) AS eoh_units,
   SUM(base.eoh_dollars) AS eoh_dollars,
   SUM(base.boh_units) AS boh_units,
   SUM(base.boh_dollars) AS boh_dollars,
   SUM(base.nonsellable_units) AS nonsellable_units,
   SUM(base.cogs) AS cogs,
   SUM(base.receipt_units) AS receipt_units,
   SUM(base.receipt_dollars) AS receipt_dollars,
   MAX(base.sales_aur) AS sales_aur,
   MAX(base.demand_aur) AS demand_aur,
   MAX(base.eoh_aur) AS eoh_aur,
   MAX(base.receipt_aur) AS receipt_aur,
   IFNULL(ABS(MAX(COALESCE(IF(base.sales_aur = 0, NULL, base.sales_aur), IF(base.demand_aur = 0, NULL, base.demand_aur)
       , IF(base.eoh_aur = 0, NULL, base.eoh_aur), IF(base.receipt_aur = 0, NULL, base.receipt_aur), IF(base.on_order_aur
          = 0, NULL, base.on_order_aur), IF(base.com_demand_aur = 0, NULL, base.com_demand_aur)))), 0) AS price_band_aur
   ,
   SUM(base.store_fulfill_units) AS store_fulfill_units,
   SUM(base.store_fulfill_dollars) AS store_fulfill_dollars,
   SUM(base.dropship_units) AS dropship_units,
   SUM(base.dropship_dollars) AS dropship_dollars,
   SUM(base.demand_dropship_units) AS demand_dropship_units,
   SUM(base.demand_dropship_dollars) AS demand_dropship_dollars,
   SUM(base.receipt_dropship_units) AS receipt_dropship_units,
   SUM(base.receipt_dropship_dollars) AS receipt_dropship_dollars,
   SUM(base.sales_cost) AS sales_cost,
   SUM(base.return_cost) AS return_cost,
   SUM(base.shipped_cost) AS shipped_cost,
   SUM(base.store_fulfill_cost) AS store_fulfill_cost,
   SUM(base.dropship_cost) AS dropship_cost,
   SUM(base.eoh_cost) AS eoh_cost,
   SUM(base.boh_cost) AS boh_cost,
   SUM(base.receipt_cost) AS receipt_cost,
   SUM(base.receipt_dropship_cost) AS receipt_dropship_cost,
   SUM(base.sales_pm) AS sales_pm,
   SUM(base.on_order_units) AS on_order_units,
   SUM(base.on_order_retail_dollars) AS on_order_retail_dollars,
   SUM(base.on_order_cost_dollars) AS on_order_cost_dollars
  FROM (SELECT base.sku_idnt,
      base.loc_idnt,
      base.day_idnt,
      df.day_dt,
      base.date_event_type AS date_event,
       CASE
       WHEN LOWER(base.price_type) = LOWER('R')
       THEN 'REGULAR'
       WHEN LOWER(base.price_type) = LOWER('P')
       THEN 'PROMOTION'
       WHEN LOWER(base.price_type) = LOWER('C')
       THEN 'CLEARANCE'
       ELSE NULL
       END AS price_type,
      IF(base.anniv_retail_original = 0, NULL, base.anniv_retail_original) AS anniv_retail_original,
      IF(base.anniv_retail_special = 0, NULL, base.anniv_retail_special) AS anniv_retail_special,
      COALESCE(dma.dma_code, '0') AS dma_code,
      0 AS product_views,
      0 AS cart_adds,
      0 AS order_units,
      0 AS instock_views,
      0 AS scored_views,
      0 AS demand,
      0 AS wishlist_adds,
      base.sales_units,
      base.sales_dollars,
      base.return_units,
      base.return_dollars,
      base.ntn,
      base.demand_units,
      base.demand_dollars,
      base.demand_cancel_units,
      base.demand_cancel_dollars,
      base.demand_dropship_units,
      base.demand_dropship_dollars,
      base.store_fulfill_units,
      base.store_fulfill_dollars,
      base.dropship_units,
      base.dropship_dollars,
      base.shipped_units,
      base.shipped_dollars,
      base.eoh_units,
      base.eoh_dollars,
      base.boh_units,
      base.boh_dollars,
      base.nonsellable_units,
      base.cogs,
      base.receipt_units,
      base.receipt_dollars,
      base.receipt_dropship_units,
      base.receipt_dropship_dollars,
      base.sales_cost,
      base.return_cost,
      base.shipped_cost,
      base.store_fulfill_cost,
      base.dropship_cost,
      base.eoh_cost,
      base.boh_cost,
      base.receipt_cost,
      base.receipt_dropship_cost,
      base.sales_pm,
      base.sales_aur,
      base.demand_aur,
      base.eoh_aur,
      base.receipt_aur,
      0 AS com_demand_aur,
      0 AS on_order_aur,
      0 AS on_order_units,
      0 AS on_order_retail_dollars,
      0 AS on_order_cost_dollars
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_base AS base
      INNER JOIN locs AS loc ON CAST(base.loc_idnt AS FLOAT64) = loc.loc_idnt
      INNER JOIN date_filter AS df ON base.day_idnt = df.day_idnt
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dma_lkp_vw AS dma ON CAST(base.loc_idnt AS FLOAT64) = dma.stor_num
     UNION ALL
     SELECT clk.sku_idnt,
      clk.loc_idnt,
      clk.day_idnt,
      clk.day_dt,
      clk.date_event,
      clk.price_type,
      CAST(NULL AS NUMERIC) AS anniv_retail_original,
      CAST(NULL AS NUMERIC) AS anniv_retail_special,
      clk.dma_code,
      clk.product_views,
      clk.cart_adds,
      clk.order_units,
      clk.instock_views,
      clk.scored_views,
      clk.demand,
      clk.wishlist_adds,
      0 AS sales_units,
      0 AS sales_dollars,
      0 AS return_units,
      0 AS return_dollars,
      0 AS ntn,
      0 AS demand_units,
      0 AS demand_dollars,
      0 AS demand_cancel_units,
      0 AS demand_cancel_dollars,
      0 AS demand_dropship_units,
      0 AS demand_dropship_dollars,
      0 AS store_fulfill_units,
      0 AS store_fulfill_dollars,
      0 AS dropship_units,
      0 AS dropship_dollars,
      0 AS shipped_units,
      0 AS shipped_dollars,
      0 AS eoh_units,
      0 AS eoh_dollars,
      0 AS boh_units,
      0 AS boh_dollars,
      0 AS nonsellable_units,
      0 AS cogs,
      0 AS receipt_units,
      0 AS receipt_dollars,
      0 AS receipt_dropship_units,
      0 AS receipt_dropship_dollars,
      0 AS sales_cost,
      0 AS return_cost,
      0 AS shipped_cost,
      0 AS store_fulfill_cost,
      0 AS dropship_cost,
      0 AS eoh_cost,
      0 AS boh_cost,
      0 AS receipt_cost,
      0 AS receipt_dropship_cost,
      0 AS sales_pm,
      0 AS sales_aur,
      0 AS demand_aur,
      0 AS eoh_aur,
      0 AS receipt_aur,
       CASE
       WHEN clk.order_units = 0
       THEN 0
       ELSE clk.demand / clk.order_units
       END AS com_demand_aur,
      0 AS on_order_aur,
      0 AS on_order_units,
      0 AS on_order_retail_dollars,
      0 AS on_order_cost_dollars
     FROM pfd AS clk
      INNER JOIN locs AS loc0 ON CAST(clk.loc_idnt AS FLOAT64) = loc0.loc_idnt
      INNER JOIN date_filter AS df ON clk.day_idnt = df.day_idnt
     WHERE clk.day_dt BETWEEN {{params.start_date_middle}} AND {{params.end_date}}
     UNION ALL
     SELECT base0.rms_sku_num AS sku_idnt,
      SUBSTR(CAST(base0.store_num AS STRING), 1, 4) AS loc_idnt,
      df.day_idnt,
      df.day_dt,
       CASE
       WHEN df.anniv_ind = 1
       THEN 'Anniversary'
       WHEN df.cyber_ind = 1
       THEN 'Cyber'
       ELSE NULL
       END AS date_event,
      SUBSTR('NA', 1, 2) AS price_type,
      CAST(NULL AS NUMERIC) AS anniv_retail_original,
      CAST(NULL AS NUMERIC) AS anniv_retail_special,
      COALESCE(dma0.dma_code, '0') AS dma_code,
      0 AS product_views,
      0 AS cart_adds,
      0 AS order_units,
      0 AS instock_views,
      0 AS scored_views,
      0 AS demand,
      0 AS wishlist_adds,
      0 AS sales_units,
      0 AS sales_dollars,
      0 AS return_units,
      0 AS return_dollars,
      0 AS ntn,
      0 AS demand_units,
      0 AS demand_dollars,
      0 AS demand_cancel_units,
      0 AS demand_cancel_dollars,
      0 AS demand_dropship_units,
      0 AS demand_dropship_dollars,
      0 AS store_fulfill_units,
      0 AS store_fulfill_dollars,
      0 AS dropship_units,
      0 AS dropship_dollars,
      0 AS shipped_units,
      0 AS shipped_dollars,
      0 AS eoh_units,
      0 AS eoh_dollars,
      0 AS boh_units,
      0 AS boh_dollars,
      0 AS nonsellable_units,
      0 AS cogs,
      0 AS receipt_units,
      0 AS receipt_dollars,
      0 AS receipt_dropship_units,
      0 AS receipt_dropship_dollars,
      0 AS sales_cost,
      0 AS return_cost,
      0 AS shipped_cost,
      0 AS store_fulfill_cost,
      0 AS dropship_cost,
      0 AS eoh_cost,
      0 AS boh_cost,
      0 AS receipt_cost,
      0 AS receipt_dropship_cost,
      0 AS sales_pm,
      0 AS sales_aur,
      0 AS demand_aur,
      0 AS eoh_aur,
      0 AS receipt_aur,
      0 AS com_demand_aur,
       CASE
       WHEN base0.quantity_open = 0
       THEN 0
       ELSE base0.anticipated_retail_amt
       END AS on_order_aur,
      base0.quantity_open AS on_order_units,
       base0.anticipated_retail_amt * base0.quantity_open AS on_order_retail_dollars,
       base0.unit_estimated_landing_cost * base0.quantity_open AS on_order_cost_dollars
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS base0
      INNER JOIN locs AS loc1 ON base0.store_num = loc1.loc_idnt
      INNER JOIN date_filter AS df ON base0.week_num = df.wk_idnt
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dma_lkp_vw AS dma0 ON base0.store_num = dma0.stor_num
     WHERE base0.quantity_open > 0
      AND (LOWER(base0.status) = LOWER('CLOSED') AND base0.end_ship_date >= DATE_SUB((SELECT MAX(week_end_day_date)
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
            WHERE week_end_day_date < CURRENT_DATE), INTERVAL 45 DAY) OR LOWER(base0.status) IN (LOWER('APPROVED'),
          LOWER('WORKSHEET')))) AS base
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS ps ON ps.store_num = CAST(base.loc_idnt AS FLOAT64)
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS price ON LOWER(price.rms_sku_num) = LOWER(base.sku_idnt) AND CAST(price.store_num AS FLOAT64)
      = ps.price_store_num AND LOWER(price.selling_retail_price_type_code) = LOWER(base.price_type)
	AND base.day_dt BETWEEN cast(FORMAT_DATE('%Y-%m-%d',(datetime(price.EFF_BEGIN_TMSTP))) as date)
	   AND date_sub(cast(FORMAT_DATE('%Y-%m-%d',(price.EFF_END_TMSTP)) as date),INTERVAL 1 DAY)
  GROUP BY base.sku_idnt,
   base.loc_idnt,
   base.day_idnt,
   date_event_type,
   price_type,
   base.dma_code);


--COLLECT STATISTICS 	PRIMARY INDEX ( sku_idnt, loc_idnt, day_idnt )     ,column ( day_idnt ) 		ON merch_clk


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_sku_clk_rms
(SELECT ap.sku_idnt,
  ap.day_idnt,
  dt.day_dt,
  dt.day_dt_aligned,
  ap.date_event_type,
  dt.event_type AS event_phase_orig,
  dt.event_day AS event_day_orig,
  dt.event_type_mod AS event_phase,
  dt.event_day_mod AS event_day,
  dt.month_idnt,
  dt.month_label,
   CASE
   WHEN an.anniv_item_ind = 1
   THEN 'Y'
   ELSE 'N'
   END AS anniv_ind,
   CASE
   WHEN LOWER(ap.price_type) = LOWER('R')
   THEN 'Reg'
   WHEN LOWER(ap.price_type) = LOWER('P')
   THEN 'Pro'
   WHEN LOWER(ap.price_type) = LOWER('C')
   THEN 'Clear'
   ELSE COALESCE(ap.price_type, 'NA')
   END AS price_type,
   CASE
   WHEN rp.rp_ind = 1
   THEN 'Y'
   ELSE 'N'
   END AS rp_ind,
  dt.ty_ly_lly AS ty_ly_ind,
  COALESCE(loc.chnl_label, '0, UNKNOWN') AS channel,
  COALESCE(SUBSTR(CAST(loc.chnl_idnt AS STRING), 1, 10), '0') AS channel_idnt,
  loc.loc_label AS location,
  loc.banner,
  loc.cluster_climate AS climate_cluster,
  loc.nr_cluster_price AS nr_price_cluster,
  loc.country,
  COALESCE(dma.dma_short, 'OTHER') AS dma_short,
  COALESCE(dma.dma_proxy_zip, 'OTHER') AS dma_proxy_zip,
  sku.style_idnt AS style_num,
  sku.style_desc,
  sku.supp_prt_nbr AS vpn,
  sku.supp_color,
  sku.colr_idnt,
  sku.style_group_idnt,
   CASE
   WHEN ds.sku_idnt IS NOT NULL
   THEN 'Y'
   ELSE 'N'
   END AS dropship_ind,
  TRIM(sku.div_label) AS division,
  TRIM(sku.sdiv_label) AS subdivision,
  TRIM(sku.dept_label) AS department,
  TRIM(sku.class_label) AS class,
  TRIM(sku.sbclass_label) AS subclass,
  sku.supp_name AS supplier,
  sku.brand_name AS brand,
  sku.npg_ind,
  sku.assortment_grouping,
  sku.quantrix_category,
  sku.ccs_category,
  sku.ccs_subcategory,
  sku.nord_role_desc,
  sku.rack_role_desc,
  sku.merch_theme,
  sku.anniversary_theme,
  sku.holiday_theme_ty AS holiday_theme,
   CASE
   WHEN LOWER(sku.gift_ind) = LOWER('GIFT')
   THEN 'GIFT'
   ELSE 'NON-GIFT'
   END AS gift_ind,
   CASE
   WHEN LOWER(sku.stocking_stuffer) = LOWER('Y')
   THEN 'Y'
   ELSE 'N'
   END AS stocking_stuffer,
  'NA' AS price_match_ind,
  'NA' AS promo_grouping,
  sku.bipoc_ind,
  um.udig,
   CASE
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) = 0.00
   THEN 'No Retail Special'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 10.00
   THEN '< $10'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 25.00
   THEN '$10 - $25'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 50.00
   THEN '$25 - $50'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 100.00
   THEN '$50 - $100'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 150.00
   THEN '$100 - $150'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 200.00
   THEN '$150 - $200'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 300.00
   THEN '$200 - $300'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 500.00
   THEN '$300 - $500'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1000.00
   THEN '$500 - $1000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) > 1000.00
   THEN '> $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) = 0.00
   THEN 'No Retail Original'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 25.00
   THEN '$10 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 50.00
   THEN '$25 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 100.00
   THEN '$50 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 150.00
   THEN '$100 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 200.00
   THEN '$150 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 300.00
   THEN '$200 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 500.00
   THEN '$300 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1000.00
   THEN '$500 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) > 1000.00
   THEN '> $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur = 0.00
   THEN 'UNKNOWN'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 25.00
   THEN '$10 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 50.00
   THEN '$25 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 100.00
   THEN '$50 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 150.00
   THEN '$100 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 200.00
   THEN '$150 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 300.00
   THEN '$200 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 500.00
   THEN '$300 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1000.00
   THEN '$500 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur > 1000.00
   THEN '> $1000'
   ELSE NULL
   END AS price_band_one,
   CASE
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) = 0.00
   THEN 'No Retail Special'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 10.00
   THEN '< $10'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 15.00
   THEN '$10 - $15'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 20.00
   THEN '$15 - $20'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 25.00
   THEN '$20 - $25'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 30.00
   THEN '$25 - $30'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 40.00
   THEN '$30 - $40'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 50.00
   THEN '$40 - $50'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 60.00
   THEN '$50 - $60'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 80.00
   THEN '$60 - $80'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 100.00
   THEN '$80 - $100'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 125.00
   THEN '$100 - $125'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 150.00
   THEN '$125 - $150'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 175.00
   THEN '$150 - $175'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 200.00
   THEN '$175 - $200'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 250.00
   THEN '$200 - $250'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 300.00
   THEN '$250 - $300'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 400.00
   THEN '$300 - $400'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 500.00
   THEN '$400 - $500'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 700.00
   THEN '$500 - $700'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 900.00
   THEN '$700 - $900'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1000.00
   THEN '$900 - $1000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1200.00
   THEN '$1000 - $1200'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1500.00
   THEN '$1200 - $1500'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 1800.00
   THEN '$1500 - $1800'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 2000.00
   THEN '$1800 - $2000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 3000.00
   THEN '$2000 - $3000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 4000.00
   THEN '$3000 - $4000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) <= 5000.00
   THEN '$4000 - $5000'
   WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt
          = 0, NULL, an.spcl_price_amt), CASE
        WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
        THEN ap.retail_original_mch
        ELSE NULL
        END), 0) > 5000.00
   THEN '> $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) = 0.00
   THEN 'No Retail Original'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 15.00
   THEN '$10 - $15'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 20.00
   THEN '$15 - $20'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 25.00
   THEN '$20 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 30.00
   THEN '$25 - $30'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 40.00
   THEN '$30 - $40'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 50.00
   THEN '$40 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 60.00
   THEN '$50 - $60'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 80.00
   THEN '$60 - $80'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 100.00
   THEN '$80 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 125.00
   THEN '$100 - $125'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 150.00
   THEN '$125 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 175.00
   THEN '$150 - $175'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 200.00
   THEN '$175 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 250.00
   THEN '$200 - $250'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 300.00
   THEN '$250 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 400.00
   THEN '$300 - $400'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 500.00
   THEN '$400 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 700.00
   THEN '$500 - $700'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 900.00
   THEN '$700 - $900'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1000.00
   THEN '$900 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1200.00
   THEN '$1000 - $1200'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1500.00
   THEN '$1200 - $1500'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 1800.00
   THEN '$1500 - $1800'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 2000.00
   THEN '$1800 - $2000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 3000.00
   THEN '$2000 - $3000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 4000.00
   THEN '$3000 - $4000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) <= 5000.00
   THEN '$4000 - $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Anniversary') AND IFNULL(an.anniv_item_ind, 0) = 0 AND IFNULL(ap.retail_original_mch
      , 0) > 5000.00
   THEN '> $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur = 0.00
   THEN 'UNKNOWN'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 10.00
   THEN '< $10'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 15.00
   THEN '$10 - $15'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 20.00
   THEN '$15 - $20'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 25.00
   THEN '$20 - $25'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 30.00
   THEN '$25 - $30'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 40.00
   THEN '$30 - $40'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 50.00
   THEN '$40 - $50'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 60.00
   THEN '$50 - $60'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 80.00
   THEN '$60 - $80'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 100.00
   THEN '$80 - $100'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 125.00
   THEN '$100 - $125'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 150.00
   THEN '$125 - $150'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 175.00
   THEN '$150 - $175'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 200.00
   THEN '$175 - $200'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 250.00
   THEN '$200 - $250'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 300.00
   THEN '$250 - $300'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 400.00
   THEN '$300 - $400'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 500.00
   THEN '$400 - $500'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 700.00
   THEN '$500 - $700'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 900.00
   THEN '$700 - $900'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1000.00
   THEN '$900 - $1000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1200.00
   THEN '$1000 - $1200'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1500.00
   THEN '$1200 - $1500'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 1800.00
   THEN '$1500 - $1800'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 2000.00
   THEN '$1800 - $2000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 3000.00
   THEN '$2000 - $3000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 4000.00
   THEN '$3000 - $4000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur <= 5000.00
   THEN '$4000 - $5000'
   WHEN LOWER(ap.date_event_type) = LOWER('Cyber') AND ap.price_band_aur > 5000.00
   THEN '> $5000'
   ELSE NULL
   END AS price_band_two,
  ap.sales_units,
  ap.sales_dollars,
  ap.return_units,
  ap.return_dollars,
  ap.ntn,
  ap.demand_units,
  ap.demand_dollars,
  ap.demand_cancel_units,
  ap.demand_cancel_dollars,
  ap.shipped_units,
  ap.shipped_dollars,
  ap.eoh_units,
  ap.eoh_dollars,
  ap.boh_units,
  ap.boh_dollars,
  ap.nonsellable_units,
  ap.cogs,
  ap.receipt_units,
  ap.receipt_dollars,
  ap.sales_aur,
  ap.demand_aur,
  ap.eoh_aur,
  ap.store_fulfill_units,
  ap.store_fulfill_dollars,
  ap.dropship_units,
  ap.dropship_dollars,
  ap.demand_dropship_units,
  ap.demand_dropship_dollars,
  ap.receipt_dropship_units,
  ap.receipt_dropship_dollars,
  ap.on_order_units,
  ROUND(CAST(ap.on_order_retail_dollars AS NUMERIC), 2) AS on_order_retail_dollars,
  ap.product_views,
  ap.cart_adds,
  ap.order_units,
  ROUND(CAST(ap.instock_views AS NUMERIC), 5) AS instock_views,
  ap.demand,
  ap.wishlist_adds,
  IFNULL(ap.retail_original_mch, 0) AS retail_original,
  CAST(IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt = 0, NULL, an.spcl_price_amt), CASE
      WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
      THEN ap.retail_original_mch
      ELSE NULL
      END), 0) AS NUMERIC) AS retail_special,
  CAST(CASE
    WHEN an.anniv_item_ind = 1 AND IFNULL(COALESCE(IF(ap.retail_special_mch = 0, NULL, ap.retail_special_mch), IF(an.spcl_price_amt = 0, NULL, an.spcl_price_amt), CASE
          WHEN an.anniv_item_ind = 1 AND LOWER(sku.div_label) = LOWER('340, BEAUTY')
          THEN ap.retail_original_mch
          ELSE NULL
          END), 0) > 0 AND IFNULL(ap.retail_original_mch, 0) > 0
    THEN IFNULL(COALESCE(IF(ap.eoh_units + ap.sales_units + ap.on_order_units + ap.receipt_dropship_units + ap.return_units = 0, NULL, ap.eoh_units + ap.sales_units + ap.on_order_units + ap.receipt_dropship_units + ap.return_units), IF(ap.demand_units = 0, NULL, ap.demand_units), IF(ap.receipt_units
         = 0, NULL, ap.receipt_units), IF(ap.return_units = 0, NULL, ap.return_units), IF(ap.boh_units = 0, NULL, ap.boh_units)), 0)
    ELSE 0
    END AS NUMERIC) AS units,
  sku.last_receipt_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  process_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(CURRENT_DATETIME('PST8PDT') as string)) as process_tmstp_tz,
  ap.sales_cost,
  ap.return_cost,
  ap.shipped_cost,
  ap.store_fulfill_cost,
  ap.dropship_cost,
  ap.eoh_cost,
  ap.boh_cost,
  ap.receipt_cost,
  ap.receipt_dropship_cost,
  ap.sales_pm,
  ROUND(CAST(IFNULL(ap.on_order_cost_dollars, 0) AS NUMERIC), 2) AS ap_on_order_cost_dollars,
  CAST(IFNULL(ap.scored_views, 0) AS NUMERIC) AS ap_scored_views,
  sku.general_merch_manager_executive_vice_president,
  sku.div_merch_manager_senior_vice_president,
  sku.div_merch_manager_vice_president,
  sku.merch_director,
  sku.buyer,
  sku.merch_planning_executive_vice_president,
  sku.merch_planning_senior_vice_president,
  sku.merch_planning_vice_president,
  sku.merch_planning_director_manager,
  sku.assortment_planner,
  sku.anchor_brands_ind
 FROM merch_clk AS ap
  INNER JOIN locs AS loc ON CAST(ap.loc_idnt AS FLOAT64) = loc.loc_idnt
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.scaled_event_dates AS dt ON ap.day_idnt = dt.day_idnt
  INNER JOIN prod_hier AS sku ON LOWER(ap.sku_idnt) = LOWER(sku.sku_idnt)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_sku_chnl_date AS an ON LOWER(ap.date_event_type) = LOWER('Anniversary')
       AND LOWER(ap.sku_idnt) = LOWER(an.sku_idnt) AND LOWER(an.channel_country) = LOWER('US') AND LOWER(loc.event_selling_channel
       ) = LOWER(an.selling_channel) AND LOWER(loc.event_store_type_code) = LOWER(an.store_type_code) AND ap.day_idnt =
    an.day_idnt
  LEFT JOIN rp ON LOWER(ap.sku_idnt) = LOWER(rp.sku_idnt) AND CAST(ap.loc_idnt AS FLOAT64) = rp.loc_idnt AND ap.day_idnt
     = rp.day_idnt
  LEFT JOIN dropship AS ds ON LOWER(ap.sku_idnt) = LOWER(ds.sku_idnt) AND dt.day_dt = ds.day_dt
  LEFT JOIN dma ON LOWER(ap.dma_code) = LOWER(dma.dma_code)
  LEFT JOIN udig_main AS um ON LOWER(ap.sku_idnt) = LOWER(um.sku_idnt) AND dt.yr_idnt = um.udig_year);


--COLLECT STATISTICS     PRIMARY INDEX ( sku_idnt, day_idnt, location )     ,COLUMN ( day_dt )     ,COLUMN ( date_event_type )         ON {{params.gcp_project_id}}.{{params.environment_schema}}.scaled_event_sku_clk_rms