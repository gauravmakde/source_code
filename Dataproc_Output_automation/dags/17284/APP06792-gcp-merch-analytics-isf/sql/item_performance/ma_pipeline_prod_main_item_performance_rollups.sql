--removing comments as we were getting errors in testing


--Images Table: gets url's for images for the performance dashboard.  is_hero_image means that it's the top image for the
--item, we are filtering to only when that is true to hopefully get the best image for the item (there are multiple angles / types of images and
--we want to try to get the best one to represent the item)



CREATE TEMPORARY TABLE IF NOT EXISTS images
CLUSTER BY rms_style_group_num, color_num
AS
SELECT rms_style_group_num,
 color_num,
 asset_id,
 image_url
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_image_asset_dim
WHERE LOWER(is_hero_image) = LOWER('Y')
 AND CURRENT_DATE('PST8PDT') <= CAST(eff_end_tmstp AS DATE)
QUALIFY (RANK() OVER (PARTITION BY rms_style_group_num, color_num ORDER BY eff_begin_tmstp DESC)) = 1;


--COLLECT STATS PRIMARY INDEX(rms_style_group_num, color_num) ON images


CREATE TEMPORARY TABLE IF NOT EXISTS udig_sku
CLUSTER BY sku_idnt
AS
SELECT sku_idnt,
 banner,
  CASE
  WHEN LOWER(banner) = LOWER('RACK') AND MIN(CASE
      WHEN LOWER(item_group) = LOWER('1, HOL23_N_CYBER_SPECIAL PURCHASE')
      THEN 1
      WHEN LOWER(item_group) = LOWER('2, CYBER 2023_US')
      THEN 2
      WHEN LOWER(item_group) = LOWER('1, HOL23_NR_CYBER_SPECIAL PURCHASE')
      THEN 3
      ELSE NULL
      END) = 3
  THEN '1, HOL23_NR_CYBER_SPECIAL PURCHASE'
  WHEN LOWER(banner) = LOWER('NORDSTROM') AND MIN(CASE
      WHEN LOWER(item_group) = LOWER('1, HOL23_N_CYBER_SPECIAL PURCHASE')
      THEN 1
      WHEN LOWER(item_group) = LOWER('2, CYBER 2023_US')
      THEN 2
      WHEN LOWER(item_group) = LOWER('1, HOL23_NR_CYBER_SPECIAL PURCHASE')
      THEN 3
      ELSE NULL
      END) = 1
  THEN '1, HOL23_N_CYBER_SPECIAL PURCHASE'
  WHEN LOWER(banner) = LOWER('NORDSTROM') AND MIN(CASE
      WHEN LOWER(item_group) = LOWER('1, HOL23_N_CYBER_SPECIAL PURCHASE')
      THEN 1
      WHEN LOWER(item_group) = LOWER('2, CYBER 2023_US')
      THEN 2
      WHEN LOWER(item_group) = LOWER('1, HOL23_NR_CYBER_SPECIAL PURCHASE')
      THEN 3
      ELSE NULL
      END) = 2
  THEN '2, CYBER 2023_US'
  ELSE NULL
  END AS udig
FROM (SELECT DISTINCT sku_idnt,
    CASE
    WHEN CAST(udig_colctn_idnt AS FLOAT64) = 50009
    THEN 'RACK'
    ELSE 'NORDSTROM'
    END AS banner,
     udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_usr_vws.udig_itm_grp_sku_lkup
  WHERE LOWER(udig_colctn_idnt) IN (LOWER('50008'), LOWER('50009'), LOWER('25032'))
   AND LOWER(udig_itm_grp_nm) IN (LOWER('HOL23_NR_CYBER_SPECIAL PURCHASE'), LOWER('HOL23_N_CYBER_SPECIAL PURCHASE'),
     LOWER('CYBER 2023_US'))) AS t2
GROUP BY sku_idnt,
 banner;


--COLLECT STATS 	PRIMARY INDEX (sku_idnt) 	ON udig_sku


CREATE TEMPORARY TABLE IF NOT EXISTS fanatics
CLUSTER BY sku_idnt
AS
SELECT DISTINCT psd.rms_sku_num AS sku_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psd
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim AS payto ON LOWER(psd.prmy_supp_num) = LOWER(payto.order_from_vendor_num
   )
WHERE LOWER(payto.payto_vendor_num) = LOWER('5179609');


--COLLECT STATS      PRIMARY INDEX (sku_idnt)     ON fanatics


CREATE TEMPORARY TABLE IF NOT EXISTS last_receipt
CLUSTER BY sku_idnt, loc_idnt
AS
SELECT rms_sku_num AS sku_idnt,
 store_num AS loc_idnt,
 po_receipt_last_date AS last_receipt_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_receipt_date_sku_store_fact AS r;


--COLLECT STATS      PRIMARY INDEX (sku_idnt, loc_idnt)     ON last_receipt


CREATE TEMPORARY TABLE IF NOT EXISTS loc
CLUSTER BY store_num
AS
SELECT store_num,
 store_name,
 channel_country AS country,
  CASE
  WHEN channel_num IN (110, 120, 130, 140, 111, 121, 310, 311)
  THEN 'FP'
  WHEN channel_num IN (211, 221, 261, 210, 220, 250, 260)
  THEN 'OP'
  ELSE NULL
  END AS banner,
  CASE
  WHEN LOWER(selling_channel) = LOWER('ONLINE')
  THEN 'DIGITAL'
  ELSE selling_channel
  END AS selling_channel,
   SUBSTR(CAST(channel_num AS STRING), 1, 50) || ', ' || SUBSTR(channel_desc, 1, 100) AS channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE channel_num IN (110, 111, 120, 121, 130, 140, 210, 211, 220, 221, 250, 260, 261, 310, 311)
GROUP BY store_num,
 store_name,
 country,
 banner,
 selling_channel,
 channel;


--COLLECT STATS      PRIMARY INDEX (store_num)      ON loc


CREATE TEMPORARY TABLE IF NOT EXISTS receipts_base
CLUSTER BY sku_idnt, channel
AS
SELECT r.sku_idnt,
 r.mnth_idnt,
 r.week_num,
 r.week_start_day_date,
 loc.channel,
 loc.banner,
 loc.country,
 loc.selling_channel,
  CASE
  WHEN LOWER(r.ds_ind) = LOWER('Y')
  THEN 1
  ELSE 0
  END AS ds_ind,
  CASE
  WHEN LOWER(r.rp_ind) = LOWER('Y')
  THEN 1
  ELSE 0
  END AS rp_ind,
 r.price_type,
 MAX(l.last_receipt_date) AS last_receipt_date,
 SUM(r.receipt_tot_units + r.receipt_rsk_units) AS rcpt_tot_units,
 SUM(r.receipt_tot_retail + r.receipt_rsk_retail) AS rcpt_tot_retail,
 SUM(r.receipt_tot_cost + r.receipt_rsk_cost) AS rcpt_tot_cost,
 SUM(r.receipt_ds_units) AS rcpt_ds_units,
 SUM(r.receipt_ds_retail) AS rcpt_ds_retail,
 SUM(r.receipt_ds_cost) AS rcpt_ds_cost,
 SUM(r.receipt_rsk_units) AS rcpt_rsk_units,
 SUM(r.receipt_rsk_retail) AS rcpt_rsk_retail,
 SUM(r.receipt_rsk_cost) AS rcpt_rsk_cost
FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact AS r
 INNER JOIN loc ON r.store_num = loc.store_num
 LEFT JOIN last_receipt AS l ON LOWER(l.sku_idnt) = LOWER(r.sku_idnt) AND r.store_num = l.loc_idnt
WHERE r.week_num >= 202301
GROUP BY r.sku_idnt,
 r.mnth_idnt,
 r.week_num,
 r.week_start_day_date,
 loc.channel,
 loc.banner,
 loc.country,
 loc.selling_channel,
 ds_ind,
 rp_ind,
 r.price_type
UNION ALL
SELECT r0.sku_idnt,
 r0.mnth_idnt,
 r0.week_num,
 r0.week_start_day_date,
 loc0.channel,
 loc0.banner,
 loc0.country,
 loc0.selling_channel,
  CASE
  WHEN r0.receipt_ds_units > 0
  THEN 1
  ELSE 0
  END AS ds_ind,
 r0.rp_ind,
 r0.price_type,
 MAX(l0.last_receipt_date) AS last_receipt_date,
 SUM(r0.receipt_po_units + r0.receipt_ds_units + r0.receipt_rsk_units) AS rcpt_tot_units,
 SUM(r0.receipt_po_retail + r0.receipt_ds_retail + r0.receipt_rsk_retail) AS rcpt_tot_retail,
 0 AS rcpt_tot_cost,
 SUM(r0.receipt_ds_units) AS rcpt_ds_units,
 SUM(r0.receipt_ds_retail) AS rcpt_ds_retail,
 0 AS rcpt_ds_cost,
 SUM(r0.receipt_rsk_units) AS rcpt_rsk_units,
 SUM(r0.receipt_rsk_retail) AS rcpt_rsk_retail,
 0 AS rcpt_rsk_cost
FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm AS r0
 INNER JOIN loc AS loc0 ON r0.store_num = loc0.store_num
 LEFT JOIN last_receipt AS l0 ON LOWER(l0.sku_idnt) = LOWER(r0.sku_idnt) AND r0.store_num = l0.loc_idnt
WHERE r0.week_num < 202301
GROUP BY r0.sku_idnt,
 r0.mnth_idnt,
 r0.week_num,
 r0.week_start_day_date,
 loc0.channel,
 loc0.banner,
 loc0.country,
 loc0.selling_channel,
 ds_ind,
 r0.rp_ind,
 r0.price_type;


CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy
CLUSTER BY channel_country, sku_idnt, style_num
AS
SELECT DISTINCT sku.supp_part_num AS vpn,
 sku.style_group_num,
 sku.rms_sku_num AS sku_idnt,
 sku.rms_style_num AS style_num,
 sku.style_desc,
 sku.color_num,
 sku.supp_color,
 sku.channel_country,
 sku.prmy_supp_num AS supplier_idnt,
 supp.vendor_name AS supp_name,
 sku.brand_name,
 npg.npg_flag,
 CAST(sku.div_num AS INTEGER) AS div_idnt,
 sku.div_desc,
 CAST(sku.grp_num AS INTEGER) AS subdiv_idnt,
 sku.grp_desc AS subdiv_desc,
 CAST(sku.dept_num AS INTEGER) AS dept_idnt,
 sku.dept_desc,
 CAST(sku.class_num AS INTEGER) AS class_idnt,
 sku.sbclass_desc,
 CAST(sku.sbclass_num AS INTEGER) AS sbclass_idnt,
 sku.class_desc,
 i.asset_id,
 i.image_url,
 ccs.quantrix_category,
 ccs.ccs_category,
 ccs.ccs_subcategory,
 ccs.nord_role_desc,
 ccs.rack_role_desc,
 ccs.parent_group,
 ccs.merch_themes,
 ccs.holiday_theme_ty,
 ccs.gift_ind,
 ccs.stocking_stuffer,
 ccs.assortment_grouping
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ccs_categories.ccs_merch_themes AS ccs ON sku.div_num = ccs.div_idnt AND sku.dept_num = ccs.dept_idnt
      AND sku.class_num = ccs.class_idnt AND sku.sbclass_num = ccs.sbclass_idnt
 LEFT JOIN images AS i ON LOWER(sku.style_group_num) = LOWER(i.rms_style_group_num) AND LOWER(sku.color_num) = LOWER(i.color_num
    )
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(sku.prmy_supp_num) = LOWER(supp.vendor_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS npg ON LOWER(npg.vendor_num) = LOWER(sku.prmy_supp_num)
WHERE LOWER(sku.div_desc) NOT LIKE LOWER('INACTIVE%')
QUALIFY (DENSE_RANK() OVER (PARTITION BY sku_idnt, sku.channel_country ORDER BY sku.dw_sys_load_tmstp DESC)) = 1;


--COLLECT STATS      PRIMARY INDEX(channel_country, sku_idnt, style_num)      ON hierarchy


CREATE TEMPORARY TABLE IF NOT EXISTS dates_filter
CLUSTER BY day_date
AS
SELECT day_date,
 week_label,
 week_start_day_date,
 week_end_day_date,
 month_label,
 month_start_day_date,
 month_end_day_date,
 quarter_label,
 week_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 current_month_ind,
 DENSE_RANK() OVER (ORDER BY week_idnt DESC) AS week_rank
FROM (SELECT day_date,
   week_label,
   week_start_day_date,
   week_end_day_date,
   month_label,
   month_start_day_date,
   month_end_day_date,
   quarter_label,
   week_idnt,
   month_idnt,
   quarter_idnt,
   fiscal_year_num,
    CASE
    WHEN month_end_day_date <= CURRENT_DATE('PST8PDT')
    THEN 0
    ELSE 1
    END AS current_month_ind
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tme
  WHERE week_end_day_date <= CURRENT_DATE('PST8PDT')
  GROUP BY day_date,
   week_label,
   week_start_day_date,
   week_end_day_date,
   month_label,
   month_start_day_date,
   month_end_day_date,
   quarter_label,
   week_idnt,
   month_idnt,
   quarter_idnt,
   fiscal_year_num,
   current_month_ind) AS t1
QUALIFY (DENSE_RANK() OVER (ORDER BY week_idnt DESC)) < 14;


--COLLECT STATS 		PRIMARY INDEX(day_date) 		ON dates_filter


CREATE TEMPORARY TABLE weekly_base AS (
    WITH date_filter AS ( SELECT week_label,
   month_label,
   week_idnt,
   month_idnt,
   quarter_idnt,
   fiscal_year_num AS year_idnt,
   week_rank
  FROM dates_filter
  WHERE week_rank < 14
  GROUP BY week_label,
   month_label,
   week_idnt,
   month_idnt,
   quarter_idnt,
   year_idnt,
   week_rank) ,
   
    channel_rollup AS (

    SELECT channel
    , selling_channel 
    FROM loc
    GROUP BY 1,2
  )
   SELECT r.week_num AS week_idnt,
   d.week_label,
   r.mnth_idnt AS month_idnt,
   d.month_label,
   d.quarter_idnt,
   d.year_idnt,
   r.country,
   r.banner,
   r.channel,
   r.selling_channel,
   r.price_type,
   r.sku_idnt,
   s.customer_choice,
   0 AS days_live,
   NULL AS days_published,
   MAX(r.rp_ind) AS rp_ind,
   MAX(r.ds_ind) AS ds_ind,
   0 AS net_sales_units,
   0 AS net_sales_dollars,
   0 AS return_units,
   0 AS return_dollars,
   0 AS demand_units,
   0 AS demand_dollars,
   0 AS demand_cancel_units,
   0 AS demand_cancel_dollars,
   0 AS demand_dropship_units,
   0 AS demand_dropship_dollars,
   0 AS shipped_units,
   0 AS shipped_dollars,
   0 AS store_fulfill_units,
   0 AS store_fulfill_dollars,
   0 AS boh_units,
   0 AS boh_dollars,
   0 AS boh_cost,
   0 AS eoh_units,
   0 AS eoh_dollars,
   0 AS eoh_cost,
   0 AS nonsellable_units,
   0 AS asoh_units,
   SUM(COALESCE(r.rcpt_tot_units, 0)) AS receipt_units,
   SUM(COALESCE(r.rcpt_tot_retail, 0)) AS receipt_dollars,
   SUM(COALESCE(r.rcpt_tot_cost, 0)) AS receipt_cost,
   SUM(COALESCE(r.rcpt_rsk_units, 0)) AS reserve_stock_units,
   SUM(COALESCE(r.rcpt_rsk_retail, 0)) AS reserve_stock_dollars,
   SUM(COALESCE(r.rcpt_rsk_cost, 0)) AS reserve_stock_cost,
   SUM(COALESCE(r.rcpt_ds_units, 0)) AS receipt_dropship_units,
   SUM(COALESCE(r.rcpt_ds_retail, 0)) AS receipt_dropship_dollars,
   SUM(COALESCE(r.rcpt_ds_cost, 0)) AS receipt_dropship_cost,
   MAX(r.last_receipt_date) AS last_receipt_date,
   0 AS oo_units,
   0 AS oo_dollars,
   0 AS backorder_units,
   0 AS backorder_dollars,
   0 AS instock_traffic,
   0 AS traffic,
   0 AS add_qty,
   0 AS product_views,
   0 AS instock_views,
   0 AS order_demand,
   0 AS order_quantity,
   0 AS cost_of_goods_sold,
   0 AS product_margin,
   0 AS sales_pm
  FROM receipts_base AS r
   INNER JOIN date_filter AS d ON r.week_num = d.week_idnt
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS s ON LOWER(r.sku_idnt) = LOWER(s.sku_idnt) AND LOWER(r.country) =
     LOWER(s.channel_country)
  GROUP BY week_idnt,
   d.week_label,
   month_idnt,
   d.month_label,
   d.quarter_idnt,
   d.year_idnt,
   r.country,
   r.banner,
   r.channel,
   r.selling_channel,
   r.price_type,
   r.sku_idnt,
   s.customer_choice
UNION ALL 

SELECT f.week_idnt,
   d.week_label,
   f.month_idnt,
   d.month_label,
   f.quarter_idnt,
   f.year_idnt,
   f.country,
   f.banner,
   f.channel,
   c.selling_channel,
   f.price_type,
   f.sku_idnt,
   s.customer_choice,
   SUM(f.days_live) AS days_live,
   MIN(f.days_published) AS days_published,
   MAX(f.rp_flag) AS rp_flag,
   MAX(f.dropship_flag) AS dropship_flag,
   SUM(COALESCE(f.net_sales_units, 0)) AS net_sales_units,
   SUM(COALESCE(f.net_sales_dollars, 0)) AS net_sales_dollars,
   SUM(COALESCE(f.return_units, 0)) AS return_units,
   SUM(COALESCE(f.return_dollars, 0)) AS return_dollars,
   SUM(COALESCE(f.demand_units, 0)) AS demand_units,
   SUM(COALESCE(f.demand_dollars, 0)) AS demand_dollars,
   SUM(COALESCE(f.demand_cancel_units, 0)) AS demand_cancel_units,
   SUM(COALESCE(f.demand_cancel_dollars, 0)) AS demand_cancel_dollars,
   SUM(COALESCE(f.demand_dropship_units, 0)) AS demand_dropship_units,
   SUM(COALESCE(f.demand_dropship_dollars, 0)) AS demand_dropship_dollars,
   SUM(COALESCE(f.shipped_units, 0)) AS shipped_units,
   SUM(COALESCE(f.shipped_dollars, 0)) AS shipped_dollars,
   SUM(COALESCE(f.store_fulfill_units, 0)) AS store_fulfill_units,
   SUM(COALESCE(f.store_fulfill_dollars, 0)) AS store_fulfill_dollars,
   SUM(CASE
     WHEN f.day_date = d.week_start_day_date
     THEN f.boh_units
     ELSE 0
     END) AS boh_units,
   SUM(CASE
     WHEN f.day_date = d.week_start_day_date
     THEN f.boh_dollars
     ELSE 0
     END) AS boh_dollars,
   SUM(CASE
     WHEN f.day_date = d.week_start_day_date
     THEN f.boh_cost
     ELSE 0
     END) AS boh_cost,
   SUM(CASE
     WHEN f.day_date = d.week_end_day_date
     THEN f.eoh_units
     ELSE 0
     END) AS eoh_units,
   SUM(CASE
     WHEN f.day_date = d.week_end_day_date
     THEN f.eoh_dollars
     ELSE 0
     END) AS eoh_dollars,
   SUM(CASE
     WHEN f.day_date = d.week_end_day_date
     THEN f.eoh_cost
     ELSE 0
     END) AS eoh_cost,
   SUM(CASE
     WHEN f.day_date = d.week_end_day_date
     THEN f.nonsellable_units
     ELSE 0
     END) AS nonsellable_units,
   SUM(CASE
     WHEN f.day_date = d.week_end_day_date
     THEN f.asoh_units
     ELSE 0
     END) AS asoh_units,
   0 AS receipt_units,
   0 AS receipt_dollars,
   0 AS receipt_cost,
   0 AS reserve_stock_units,
   0 AS reserve_stock_dollars,
   0 AS reserve_stock_cost,
   0 AS receipt_dropship_units,
   0 AS receipt_dropship_dollars,
   0 AS receipt_dropship_cost,
   MAX(f.last_receipt_date) AS last_receipt_date,
   SUM(COALESCE(f.oo_units, 0)) AS oo_units,
   SUM(COALESCE(f.oo_dollars, 0)) AS oo_dollars,
   SUM(COALESCE(f.backorder_units, 0)) AS backorder_units,
   SUM(COALESCE(f.backorder_dollars, 0)) AS backorder_dollars,
   SUM(COALESCE(f.instock_traffic, 0)) AS instock_traffic,
   SUM(COALESCE(f.traffic, 0)) AS traffic,
   SUM(COALESCE(f.add_qty, 0)) AS add_qty,
   SUM(COALESCE(f.product_views, 0)) AS product_views,
   SUM(COALESCE(f.instock_views, 0)) AS instock_views,
   SUM(COALESCE(f.order_demand, 0)) AS order_demand,
   SUM(COALESCE(f.order_quantity, 0)) AS order_quantity,
   SUM(COALESCE(f.cost_of_goods_sold, 0)) AS cost_of_goods_sold,
   SUM(COALESCE(f.product_margin, 0)) AS product_margin,
   SUM(COALESCE(f.sales_pm, 0)) AS sales_pm
  FROM `{{params.gcp_project_id}}`.t2dl_das_selection.item_performance_daily_base AS f
   INNER JOIN dates_filter AS d ON f.day_date = d.day_date
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS s ON LOWER(f.sku_idnt) = LOWER(s.sku_idnt) AND LOWER(f.country) =
     LOWER(s.channel_country)
   INNER JOIN channel_rollup AS c ON LOWER(f.channel) = LOWER(c.channel)
  WHERE d.week_rank < 14
  GROUP BY f.week_idnt,
   d.week_label,
   f.month_idnt,
   d.month_label,
   f.quarter_idnt,
   f.year_idnt,
   f.country,
   f.banner,
   f.channel,
   c.selling_channel,
   f.price_type,
   f.sku_idnt,
   s.customer_choice);




TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_performance_weekly{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_performance_weekly{{params.env_suffix}} --{env_suffix}
SELECT f.week_idnt,
 f.week_label,
 f.month_idnt,
 f.month_label,
 f.quarter_idnt,
 f.year_idnt,
 f.country,
 f.banner,
 f.channel,
 f.price_type,
 hierarchy.vpn,
 hierarchy.style_group_num,
 hierarchy.style_num,
 hierarchy.style_desc,
 CAST(TRUNC(CAST(hierarchy.color_num AS FLOAT64))AS INT64) AS color_num,
 hierarchy.supp_color,
 hierarchy.asset_id,
 hierarchy.image_url,
 CAST(TRUNC(CAST(hierarchy.supplier_idnt AS FLOAT64)) AS INT64) AS supplier_idnt,
 hierarchy.supp_name AS supplier_name,
 hierarchy.brand_name,
 hierarchy.div_idnt,
 hierarchy.div_desc,
 hierarchy.subdiv_idnt,
 hierarchy.subdiv_desc,
 hierarchy.dept_idnt,
 hierarchy.dept_desc,
 hierarchy.class_idnt,
 hierarchy.class_desc,
 hierarchy.sbclass_idnt,
 hierarchy.sbclass_desc,
 hierarchy.quantrix_category,
 hierarchy.ccs_category,
 hierarchy.ccs_subcategory,
 hierarchy.nord_role_desc,
 hierarchy.rack_role_desc,
 hierarchy.parent_group,
 hierarchy.merch_themes,
 hierarchy.holiday_theme_ty,
 hierarchy.gift_ind,
 hierarchy.stocking_stuffer,
 hierarchy.assortment_grouping,
 u.udig,
  CASE
  WHEN fan.sku_idnt IS NOT NULL
  THEN 1
  ELSE 0
  END AS fanatics_flag,
  CASE
  WHEN LOWER(cc.cc_type) = LOWER('NEW')
  THEN 1
  ELSE 0
  END AS new_flag,
  CASE
  WHEN LOWER(cc.cc_type) = LOWER('CF')
  THEN 1
  ELSE 0
  END AS cf_flag,
 MAX(f.days_live) AS days_live,
 MIN(f.days_published) AS days_published,
 MAX(hierarchy.npg_flag) AS npg_flag,
 MAX(f.rp_ind) AS rp_flag,
 MAX(f.ds_ind) AS dropship_flag,
 item_stg.intended_season,
 item_stg.intended_exit_month_year,
 item_stg.intended_lifecycle_type,
 item_stg.scaled_event,
 item_stg.holiday_or_celebration,
 SUM(COALESCE(f.net_sales_units,0)) AS net_sales_units,
 SUM(COALESCE(f.net_sales_dollars,0)) AS net_sales_dollars,
 SUM(COALESCE(f.return_units,0)) AS return_units,
 SUM(COALESCE(f.return_dollars,0)) AS return_dollars,
 SUM(COALESCE(f.demand_units,0)) AS demand_units,
 SUM(COALESCE(f.demand_dollars,0)) AS demand_dollars,
 SUM(COALESCE(f.demand_cancel_units,0)) AS demand_cancel_units,
 SUM(COALESCE(f.demand_cancel_dollars,0)) AS demand_cancel_dollars,
 SUM(COALESCE(f.demand_dropship_units,0)) AS demand_dropship_units,
 SUM(COALESCE(f.demand_dropship_dollars,0)) AS demand_dropship_dollars,
 SUM(COALESCE(f.shipped_units,0)) AS shipped_units,
 SUM(COALESCE(f.shipped_dollars,0)) AS shipped_dollars,
 SUM(COALESCE(f.store_fulfill_units,0)) AS store_fulfill_units,
 SUM(COALESCE(f.store_fulfill_dollars,0)) AS store_fulfill_dollars,
 SUM(COALESCE(f.boh_units,0)) AS boh_units,
 SUM(COALESCE(f.boh_dollars,0)) AS boh_dollars,
 SUM(COALESCE(f.boh_cost,0)) AS boh_cost,
 SUM(COALESCE(f.eoh_units,0)) AS eoh_units,
 SUM(COALESCE(f.eoh_dollars,0)) AS eoh_dollars,
 SUM(COALESCE(f.eoh_cost,0)) AS eoh_cost,
 SUM(COALESCE(f.nonsellable_units,0)) AS nonsellable_units,
 SUM(COALESCE(f.asoh_units,0)) AS asoh_units,
 SUM(COALESCE(f.receipt_units,0)) AS receipt_units,
 SUM(COALESCE(f.receipt_dollars,0)) AS receipt_dollars,
 SUM(COALESCE(f.receipt_cost,0)) AS receipt_cost,
 SUM(COALESCE(f.reserve_stock_units,0)) AS reserve_stock_units,
 SUM(COALESCE(f.reserve_stock_dollars,0)) AS reserve_stock_dollars,
 SUM(COALESCE(f.reserve_stock_cost,0)) AS reserve_stock_cost,
 SUM(COALESCE(f.receipt_dropship_units,0)) AS receipt_dropship_units,
 SUM(COALESCE(f.receipt_dropship_dollars,0)) AS receipt_dropship_dollars,
 SUM(COALESCE(f.receipt_dropship_cost,0)) AS receipt_dropship_cost,
 MAX(f.last_receipt_date) AS last_receipt_date,
 SUM(COALESCE(f.oo_units,0)) AS oo_units,
 SUM(COALESCE(f.oo_dollars,0)) AS oo_dollars,
 SUM(COALESCE(f.backorder_units,0)) AS backorder_units,
 SUM(COALESCE(f.backorder_dollars,0)) AS backorder_dollars,
 SUM(COALESCE(CAST(f.instock_traffic AS NUMERIC),0)) AS instock_traffic,
 SUM(COALESCE(CAST(f.traffic AS NUMERIC),0)) AS traffic,
 SUM(COALESCE(f.add_qty,0)) AS add_qty,
 SUM(COALESCE(f.product_views,0)) AS product_views,
 SUM(COALESCE(f.instock_views,0)) AS instock_views,
 SUM(COALESCE(f.order_demand,0)) AS order_demand,
 SUM(COALESCE(f.order_quantity,0)) AS order_quantity,
 SUM(COALESCE(f.cost_of_goods_sold,0)) AS cost_of_goods_sold,
 SUM(COALESCE(f.product_margin,0)) AS product_margin,
 SUM(COALESCE(f.sales_pm,0)) AS sales_pm
FROM weekly_base AS f
 INNER JOIN hierarchy ON LOWER(f.sku_idnt) = LOWER(hierarchy.sku_idnt) AND LOWER(f.country) = LOWER(hierarchy.channel_country
    )
 LEFT JOIN udig_sku AS u ON LOWER(u.sku_idnt) = LOWER(f.sku_idnt) AND LOWER(CASE
     WHEN LOWER(u.banner) = LOWER('RACK')
     THEN 'OP'
     WHEN LOWER(u.banner) = LOWER('NORDSTROM')
     THEN 'FP'
     ELSE NULL
     END) = LOWER(f.banner)
 LEFT JOIN fanatics AS fan ON LOWER(f.sku_idnt) = LOWER(fan.sku_idnt)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.cc_type AS cc ON LOWER(cc.customer_choice) = LOWER(f.customer_choice) AND LOWER(cc.channel_country
       ) = LOWER(f.country) AND LOWER(cc.channel) = LOWER(f.selling_channel) AND LOWER(CASE
      WHEN LOWER(cc.channel_brand) = LOWER('NORDSTROM')
      THEN 'FP'
      WHEN LOWER(cc.channel_brand) = LOWER('NORDSTROM_RACK')
      THEN 'OP'
      ELSE NULL
      END) = LOWER(f.banner) AND f.month_idnt = cc.mnth_idnt
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.item_intent_lookup AS item_stg ON LOWER(item_stg.rms_style_num) =
    LOWER(hierarchy.style_num) AND LOWER(item_stg.sku_nrf_color_num) = LOWER(hierarchy.color_num) AND item_stg.channel_num
    = CAST(SUBSTR(f.channel, 0, 3) AS FLOAT64)
GROUP BY f.week_idnt,
 f.week_label,
 f.month_idnt,
 f.month_label,
 f.quarter_idnt,
 f.year_idnt,
 f.country,
 f.banner,
 f.channel,
 f.price_type,
 hierarchy.vpn,
 hierarchy.style_group_num,
 hierarchy.style_num,
 hierarchy.style_desc,
 cast(TRUNC(CAST(hierarchy.color_num AS FLOAT64)) as int64),
 hierarchy.supp_color,
 hierarchy.asset_id,
 hierarchy.image_url,
 CAST(TRUNC(CAST(hierarchy.supplier_idnt AS FLOAT64)) AS INT64),
 supplier_name,
 hierarchy.brand_name,
 hierarchy.div_idnt,
 hierarchy.div_desc,
 hierarchy.subdiv_idnt,
 hierarchy.subdiv_desc,
 hierarchy.dept_idnt,
 hierarchy.dept_desc,
 hierarchy.class_idnt,
 hierarchy.class_desc,
 hierarchy.sbclass_idnt,
 hierarchy.sbclass_desc,
 hierarchy.quantrix_category,
 hierarchy.ccs_category,
 hierarchy.ccs_subcategory,
 hierarchy.nord_role_desc,
 hierarchy.rack_role_desc,
 hierarchy.parent_group,
 hierarchy.merch_themes,
 hierarchy.holiday_theme_ty,
 hierarchy.gift_ind,
 hierarchy.stocking_stuffer,
 hierarchy.assortment_grouping,
 u.udig,
 fanatics_flag,
 new_flag,
 cf_flag,
 item_stg.intended_season,
 item_stg.intended_exit_month_year,
 item_stg.intended_lifecycle_type,
 item_stg.scaled_event,
 item_stg.holiday_or_celebration;



CREATE TEMPORARY TABLE monthly_base AS SELECT f.month_idnt,
 d.month_label,
 f.quarter_idnt,
 f.year_idnt,
 f.country,
 f.banner,
 f.channel,
 t1.selling_channel,
 f.price_type,
 f.fanatics_flag,
 f.sku_idnt,
 f.udig,
 f.days_live,
 f.days_published,
 f.rp_flag,
 f.dropship_flag,
 COALESCE(f.net_sales_units, 0) AS net_sales_units,
 COALESCE(f.net_sales_dollars, 0) AS net_sales_dollars,
 COALESCE(f.return_units, 0) AS return_units,
 COALESCE(f.return_dollars, 0) AS return_dollars,
 COALESCE(f.demand_units, 0) AS demand_units,
 COALESCE(f.demand_dollars, 0) AS demand_dollars,
 COALESCE(f.demand_cancel_units, 0) AS demand_cancel_units,
 COALESCE(f.demand_cancel_dollars, 0) AS demand_cancel_dollars,
 COALESCE(f.demand_dropship_units, 0) AS demand_dropship_units,
 COALESCE(f.demand_dropship_dollars, 0) AS demand_dropship_dollars,
 COALESCE(f.shipped_units, 0) AS shipped_units,
 COALESCE(f.shipped_dollars, 0) AS shipped_dollars,
 COALESCE(f.store_fulfill_units, 0) AS store_fulfill_units,
 COALESCE(f.store_fulfill_dollars, 0) AS store_fulfill_dollars,
  CASE
  WHEN f.day_date = d.month_start_day_date
  THEN f.boh_units
  ELSE 0
  END AS boh_units,
  CASE
  WHEN f.day_date = d.month_start_day_date
  THEN f.boh_dollars
  ELSE 0
  END AS boh_dollars,
  CASE
  WHEN f.day_date = d.month_start_day_date
  THEN f.boh_cost
  ELSE 0
  END AS boh_cost,
  CASE
  WHEN f.day_date = d.month_end_day_date
  THEN f.eoh_units
  ELSE 0
  END AS eoh_units,
  CASE
  WHEN f.day_date = d.month_end_day_date
  THEN f.eoh_dollars
  ELSE 0
  END AS eoh_dollars,
  CASE
  WHEN f.day_date = d.month_end_day_date
  THEN f.eoh_cost
  ELSE 0
  END AS eoh_cost,
  CASE
  WHEN f.day_date = d.month_end_day_date
  THEN f.nonsellable_units
  ELSE 0
  END AS nonsellable_units,
  CASE
  WHEN f.day_date = d.month_end_day_date
  THEN f.asoh_units
  ELSE 0
  END AS asoh_units,
 f.last_receipt_date,
 COALESCE(f.oo_units, 0) AS oo_units,
 COALESCE(f.oo_dollars, 0) AS oo_dollars,
 COALESCE(f.instock_traffic, 0) AS instock_traffic,
 COALESCE(f.traffic, 0) AS traffic,
 COALESCE(f.backorder_units, 0) AS backorder_units,
 COALESCE(f.backorder_dollars, 0) AS backorder_dollars,
 COALESCE(f.add_qty, 0) AS add_qty,
 COALESCE(f.product_views, 0) AS product_views,
 COALESCE(f.instock_views, 0) AS instock_views,
 COALESCE(f.order_demand, 0) AS order_demand,
 COALESCE(f.order_quantity, 0) AS order_quantity,
 COALESCE(f.cost_of_goods_sold, 0) AS cost_of_goods_sold,
 COALESCE(f.product_margin, 0) AS product_margin,
 COALESCE(f.sales_pm, 0) AS sales_pm
FROM `{{params.gcp_project_id}}`.t2dl_das_selection.item_performance_daily_base AS f
 INNER JOIN dates_filter AS d ON f.day_date = d.day_date
 INNER JOIN (SELECT channel,
   selling_channel
  FROM loc
  GROUP BY selling_channel,
   channel) AS t1 ON LOWER(f.channel) = LOWER(t1.channel) AND d.current_month_ind = 0;


--COLLECT STATS PRIMARY INDEX(sku_idnt, channel, country, month_idnt ) ON monthly_base



CREATE TEMPORARY TABLE monthly_base_receipts AS (
 WITH date_filter AS (SELECT month_label,
   month_idnt,
   quarter_idnt,
   fiscal_year_num AS year_idnt
  FROM dates_filter
  WHERE current_month_ind = 0
  GROUP BY month_label,
   month_idnt,
   quarter_idnt,
   year_idnt)
   
    (SELECT r.mnth_idnt AS month_idnt,
   d.month_label,
   d.quarter_idnt,
   d.year_idnt,
   r.country,
   r.banner,
   r.channel,
   r.selling_channel,
   r.price_type,
   r.sku_idnt,
   s.customer_choice,
   0 AS days_live,
   NULL AS days_published,
   MAX(r.rp_ind) AS rp_ind,
   MAX(r.ds_ind) AS ds_ind,
   0 AS net_sales_units,
   0 AS net_sales_dollars,
   0 AS return_units,
   0 AS return_dollars,
   0 AS demand_units,
   0 AS demand_dollars,
   0 AS demand_cancel_units,
   0 AS demand_cancel_dollars,
   0 AS demand_dropship_units,
   0 AS demand_dropship_dollars,
   0 AS shipped_units,
   0 AS shipped_dollars,
   0 AS store_fulfill_units,
   0 AS store_fulfill_dollars,
   0 AS boh_units,
   0 AS boh_dollars,
   0 AS boh_cost,
   0 AS eoh_units,
   0 AS eoh_dollars,
   0 AS eoh_cost,
   0 AS nonsellable_units,
   0 AS asoh_units,
   SUM(COALESCE(r.rcpt_tot_units, 0)) AS receipt_units,
   SUM(COALESCE(r.rcpt_tot_retail, 0)) AS receipt_dollars,
   SUM(COALESCE(r.rcpt_tot_cost, 0)) AS receipt_cost,
   SUM(COALESCE(r.rcpt_rsk_units, 0)) AS reserve_stock_units,
   SUM(COALESCE(r.rcpt_rsk_retail, 0)) AS reserve_stock_dollars,
   SUM(COALESCE(r.rcpt_rsk_cost, 0)) AS reserve_stock_cost,
   SUM(COALESCE(r.rcpt_ds_units, 0)) AS receipt_dropship_units,
   SUM(COALESCE(r.rcpt_ds_retail, 0)) AS receipt_dropship_dollars,
   SUM(COALESCE(r.rcpt_ds_cost, 0)) AS receipt_dropship_cost,
   MAX(r.last_receipt_date) AS last_receipt_date,
   0 AS oo_units,
   0 AS oo_dollars,
   0 AS backorder_units,
   0 AS backorder_dollars,
   0 AS instock_traffic,
   0 AS traffic,
   0 AS add_qty,
   0 AS product_views,
   0 AS instock_views,
   0 AS order_demand,
   0 AS order_quantity,
   0 AS cost_of_goods_sold,
   0 AS product_margin,
   0 AS sales_pm
  FROM receipts_base AS r
   INNER JOIN date_filter AS d ON r.mnth_idnt = d.month_idnt
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS s ON LOWER(r.sku_idnt) = LOWER(s.sku_idnt) AND LOWER(r.country) =
     LOWER(s.channel_country)
  GROUP BY month_idnt,
   d.month_label,
   d.quarter_idnt,
   d.year_idnt,
   r.country,
   r.banner,
   r.channel,
   r.selling_channel,
   r.price_type,
   r.sku_idnt,
   s.customer_choice)
UNION ALL 

SELECT f.month_idnt,
 f.month_label,
 f.quarter_idnt,
 f.year_idnt,
 f.country,
 f.banner,
 f.channel,
 f.selling_channel,
 f.price_type,
 f.sku_idnt,
 s.customer_choice,
 SUM(f.days_live) AS days_live,
 MIN(f.days_published) AS days_published,
 MAX(f.rp_flag) AS rp_flag,
 MAX(f.dropship_flag) AS dropship_flag,
 SUM(COALESCE(f.net_sales_units,0)) AS net_sales_units,
 SUM(COALESCE(f.net_sales_dollars,0)) AS net_sales_dollars,
 SUM(COALESCE(f.return_units,0)) AS return_units,
 SUM(COALESCE(f.return_dollars,0)) AS return_dollars,
 SUM(COALESCE(f.demand_units,0)) AS demand_units,
 SUM(COALESCE(f.demand_dollars,0)) AS demand_dollars,
 SUM(COALESCE(f.demand_cancel_units,0)) AS demand_cancel_units,
 SUM(COALESCE(f.demand_cancel_dollars,0)) AS demand_cancel_dollars,
 SUM(COALESCE(f.demand_dropship_units,0)) AS demand_dropship_units,
 SUM(COALESCE(f.demand_dropship_dollars,0)) AS demand_dropship_dollars,
 SUM(COALESCE(f.shipped_units,0)) AS shipped_units,
 SUM(COALESCE(f.shipped_dollars,0)) AS shipped_dollars,
 SUM(COALESCE(f.store_fulfill_units,0)) AS store_fulfill_units,
 SUM(COALESCE(f.store_fulfill_dollars,0)) AS store_fulfill_dollars,
 SUM(f.boh_units) AS boh_units,
 SUM(f.boh_dollars) AS boh_dollars,
 SUM(f.boh_cost) AS boh_cost,
 SUM(f.eoh_units) AS eoh_units,
 SUM(f.eoh_dollars) AS eoh_dollars,
 SUM(f.eoh_cost) AS eoh_cost,
 SUM(f.nonsellable_units) AS nonsellable_units,
 SUM(f.asoh_units) AS asoh_units,
 0 AS receipt_units,
 0 AS receipt_dollars,
 0 AS receipt_cost,
 0 AS reserve_stock_units,
 0 AS reserve_stock_dollars,
 0 AS reserve_stock_cost,
 0 AS receipt_dropship_units,
 0 AS receipt_dropship_dollars,
 0 AS receipt_dropship_cost,
 MAX(f.last_receipt_date) AS last_receipt_date,
 SUM(COALESCE(f.oo_units,0)) AS oo_units,
 SUM(COALESCE(f.oo_dollars,0)) AS oo_dollars,
 SUM(COALESCE(f.backorder_units,0)) AS backorder_units,
 SUM(COALESCE(f.backorder_dollars,0)) AS backorder_dollars,
 SUM(COALESCE(f.instock_traffic,0)) AS instock_traffic,
 SUM(COALESCE(f.traffic,0)) AS traffic,
 SUM(COALESCE(f.add_qty,0)) AS add_qty,
 SUM(COALESCE(f.product_views,0)) AS product_views,
 SUM(COALESCE(f.instock_views,0)) AS instock_views,
 SUM(COALESCE(f.order_demand,0)) AS order_demand,
 SUM(COALESCE(f.order_quantity,0)) AS order_quantity,
 SUM(COALESCE(f.cost_of_goods_sold,0)) AS cost_of_goods_sold,
 SUM(COALESCE(f.product_margin,0)) AS product_margin,
 SUM(COALESCE(f.sales_pm,0)) AS sales_pm
FROM monthly_base AS f
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS s ON LOWER(f.sku_idnt) = LOWER(s.sku_idnt) AND LOWER(f.country) =
   LOWER(s.channel_country)
GROUP BY f.month_idnt,
 f.month_label,
 f.quarter_idnt,
 f.year_idnt,
 f.country,
 f.banner,
 f.channel,
 f.selling_channel,
 f.price_type,
 f.sku_idnt,
 s.customer_choice

);





TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_performance_monthly{{params.env_suffix}};




INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_performance_monthly{{params.env_suffix}} --{env_suffix}
SELECT f.month_idnt,
 f.month_label,
 f.quarter_idnt,
 f.year_idnt,
 f.country,
 f.banner,
 f.channel,
 f.price_type,
 hierarchy.vpn,
 hierarchy.style_group_num,
 hierarchy.style_num,
 hierarchy.style_desc,
 CAST(TRUNC(CAST(hierarchy.color_num AS FLOAT64)) AS INT64) AS color_num,
 hierarchy.supp_color,
 hierarchy.asset_id,
 hierarchy.image_url,
 CAST(TRUNC(CAST(hierarchy.supplier_idnt AS FLOAT64)) AS INT64) AS supplier_idnt,
 hierarchy.supp_name AS supplier_name,
 hierarchy.brand_name,
 hierarchy.div_idnt,
 hierarchy.div_desc,
 hierarchy.subdiv_idnt,
 hierarchy.subdiv_desc,
 hierarchy.dept_idnt,
 hierarchy.dept_desc,
 hierarchy.class_idnt,
 hierarchy.class_desc,
 hierarchy.sbclass_idnt,
 hierarchy.sbclass_desc,
 hierarchy.quantrix_category,
 hierarchy.ccs_category,
 hierarchy.ccs_subcategory,
 hierarchy.nord_role_desc,
 hierarchy.rack_role_desc,
 hierarchy.parent_group,
 hierarchy.merch_themes,
 hierarchy.holiday_theme_ty,
 hierarchy.gift_ind,
 hierarchy.stocking_stuffer,
 hierarchy.assortment_grouping,
 u.udig,
  CASE
  WHEN fan.sku_idnt IS NOT NULL
  THEN 1
  ELSE 0
  END AS fanatics_flag,
  CASE
  WHEN LOWER(cc.cc_type) = LOWER('NEW')
  THEN 1
  ELSE 0
  END AS new_flag,
  CASE
  WHEN LOWER(cc.cc_type) = LOWER('CF')
  THEN 1
  ELSE 0
  END AS cf_flag,
 MAX(f.days_live) AS days_live,
 MIN(f.days_published) AS days_published,
 MAX(hierarchy.npg_flag) AS npg_flag,
 MAX(f.rp_ind) AS rp_flag,
 MAX(f.ds_ind) AS dropship_flag,
 SUM(COALESCE(f.net_sales_units,0)) AS net_sales_units,
 SUM(COALESCE(f.net_sales_dollars,0)) AS net_sales_dollars,
 SUM(COALESCE(f.return_units,0)) AS return_units,
 SUM(COALESCE(f.return_dollars,0)) AS return_dollars,
 SUM(COALESCE(f.demand_units,0)) AS demand_units,
 SUM(COALESCE(f.demand_dollars,0)) AS demand_dollars,
 SUM(COALESCE(f.demand_cancel_units,0)) AS demand_cancel_units,
 SUM(COALESCE(f.demand_cancel_dollars,0)) AS demand_cancel_dollars,
 SUM(COALESCE(f.demand_dropship_units,0)) AS demand_dropship_units,
 SUM(COALESCE(f.demand_dropship_dollars,0)) AS demand_dropship_dollars,
 SUM(COALESCE(f.shipped_units,0)) AS shipped_units,
 SUM(COALESCE(f.shipped_dollars,0)) AS shipped_dollars,
 SUM(COALESCE(f.store_fulfill_units,0)) AS store_fulfill_units,
 SUM(COALESCE(f.store_fulfill_dollars,0)) AS store_fulfill_dollars,
 SUM(COALESCE(f.boh_units,0)) AS boh_units,
 SUM(COALESCE(f.boh_dollars,0)) AS boh_dollars,
 SUM(COALESCE(f.boh_cost,0)) AS boh_cost,
 SUM(COALESCE(f.eoh_units,0)) AS eoh_units,
 SUM(COALESCE(f.eoh_dollars,0)) AS eoh_dollars,
 SUM(COALESCE(f.eoh_cost,0)) AS eoh_cost,
 SUM(COALESCE(f.nonsellable_units,0)) AS nonsellable_units,
 SUM(COALESCE(f.asoh_units,0)) AS asoh_units,
 SUM(COALESCE(f.receipt_units,0)) AS receipt_units,
 SUM(COALESCE(f.receipt_dollars,0)) AS receipt_dollars,
 SUM(COALESCE(f.receipt_cost,0)) AS receipt_cost,
 SUM(COALESCE(f.reserve_stock_units,0)) AS reserve_stock_units,
 SUM(COALESCE(f.reserve_stock_dollars,0)) AS reserve_stock_dollars,
 SUM(COALESCE(f.reserve_stock_cost,0)) AS reserve_stock_cost,
 SUM(COALESCE(f.receipt_dropship_units,0)) AS receipt_dropship_units,
 SUM(COALESCE(f.receipt_dropship_dollars,0)) AS receipt_dropship_dollars,
 SUM(COALESCE(f.receipt_dropship_cost,0)) AS receipt_dropship_cost,
 MAX(f.last_receipt_date) AS last_receipt_date,
 SUM(COALESCE(f.oo_units,0)) AS oo_units,
 SUM(COALESCE(f.oo_dollars,0)) AS oo_dollars,
 SUM(COALESCE(f.backorder_units,0)) AS backorder_units,
 SUM(COALESCE(f.backorder_dollars,0)) AS backorder_dollars,
 SUM(COALESCE(CAST(f.instock_traffic AS NUMERIC),0)) AS instock_traffic,
 SUM(COALESCE(CAST(f.traffic AS NUMERIC),0)) AS traffic,
 SUM(COALESCE(f.add_qty,0)) AS add_qty,
 SUM(COALESCE(f.product_views,0)) AS product_views,
 SUM(COALESCE(f.instock_views,0)) AS instock_views,
 SUM(COALESCE(f.order_demand,0)) AS order_demand,
 SUM(COALESCE(f.order_quantity,0)) AS order_quantity,
 SUM(COALESCE(f.cost_of_goods_sold,0)) AS cost_of_goods_sold,
 SUM(COALESCE(f.product_margin,0)) AS product_margin,
 SUM(COALESCE(f.sales_pm,0)) AS sales_pm
FROM monthly_base_receipts AS f
 INNER JOIN hierarchy ON LOWER(f.sku_idnt) = LOWER(hierarchy.sku_idnt) AND LOWER(f.country) = LOWER(hierarchy.channel_country
    )
 LEFT JOIN udig_sku AS u ON LOWER(f.sku_idnt) = LOWER(u.sku_idnt)
 LEFT JOIN fanatics AS fan ON LOWER(f.sku_idnt) = LOWER(fan.sku_idnt)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.cc_type AS cc ON LOWER(cc.customer_choice) = LOWER(f.customer_choice) AND LOWER(cc.channel
       ) = LOWER(f.selling_channel) AND LOWER(cc.channel_country) = LOWER(f.country) AND LOWER(CASE
      WHEN LOWER(cc.channel_brand) = LOWER('NORDSTROM')
      THEN 'FP'
      WHEN LOWER(cc.channel_brand) = LOWER('NORDSTROM_RACK')
      THEN 'OP'
      ELSE NULL
      END) = LOWER(f.banner) AND f.month_idnt = cc.mnth_idnt
GROUP BY f.month_idnt,
 f.month_label,
 f.quarter_idnt,
 f.year_idnt,
 f.country,
 f.banner,
 f.channel,
 f.price_type,
 hierarchy.vpn,
 hierarchy.style_group_num,
 hierarchy.style_num,
 hierarchy.style_desc,
 hierarchy.color_num,
 hierarchy.supp_color,
 hierarchy.asset_id,
 hierarchy.image_url,
 CAST(TRUNC(CAST(hierarchy.supplier_idnt AS FLOAT64)) AS INT64),
 supplier_name,
 hierarchy.brand_name,
 hierarchy.div_idnt,
 hierarchy.div_desc,
 hierarchy.subdiv_idnt,
 hierarchy.subdiv_desc,
 hierarchy.dept_idnt,
 hierarchy.dept_desc,
 hierarchy.class_idnt,
 hierarchy.class_desc,
 hierarchy.sbclass_idnt,
 hierarchy.sbclass_desc,
 hierarchy.quantrix_category,
 hierarchy.ccs_category,
 hierarchy.ccs_subcategory,
 hierarchy.nord_role_desc,
 hierarchy.rack_role_desc,
 hierarchy.parent_group,
 hierarchy.merch_themes,
 hierarchy.holiday_theme_ty,
 hierarchy.gift_ind,
 hierarchy.stocking_stuffer,
 hierarchy.assortment_grouping,
 u.udig,
 fanatics_flag,
 new_flag,
 cf_flag;