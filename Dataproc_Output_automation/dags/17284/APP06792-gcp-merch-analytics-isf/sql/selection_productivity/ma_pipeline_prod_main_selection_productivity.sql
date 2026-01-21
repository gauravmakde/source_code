
/*
Purpose:        Creates table with deciles to use in Decile Analysis Tableau report.
				Inserts data in {{params.environment_schema}} tables for Selection Productivity
                    selection_productivity
Variable(s):     {{params.environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{params.env_suffix}} '' or '_dev' tablesuffix for prod testing
                 {{params.start_date}} as beginning of time period
                 {{params.end_date}} as end of time period
Author(s):       Sara Scott
*/








CREATE TEMPORARY TABLE IF NOT EXISTS calendar
CLUSTER BY month_idnt
AS
SELECT month_idnt,
 month_start_day_date,
 month_end_day_date,
 month_abrv,
 quarter_idnt,
 half_idnt,
 year_idnt,
 cur_month,
 MIN(month_start_day_date) OVER (ORDER BY month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
 window_start_date,
 MAX(month_end_day_date) OVER (ORDER BY month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
 window_end_date
FROM (SELECT month_idnt,
   month_start_day_date,
   month_end_day_date,
   month_abrv,
   quarter_idnt,
   fiscal_halfyear_num AS half_idnt,
   fiscal_year_num AS year_idnt,
    CASE
    WHEN CURRENT_DATE >= month_start_day_date AND CURRENT_DATE <= month_end_day_date
    THEN 1
    ELSE 0
    END AS cur_month
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date BETWEEN {{params.start_date}} AND  {{params.end_date}}
  GROUP BY month_idnt,
   month_start_day_date,
   month_end_day_date,
   month_abrv,
   quarter_idnt,
   half_idnt,
   year_idnt,
   cur_month) AS t1;


CREATE TEMPORARY TABLE IF NOT EXISTS fanatics
CLUSTER BY sku_idnt
AS
SELECT DISTINCT psd.rms_sku_num AS sku_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psd
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim AS payto ON LOWER(psd.prmy_supp_num) = LOWER(payto.order_from_vendor_num
   )
WHERE LOWER(payto.payto_vendor_num) = LOWER('5179609');


CREATE TEMPORARY TABLE IF NOT EXISTS anniversary
CLUSTER BY sku_idnt, channel, month_idnt
AS
SELECT a.sku_idnt,
 a.channel_country,
  CASE
  WHEN LOWER(a.selling_channel) = LOWER('ONLINE')
  THEN 'DIGITAL'
  ELSE a.selling_channel
  END AS channel,
 'NORDSTROM' AS channel_brand,
 cal.month_idnt,
 1 AS anniversary_flag
FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.anniversary_sku_chnl_date AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c ON a.day_idnt = c.day_idnt
 INNER JOIN calendar AS cal ON c.month_idnt = cal.month_idnt
GROUP BY a.sku_idnt,
 a.channel_country,
 channel,
 channel_brand,
 cal.month_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS los_agg_fact
CLUSTER BY month_idnt, customer_choice, channel_country
AS
SELECT month_idnt,
 customer_choice,
 channel_country,
 channel_brand,
 channel,
 SUM(days_live) AS days_live
FROM (SELECT base.month_idnt,
   base.customer_choice,
   base.channel_country,
   base.channel_brand,
   base.channel,
   MAX(base.days_live) AS days_live
  FROM `{{params.gcp_project_id}}`.t2dl_das_selection.selection_productivity_base AS base
   INNER JOIN calendar ON base.month_idnt = calendar.month_idnt
  GROUP BY base.day_date,
   base.month_idnt,
   base.quarter_idnt,
   base.half_idnt,
   base.year_idnt,
   base.customer_choice,
   base.channel,
   base.channel_brand,
   base.channel_country) AS a
GROUP BY month_idnt,
 customer_choice,
 channel_country,
 channel_brand,
 channel;



CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy
CLUSTER BY sku_idnt, channel_country
AS
SELECT DISTINCT TRIM(COALESCE(FORMAT('%11d', sku.dept_num), 'UNKNOWN') || '_' || COALESCE(sku.prmy_supp_num, 'UNKNOWN')
      || '_' || COALESCE(sku.supp_part_num, 'UNKNOWN') || '_' || COALESCE(sku.color_num, 'UNKNOWN')) AS customer_choice
 ,
 sku.rms_sku_num AS sku_idnt,
 sku.channel_country,
 sku.prmy_supp_num AS supplier_idnt,
 supp.vendor_name AS supplier_name,
 CAST(sku.div_num AS INTEGER) AS div_idnt,
 sku.div_desc,
 CAST(sku.grp_num AS INTEGER) AS subdiv_idnt,
 sku.grp_desc AS subdiv_desc,
 CAST(sku.dept_num AS INTEGER) AS dept_idnt,
 sku.dept_desc,
 CAST(sku.class_num AS INTEGER) AS class_idnt,
 sku.class_desc,
 CAST(sku.sbclass_num AS INTEGER) AS sbclass_idnt,
 sku.sbclass_desc,
 sku.supp_part_num,
  CASE
  WHEN LOWER(sku.npg_ind) = LOWER('Y')
  THEN 1
  ELSE 0
  END AS npg_flag,
 ccs.quantrix_category,
 ccs.ccs_category,
 ccs.ccs_subcategory,
 ccs.nord_role_desc,
 ccs.rack_role_desc,
 ccs.parent_group
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_ccs_categories.ccs_merch_themes AS ccs ON sku.div_num = ccs.div_idnt AND sku.dept_num = ccs.dept_idnt
      AND sku.class_num = ccs.class_idnt AND sku.sbclass_num = ccs.sbclass_idnt
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(sku.prmy_supp_num) = LOWER(supp.vendor_num)
WHERE LOWER(sku.div_desc) NOT LIKE LOWER('INACTIVE%')
QUALIFY (DENSE_RANK() OVER (PARTITION BY sku_idnt, sku.channel_country ORDER BY sku.dw_sys_load_tmstp DESC)) = 1;



CREATE TEMPORARY TABLE IF NOT EXISTS receipts_base
CLUSTER BY sku_idnt, channel
AS
SELECT r.sku_idnt,
 r.mnth_idnt,
 cal.month_abrv,
 cal.quarter_idnt,
 cal.half_idnt,
 cal.year_idnt,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK CANADA'), LOWER('FULL LINE CANADA'), LOWER('N.CA'))
  THEN 'CA'
  ELSE 'US'
  END AS channel_country,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('OFFPRICE ONLINE'), LOWER('RACK CANADA'))
  THEN 'NORDSTROM_RACK'
  ELSE 'NORDSTROM'
  END AS channel_brand,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('FULL LINE CANADA'
     ))
  THEN 'STORE'
  ELSE 'DIGITAL'
  END AS channel,
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
 SUM(r.receipt_tot_units + r.receipt_rsk_units) AS rcpt_tot_units,
 SUM(r.receipt_tot_retail + r.receipt_rsk_retail) AS rcpt_tot_retail
FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact AS r
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON r.store_num = st.store_num
 INNER JOIN calendar AS cal ON r.mnth_idnt = cal.month_idnt AND cal.cur_month = 0
WHERE r.week_num >= 202301
 AND st.business_unit_num NOT IN (8000, 5500)
GROUP BY r.sku_idnt,
 r.mnth_idnt,
 cal.month_abrv,
 cal.quarter_idnt,
 cal.half_idnt,
 cal.year_idnt,
 channel_country,
 channel_brand,
 channel,
 ds_ind,
 rp_ind
UNION ALL
SELECT r0.sku_idnt,
 r0.mnth_idnt,
 cal0.month_abrv,
 cal0.quarter_idnt,
 cal0.half_idnt,
 cal0.year_idnt,
  CASE
  WHEN LOWER(st0.business_unit_desc) IN (LOWER('RACK CANADA'), LOWER('FULL LINE CANADA'), LOWER('N.CA'))
  THEN 'CA'
  ELSE 'US'
  END AS channel_country,
  CASE
  WHEN LOWER(st0.business_unit_desc) IN (LOWER('RACK'), LOWER('OFFPRICE ONLINE'), LOWER('RACK CANADA'))
  THEN 'NORDSTROM_RACK'
  ELSE 'NORDSTROM'
  END AS channel_brand,
  CASE
  WHEN LOWER(st0.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('FULL LINE CANADA'
     ))
  THEN 'STORE'
  ELSE 'DIGITAL'
  END AS channel,
  CASE
  WHEN r0.receipt_ds_units > 0
  THEN 1
  ELSE 0
  END AS ds_ind,
 r0.rp_ind,
 SUM(r0.receipt_po_units + r0.receipt_ds_units + r0.receipt_rsk_units) AS rcpt_tot_units,
 SUM(r0.receipt_po_retail + r0.receipt_ds_retail + r0.receipt_rsk_retail) AS rcpt_tot_retail
FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm AS r0
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st0 ON r0.store_num = st0.store_num
 INNER JOIN calendar AS cal0 ON r0.mnth_idnt = cal0.month_idnt AND cal0.cur_month = 0
WHERE r0.week_num < 202301
 AND st0.business_unit_num NOT IN (8000, 5500)
GROUP BY r0.sku_idnt,
 r0.mnth_idnt,
 cal0.month_abrv,
 cal0.quarter_idnt,
 cal0.half_idnt,
 cal0.year_idnt,
 channel_country,
 channel_brand,
 channel,
 ds_ind,
 r0.rp_ind;


CREATE TEMPORARY TABLE IF NOT EXISTS cc_stage
CLUSTER BY month_idnt, customer_choice
AS
SELECT base.month_idnt,
 base.month_abrv,
 base.quarter_idnt,
 base.half_idnt,
 base.year_idnt,
 base.customer_choice,
 base.channel,
 base.channel_country,
 base.channel_brand,
 h.div_idnt,
 h.div_desc,
 h.subdiv_idnt,
 h.subdiv_desc,
 h.dept_idnt,
 h.dept_desc,
 h.quantrix_category,
 h.ccs_category,
 h.ccs_subcategory,
 h.nord_role_desc,
 h.rack_role_desc,
 h.parent_group,
 h.supplier_name,
 h.supp_part_num,
 MAX(los.days_live) AS days_live,
 MIN(base.days_published) AS days_published,
 MAX(base.new_flag) AS new_flag,
 MAX(base.cf_flag) AS cf_flag,
 MAX(h.npg_flag) AS npg_flag,
 MAX(base.rp_flag) AS rp_flag,
 MAX(base.dropship_flag) AS dropship_flag,
 MAX(base.anniversary_flag) AS anniversary_flag,
 MAX(base.fanatics_flag) AS fanatics_flag,
 SUM(base.demand_dollars) AS demand_dollars,
 SUM(base.demand_units) AS demand_units,
 SUM(base.sales_dollars) AS sales_dollars,
 SUM(base.sales_units) AS sales_units,
 0 AS receipt_units,
 0 AS receipt_dollars,
 SUM(COALESCE(base.product_views, 0)) AS product_views,
 SUM(COALESCE(base.order_sessions, 0)) AS order_sessions,
 SUM(COALESCE(base.instock_views, 0)) AS instock_views,
 SUM(COALESCE(base.product_view_sessions, 0)) AS product_view_sessions,
 SUM(COALESCE(base.order_demand, 0)) AS order_demand,
 SUM(COALESCE(base.order_quantity, 0)) AS order_quantity,
 SUM(CASE
   WHEN base.last_day_of_month = 1
   THEN base.eoh_dollars
   ELSE 0
   END) AS eoh_dollars,
 SUM(CASE
   WHEN base.last_day_of_month = 1
   THEN base.eoh_units
   ELSE 0
   END) AS eoh_units,
 SUM(CASE
   WHEN base.first_day_of_month = 1
   THEN base.boh_dollars
   ELSE 0
   END) AS boh_dollars,
 SUM(CASE
   WHEN base.first_day_of_month = 1
   THEN base.boh_units
   ELSE 0
   END) AS boh_units
FROM `{{params.gcp_project_id}}`.t2dl_das_selection.selection_productivity_base AS base
 INNER JOIN hierarchy AS h ON LOWER(base.customer_choice) = LOWER(h.customer_choice) AND LOWER(base.channel_country) =
    LOWER(h.channel_country) AND LOWER(base.sku_idnt) = LOWER(h.sku_idnt)
 LEFT JOIN los_agg_fact AS los ON LOWER(base.customer_choice) = LOWER(los.customer_choice) AND LOWER(base.channel_country
       ) = LOWER(los.channel_country) AND LOWER(base.channel) = LOWER(los.channel) AND LOWER(base.channel_brand) = LOWER(los
     .channel_brand) AND base.month_idnt = los.month_idnt
 INNER JOIN calendar AS cal ON base.month_idnt = cal.month_idnt AND cal.cur_month = 0
GROUP BY base.month_idnt,
 base.month_abrv,
 base.quarter_idnt,
 base.half_idnt,
 base.year_idnt,
 base.customer_choice,
 base.channel,
 base.channel_country,
 base.channel_brand,
 h.div_idnt,
 h.div_desc,
 h.subdiv_idnt,
 h.subdiv_desc,
 h.dept_idnt,
 h.dept_desc,
 h.quantrix_category,
 h.ccs_category,
 h.ccs_subcategory,
 h.nord_role_desc,
 h.rack_role_desc,
 h.parent_group,
 h.supplier_name,
 h.supp_part_num
UNION ALL
SELECT r.mnth_idnt AS month_idnt,
 r.month_abrv,
 r.quarter_idnt,
 r.half_idnt,
 r.year_idnt,
 s.customer_choice,
 r.channel,
 r.channel_country,
 r.channel_brand,
 h0.div_idnt,
 h0.div_desc,
 h0.subdiv_idnt,
 h0.subdiv_desc,
 h0.dept_idnt,
 h0.dept_desc,
 h0.quantrix_category,
 h0.ccs_category,
 h0.ccs_subcategory,
 h0.nord_role_desc,
 h0.rack_role_desc,
 h0.parent_group,
 h0.supplier_name,
 h0.supp_part_num,
 MAX(los0.days_live) AS days_live,
 CAST(NULL AS DATE) AS days_published,
  CASE
  WHEN LOWER(c.cc_type) = LOWER('NEW')
  THEN 1
  ELSE 0
  END AS new_flag,
 COALESCE(CASE
   WHEN LOWER(c.cc_type) = LOWER('CF')
   THEN 1
   ELSE 0
   END, 0) AS cf_flag,
 MAX(h0.npg_flag) AS npg_flag,
 MAX(r.rp_ind) AS rp_ind,
 MAX(r.ds_ind) AS ds_ind,
 MAX(a.anniversary_flag) AS anniversary_flag,
 MAX(CASE
   WHEN f.sku_idnt IS NOT NULL
   THEN 1
   ELSE 0
   END) AS fanatics_flag,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS sales_dollars,
 0 AS sales_units,
 SUM(COALESCE(r.rcpt_tot_units, 0)) AS receipt_units,
 SUM(COALESCE(r.rcpt_tot_retail, 0)) AS receipt_dollars,
 0 AS product_views,
 0 AS order_sessions,
 0 AS instock_views,
 0 AS product_view_sessions,
 0 AS order_demand,
 0 AS order_quantity,
 0 AS eoh_dollars,
 0 AS eoh_units,
 0 AS boh_dollars,
 0 AS boh_units
FROM receipts_base AS r
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS s ON LOWER(r.sku_idnt) = LOWER(s.sku_idnt) AND LOWER(r.channel_country
    ) = LOWER(s.channel_country)
 INNER JOIN hierarchy AS h0 ON LOWER(s.customer_choice) = LOWER(h0.customer_choice) AND LOWER(r.channel_country) = LOWER(h0
     .channel_country) AND LOWER(r.sku_idnt) = LOWER(h0.sku_idnt)
 LEFT JOIN los_agg_fact AS los0 ON LOWER(s.customer_choice) = LOWER(los0.customer_choice) AND LOWER(r.channel_country) =
      LOWER(los0.channel_country) AND LOWER(r.channel) = LOWER(los0.channel) AND LOWER(r.channel_brand) = LOWER(los0.channel_brand
     ) AND r.mnth_idnt = los0.month_idnt
 LEFT JOIN anniversary AS a ON LOWER(r.sku_idnt) = LOWER(a.sku_idnt) AND r.mnth_idnt = a.month_idnt AND LOWER(r.channel_brand
      ) = LOWER(a.channel_brand) AND LOWER(r.channel) = LOWER(a.channel) AND LOWER(r.channel_country) = LOWER(a.channel_country
    )
 LEFT JOIN fanatics AS f ON LOWER(r.sku_idnt) = LOWER(f.sku_idnt)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.cc_type AS c ON LOWER(c.customer_choice) = LOWER(s.customer_choice) AND r.mnth_idnt =
      c.mnth_idnt AND LOWER(c.channel_country) = LOWER(r.channel_country) AND LOWER(c.channel_brand) = LOWER(r.channel_brand
     ) AND LOWER(c.channel) = LOWER(r.channel)
GROUP BY month_idnt,
 r.month_abrv,
 r.quarter_idnt,
 r.half_idnt,
 r.year_idnt,
 s.customer_choice,
 r.channel,
 r.channel_country,
 r.channel_brand,
 h0.div_idnt,
 h0.div_desc,
 h0.subdiv_idnt,
 h0.subdiv_desc,
 h0.dept_idnt,
 h0.dept_desc,
 h0.quantrix_category,
 h0.ccs_category,
 h0.ccs_subcategory,
 h0.nord_role_desc,
 h0.rack_role_desc,
 h0.parent_group,
 h0.supplier_name,
 h0.supp_part_num,
 days_published,
 new_flag,
 cf_flag;



CREATE TEMPORARY TABLE IF NOT EXISTS cc_month_base
CLUSTER BY month_idnt, customer_choice
AS
SELECT month_idnt,
 month_abrv,
 quarter_idnt,
 half_idnt,
 year_idnt,
 customer_choice,
 channel,
 channel_country,
 channel_brand,
 div_idnt,
 div_desc,
 subdiv_idnt,
 subdiv_desc,
 dept_idnt,
 dept_desc,
 quantrix_category,
 ccs_category,
 ccs_subcategory,
 nord_role_desc,
 rack_role_desc,
 parent_group,
 supplier_name,
 supp_part_num,
 MAX(days_live) AS days_live,
 MIN(days_published) AS days_published,
 MAX(new_flag) AS new_flag,
 MAX(cf_flag) AS cf_flag,
 MAX(npg_flag) AS npg_flag,
 MAX(rp_flag) AS rp_flag,
 MAX(dropship_flag) AS dropship_flag,
 MAX(anniversary_flag) AS anniversary_flag,
 MAX(fanatics_flag) AS fanatics_flag,
 SUM(COALESCE(demand_dollars, 0)) AS demand_dollars,
 SUM(COALESCE(demand_units, 0)) AS demand_units,
 SUM(COALESCE(sales_dollars, 0)) AS sales_dollars,
 SUM(COALESCE(sales_units, 0)) AS sales_units,
 SUM(receipt_units) AS receipt_units,
 SUM(receipt_dollars) AS receipt_dollars,
 SUM(product_views) AS product_views,
 SUM(order_sessions) AS order_sessions,
 SUM(instock_views) AS instock_views,
 SUM(product_view_sessions) AS product_view_sessions,
 SUM(order_demand) AS order_demand,
 SUM(order_quantity) AS order_quantity,
 SUM(COALESCE(eoh_dollars, 0)) AS eoh_dollars,
 SUM(COALESCE(eoh_units, 0)) AS eoh_units,
 SUM(COALESCE(boh_dollars, 0)) AS boh_dollars,
 SUM(COALESCE(boh_units, 0)) AS boh_units
FROM cc_stage
GROUP BY month_idnt,
 month_abrv,
 quarter_idnt,
 half_idnt,
 year_idnt,
 customer_choice,
 channel,
 channel_country,
 channel_brand,
 div_idnt,
 div_desc,
 subdiv_idnt,
 subdiv_desc,
 dept_idnt,
 dept_desc,
 quantrix_category,
 ccs_category,
 ccs_subcategory,
 nord_role_desc,
 rack_role_desc,
 parent_group,
 supplier_name,
 supp_part_num;



CREATE TEMPORARY TABLE IF NOT EXISTS cc_month
CLUSTER BY month_idnt, customer_choice
AS
SELECT month_idnt,
 month_abrv,
 quarter_idnt,
 half_idnt,
 year_idnt,
 customer_choice,
 channel,
 channel_brand,
 channel_country,
 div_idnt,
 div_desc,
 subdiv_idnt,
 subdiv_desc,
 dept_idnt,
 dept_desc,
 supplier_name,
 supp_part_num,
 quantrix_category,
 ccs_category,
 ccs_subcategory,
 nord_role_desc,
 rack_role_desc,
 parent_group,
 days_live,
 days_published,
 new_flag,
 cf_flag,
 npg_flag,
 rp_flag,
 dropship_flag,
 anniversary_flag,
 fanatics_flag,
 sales_dollars,
 demand_dollars,
 demand_units,
 sales_units,
 receipt_dollars,
 receipt_units,
 eoh_dollars,
 eoh_units,
 boh_dollars,
 boh_units,
 product_views,
 order_sessions,
 instock_views,
 product_view_sessions,
 order_demand,
 order_quantity,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category ORDER BY sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS
 channel_dept_category_sales,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category ORDER BY demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS
 channel_dept_category_demand,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt ORDER BY sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_dept_sales,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt ORDER BY demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_dept_demand,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt
  ORDER BY sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_subdiv_sales,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt
  ORDER BY demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_subdiv_demand,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt ORDER BY
    sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_div_sales,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country, div_idnt ORDER BY
    demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_div_demand,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country ORDER BY sales_dollars DESC
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_sales,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_brand, channel_country ORDER BY demand_dollars DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_demand,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_dept_cat,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_dept_cat,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_dept,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_dept,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_subdiv,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_subdiv,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_div,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand, div_idnt RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_div,
 SUM(sales_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel,
 SUM(demand_dollars) OVER (PARTITION BY month_idnt, channel, channel_country, channel_brand RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel
FROM (SELECT month_idnt,
   month_abrv,
   quarter_idnt,
   half_idnt,
   year_idnt,
   customer_choice,
   channel,
   channel_brand,
   channel_country,
   div_idnt,
   div_desc,
   subdiv_idnt,
   subdiv_desc,
   dept_idnt,
   dept_desc,
   supplier_name,
   supp_part_num,
   quantrix_category,
   ccs_category,
   ccs_subcategory,
   nord_role_desc,
   rack_role_desc,
   parent_group,
   days_live,
   days_published,
   new_flag,
   cf_flag,
   npg_flag,
   rp_flag,
   dropship_flag,
   anniversary_flag,
   fanatics_flag,
   sales_dollars,
   demand_dollars,
   demand_units,
   sales_units,
   receipt_dollars,
   receipt_units,
   eoh_dollars,
   eoh_units,
   boh_dollars,
   boh_units,
   product_views,
   order_sessions,
   instock_views,
   product_view_sessions,
   order_demand,
   order_quantity
  FROM cc_month_base
  GROUP BY month_idnt,
   month_abrv,
   quarter_idnt,
   half_idnt,
   year_idnt,
   customer_choice,
   channel,
   channel_country,
   channel_brand,
   div_idnt,
   div_desc,
   subdiv_idnt,
   subdiv_desc,
   dept_idnt,
   dept_desc,
   quantrix_category,
   ccs_category,
   ccs_subcategory,
   nord_role_desc,
   rack_role_desc,
   parent_group,
   supplier_name,
   supp_part_num,
   days_live,
   days_published,
   new_flag,
   cf_flag,
   npg_flag,
   rp_flag,
   dropship_flag,
   anniversary_flag,
   fanatics_flag,
   demand_dollars,
   demand_units,
   sales_dollars,
   sales_units,
   receipt_units,
   receipt_dollars,
   product_views,
   order_sessions,
   instock_views,
   product_view_sessions,
   order_demand,
   order_quantity,
   eoh_dollars,
   eoh_units,
   boh_dollars,
   boh_units) AS t0;



CREATE TEMPORARY TABLE IF NOT EXISTS cc_quarter_base
CLUSTER BY quarter_idnt, customer_choice
AS
SELECT base.quarter_idnt,
 base.half_idnt,
 base.year_idnt,
 base.customer_choice,
 base.channel,
 base.channel_country,
 base.channel_brand,
 h.div_idnt,
 h.div_desc,
 h.subdiv_idnt,
 h.subdiv_desc,
 h.dept_idnt,
 h.dept_desc,
 h.quantrix_category,
 h.ccs_category,
 h.ccs_subcategory,
 h.nord_role_desc,
 h.rack_role_desc,
 h.parent_group,
 h.supplier_name,
 h.supp_part_num,
 MAX(base.new_flag) AS new_flag,
 MAX(base.cf_flag) AS cf_flag,
 MAX(h.npg_flag) AS npg_flag,
 MAX(base.rp_flag) AS rp_flag,
 MAX(base.dropship_flag) AS dropship_flag,
 MAX(base.anniversary_flag) AS anniversary_flag,
 MAX(base.fanatics_flag) AS fanatics_flag,
 SUM(base.demand_dollars) AS demand_dollars,
 SUM(base.sales_dollars) AS sales_dollars,
 SUM(CASE
   WHEN base.last_day_of_qtr = 1
   THEN base.eoh_dollars
   ELSE 0
   END) AS eoh_dollars,
 SUM(CASE
   WHEN base.last_day_of_qtr = 1
   THEN base.eoh_units
   ELSE 0
   END) AS eoh_units,
 SUM(CASE
   WHEN base.first_day_of_qtr = 1
   THEN base.boh_dollars
   ELSE 0
   END) AS boh_dollars,
 SUM(CASE
   WHEN base.first_day_of_qtr = 1
   THEN base.boh_units
   ELSE 0
   END) AS boh_units
FROM `{{params.gcp_project_id}}`.t2dl_das_selection.selection_productivity_base AS base
 INNER JOIN hierarchy AS h ON LOWER(base.customer_choice) = LOWER(h.customer_choice) AND LOWER(base.channel_country) =
    LOWER(h.channel_country) AND LOWER(base.sku_idnt) = LOWER(h.sku_idnt)
 INNER JOIN calendar AS cal ON base.month_idnt = cal.month_idnt AND cal.cur_month = 0
GROUP BY base.quarter_idnt,
 base.half_idnt,
 base.year_idnt,
 base.customer_choice,
 base.channel,
 base.channel_country,
 base.channel_brand,
 h.div_idnt,
 h.div_desc,
 h.subdiv_idnt,
 h.subdiv_desc,
 h.dept_idnt,
 h.dept_desc,
 h.quantrix_category,
 h.ccs_category,
 h.ccs_subcategory,
 h.nord_role_desc,
 h.rack_role_desc,
 h.parent_group,
 h.supplier_name,
 h.supp_part_num;




CREATE TEMPORARY TABLE IF NOT EXISTS cc_quarter
CLUSTER BY quarter_idnt, customer_choice
AS
SELECT quarter_idnt,
 half_idnt,
 year_idnt,
 customer_choice,
 channel,
 channel_brand,
 channel_country,
 div_idnt,
 div_desc,
 subdiv_idnt,
 subdiv_desc,
 dept_idnt,
 dept_desc,
 supplier_name,
 supp_part_num,
 quantrix_category,
 ccs_category,
 ccs_subcategory,
 nord_role_desc,
 rack_role_desc,
 parent_group,
 new_flag,
 cf_flag,
 npg_flag,
 rp_flag,
 dropship_flag,
 anniversary_flag,
 fanatics_flag,
 sales_dollars,
 demand_dollars,
 eoh_dollars,
 eoh_units,
 boh_dollars,
 boh_units,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category ORDER BY sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS
 channel_dept_category_sales,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category ORDER BY demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS
 channel_dept_category_demand,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt ORDER BY sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_dept_sales,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt,
    dept_idnt ORDER BY demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_dept_demand,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt
  ORDER BY sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_subdiv_sales,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt, subdiv_idnt
  ORDER BY demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_subdiv_demand,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt ORDER BY
    sales_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_div_sales,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country, div_idnt ORDER BY
    demand_dollars DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_div_demand,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country ORDER BY sales_dollars DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_sales,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_brand, channel_country ORDER BY demand_dollars
    DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS channel_demand,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_dept_cat,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt, quantrix_category RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_dept_cat,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_dept,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt,
    dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_dept,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_subdiv,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt, subdiv_idnt
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_subdiv,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel_div,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand, div_idnt RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel_div,
 SUM(sales_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_channel,
 SUM(demand_dollars) OVER (PARTITION BY quarter_idnt, channel, channel_country, channel_brand RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_channel
FROM (SELECT quarter_idnt,
   half_idnt,
   year_idnt,
   customer_choice,
   channel,
   channel_brand,
   channel_country,
   div_idnt,
   div_desc,
   subdiv_idnt,
   subdiv_desc,
   dept_idnt,
   dept_desc,
   supplier_name,
   supp_part_num,
   quantrix_category,
   ccs_category,
   ccs_subcategory,
   nord_role_desc,
   rack_role_desc,
   parent_group,
   new_flag,
   cf_flag,
   npg_flag,
   rp_flag,
   dropship_flag,
   anniversary_flag,
   fanatics_flag,
   sales_dollars,
   demand_dollars,
   eoh_dollars,
   eoh_units,
   boh_dollars,
   boh_units
  FROM cc_quarter_base
  GROUP BY quarter_idnt,
   half_idnt,
   year_idnt,
   customer_choice,
   channel,
   channel_country,
   channel_brand,
   div_idnt,
   div_desc,
   subdiv_idnt,
   subdiv_desc,
   dept_idnt,
   dept_desc,
   quantrix_category,
   ccs_category,
   ccs_subcategory,
   nord_role_desc,
   rack_role_desc,
   parent_group,
   supplier_name,
   supp_part_num,
   new_flag,
   cf_flag,
   npg_flag,
   rp_flag,
   dropship_flag,
   anniversary_flag,
   fanatics_flag,
   demand_dollars,
   sales_dollars,
   eoh_dollars,
   eoh_units,
   boh_dollars,
   boh_units) AS t0;




DELETE FROM `{{params.gcp_project_id}}`.t2dl_das_selection.selection_productivity
WHERE month_idnt IN (SELECT DISTINCT month_idnt
  FROM calendar);


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_selection.selection_productivity
(SELECT d.month_idnt,
  d.month_abrv,
  d.quarter_idnt,
  d.half_idnt,
  d.year_idnt,
  d.customer_choice,
  d.channel,
  d.channel_brand,
  d.channel_country,
  d.div_idnt,
  d.div_desc,
  d.subdiv_idnt,
  d.subdiv_desc,
  d.dept_idnt,
  d.dept_desc,
  d.supplier_name,
  d.supp_part_num,
  d.quantrix_category,
  d.ccs_category,
  d.ccs_subcategory,
  d.nord_role_desc,
  d.rack_role_desc,
  d.parent_group,
  d.days_live,
  d.days_published,
  d.new_flag,
  d.cf_flag,
   CASE
   WHEN d.npg_flag = 1
   THEN 'Y'
   ELSE 'N'
   END AS npg_flag,
  d.rp_flag,
  d.dropship_flag,
  d.anniversary_flag,
   CASE
   WHEN d.fanatics_flag = 1
   THEN 'Y'
   ELSE 'N'
   END AS fanatics_flag,
   CASE
   WHEN d.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN d.channel_dept_category_demand >= 0.9 * d.demand_channel_dept_cat
   THEN '10'
   WHEN d.channel_dept_category_demand >= 0.8 * d.demand_channel_dept_cat
   THEN '9'
   WHEN d.channel_dept_category_demand >= 0.7 * d.demand_channel_dept_cat
   THEN '8'
   WHEN d.channel_dept_category_demand >= 0.6 * d.demand_channel_dept_cat
   THEN '7'
   WHEN d.channel_dept_category_demand >= 0.5 * d.demand_channel_dept_cat
   THEN '6'
   WHEN d.channel_dept_category_demand >= 0.4 * d.demand_channel_dept_cat
   THEN '5'
   WHEN d.channel_dept_category_demand >= 0.3 * d.demand_channel_dept_cat
   THEN '4'
   WHEN d.channel_dept_category_demand >= 0.2 * d.demand_channel_dept_cat
   THEN '3'
   WHEN d.channel_dept_category_demand >= 0.1 * d.demand_channel_dept_cat
   THEN '2'
   ELSE '1'
   END AS channel_dept_cat_demand_deciles,
   CASE
   WHEN d.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN d.channel_dept_category_sales >= 0.9 * d.sales_channel_dept_cat
   THEN '10'
   WHEN d.channel_dept_category_sales >= 0.8 * d.sales_channel_dept_cat
   THEN '9'
   WHEN d.channel_dept_category_sales >= 0.7 * d.sales_channel_dept_cat
   THEN '8'
   WHEN d.channel_dept_category_sales >= 0.6 * d.sales_channel_dept_cat
   THEN '7'
   WHEN d.channel_dept_category_sales >= 0.5 * d.sales_channel_dept_cat
   THEN '6'
   WHEN d.channel_dept_category_sales >= 0.4 * d.sales_channel_dept_cat
   THEN '5'
   WHEN d.channel_dept_category_sales >= 0.3 * d.sales_channel_dept_cat
   THEN '4'
   WHEN d.channel_dept_category_sales >= 0.2 * d.sales_channel_dept_cat
   THEN '3'
   WHEN d.channel_dept_category_sales >= 0.1 * d.sales_channel_dept_cat
   THEN '2'
   ELSE '1'
   END AS channel_dept_cat_sales_deciles,
   CASE
   WHEN d.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN d.channel_dept_demand >= 0.9 * d.demand_channel_dept
   THEN '10'
   WHEN d.channel_dept_demand >= 0.8 * d.demand_channel_dept
   THEN '9'
   WHEN d.channel_dept_demand >= 0.7 * d.demand_channel_dept
   THEN '8'
   WHEN d.channel_dept_demand >= 0.6 * d.demand_channel_dept
   THEN '7'
   WHEN d.channel_dept_demand >= 0.5 * d.demand_channel_dept
   THEN '6'
   WHEN d.channel_dept_demand >= 0.4 * d.demand_channel_dept
   THEN '5'
   WHEN d.channel_dept_demand >= 0.3 * d.demand_channel_dept
   THEN '4'
   WHEN d.channel_dept_demand >= 0.2 * d.demand_channel_dept
   THEN '3'
   WHEN d.channel_dept_demand >= 0.1 * d.demand_channel_dept
   THEN '2'
   ELSE '1'
   END AS channel_dept_demand_deciles,
   CASE
   WHEN d.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN d.channel_dept_sales >= 0.9 * d.sales_channel_dept
   THEN '10'
   WHEN d.channel_dept_sales >= 0.8 * d.sales_channel_dept
   THEN '9'
   WHEN d.channel_dept_sales >= 0.7 * d.sales_channel_dept
   THEN '8'
   WHEN d.channel_dept_sales >= 0.6 * d.sales_channel_dept
   THEN '7'
   WHEN d.channel_dept_sales >= 0.5 * d.sales_channel_dept
   THEN '6'
   WHEN d.channel_dept_sales >= 0.4 * d.sales_channel_dept
   THEN '5'
   WHEN d.channel_dept_sales >= 0.3 * d.sales_channel_dept
   THEN '4'
   WHEN d.channel_dept_sales >= 0.2 * d.sales_channel_dept
   THEN '3'
   WHEN d.channel_dept_sales >= 0.1 * d.sales_channel_dept
   THEN '2'
   ELSE '1'
   END AS channel_dept_sales_deciles,
   CASE
   WHEN d.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN d.channel_subdiv_demand >= 0.9 * d.demand_channel_subdiv
   THEN '10'
   WHEN d.channel_subdiv_demand >= 0.8 * d.demand_channel_subdiv
   THEN '9'
   WHEN d.channel_subdiv_demand >= 0.7 * d.demand_channel_subdiv
   THEN '8'
   WHEN d.channel_subdiv_demand >= 0.6 * d.demand_channel_subdiv
   THEN '7'
   WHEN d.channel_subdiv_demand >= 0.5 * d.demand_channel_subdiv
   THEN '6'
   WHEN d.channel_subdiv_demand >= 0.4 * d.demand_channel_subdiv
   THEN '5'
   WHEN d.channel_subdiv_demand >= 0.3 * d.demand_channel_subdiv
   THEN '4'
   WHEN d.channel_subdiv_demand >= 0.2 * d.demand_channel_subdiv
   THEN '3'
   WHEN d.channel_subdiv_demand >= 0.1 * d.demand_channel_subdiv
   THEN '2'
   ELSE '1'
   END AS channel_subdiv_demand_deciles,
   CASE
   WHEN d.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN d.channel_subdiv_sales >= 0.9 * d.sales_channel_subdiv
   THEN '10'
   WHEN d.channel_subdiv_sales >= 0.8 * d.sales_channel_subdiv
   THEN '9'
   WHEN d.channel_subdiv_sales >= 0.7 * d.sales_channel_subdiv
   THEN '8'
   WHEN d.channel_subdiv_sales >= 0.6 * d.sales_channel_subdiv
   THEN '7'
   WHEN d.channel_subdiv_sales >= 0.5 * d.sales_channel_subdiv
   THEN '6'
   WHEN d.channel_subdiv_sales >= 0.4 * d.sales_channel_subdiv
   THEN '5'
   WHEN d.channel_subdiv_sales >= 0.3 * d.sales_channel_subdiv
   THEN '4'
   WHEN d.channel_subdiv_sales >= 0.2 * d.sales_channel_subdiv
   THEN '3'
   WHEN d.channel_subdiv_sales >= 0.1 * d.sales_channel_subdiv
   THEN '2'
   ELSE '1'
   END AS channel_subdiv_sales_deciles,
   CASE
   WHEN d.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN d.channel_div_demand >= 0.9 * d.demand_channel_div
   THEN '10'
   WHEN d.channel_div_demand >= 0.8 * d.demand_channel_div
   THEN '9'
   WHEN d.channel_div_demand >= 0.7 * d.demand_channel_div
   THEN '8'
   WHEN d.channel_div_demand >= 0.6 * d.demand_channel_div
   THEN '7'
   WHEN d.channel_div_demand >= 0.5 * d.demand_channel_div
   THEN '6'
   WHEN d.channel_div_demand >= 0.4 * d.demand_channel_div
   THEN '5'
   WHEN d.channel_div_demand >= 0.3 * d.demand_channel_div
   THEN '4'
   WHEN d.channel_div_demand >= 0.2 * d.demand_channel_div
   THEN '3'
   WHEN d.channel_div_demand >= 0.1 * d.demand_channel_div
   THEN '2'
   ELSE '1'
   END AS channel_div_demand_deciles,
   CASE
   WHEN d.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN d.channel_div_sales >= 0.9 * d.sales_channel_div
   THEN '10'
   WHEN d.channel_div_sales >= 0.8 * d.sales_channel_div
   THEN '9'
   WHEN d.channel_div_sales >= 0.7 * d.sales_channel_div
   THEN '8'
   WHEN d.channel_div_sales >= 0.6 * d.sales_channel_div
   THEN '7'
   WHEN d.channel_div_sales >= 0.5 * d.sales_channel_div
   THEN '6'
   WHEN d.channel_div_sales >= 0.4 * d.sales_channel_div
   THEN '5'
   WHEN d.channel_div_sales >= 0.3 * d.sales_channel_div
   THEN '4'
   WHEN d.channel_div_sales >= 0.2 * d.sales_channel_div
   THEN '3'
   WHEN d.channel_div_sales >= 0.1 * d.sales_channel_div
   THEN '2'
   ELSE '1'
   END AS channel_div_sales_deciles,
   CASE
   WHEN d.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN d.channel_demand >= 0.9 * d.demand_channel
   THEN '10'
   WHEN d.channel_demand >= 0.8 * d.demand_channel
   THEN '9'
   WHEN d.channel_demand >= 0.7 * d.demand_channel
   THEN '8'
   WHEN d.channel_demand >= 0.6 * d.demand_channel
   THEN '7'
   WHEN d.channel_demand >= 0.5 * d.demand_channel
   THEN '6'
   WHEN d.channel_demand >= 0.4 * d.demand_channel
   THEN '5'
   WHEN d.channel_demand >= 0.3 * d.demand_channel
   THEN '4'
   WHEN d.channel_demand >= 0.2 * d.demand_channel
   THEN '3'
   WHEN d.channel_demand >= 0.1 * d.demand_channel
   THEN '2'
   ELSE '1'
   END AS channel_demand_deciles,
   CASE
   WHEN d.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN d.channel_sales >= 0.9 * d.sales_channel
   THEN '10'
   WHEN d.channel_sales >= 0.8 * d.sales_channel
   THEN '9'
   WHEN d.channel_sales >= 0.7 * d.sales_channel
   THEN '8'
   WHEN d.channel_sales >= 0.6 * d.sales_channel
   THEN '7'
   WHEN d.channel_sales >= 0.5 * d.sales_channel
   THEN '6'
   WHEN d.channel_sales >= 0.4 * d.sales_channel
   THEN '5'
   WHEN d.channel_sales >= 0.3 * d.sales_channel
   THEN '4'
   WHEN d.channel_sales >= 0.2 * d.sales_channel
   THEN '3'
   WHEN d.channel_sales >= 0.1 * d.sales_channel
   THEN '2'
   ELSE '1'
   END AS channel_sales_deciles,
   CASE
   WHEN qtr.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN qtr.channel_dept_category_demand >= 0.9 * qtr.demand_channel_dept_cat
   THEN '10'
   WHEN qtr.channel_dept_category_demand >= 0.8 * qtr.demand_channel_dept_cat
   THEN '9'
   WHEN qtr.channel_dept_category_demand >= 0.7 * qtr.demand_channel_dept_cat
   THEN '8'
   WHEN qtr.channel_dept_category_demand >= 0.6 * qtr.demand_channel_dept_cat
   THEN '7'
   WHEN qtr.channel_dept_category_demand >= 0.5 * qtr.demand_channel_dept_cat
   THEN '6'
   WHEN qtr.channel_dept_category_demand >= 0.4 * qtr.demand_channel_dept_cat
   THEN '5'
   WHEN qtr.channel_dept_category_demand >= 0.3 * qtr.demand_channel_dept_cat
   THEN '4'
   WHEN qtr.channel_dept_category_demand >= 0.2 * qtr.demand_channel_dept_cat
   THEN '3'
   WHEN qtr.channel_dept_category_demand >= 0.1 * qtr.demand_channel_dept_cat
   THEN '2'
   ELSE '1'
   END AS qtr_channel_dept_cat_demand_deciles,
   CASE
   WHEN qtr.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN qtr.channel_dept_category_sales >= 0.9 * qtr.sales_channel_dept_cat
   THEN '10'
   WHEN qtr.channel_dept_category_sales >= 0.8 * qtr.sales_channel_dept_cat
   THEN '9'
   WHEN qtr.channel_dept_category_sales >= 0.7 * qtr.sales_channel_dept_cat
   THEN '8'
   WHEN qtr.channel_dept_category_sales >= 0.6 * qtr.sales_channel_dept_cat
   THEN '7'
   WHEN qtr.channel_dept_category_sales >= 0.5 * qtr.sales_channel_dept_cat
   THEN '6'
   WHEN qtr.channel_dept_category_sales >= 0.4 * qtr.sales_channel_dept_cat
   THEN '5'
   WHEN qtr.channel_dept_category_sales >= 0.3 * qtr.sales_channel_dept_cat
   THEN '4'
   WHEN qtr.channel_dept_category_sales >= 0.2 * qtr.sales_channel_dept_cat
   THEN '3'
   WHEN qtr.channel_dept_category_sales >= 0.1 * qtr.sales_channel_dept_cat
   THEN '2'
   ELSE '1'
   END AS qtr_channel_dept_cat_sales_deciles,
   CASE
   WHEN qtr.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN qtr.channel_dept_demand >= 0.9 * qtr.demand_channel_dept
   THEN '10'
   WHEN qtr.channel_dept_demand >= 0.8 * qtr.demand_channel_dept
   THEN '9'
   WHEN qtr.channel_dept_demand >= 0.7 * qtr.demand_channel_dept
   THEN '8'
   WHEN qtr.channel_dept_demand >= 0.6 * qtr.demand_channel_dept
   THEN '7'
   WHEN qtr.channel_dept_demand >= 0.5 * qtr.demand_channel_dept
   THEN '6'
   WHEN qtr.channel_dept_demand >= 0.4 * qtr.demand_channel_dept
   THEN '5'
   WHEN qtr.channel_dept_demand >= 0.3 * qtr.demand_channel_dept
   THEN '4'
   WHEN qtr.channel_dept_demand >= 0.2 * qtr.demand_channel_dept
   THEN '3'
   WHEN qtr.channel_dept_demand >= 0.1 * qtr.demand_channel_dept
   THEN '2'
   ELSE '1'
   END AS qtr_channel_dept_demand_deciles,
   CASE
   WHEN qtr.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN qtr.channel_dept_sales >= 0.9 * qtr.sales_channel_dept
   THEN '10'
   WHEN qtr.channel_dept_sales >= 0.8 * qtr.sales_channel_dept
   THEN '9'
   WHEN qtr.channel_dept_sales >= 0.7 * qtr.sales_channel_dept
   THEN '8'
   WHEN qtr.channel_dept_sales >= 0.6 * qtr.sales_channel_dept
   THEN '7'
   WHEN qtr.channel_dept_sales >= 0.5 * qtr.sales_channel_dept
   THEN '6'
   WHEN qtr.channel_dept_sales >= 0.4 * qtr.sales_channel_dept
   THEN '5'
   WHEN qtr.channel_dept_sales >= 0.3 * qtr.sales_channel_dept
   THEN '4'
   WHEN qtr.channel_dept_sales >= 0.2 * qtr.sales_channel_dept
   THEN '3'
   WHEN qtr.channel_dept_sales >= 0.1 * qtr.sales_channel_dept
   THEN '2'
   ELSE '1'
   END AS qtr_channel_dept_sales_deciles,
   CASE
   WHEN qtr.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN qtr.channel_subdiv_demand >= 0.9 * qtr.demand_channel_subdiv
   THEN '10'
   WHEN qtr.channel_subdiv_demand >= 0.8 * qtr.demand_channel_subdiv
   THEN '9'
   WHEN qtr.channel_subdiv_demand >= 0.7 * qtr.demand_channel_subdiv
   THEN '8'
   WHEN qtr.channel_subdiv_demand >= 0.6 * qtr.demand_channel_subdiv
   THEN '7'
   WHEN qtr.channel_subdiv_demand >= 0.5 * qtr.demand_channel_subdiv
   THEN '6'
   WHEN qtr.channel_subdiv_demand >= 0.4 * qtr.demand_channel_subdiv
   THEN '5'
   WHEN qtr.channel_subdiv_demand >= 0.3 * qtr.demand_channel_subdiv
   THEN '4'
   WHEN qtr.channel_subdiv_demand >= 0.2 * qtr.demand_channel_subdiv
   THEN '3'
   WHEN qtr.channel_subdiv_demand >= 0.1 * qtr.demand_channel_subdiv
   THEN '2'
   ELSE '1'
   END AS qtr_channel_subdiv_demand_deciles,
   CASE
   WHEN qtr.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN qtr.channel_subdiv_sales >= 0.9 * qtr.sales_channel_subdiv
   THEN '10'
   WHEN qtr.channel_subdiv_sales >= 0.8 * qtr.sales_channel_subdiv
   THEN '9'
   WHEN qtr.channel_subdiv_sales >= 0.7 * qtr.sales_channel_subdiv
   THEN '8'
   WHEN qtr.channel_subdiv_sales >= 0.6 * qtr.sales_channel_subdiv
   THEN '7'
   WHEN qtr.channel_subdiv_sales >= 0.5 * qtr.sales_channel_subdiv
   THEN '6'
   WHEN qtr.channel_subdiv_sales >= 0.4 * qtr.sales_channel_subdiv
   THEN '5'
   WHEN qtr.channel_subdiv_sales >= 0.3 * qtr.sales_channel_subdiv
   THEN '4'
   WHEN qtr.channel_subdiv_sales >= 0.2 * qtr.sales_channel_subdiv
   THEN '3'
   WHEN qtr.channel_subdiv_sales >= 0.1 * qtr.sales_channel_subdiv
   THEN '2'
   ELSE '1'
   END AS qtr_channel_subdiv_sales_deciles,
   CASE
   WHEN qtr.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN qtr.channel_div_demand >= 0.9 * qtr.demand_channel_div
   THEN '10'
   WHEN qtr.channel_div_demand >= 0.8 * qtr.demand_channel_div
   THEN '9'
   WHEN qtr.channel_div_demand >= 0.7 * qtr.demand_channel_div
   THEN '8'
   WHEN qtr.channel_div_demand >= 0.6 * qtr.demand_channel_div
   THEN '7'
   WHEN qtr.channel_div_demand >= 0.5 * qtr.demand_channel_div
   THEN '6'
   WHEN qtr.channel_div_demand >= 0.4 * qtr.demand_channel_div
   THEN '5'
   WHEN qtr.channel_div_demand >= 0.3 * qtr.demand_channel_div
   THEN '4'
   WHEN qtr.channel_div_demand >= 0.2 * qtr.demand_channel_div
   THEN '3'
   WHEN qtr.channel_div_demand >= 0.1 * qtr.demand_channel_div
   THEN '2'
   ELSE '1'
   END AS qtr_channel_div_demand_deciles,
   CASE
   WHEN qtr.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN qtr.channel_div_sales >= 0.9 * qtr.sales_channel_div
   THEN '10'
   WHEN qtr.channel_div_sales >= 0.8 * qtr.sales_channel_div
   THEN '9'
   WHEN qtr.channel_div_sales >= 0.7 * qtr.sales_channel_div
   THEN '8'
   WHEN qtr.channel_div_sales >= 0.6 * qtr.sales_channel_div
   THEN '7'
   WHEN qtr.channel_div_sales >= 0.5 * qtr.sales_channel_div
   THEN '6'
   WHEN qtr.channel_div_sales >= 0.4 * qtr.sales_channel_div
   THEN '5'
   WHEN qtr.channel_div_sales >= 0.3 * qtr.sales_channel_div
   THEN '4'
   WHEN qtr.channel_div_sales >= 0.2 * qtr.sales_channel_div
   THEN '3'
   WHEN qtr.channel_div_sales >= 0.1 * qtr.sales_channel_div
   THEN '2'
   ELSE '1'
   END AS qtr_channel_div_sales_deciles,
   CASE
   WHEN qtr.demand_dollars <= 0
   THEN 'Demand <=0'
   WHEN qtr.channel_demand >= 0.9 * qtr.demand_channel
   THEN '10'
   WHEN qtr.channel_demand >= 0.8 * qtr.demand_channel
   THEN '9'
   WHEN qtr.channel_demand >= 0.7 * qtr.demand_channel
   THEN '8'
   WHEN qtr.channel_demand >= 0.6 * qtr.demand_channel
   THEN '7'
   WHEN qtr.channel_demand >= 0.5 * qtr.demand_channel
   THEN '6'
   WHEN qtr.channel_demand >= 0.4 * qtr.demand_channel
   THEN '5'
   WHEN qtr.channel_demand >= 0.3 * qtr.demand_channel
   THEN '4'
   WHEN qtr.channel_demand >= 0.2 * qtr.demand_channel
   THEN '3'
   WHEN qtr.channel_demand >= 0.1 * qtr.demand_channel
   THEN '2'
   ELSE '1'
   END AS qtr_channel_demand_deciles,
   CASE
   WHEN qtr.sales_dollars <= 0
   THEN 'Sales <=0'
   WHEN qtr.channel_sales >= 0.9 * qtr.sales_channel
   THEN '10'
   WHEN qtr.channel_sales >= 0.8 * qtr.sales_channel
   THEN '9'
   WHEN qtr.channel_sales >= 0.7 * qtr.sales_channel
   THEN '8'
   WHEN qtr.channel_sales >= 0.6 * qtr.sales_channel
   THEN '7'
   WHEN qtr.channel_sales >= 0.5 * qtr.sales_channel
   THEN '6'
   WHEN qtr.channel_sales >= 0.4 * qtr.sales_channel
   THEN '5'
   WHEN qtr.channel_sales >= 0.3 * qtr.sales_channel
   THEN '4'
   WHEN qtr.channel_sales >= 0.2 * qtr.sales_channel
   THEN '3'
   WHEN qtr.channel_sales >= 0.1 * qtr.sales_channel
   THEN '2'
   ELSE '1'
   END AS qtr_channel_sales_deciles,
  SUM(d.sales_dollars) AS sales_dollars,
  SUM(d.demand_dollars) AS demand_dollars,
  SUM(d.demand_units) AS demand_units,
  SUM(d.sales_units) AS sales_units,
  ROUND(CAST(SUM(d.receipt_dollars) AS NUMERIC), 2) AS receipt_dollars,
  SUM(d.receipt_units) AS receipt_units,
  CAST(SUM(COALESCE(qtr.eoh_dollars, 0)) AS NUMERIC) AS qtr_eoh_dollars,
  CAST(SUM(COALESCE(qtr.eoh_units, 0)) AS NUMERIC) AS qtr_eoh_units,
  CAST(SUM(COALESCE(qtr.boh_dollars, 0)) AS NUMERIC) AS qtr_boh_dollars,
  CAST(SUM(COALESCE(qtr.boh_units, 0)) AS NUMERIC) AS qtr_boh_units,
  CAST(SUM(d.eoh_dollars) AS NUMERIC) AS eoh_dollars,
  CAST(SUM(d.eoh_units) AS NUMERIC) AS eoh_units,
  CAST(SUM(d.boh_dollars) AS NUMERIC) AS boh_dollars,
  CAST(SUM(d.boh_units) AS NUMERIC) AS boh_units,
  SUM(d.product_views) AS product_views,
  SUM(d.order_sessions) AS order_sessions,
  SUM(d.instock_views) AS instock_views,
  CAST(SUM(d.product_view_sessions) AS INTEGER) AS product_view_sessions,
  SUM(d.order_demand) AS order_demand,
  SUM(d.order_quantity) AS order_quantity
 FROM cc_month AS d
  LEFT JOIN cc_quarter AS qtr ON d.quarter_idnt = qtr.quarter_idnt AND LOWER(d.channel) = LOWER(qtr.channel) AND LOWER(d
           .channel_country) = LOWER(qtr.channel_country) AND LOWER(d.channel_brand) = LOWER(qtr.channel_brand) AND d.div_idnt
         = qtr.div_idnt AND d.dept_idnt = qtr.dept_idnt AND LOWER(d.customer_choice) = LOWER(qtr.customer_choice) AND
     LOWER(d.supp_part_num) = LOWER(qtr.supp_part_num) AND LOWER(d.supplier_name) = LOWER(qtr.supplier_name)
 GROUP BY d.month_idnt,
  d.month_abrv,
  d.quarter_idnt,
  d.half_idnt,
  d.year_idnt,
  d.customer_choice,
  d.channel,
  d.channel_brand,
  d.channel_country,
  d.div_idnt,
  d.div_desc,
  d.subdiv_idnt,
  d.subdiv_desc,
  d.dept_idnt,
  d.dept_desc,
  d.supplier_name,
  d.supp_part_num,
  d.quantrix_category,
  d.ccs_category,
  d.ccs_subcategory,
  d.nord_role_desc,
  d.rack_role_desc,
  d.parent_group,
  d.days_live,
  d.days_published,
  d.new_flag,
  d.cf_flag,
  npg_flag,
  d.rp_flag,
  d.dropship_flag,
  d.anniversary_flag,
  fanatics_flag,
  channel_dept_cat_demand_deciles,
  channel_dept_cat_sales_deciles,
  channel_dept_demand_deciles,
  channel_dept_sales_deciles,
  channel_subdiv_demand_deciles,
  channel_subdiv_sales_deciles,
  channel_div_demand_deciles,
  channel_div_sales_deciles,
  channel_demand_deciles,
  channel_sales_deciles,
  qtr_channel_dept_cat_demand_deciles,
  qtr_channel_dept_cat_sales_deciles,
  qtr_channel_dept_demand_deciles,
  qtr_channel_dept_sales_deciles,
  qtr_channel_subdiv_demand_deciles,
  qtr_channel_subdiv_sales_deciles,
  qtr_channel_div_demand_deciles,
  qtr_channel_div_sales_deciles,
  qtr_channel_demand_deciles,
  qtr_channel_sales_deciles);