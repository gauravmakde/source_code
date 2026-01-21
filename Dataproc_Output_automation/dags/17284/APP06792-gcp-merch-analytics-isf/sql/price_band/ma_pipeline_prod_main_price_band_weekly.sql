CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup
----CLUSTER BY week_idnt
AS
SELECT re.ty_ly_lly_ind AS ty_ly_ind,
 re.fiscal_year_num,
 re.fiscal_halfyear_num,
 re.quarter_abrv,
 re.quarter_label,
 re.month_idnt,
 re.month_abrv,
 re.month_label,
 re.month_start_day_date,
 re.month_end_day_date,
 dcd.week_idnt AS week_idnt_true,
 re.week_idnt,
 re.fiscal_week_num,
   TRIM(FORMAT('%11d', re.fiscal_year_num)) || ' ' || re.week_desc AS week_label,
    re.month_abrv || ' ' || 'WK' || TRIM(FORMAT('%11d', re.week_num_of_fiscal_month)) AS week_of_month_abrv,
   SUBSTR(re.week_label, 0, 9) || 'WK' || TRIM(FORMAT('%11d', re.week_num_of_fiscal_month)) AS week_of_month_label,
 re.week_start_day_date,
 re.week_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS re
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd ON re.day_date = dcd.day_date
WHERE LOWER(re.ty_ly_lly_ind) IN (LOWER('TY'), LOWER('LY'))
 AND re.day_date = re.week_end_day_date
 AND re.week_end_day_date <= DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY);




CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup
----CLUSTER BY store_num
AS
SELECT DISTINCT RPAD(CAST(store_num AS STRING), 4, ' ') AS store_num,
 channel_num,
   TRIM(FORMAT('%11d', channel_num)) || ', ' || channel_desc AS channel_label,
 selling_channel AS channel_type,
 channel_brand AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(store_country_code) = LOWER('US')
 AND (store_close_date IS NULL OR store_close_date >= (SELECT MIN(week_start_day_date)
     FROM date_lkup))
 AND channel_num NOT IN (140, 240, 920, 930, 940, 990)
 AND channel_num IS NOT NULL;





CREATE TEMPORARY TABLE IF NOT EXISTS category_lkup
----CLUSTER BY dept_num, class_num, sbclass_num
AS
SELECT DISTINCT h.dept_num,
 h.class_num,
 h.sbclass_num,
 COALESCE(cat1.category, cat2.category, 'UNKNOWN') AS quantrix_category,
 COALESCE(cat1.category_group, cat2.category_group, 'UNKNOWN') AS quantrix_category_group
FROM (SELECT DISTINCT dept_num,
   class_num,
   sbclass_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  WHERE LOWER(channel_country) = LOWER('US')) AS h
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat1 ON h.dept_num = cat1.dept_num AND h.class_num = CAST(cat1.class_num AS FLOAT64)
     AND h.sbclass_num = CAST(cat1.sbclass_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat2 ON h.dept_num = cat2.dept_num AND h.class_num = CAST(cat2.class_num AS FLOAT64)
     AND CAST(cat2.sbclass_num AS FLOAT64) = - 1;



CREATE TEMPORARY TABLE IF NOT EXISTS sku_lkup
----CLUSTER BY rms_sku_num
AS
SELECT DISTINCT sku.rms_sku_num,
   TRIM(FORMAT('%11d', sku.div_num)) || ', ' || TRIM(sku.div_desc) AS div_label,
   TRIM(FORMAT('%11d', sku.grp_num)) || ', ' || TRIM(sku.grp_desc) AS subdiv_label,
   TRIM(FORMAT('%11d', sku.dept_num)) || ', ' || TRIM(sku.dept_desc) AS dept_label,
   TRIM(FORMAT('%11d', sku.class_num)) || ', ' || TRIM(sku.class_desc) AS class_label,
   TRIM(FORMAT('%11d', sku.sbclass_num)) || ', ' || TRIM(sku.sbclass_desc) AS subclass_label,
 sku.div_num,
 sku.grp_num AS subdiv_num,
 sku.dept_num,
 sku.class_num,
 sku.sbclass_num AS subclass_num,
 cat.quantrix_category,
 cat.quantrix_category_group,
 COALESCE(sku.prmy_supp_num, FORMAT('%11d', - 1)) AS supplier_number,
 supp.vendor_name AS supplier_name,
 COALESCE(sku.brand_name, 'UNKNOWN') AS brand_name,
 sku.npg_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(sku.prmy_supp_num) = LOWER(supp.vendor_num)
 LEFT JOIN category_lkup AS cat ON sku.dept_num = cat.dept_num AND sku.class_num = cat.class_num AND sku.sbclass_num =
   cat.sbclass_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dd ON sku.dept_num = dd.dept_num AND LOWER(dd.active_store_ind) = LOWER('A'
    )
WHERE LOWER(sku.channel_country) = LOWER('US')
 AND LOWER(sku.smart_sample_ind) <> LOWER('Y')
 AND LOWER(sku.gwp_ind) <> LOWER('Y');


/*
 * PRICE BAND LOOKUP
 * 
 */


--drop table price_band_lkup;


CREATE TEMPORARY TABLE IF NOT EXISTS price_band_lkup
----CLUSTER BY next_highest_dollar
AS
SELECT *
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.price_bands_by_dollar;




CREATE TEMPORARY TABLE IF NOT EXISTS sales_reg
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT s.rms_sku_num,
 s.store_num,
 dt.week_idnt,
 s.rp_ind,
 s.dropship_ind AS ds_ind,
 RPAD('R', 1, ' ') AS price_type,
 COALESCE(s.net_sales_tot_regular_retl, 0) AS net_sales_tot_retl,
 COALESCE(s.net_sales_tot_regular_cost, 0) AS net_sales_tot_cost,
 COALESCE(s.net_sales_tot_regular_units, 0) AS net_sales_tot_units,
 COALESCE(CASE
   WHEN LOWER(s.wac_avlbl_ind) = LOWER('Y')
   THEN s.net_sales_tot_regular_retl
   ELSE NULL
   END, 0) AS net_sales_tot_retl_with_cost,
 CEIL(s.net_sales_tot_retl * 1.00 / (s.net_sales_tot_units * 1.00)) AS sales_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
 INNER JOIN date_lkup AS dt ON s.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON s.store_num = CAST(store.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(s.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE s.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND s.net_sales_tot_units <> 0;



CREATE TEMPORARY TABLE IF NOT EXISTS sales_clr
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT s.rms_sku_num,
 s.store_num,
 dt.week_idnt,
 s.rp_ind,
 s.dropship_ind AS ds_ind,
 RPAD('C', 1, ' ') AS price_type,
 COALESCE(s.net_sales_tot_clearance_retl, 0) AS net_sales_tot_retl,
 COALESCE(s.net_sales_tot_clearance_cost, 0) AS net_sales_tot_cost,
 COALESCE(s.net_sales_tot_clearance_units, 0) AS net_sales_tot_units,
 COALESCE(CASE
   WHEN LOWER(s.wac_avlbl_ind) = LOWER('Y')
   THEN s.net_sales_tot_clearance_retl
   ELSE NULL
   END, 0) AS net_sales_tot_retl_with_cost,
 CEIL(s.net_sales_tot_retl * 1.00 / (s.net_sales_tot_units * 1.00)) AS sales_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
 INNER JOIN date_lkup AS dt ON s.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON s.store_num = CAST(store.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(s.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE s.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND s.net_sales_tot_units <> 0;


/* 
 * SALES PROMO
 * 
 * Promo only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures 
 * NAP table has RP and DS indicator so bring it in here rather than joining to separate RP table 
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 * 
 */


--drop table sales_promo


-- for calculating product margin we need to use only sales where item cost is known


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


CREATE TEMPORARY TABLE IF NOT EXISTS sales_pro
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT s.rms_sku_num,
 s.store_num,
 dt.week_idnt,
 s.rp_ind,
 s.dropship_ind AS ds_ind,
 RPAD('P', 1, ' ') AS price_type,
 COALESCE(s.net_sales_tot_promo_retl, 0) AS net_sales_tot_retl,
 COALESCE(s.net_sales_tot_promo_cost, 0) AS net_sales_tot_cost,
 COALESCE(s.net_sales_tot_promo_units, 0) AS net_sales_tot_units,
 COALESCE(CASE
   WHEN LOWER(s.wac_avlbl_ind) = LOWER('Y')
   THEN s.net_sales_tot_promo_retl
   ELSE NULL
   END, 0) AS net_sales_tot_retl_with_cost,
 CEIL(s.net_sales_tot_retl * 1.00 / (s.net_sales_tot_units * 1.00)) AS sales_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
 INNER JOIN date_lkup AS dt ON s.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON s.store_num = CAST(store.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(s.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE s.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND s.net_sales_tot_units <> 0;


/*
 * SALES STAGING BASE
 * 
 * Resulting table will be at SKU + Loc + Week grain
 * RP_ind and Price Type are also included but not needed to define a unique row
 * We'll use this as the fact table for bringing in all our other dimensions
 * 
 */


--drop table sales_stg_base;


CREATE TEMPORARY TABLE IF NOT EXISTS sales_stg_base
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT *
FROM sales_reg
UNION ALL
SELECT *
FROM sales_clr
UNION ALL
SELECT *
FROM sales_pro;


-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point


/*
 *  SALES STAGING
 * 
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band 
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *  
 */


--drop table sales_stg;


-- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band


-- staging data uses realigned week_idnt so we join on that instead of week_idnt_true


-- left join because lookup table does not contain all possible price values


-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique


-- set index on dimensions that will eventually define a unique row after grouping in a later step


CREATE TEMPORARY TABLE IF NOT EXISTS sales_stg
----CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT dt.week_idnt,
 s.banner,
 s.channel_type,
 s.channel_label,
 s.channel_num,
 sku.div_label,
 sku.subdiv_label,
 sku.dept_label,
 sku.class_label,
 sku.subclass_label,
 sku.div_num,
 sku.subdiv_num,
 sku.dept_num,
 sku.class_num,
 sku.subclass_num,
 sku.quantrix_category,
 sku.quantrix_category_group,
 sku.supplier_number,
 sku.supplier_name,
 sku.brand_name,
 sku.npg_ind,
 b.rp_ind,
 b.ds_ind,
 b.price_type,
 COALESCE(pb.price_band_one_lower_bound, (SELECT MAX(price_band_one_lower_bound)
   FROM price_band_lkup)) AS price_band_one_lower_bound,
 COALESCE(pb.price_band_one_upper_bound, (SELECT MAX(price_band_one_upper_bound)
   FROM price_band_lkup)) AS price_band_one_upper_bound,
 COALESCE(pb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
   FROM price_band_lkup)) AS price_band_two_lower_bound,
 COALESCE(pb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
   FROM price_band_lkup)) AS price_band_two_upper_bound,
 b.net_sales_tot_retl,
 b.net_sales_tot_cost,
 b.net_sales_tot_units,
 b.net_sales_tot_retl_with_cost
FROM sales_stg_base AS b
 INNER JOIN date_lkup AS dt ON b.week_idnt = dt.week_idnt
 INNER JOIN store_lkup AS s ON b.store_num = CAST(s.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(b.rms_sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN price_band_lkup AS pb ON b.sales_price_next_highest_dollar = pb.next_highest_dollar;


/*
 * SALES FINAL
 * 
 * Aggregate up to desired level for use in final data set
 *  
 */


--drop table sales;


CREATE TEMPORARY TABLE IF NOT EXISTS sales
----CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound,
 SUM(net_sales_tot_units) AS sales_u,
 SUM(net_sales_tot_cost) AS sales_c,
 SUM(net_sales_tot_retl) AS sales_r,
 SUM(net_sales_tot_retl_with_cost) AS sales_r_with_cost,
 0 AS demand_u,
 0 AS demand_r,
 0 AS eoh_u,
 0 AS eoh_c,
 0 AS eoh_r,
 0 AS eop_u,
 0 AS eop_c,
 0 AS eop_r,
 0 AS oo_4wk_u,
 0 AS oo_4wk_c,
 0 AS oo_4wk_r,
 0 AS oo_5wk_13wk_u,
 0 AS oo_5wk_13wk_c,
 0 AS oo_5wk_13wk_r,
 0 AS oo_14wk_u,
 0 AS oo_14wk_c,
 0 AS oo_14wk_r,
 0 AS rp_ant_spd_4wk_u,
 0 AS rp_ant_spd_4wk_c,
 0 AS rp_ant_spd_4wk_r,
 0 AS rp_ant_spd_5wk_13wk_u,
 0 AS rp_ant_spd_5wk_13wk_c,
 0 AS rp_ant_spd_5wk_13wk_r,
 0 AS rp_ant_spd_14wk_u,
 0 AS rp_ant_spd_14wk_c,
 0 AS rp_ant_spd_14wk_r
FROM sales_stg AS s
GROUP BY week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound;


/*
 * SALES CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS sales_reg;


DROP TABLE IF EXISTS sales_clr;


DROP TABLE IF EXISTS sales_pro;


DROP TABLE IF EXISTS sales_stg_base;


DROP TABLE IF EXISTS sales_stg;


/* 
 * DEMAND REG PRICE
 * 
 * Reg price only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures 
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table 
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 * 
 */


--drop table demand_reg;


-- dropship = Y


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


-- dropship = N


-- Calculate demand non-DS = total demand - demand DS


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


CREATE TEMPORARY TABLE IF NOT EXISTS demand_reg
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT d.rms_sku_num,
 d.store_num,
 dt.week_idnt,
 d.rp_ind,
 RPAD('R', 1, ' ') AS price_type,
 RPAD('Y', 1, ' ') AS ds_ind,
 COALESCE(d.jwn_demand_dropship_fulfilled_regular_retail_amt_ty, 0) AS demand_tot_retl,
 COALESCE(d.jwn_demand_dropship_fulfilled_regular_units_ty, 0) AS demand_tot_units,
 CEIL(COALESCE(d.jwn_demand_dropship_fulfilled_regular_retail_amt_ty, 0) * 1.00 / (COALESCE(d.jwn_demand_dropship_fulfilled_regular_units_ty
      , 0) * 1.00)) AS demand_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d
 INNER JOIN date_lkup AS dt ON d.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON d.store_num = CAST(store.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(d.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE d.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND COALESCE(d.jwn_demand_dropship_fulfilled_regular_units_ty, 0) <> 0
UNION ALL
SELECT d0.rms_sku_num,
 d0.store_num,
 dt0.week_idnt,
 d0.rp_ind,
 RPAD('R', 1, ' ') AS price_type,
 RPAD('N', 1, ' ') AS ds_ind,
  COALESCE(d0.jwn_demand_regular_retail_amt_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_retail_amt_ty, 0
   ) AS demand_tot_retl,
  COALESCE(d0.jwn_demand_regular_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_units_ty, 0) AS
 demand_tot_units,
 CEIL((COALESCE(d0.jwn_demand_regular_retail_amt_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_retail_amt_ty
       , 0)) * 1.00 / ((COALESCE(d0.jwn_demand_regular_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_units_ty
        , 0)) * 1.00)) AS demand_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d0
 INNER JOIN date_lkup AS dt0 ON d0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup AS store0 ON d0.store_num = CAST(store0.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku0 ON LOWER(d0.rms_sku_num) = LOWER(sku0.rms_sku_num)
WHERE d0.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND COALESCE(d0.jwn_demand_regular_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_units_ty, 0) <> 0;


/* 
 * DEMAND CLEARANCE
 * 
 * Clearance only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures 
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table 
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 * 
 */


--drop table demand_clr;


--dropship = Y


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


-- dropship = N


-- Calculate demand non-DS = total demand - demand DS


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


CREATE TEMPORARY TABLE IF NOT EXISTS demand_clr
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT d.rms_sku_num,
 d.store_num,
 dt.week_idnt,
 d.rp_ind,
 RPAD('C', 1, ' ') AS price_type,
 RPAD('Y', 1, ' ') AS ds_ind,
 COALESCE(d.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty, 0) AS demand_tot_retl,
 COALESCE(d.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) AS demand_tot_units,
 CEIL(COALESCE(d.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty, 0) * 1.00 / (COALESCE(d.jwn_demand_dropship_fulfilled_clearance_units_ty
      , 0) * 1.00)) AS demand_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d
 INNER JOIN date_lkup AS dt ON d.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON d.store_num = CAST(store.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(d.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE d.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND COALESCE(d.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) <> 0
UNION ALL
SELECT d0.rms_sku_num,
 d0.store_num,
 dt0.week_idnt,
 d0.rp_ind,
 RPAD('C', 1, ' ') AS price_type,
 RPAD('N', 1, ' ') AS ds_ind,
  COALESCE(d0.jwn_demand_clearance_retail_amt_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty
   , 0) AS demand_tot_retl,
  COALESCE(d0.jwn_demand_clearance_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) AS
 demand_tot_units,
 CEIL((COALESCE(d0.jwn_demand_clearance_retail_amt_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty
       , 0)) * 1.00 / ((COALESCE(d0.jwn_demand_clearance_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_clearance_units_ty
        , 0)) * 1.00)) AS demand_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d0
 INNER JOIN date_lkup AS dt0 ON d0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup AS store0 ON d0.store_num = CAST(store0.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku0 ON LOWER(d0.rms_sku_num) = LOWER(sku0.rms_sku_num)
WHERE d0.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND COALESCE(d0.jwn_demand_clearance_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) <>
  0;


/* 
 * DEMAND PROMO
 * 
 * Promo only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures 
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table 
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 * 
 */


--drop table demand_pro;


-- dropship = Y


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


-- dropship = N


-- Calculate demand non-DS = total demand - demand DS


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


CREATE TEMPORARY TABLE IF NOT EXISTS demand_pro
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT d.rms_sku_num,
 d.store_num,
 dt.week_idnt,
 d.rp_ind,
 RPAD('P', 1, ' ') AS price_type,
 RPAD('Y', 1, ' ') AS ds_ind,
 COALESCE(d.jwn_demand_dropship_fulfilled_promo_retail_amt_ty, 0) AS demand_tot_retl,
 COALESCE(d.jwn_demand_dropship_fulfilled_promo_units_ty, 0) AS demand_tot_units,
 CEIL(COALESCE(d.jwn_demand_dropship_fulfilled_promo_retail_amt_ty, 0) * 1.00 / (COALESCE(d.jwn_demand_dropship_fulfilled_promo_units_ty
      , 0) * 1.00)) AS demand_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d
 INNER JOIN date_lkup AS dt ON d.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON d.store_num = CAST(store.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(d.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE d.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND COALESCE(d.jwn_demand_dropship_fulfilled_promo_units_ty, 0) <> 0
UNION ALL
SELECT d0.rms_sku_num,
 d0.store_num,
 dt0.week_idnt,
 d0.rp_ind,
 RPAD('P', 1, ' ') AS price_type,
 RPAD('N', 1, ' ') AS ds_ind,
  COALESCE(d0.jwn_demand_promo_retail_amt_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_promo_retail_amt_ty, 0) AS
 demand_tot_retl,
  COALESCE(d0.jwn_demand_promo_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_promo_units_ty, 0) AS
 demand_tot_units,
 CEIL((COALESCE(d0.jwn_demand_promo_retail_amt_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_promo_retail_amt_ty, 0
       )) * 1.00 / ((COALESCE(d0.jwn_demand_promo_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_promo_units_ty
        , 0)) * 1.00)) AS demand_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d0
 INNER JOIN date_lkup AS dt0 ON d0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup AS store0 ON d0.store_num = CAST(store0.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku0 ON LOWER(d0.rms_sku_num) = LOWER(sku0.rms_sku_num)
WHERE d0.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup)
 AND COALESCE(d0.jwn_demand_promo_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_promo_units_ty, 0) <> 0;


/*
 * DEMAND STAGING BASE
 * 
 * Resulting table will be at SKU + Loc + Week grain
 * RP_ind and Price Type are also included but not needed to define a unique row
 * We'll use this as the fact table for bringing in all our other dimensions
 * 
 */


--drop table demand_stg_base;


CREATE TEMPORARY TABLE IF NOT EXISTS demand_stg_base
----CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT *
FROM demand_reg
UNION ALL
SELECT *
FROM demand_clr
UNION ALL
SELECT *
FROM demand_pro;


-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point


/*
 * DEMAND STAGING
 * 
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band 
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *  
 */


--drop table demand_stg;


-- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band


-- staging data uses realigned week_idnt so we join on that instead of week_idnt_true


-- left join because lookup table does not contain all possible price values


-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique


-- set index on dimensions that will eventually define a unique row after grouping in a later step


CREATE TEMPORARY TABLE IF NOT EXISTS demand_stg
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT dt.week_idnt,
 s.banner,
 s.channel_type,
 s.channel_label,
 s.channel_num,
 sku.div_label,
 sku.subdiv_label,
 sku.dept_label,
 sku.class_label,
 sku.subclass_label,
 sku.div_num,
 sku.subdiv_num,
 sku.dept_num,
 sku.class_num,
 sku.subclass_num,
 sku.quantrix_category,
 sku.quantrix_category_group,
 sku.supplier_number,
 sku.supplier_name,
 sku.brand_name,
 sku.npg_ind,
 b.rp_ind,
 b.ds_ind,
 b.price_type,
 COALESCE(pb.price_band_one_lower_bound, (SELECT MAX(price_band_one_lower_bound)
   FROM price_band_lkup)) AS price_band_one_lower_bound,
 COALESCE(pb.price_band_one_upper_bound, (SELECT MAX(price_band_one_upper_bound)
   FROM price_band_lkup)) AS price_band_one_upper_bound,
 COALESCE(pb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
   FROM price_band_lkup)) AS price_band_two_lower_bound,
 COALESCE(pb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
   FROM price_band_lkup)) AS price_band_two_upper_bound,
 b.demand_tot_retl,
 b.demand_tot_units
FROM demand_stg_base AS b
 INNER JOIN date_lkup AS dt ON b.week_idnt = dt.week_idnt
 INNER JOIN store_lkup AS s ON b.store_num = CAST(s.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(b.rms_sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN price_band_lkup AS pb ON b.demand_price_next_highest_dollar = pb.next_highest_dollar;


/*
 * DEMAND FINAL
 * 
 * Aggregate up to desired level for use in final data set
 *  
 */


--drop table demand;


CREATE TEMPORARY TABLE IF NOT EXISTS demand
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound,
 0 AS sales_u,
 0 AS sales_c,
 0 AS sales_r,
 0 AS sales_r_with_cost,
 SUM(demand_tot_units) AS demand_u,
 SUM(demand_tot_retl) AS demand_r,
 0 AS eoh_u,
 0 AS eoh_c,
 0 AS eoh_r,
 0 AS eop_u,
 0 AS eop_c,
 0 AS eop_r,
 0 AS oo_4wk_u,
 0 AS oo_4wk_c,
 0 AS oo_4wk_r,
 0 AS oo_5wk_13wk_u,
 0 AS oo_5wk_13wk_c,
 0 AS oo_5wk_13wk_r,
 0 AS oo_14wk_u,
 0 AS oo_14wk_c,
 0 AS oo_14wk_r,
 0 AS rp_ant_spd_4wk_u,
 0 AS rp_ant_spd_4wk_c,
 0 AS rp_ant_spd_4wk_r,
 0 AS rp_ant_spd_5wk_13wk_u,
 0 AS rp_ant_spd_5wk_13wk_c,
 0 AS rp_ant_spd_5wk_13wk_r,
 0 AS rp_ant_spd_14wk_u,
 0 AS rp_ant_spd_14wk_c,
 0 AS rp_ant_spd_14wk_r
FROM demand_stg AS d
GROUP BY week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound;


/*
 * DEMAND CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS demand_reg;


DROP TABLE IF EXISTS demand_clr;


DROP TABLE IF EXISTS demand_pro;


DROP TABLE IF EXISTS demand_stg_base;


DROP TABLE IF EXISTS demand_stg;


/*
 * At initial launch we created Inventory Reg and Inventory Clr, then unioned them, then joined to price band table, then aggregated.
 * As fiscal year end approached we hit CPU timeout errors at the step of unioning Reg & Clr.
 * To get around that, we changed the sequence to join to price band table and aggregate each of Reg and Clr first, then union the aggregated Reg & Clr as the final step.
 */


/*
 * INVENTORY REG BASE
 * 
 * Reg price only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures 
 * Select relevant inventory fields and calculate price, keeping data at same grain as NAP fact table
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table 
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * 
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 * 
 */


--drop table inventory_reg_base;


-- in some situations eop = 0 but eoh <> 0, so we need a price for those situations


-- price will be positive even if inventory is negative (both R and U will be negative)


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


-- only keep records that have inventory


-- adding condition on partitioned field for better performance (logically redundant due to inner join)


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_reg_base
--CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT i.rms_sku_num,
 i.store_num,
 dt.week_idnt,
 i.rp_ind,
 RPAD('R', 1, ' ') AS price_type,
 COALESCE(i.eoh_regular_units, 0) AS eoh_u,
 COALESCE(i.eoh_regular_cost, 0) AS eoh_c,
 COALESCE(i.eoh_regular_retail, 0) AS eoh_r,
  COALESCE(i.eoh_regular_units, 0) + COALESCE(i.eoh_in_transit_regular_units, 0) AS eop_u,
  COALESCE(i.eoh_regular_cost, 0) + COALESCE(i.eoh_in_transit_regular_cost, 0) AS eop_c,
  COALESCE(i.eoh_regular_retail, 0) + COALESCE(i.eoh_in_transit_regular_retail, 0) AS eop_r,
 CEIL(CASE
   WHEN COALESCE(i.eoh_regular_units, 0) + COALESCE(i.eoh_in_transit_regular_units, 0) = 0
   THEN COALESCE(i.eoh_regular_retail, 0) * 1.00 / COALESCE(i.eoh_regular_units, 0) * 1.00
   ELSE (COALESCE(i.eoh_regular_retail, 0) + COALESCE(i.eoh_in_transit_regular_retail, 0)) * 1.00 / (COALESCE(i.eoh_regular_units
        , 0) + COALESCE(i.eoh_in_transit_regular_units, 0)) * 1.00
   END) AS inv_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i
 INNER JOIN date_lkup AS dt ON i.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON LOWER(i.store_num) = LOWER(store.store_num)
 INNER JOIN sku_lkup AS sku ON LOWER(i.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE (COALESCE(i.eoh_regular_units, 0) <> 0 OR COALESCE(i.eoh_regular_units, 0) + COALESCE(i.eoh_in_transit_regular_units
      , 0) <> 0)
 AND i.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup);


/*
 * INVENTORY REG STAGING
 * 
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band 
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *  
 */


--drop table inventory_reg_stg;


-- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band


-- staging data uses realigned week_idnt so we join on that instead of week_idnt_true


-- left join because lookup table does not contain all possible price values


-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique


-- set index on dimensions that will eventually define a unique row after grouping in a later step


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_reg_stg
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT dt.week_idnt,
 s.banner,
 s.channel_type,
 s.channel_label,
 s.channel_num,
 sku.div_label,
 sku.subdiv_label,
 sku.dept_label,
 sku.class_label,
 sku.subclass_label,
 sku.div_num,
 sku.subdiv_num,
 sku.dept_num,
 sku.class_num,
 sku.subclass_num,
 sku.quantrix_category,
 sku.quantrix_category_group,
 sku.supplier_number,
 sku.supplier_name,
 sku.brand_name,
 sku.npg_ind,
 i.rp_ind,
 i.price_type,
 COALESCE(pb.price_band_one_lower_bound, (SELECT MAX(price_band_one_lower_bound)
   FROM price_band_lkup)) AS price_band_one_lower_bound,
 COALESCE(pb.price_band_one_upper_bound, (SELECT MAX(price_band_one_upper_bound)
   FROM price_band_lkup)) AS price_band_one_upper_bound,
 COALESCE(pb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
   FROM price_band_lkup)) AS price_band_two_lower_bound,
 COALESCE(pb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
   FROM price_band_lkup)) AS price_band_two_upper_bound,
 i.eoh_u,
 i.eoh_c,
 i.eoh_r,
 i.eop_u,
 i.eop_c,
 i.eop_r
FROM inventory_reg_base AS i
 INNER JOIN date_lkup AS dt ON i.week_idnt = dt.week_idnt
 INNER JOIN store_lkup AS s ON LOWER(i.store_num) = LOWER(s.store_num)
 INNER JOIN sku_lkup AS sku ON LOWER(i.rms_sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN price_band_lkup AS pb ON i.inv_price_next_highest_dollar = pb.next_highest_dollar;


/*
 * INVENTORY REG FINAL
 * 
 * Adding ds_ind dimension
 * Aggregate up to desired level for use in final data set
 *  
 */


--drop table inventory_reg_final;


-- placeholder for other measures (sales, demand, on order, etc)


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_reg_final
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound,
 0 AS sales_u,
 0 AS sales_c,
 0 AS sales_r,
 0 AS sales_r_with_cost,
 0 AS demand_u,
 0 AS demand_r,
 SUM(eoh_u) AS eoh_u,
 SUM(eoh_c) AS eoh_c,
 SUM(eoh_r) AS eoh_r,
 SUM(eop_u) AS eop_u,
 SUM(eop_c) AS eop_c,
 SUM(eop_r) AS eop_r,
 0 AS oo_4wk_u,
 0 AS oo_4wk_c,
 0 AS oo_4wk_r,
 0 AS oo_5wk_13wk_u,
 0 AS oo_5wk_13wk_c,
 0 AS oo_5wk_13wk_r,
 0 AS oo_14wk_u,
 0 AS oo_14wk_c,
 0 AS oo_14wk_r,
 0 AS rp_ant_spd_4wk_u,
 0 AS rp_ant_spd_4wk_c,
 0 AS rp_ant_spd_4wk_r,
 0 AS rp_ant_spd_5wk_13wk_u,
 0 AS rp_ant_spd_5wk_13wk_c,
 0 AS rp_ant_spd_5wk_13wk_r,
 0 AS rp_ant_spd_14wk_u,
 0 AS rp_ant_spd_14wk_c,
 0 AS rp_ant_spd_14wk_r
FROM inventory_reg_stg AS i
GROUP BY week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound;


/*
 * INVENTORY REG CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS inventory_reg_base;


DROP TABLE IF EXISTS inventory_reg_stg;


/*
 * INVENTORY CLR BASE
 * 
 * Clearance only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures 
 * Select relevant inventory fields and calculate price, keeping data at same grain as NAP fact table
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table 
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * 
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 * 
 */


--drop table inventory_clr_base;


-- in some situations eop = 0 but eoh <> 0, so we need a price for those situations


-- price will be positive even if inventory is negative (both R and U will be negative)


-- calculate price to two decimal places, then round up to next highest dollar for later join to price band table


-- only keep records that have inventory


-- adding condition on partitioned field for better performance (logically redundant due to inner join)


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_clr_base
--CLUSTER BY rms_sku_num, store_num, week_idnt
AS
SELECT i.rms_sku_num,
 i.store_num,
 dt.week_idnt,
 i.rp_ind,
 RPAD('C', 1, ' ') AS price_type,
 COALESCE(i.eoh_clearance_units, 0) AS eoh_u,
 COALESCE(i.eoh_clearance_cost, 0) AS eoh_c,
 COALESCE(i.eoh_clearance_retail, 0) AS eoh_r,
  COALESCE(i.eoh_clearance_units, 0) + COALESCE(i.eoh_in_transit_clearance_units, 0) AS eop_u,
  COALESCE(i.eoh_clearance_cost, 0) + COALESCE(i.eoh_in_transit_clearance_cost, 0) AS eop_c,
  COALESCE(i.eoh_clearance_retail, 0) + COALESCE(i.eoh_in_transit_clearance_retail, 0) AS eop_r,
 CEIL(CASE
   WHEN COALESCE(i.eoh_clearance_units, 0) + COALESCE(i.eoh_in_transit_clearance_units, 0) = 0
   THEN COALESCE(i.eoh_clearance_retail, 0) * 1.00 / COALESCE(i.eoh_clearance_units, 0) * 1.00
   ELSE (COALESCE(i.eoh_clearance_retail, 0) + COALESCE(i.eoh_in_transit_clearance_retail, 0)) * 1.00 / (COALESCE(i.eoh_clearance_units
        , 0) + COALESCE(i.eoh_in_transit_clearance_units, 0)) * 1.00
   END) AS inv_price_next_highest_dollar
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i
 INNER JOIN date_lkup AS dt ON i.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS s ON LOWER(i.store_num) = LOWER(s.store_num)
 INNER JOIN sku_lkup AS sku ON LOWER(i.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE (COALESCE(i.eoh_clearance_units, 0) <> 0 OR COALESCE(i.eoh_clearance_units, 0) + COALESCE(i.eoh_in_transit_clearance_units
      , 0) <> 0)
 AND i.week_num BETWEEN (SELECT MIN(week_idnt)
   FROM date_lkup) AND (SELECT MAX(week_idnt)
   FROM date_lkup);


/*
 * INVENTORY CLR STAGING
 * 
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band 
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *  
 */


--drop table inventory_clr_stg;


-- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band


-- staging data uses realigned week_idnt so we join on that instead of week_idnt_true


-- left join because lookup table does not contain all possible price values


-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique


-- set index on dimensions that will eventually define a unique row after grouping in a later step


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_clr_stg
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT dt.week_idnt,
 s.banner,
 s.channel_type,
 s.channel_label,
 s.channel_num,
 sku.div_label,
 sku.subdiv_label,
 sku.dept_label,
 sku.class_label,
 sku.subclass_label,
 sku.div_num,
 sku.subdiv_num,
 sku.dept_num,
 sku.class_num,
 sku.subclass_num,
 sku.quantrix_category,
 sku.quantrix_category_group,
 sku.supplier_number,
 sku.supplier_name,
 sku.brand_name,
 sku.npg_ind,
 i.rp_ind,
 i.price_type,
 COALESCE(pb.price_band_one_lower_bound, (SELECT MAX(price_band_one_lower_bound)
   FROM price_band_lkup)) AS price_band_one_lower_bound,
 COALESCE(pb.price_band_one_upper_bound, (SELECT MAX(price_band_one_upper_bound)
   FROM price_band_lkup)) AS price_band_one_upper_bound,
 COALESCE(pb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
   FROM price_band_lkup)) AS price_band_two_lower_bound,
 COALESCE(pb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
   FROM price_band_lkup)) AS price_band_two_upper_bound,
 i.eoh_u,
 i.eoh_c,
 i.eoh_r,
 i.eop_u,
 i.eop_c,
 i.eop_r
FROM inventory_clr_base AS i
 INNER JOIN date_lkup AS dt ON i.week_idnt = dt.week_idnt
 INNER JOIN store_lkup AS s ON LOWER(i.store_num) = LOWER(s.store_num)
 INNER JOIN sku_lkup AS sku ON LOWER(i.rms_sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN price_band_lkup AS pb ON i.inv_price_next_highest_dollar = pb.next_highest_dollar;


/*
 * INVENTORY CLR FINAL
 * 
 * Adding ds_ind dimension
 * Aggregate up to desired level for use in final data set
 *  
 */


--drop table inventory_clr_final;


-- placeholder for other measures (sales, demand, on order, etc)


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_clr_final
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound,
 0 AS sales_u,
 0 AS sales_c,
 0 AS sales_r,
 0 AS sales_r_with_cost,
 0 AS demand_u,
 0 AS demand_r,
 SUM(eoh_u) AS eoh_u,
 SUM(eoh_c) AS eoh_c,
 SUM(eoh_r) AS eoh_r,
 SUM(eop_u) AS eop_u,
 SUM(eop_c) AS eop_c,
 SUM(eop_r) AS eop_r,
 0 AS oo_4wk_u,
 0 AS oo_4wk_c,
 0 AS oo_4wk_r,
 0 AS oo_5wk_13wk_u,
 0 AS oo_5wk_13wk_c,
 0 AS oo_5wk_13wk_r,
 0 AS oo_14wk_u,
 0 AS oo_14wk_c,
 0 AS oo_14wk_r,
 0 AS rp_ant_spd_4wk_u,
 0 AS rp_ant_spd_4wk_c,
 0 AS rp_ant_spd_4wk_r,
 0 AS rp_ant_spd_5wk_13wk_u,
 0 AS rp_ant_spd_5wk_13wk_c,
 0 AS rp_ant_spd_5wk_13wk_r,
 0 AS rp_ant_spd_14wk_u,
 0 AS rp_ant_spd_14wk_c,
 0 AS rp_ant_spd_14wk_r
FROM inventory_clr_stg AS i
GROUP BY week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound;


/*
 * INVENTORY CLR CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS inventory_clr_base;


DROP TABLE IF EXISTS inventory_clr_stg;


/*
 * INVENTORY FINAL
 * 
 */


--drop table inventory;


CREATE TEMPORARY TABLE IF NOT EXISTS inventory
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT *
FROM inventory_reg_final
UNION ALL
SELECT *
FROM inventory_clr_final;


/*
 * INVENTORY FINAL CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS inventory_reg_final;


DROP TABLE IF EXISTS inventory_clr_final;


/*
 * ON ORDER STAGING
 * 
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 * Table will be at SKU + Loc but will include only dimensions we want in our final table
 * 
 * No join to date_lkup because we want to place all future On Order into the most recent completed week 
 *
 * oo_4wk column will have the data of <=4 weeks, oo_5wk_13wk column will have data for 5 to 13 weeks and oo_14wk column will have data
 * for >13 weeks
 *
 * Research in FY23 Week 37 showed that 0.04% of units (after joining to our lkup tables) had no value for anticipated_retail_amt.
 * Most of those units were beauty samples, beauty gift-with-purchase, or sunglasses cases - all items that will not be sold separately.
 * We decided that the quantity of missing-price data was negligible so did not create a price band for "Unknown".
 *  
 */


-- drop table on_order_stg;


-- we capture all future on order and assign it to most recent completed week


-- same rp_ind definition as WBR


-- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band


-- ,oo.quantity_open                                                             as on_order_units


-- ,cast((oo.quantity_open * oo.anticipated_retail_amt) AS decimal(18,2))        as on_order_retail


-- ,cast((oo.quantity_open * oo.unit_estimated_landing_cost) AS decimal(18,2))   as on_order_cost


-- left join because lookup table does not contain all possible price values


-- only get records that have a price so we can assign them to a price band


-- conditions below this point are same as in WBR as of Oct 6, 2023


-- only want items that haven't been received yet


-- only include approved POs


-- include closed POs with ship dates in the recent past


-- include POs that aren't closed yet


-- grain is still at SKU + Loc but we aren't bringing in those dimensions so index is not unique


-- set index on dimensions that will eventually define a unique row after grouping in a later step


CREATE TEMPORARY TABLE IF NOT EXISTS on_order_stg
--CLUSTER BY channel_num, dept_num, class_num, subclass_num
AS
SELECT (SELECT MAX(week_idnt)
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE week_end_day_date < current_date('PST8PDT')) AS week_idnt,
 s.banner,
 s.channel_type,
 s.channel_label,
 s.channel_num,
 sku.div_label,
 sku.subdiv_label,
 sku.dept_label,
 sku.class_label,
 sku.subclass_label,
 sku.div_num,
 sku.subdiv_num,
 sku.dept_num,
 sku.class_num,
 sku.subclass_num,
 sku.quantrix_category,
 sku.quantrix_category_group,
 sku.supplier_number,
 sku.supplier_name,
 sku.brand_name,
 sku.npg_ind,
 RPAD(CASE
   WHEN LOWER(oo.order_type) = LOWER('AUTOMATIC_REORDER')
   THEN 'Y'
   ELSE 'N'
   END, 1, ' ') AS rp_ind,
 oo.anticipated_price_type AS price_type,
 COALESCE(pb.price_band_one_lower_bound, (SELECT MAX(price_band_one_lower_bound)
   FROM price_band_lkup)) AS price_band_one_lower_bound,
 COALESCE(pb.price_band_one_upper_bound, (SELECT MAX(price_band_one_upper_bound)
   FROM price_band_lkup)) AS price_band_one_upper_bound,
 COALESCE(pb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
   FROM price_band_lkup)) AS price_band_two_lower_bound,
 COALESCE(pb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
   FROM price_band_lkup)) AS price_band_two_upper_bound,
  CASE
  WHEN DC.week_end_day_date < DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY)
  THEN oo.quantity_open
  ELSE NULL
  END AS on_order_4wk_units,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY) AND DC.week_end_day_date < DATE_ADD(current_date('PST8PDT')
     ,INTERVAL 98 DAY)
  THEN oo.quantity_open
  ELSE NULL
  END AS on_order_5wk_13wk_units,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 98 DAY)
  THEN oo.quantity_open
  ELSE NULL
  END AS on_order_14wk_units,
  CASE
  WHEN DC.week_end_day_date < DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY)
  THEN ROUND(CAST(oo.quantity_open * oo.unit_estimated_landing_cost AS NUMERIC), 2)
  ELSE NULL
  END AS on_order_4wk_cost,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY) AND DC.week_end_day_date < DATE_ADD(current_date('PST8PDT')
     ,INTERVAL 98 DAY)
  THEN ROUND(CAST(oo.quantity_open * oo.unit_estimated_landing_cost AS NUMERIC), 2)
  ELSE NULL
  END AS on_order_5wk_13wk_cost,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 98 DAY)
  THEN ROUND(CAST(oo.quantity_open * oo.unit_estimated_landing_cost AS NUMERIC), 2)
  ELSE NULL
  END AS on_order_14wk_cost,
  CASE
  WHEN DC.week_end_day_date < DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY)
  THEN ROUND(CAST(oo.quantity_open * oo.anticipated_retail_amt AS NUMERIC), 2)
  ELSE NULL
  END AS on_order_4wk_retail,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY) AND DC.week_end_day_date < DATE_ADD(current_date('PST8PDT')
     ,INTERVAL 98 DAY)
  THEN ROUND(CAST(oo.quantity_open * oo.anticipated_retail_amt AS NUMERIC), 2)
  ELSE NULL
  END AS on_order_5wk_13wk_retail,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 98 DAY)
  THEN ROUND(CAST(oo.quantity_open * oo.anticipated_retail_amt AS NUMERIC), 2)
  ELSE NULL
  END AS on_order_14wk_retail
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS oo
 LEFT JOIN (SELECT DISTINCT week_idnt,
   week_start_day_date,
   week_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS DC ON oo.week_num = DC.week_idnt
 INNER JOIN store_lkup AS s ON oo.store_num = CAST(s.store_num AS FLOAT64)
 INNER JOIN sku_lkup AS sku ON LOWER(oo.rms_sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN price_band_lkup AS pb ON pb.next_highest_dollar = CEIL(COALESCE(oo.anticipated_retail_amt, 0))
WHERE COALESCE(oo.anticipated_retail_amt, 0) <> 0
 AND oo.quantity_open > 0
 AND (LOWER(oo.status) = LOWER('CLOSED') AND oo.end_ship_date >= DATE_SUB((SELECT MAX(week_end_day_date)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
       WHERE week_end_day_date < current_date('PST8PDT')), INTERVAL 45 DAY) OR LOWER(oo.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'
      )))
 AND oo.first_approval_date IS NOT NULL;


/*
 * ON ORDER FINAL
 *  
 */


--drop table on_order;


-- placeholder for other measures (sales, demand, inventory, etc)


CREATE TEMPORARY TABLE IF NOT EXISTS on_order
--CLUSTER BY channel_num, dept_num, class_num, subclass_num
AS
SELECT week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound,
 0 AS sales_u,
 0 AS sales_c,
 0 AS sales_r,
 0 AS sales_r_with_cost,
 0 AS demand_u,
 0 AS demand_r,
 0 AS eoh_u,
 0 AS eoh_c,
 0 AS eoh_r,
 0 AS eop_u,
 0 AS eop_c,
 0 AS eop_r,
 SUM(on_order_4wk_units) AS oo_4wk_u,
 SUM(on_order_4wk_cost) AS oo_4wk_c,
 SUM(on_order_4wk_retail) AS oo_4wk_r,
 SUM(on_order_5wk_13wk_units) AS oo_5wk_13wk_u,
 SUM(on_order_5wk_13wk_cost) AS oo_5wk_13wk_c,
 SUM(on_order_5wk_13wk_retail) AS oo_5wk_13wk_r,
 SUM(on_order_14wk_units) AS oo_14wk_u,
 SUM(on_order_14wk_cost) AS oo_14wk_c,
 SUM(on_order_14wk_retail) AS oo_14wk_r,
 0 AS rp_ant_spd_4wk_u,
 0 AS rp_ant_spd_4wk_c,
 0 AS rp_ant_spd_4wk_r,
 0 AS rp_ant_spd_5wk_13wk_u,
 0 AS rp_ant_spd_5wk_13wk_c,
 0 AS rp_ant_spd_5wk_13wk_r,
 0 AS rp_ant_spd_14wk_u,
 0 AS rp_ant_spd_14wk_c,
 0 AS rp_ant_spd_14wk_r
FROM on_order_stg AS oo
GROUP BY week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound;


/*
 * ON ORDER CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS on_order_stg;


/*
 * CATEGORY LOOKUP 2
 * This table is only used to join with rp ant spd table
 * 
 */


--drop table category_lkup_2 


CREATE TEMPORARY TABLE IF NOT EXISTS category_lkup_2
--CLUSTER BY dept_num, class_num, subclass_num
AS
SELECT DISTINCT div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group
FROM sku_lkup;


--collect statistics   unique primary index (dept_num, class_num, subclass_num)  on category_lkup_2 


/*
 * RP ANT SPD STAGING
 * 
 * Similar table to ON ORDER table above
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 * 
 * No join to date_lkup because we want to place all future Ant Spd into the most recent completed week
 * 
 * Will add this data to ON ORDER columns in the final table 
 */


--drop table rp_ant_spd_stg;


-- only get records that have a price so we can assign them to a price band


CREATE TEMPORARY TABLE IF NOT EXISTS rp_ant_spd_stg
--CLUSTER BY channel_num, dept_num, class_num, subclass_num
AS
SELECT (SELECT MAX(week_idnt)
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE week_end_day_date < current_date('PST8PDT')) AS week_idnt,
 S.banner,
 S.channel_type,
 S.channel_label,
 S.channel_num,
 cat.div_label,
 cat.subdiv_label,
 cat.dept_label,
 cat.class_label,
 cat.subclass_label,
 cat.div_num,
 cat.subdiv_num,
 cat.dept_num,
 cat.class_num,
 cat.subclass_num,
 cat.quantrix_category,
 cat.quantrix_category_group,
 rp.supplier_idnt AS supplier_number,
 rp.supplier AS supplier_name,
 rp.vendor_label_name AS brand_name,
 COALESCE(vd.npg_flag, 'N') AS npg_ind,
 'Y' AS rp_ind,
 'R' AS price_type,
 COALESCE(pb.price_band_one_lower_bound, (SELECT MAX(price_band_one_lower_bound)
   FROM price_band_lkup)) AS price_band_one_lower_bound,
 COALESCE(pb.price_band_one_upper_bound, (SELECT MAX(price_band_one_upper_bound)
   FROM price_band_lkup)) AS price_band_one_upper_bound,
 COALESCE(pb.price_band_two_lower_bound, (SELECT MAX(price_band_two_lower_bound)
   FROM price_band_lkup)) AS price_band_two_lower_bound,
 COALESCE(pb.price_band_two_upper_bound, (SELECT MAX(price_band_two_upper_bound)
   FROM price_band_lkup)) AS price_band_two_upper_bound,
  CASE
  WHEN DC.week_end_day_date < DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY)
  THEN CAST(trunc(cast(rp.rp_ant_spd_u as FLOAT64)) AS INTEGER)
  ELSE NULL
  END AS rp_ant_spd_4wk_units,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY) AND DC.week_end_day_date < DATE_ADD(current_date('PST8PDT')
     ,INTERVAL 98 DAY)
  THEN CAST(trunc(cast(rp.rp_ant_spd_u as FLOAT64)) AS INTEGER)
  ELSE NULL
  END AS rp_ant_spd_5wk_13wk_units,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 98 DAY)
  THEN CAST(trunc(cast(rp.rp_ant_spd_u as FLOAT64)) AS INTEGER)
  ELSE NULL
  END AS rp_ant_spd_14wk_units,
  CASE
  WHEN DC.week_end_day_date < DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY)
  THEN rp.rp_ant_spd_c
  ELSE NULL
  END AS rp_ant_spd_4wk_cost,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY) AND DC.week_end_day_date < DATE_ADD(current_date('PST8PDT')
     ,INTERVAL 98 DAY)
  THEN rp.rp_ant_spd_c
  ELSE NULL
  END AS rp_ant_spd_5wk_13wk_cost,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 98 DAY)
  THEN rp.rp_ant_spd_c
  ELSE NULL
  END AS rp_ant_spd_14wk_cost,
  CASE
  WHEN DC.week_end_day_date < DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY)
  THEN rp.rp_ant_spd_r
  ELSE NULL
  END AS rp_ant_spd_4wk_retail,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 35 DAY) AND DC.week_end_day_date < DATE_ADD(current_date('PST8PDT')
     ,INTERVAL 98 DAY)
  THEN rp.rp_ant_spd_r
  ELSE NULL
  END AS rp_ant_spd_5wk_13wk_retail,
  CASE
  WHEN DC.week_end_day_date >= DATE_ADD(current_date('PST8PDT'), INTERVAL 98 DAY)
  THEN rp.rp_ant_spd_r
  ELSE NULL
  END AS rp_ant_spd_14wk_retail
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_ant_spend_report AS rp
 LEFT JOIN (SELECT DISTINCT week_idnt,
   week_start_day_date,
   week_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS DC ON CAST(rp.week_idnt AS FLOAT64) = DC.week_idnt
 INNER JOIN category_lkup_2 AS cat ON cat.dept_num = CAST(TRIM(SUBSTR(rp.department, 1, STRPOS(LOWER(rp.department), LOWER(' ')) - 1)) AS FLOAT64)
     AND cat.class_num = CAST(TRIM(SUBSTR(rp.class, 1, STRPOS(LOWER(rp.class), LOWER(' ')) - 1)) AS FLOAT64) AND cat.subclass_num
    = CAST(TRIM(SUBSTR(rp.subclass, 1, STRPOS(LOWER(rp.subclass), LOWER(' ')) - 1)) AS FLOAT64)
 INNER JOIN (SELECT DISTINCT channel_num,
   channel_label,
   channel_type,
   banner
  FROM store_lkup) AS S ON S.channel_num = CAST(TRIM(SUBSTR(rp.channel_number, 1, STRPOS(LOWER(rp.channel_number), LOWER(' ')) - 1)) AS FLOAT64)
  
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS vd ON LOWER(rp.supplier_idnt) = LOWER(vd.vendor_num)
 LEFT JOIN price_band_lkup AS pb ON pb.next_highest_dollar = CEIL(COALESCE(CASE
     WHEN rp.rp_ant_spd_u = 0
     THEN 0
     ELSE ROUND(CAST(rp.rp_ant_spd_r / rp.rp_ant_spd_u AS NUMERIC), 2)
     END, 0))
WHERE COALESCE(rp.rp_ant_spd_r, 0) <> 0
 AND rp.rp_ant_spd_u > 0;


/*
 * RP ANT SPEND FINAL
 *  
 */


--drop table rp_ant_spd ;


CREATE TEMPORARY TABLE IF NOT EXISTS rp_ant_spd
--CLUSTER BY channel_num, dept_num, class_num, subclass_num
AS
SELECT week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound,
 0 AS sales_u,
 0 AS sales_c,
 0 AS sales_r,
 0 AS sales_r_with_cost,
 0 AS demand_u,
 0 AS demand_r,
 0 AS eoh_u,
 0 AS eoh_c,
 0 AS eoh_r,
 0 AS eop_u,
 0 AS eop_c,
 0 AS eop_r,
 0 AS oo_4wk_u,
 0 AS oo_4wk_c,
 0 AS oo_4wk_r,
 0 AS oo_5wk_13wk_u,
 0 AS oo_5wk_13wk_c,
 0 AS oo_5wk_13wk_r,
 0 AS oo_14wk_u,
 0 AS oo_14wk_c,
 0 AS oo_14wk_r,
 CAST(SUM(rp_ant_spd_4wk_units) AS NUMERIC) AS rp_ant_spd_4wk_u,
 CAST(SUM(rp_ant_spd_4wk_cost) AS NUMERIC) AS rp_ant_spd_4wk_c,
 CAST(SUM(rp_ant_spd_4wk_retail) AS NUMERIC) AS rp_ant_spd_4wk_r,
 CAST(SUM(rp_ant_spd_5wk_13wk_units) AS NUMERIC) AS rp_ant_spd_5wk_13wk_u,
 CAST(SUM(rp_ant_spd_5wk_13wk_cost) AS NUMERIC) AS rp_ant_spd_5wk_13wk_c,
 CAST(SUM(rp_ant_spd_5wk_13wk_retail) AS NUMERIC) AS rp_ant_spd_5wk_13wk_r,
 CAST(SUM(rp_ant_spd_14wk_units) AS NUMERIC) AS rp_ant_spd_14wk_u,
 CAST(SUM(rp_ant_spd_14wk_cost) AS NUMERIC) AS rp_ant_spd_14wk_c,
 CAST(SUM(rp_ant_spd_14wk_retail) AS NUMERIC) AS rp_ant_spd_14wk_r
FROM rp_ant_spd_stg
GROUP BY week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound;


/*
 * ANT SPD CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS category_lkup_2;


DROP TABLE IF EXISTS rp_ant_spd_stg;


/*
 * FULL DATA SET - STAGING
 * 
 */


--drop table full_data_stg_base;


-- fact staging data uses realigned week_idnt so we join on that instead of week_idnt_true


CREATE TEMPORARY TABLE IF NOT EXISTS full_data_stg_base
--CLUSTER BY week_idnt, channel_num, dept_num, class_num
AS
SELECT dt.ty_ly_ind,
 dt.fiscal_year_num,
 dt.fiscal_halfyear_num,
 dt.quarter_abrv,
 dt.quarter_label,
 dt.month_idnt,
 dt.month_abrv,
 dt.month_label,
 dt.month_start_day_date,
 dt.month_end_day_date,
 dt.fiscal_week_num,
 dt.week_label,
 dt.week_of_month_label,
 dt.week_start_day_date,
 dt.week_end_day_date,
 sub.week_idnt,
 sub.banner,
 sub.channel_type,
 sub.channel_label,
 sub.channel_num,
 sub.div_label,
 sub.subdiv_label,
 sub.dept_label,
 sub.class_label,
 sub.subclass_label,
 sub.div_num,
 sub.subdiv_num,
 sub.dept_num,
 sub.class_num,
 sub.subclass_num,
 sub.quantrix_category,
 sub.quantrix_category_group,
 sub.supplier_number,
 sub.supplier_name,
 sub.brand_name,
 sub.npg_ind,
 sub.rp_ind,
 sub.ds_ind,
 sub.price_type,
 sub.price_band_one_lower_bound,
 sub.price_band_one_upper_bound,
 sub.price_band_two_lower_bound,
 sub.price_band_two_upper_bound,
 sub.sales_u,
 sub.sales_c,
 sub.sales_r,
 sub.sales_r_with_cost,
 sub.demand_u,
 sub.demand_r,
 sub.eoh_u,
 sub.eoh_c,
 sub.eoh_r,
 sub.eop_u,
 sub.eop_c,
 sub.eop_r,
 sub.oo_4wk_u,
 sub.oo_4wk_c,
 sub.oo_4wk_r,
 sub.oo_5wk_13wk_u,
 sub.oo_5wk_13wk_c,
 sub.oo_5wk_13wk_r,
 sub.oo_14wk_u,
 sub.oo_14wk_c,
 sub.oo_14wk_r,
 sub.rp_ant_spd_4wk_u,
 sub.rp_ant_spd_4wk_c,
 sub.rp_ant_spd_4wk_r,
 sub.rp_ant_spd_5wk_13wk_u,
 sub.rp_ant_spd_5wk_13wk_c,
 sub.rp_ant_spd_5wk_13wk_r,
 sub.rp_ant_spd_14wk_u,
 sub.rp_ant_spd_14wk_c,
 sub.rp_ant_spd_14wk_r
FROM (SELECT *
   FROM sales
   UNION ALL
   SELECT *
   FROM demand
   UNION ALL
   SELECT *
   FROM inventory
   UNION ALL
   SELECT *
   FROM on_order
   UNION ALL
   SELECT *
   FROM rp_ant_spd) AS sub
 LEFT JOIN date_lkup AS dt ON sub.week_idnt = dt.week_idnt;


/*
 * STAGING CLEANUP
 * 
 * Drop volatile tables that we don't need anymore
 *  
 */


DROP TABLE IF EXISTS sales;


DROP TABLE IF EXISTS demand;


DROP TABLE IF EXISTS inventory;


DROP TABLE IF EXISTS on_order;


DROP TABLE IF EXISTS rp_ant_spd;


/*
 * FULL DATA SET - FINAL
 * 
 * Explicit null handling is included in queries above where necessary.
 * Explicit null handling included here for all fields as a precautionary step in case production tables have unexpected nulls.
 */


--delete from t3dl_ace_pra.price_band_weekly -- this line exists only for testing purposes


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.price_band_weekly{{params.env_suffix}};

--insert into t3dl_ace_pra.price_band_weekly -- this line exists only for testing purposes
insert into `{{params.gcp_project_id}}`.{{params.environment_schema}}.price_band_weekly{{params.env_suffix}} -- this line exists only for testing purposes
SELECT COALESCE(ty_ly_ind, 'U') AS ty_ly_ind,
 COALESCE(fiscal_year_num, - 1) AS fiscal_year_num,
 COALESCE(fiscal_halfyear_num, - 1) AS fiscal_halfyear_num,
 COALESCE(quarter_abrv, 'U') AS quarter_abrv,
 COALESCE(quarter_label, 'U') AS quarter_label,
 COALESCE(month_idnt, - 1) AS month_idnt,
 COALESCE(month_abrv, 'U') AS month_abrv,
 COALESCE(month_label, 'U') AS month_label,
 COALESCE(month_start_day_date, DATE '1970-01-01') AS month_start_day_date,
 COALESCE(month_end_day_date, DATE '1970-01-01') AS month_end_day_date,
 COALESCE(fiscal_week_num, - 1) AS fiscal_week_num,
 COALESCE(week_label, 'U') AS week_label,
 COALESCE(week_of_month_label, 'U') AS week_of_month_label,
 COALESCE(week_start_day_date, DATE '1970-01-01') AS week_start_day_date,
 COALESCE(week_end_day_date, DATE '1970-01-01') AS week_end_day_date,
 COALESCE(week_idnt, - 1) AS week_idnt,
 COALESCE(banner, 'UNKNOWN') AS banner,
 COALESCE(channel_type, 'UNKNOWN') AS channel_type,
 COALESCE(channel_label, 'UNKNOWN') AS channel_label,
 COALESCE(channel_num, - 1) AS channel_num,
 COALESCE(div_label, 'UNKNOWN') AS div_label,
 COALESCE(subdiv_label, 'UNKNOWN') AS subdiv_label,
 COALESCE(dept_label, 'UNKNOWN') AS dept_label,
 COALESCE(class_label, 'UNKNOWN') AS class_label,
 COALESCE(subclass_label, 'UNKNOWN') AS subclass_label,
 COALESCE(div_num, - 1) AS div_num,
 COALESCE(subdiv_num, - 1) AS subdiv_num,
 COALESCE(dept_num, - 1) AS dept_num,
 COALESCE(class_num, - 1) AS class_num,
 COALESCE(subclass_num, - 1) AS subclass_num,
 COALESCE(quantrix_category, 'UNKNOWN') AS quantrix_category,
 COALESCE(quantrix_category_group, 'UNKNOWN') AS quantrix_category_group,
 COALESCE(cast(trunc(cast(supplier_number as float64)) as integer), cast(trunc(cast(FORMAT('%11d', - 1)as float64)) as integer)) AS supplier_number,
 COALESCE(supplier_name, 'UNKNOWN') AS supplier_name,
 COALESCE(brand_name, 'UNKNOWN') AS brand_name,
 COALESCE(npg_ind, 'U') AS npg_ind,
 COALESCE(rp_ind, 'U') AS rp_ind,
 COALESCE(ds_ind, 'U') AS ds_ind,
 COALESCE(price_type, 'U') AS price_type,
 COALESCE(price_band_one_lower_bound, - 1) AS price_band_one_lower_bound,
 COALESCE(price_band_one_upper_bound, - 1) AS price_band_one_upper_bound,
 COALESCE(price_band_two_lower_bound, - 1) AS price_band_two_lower_bound,
 COALESCE(price_band_two_upper_bound, - 1) AS price_band_two_upper_bound,
 SUM(sales_u) AS sales_u,
 SUM(sales_c) AS sales_c,
 SUM(sales_r) AS sales_r,
 SUM(sales_r_with_cost) AS sales_r_with_cost,
 SUM(demand_u) AS demand_u,
 CAST(SUM(demand_r) AS NUMERIC) AS demand_r,
 SUM(eoh_u) AS eoh_u,
 CAST(SUM(eoh_c) AS NUMERIC) AS eoh_c,
 CAST(SUM(eoh_r) AS NUMERIC) AS eoh_r,
 SUM(eop_u) AS eop_u,
 CAST(SUM(eop_c) AS NUMERIC) AS eop_c,
 CAST(SUM(eop_r) AS NUMERIC) AS eop_r,
 COALESCE(CAST(trunc(SUM(oo_4wk_u + rp_ant_spd_4wk_u)) AS INT64), 0) AS oo_4wk_u,
 COALESCE(CAST(SUM(oo_4wk_c + rp_ant_spd_4wk_c) AS NUMERIC), 0) AS oo_4wk_c,
 COALESCE(CAST(SUM(oo_4wk_r + rp_ant_spd_4wk_r) AS NUMERIC), 0) AS oo_4wk_r,
 COALESCE(CAST(trunc(SUM(oo_5wk_13wk_u + rp_ant_spd_5wk_13wk_u)) AS INT64), 0) AS oo_5wk_13wk_u,
 COALESCE(CAST(SUM(oo_5wk_13wk_c + rp_ant_spd_5wk_13wk_c) AS NUMERIC), 0) AS oo_5wk_13wk_c,
 COALESCE(CAST(SUM(oo_5wk_13wk_r + rp_ant_spd_5wk_13wk_r) AS NUMERIC), 0) AS oo_5wk_13wk_r,
 COALESCE(CAST(trunc(SUM(oo_14wk_u + rp_ant_spd_14wk_u)) AS INT64), 0) AS oo_14wk_u,
 COALESCE(CAST(SUM(oo_14wk_c + rp_ant_spd_14wk_c) AS NUMERIC), 0) AS oo_14wk_c,
 COALESCE(CAST(SUM(oo_14wk_r + rp_ant_spd_14wk_r) AS NUMERIC), 0) AS oo_14wk_r,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_date('PST8PDT')) AS DATETIME) AS load_timestamp
FROM full_data_stg_base AS d
GROUP BY ty_ly_ind,
 fiscal_year_num,
 fiscal_halfyear_num,
 quarter_abrv,
 quarter_label,
 month_idnt,
 month_abrv,
 month_label,
 month_start_day_date,
 month_end_day_date,
 fiscal_week_num,
 week_label,
 week_of_month_label,
 week_start_day_date,
 week_end_day_date,
 week_idnt,
 banner,
 channel_type,
 channel_label,
 channel_num,
 div_label,
 subdiv_label,
 dept_label,
 class_label,
 subclass_label,
 div_num,
 subdiv_num,
 dept_num,
 class_num,
 subclass_num,
 quantrix_category,
 quantrix_category_group,
 supplier_number,
 supplier_name,
 brand_name,
 npg_ind,
 rp_ind,
 ds_ind,
 price_type,
 price_band_one_lower_bound,
 price_band_one_upper_bound,
 price_band_two_lower_bound,
 price_band_two_upper_bound;