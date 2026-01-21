CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup
AS
SELECT DISTINCT store_num,
 channel_country,
 channel_num,
 TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel_label,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'FP'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'OP'
  ELSE NULL
  END AS banner,
  CASE
  WHEN channel_num = 111
  THEN 'NORDSTROM_CANADA_STORES'
  WHEN channel_num = 120
  THEN 'NORDSTROM_ONLINE'
  WHEN channel_num = 121
  THEN 'NORDSTROM_CANADA_ONLINE'
  WHEN channel_num = 110
  THEN 'NORDSTROM_STORES'
  WHEN channel_num = 310
  THEN 'NORD US RSWH'
  WHEN channel_num = 311
  THEN 'NORD CA RSWH'
  WHEN channel_num = 250
  THEN 'RACK_ONLINE'
  WHEN channel_num = 211
  THEN 'RACK_CANADA_STORES'
  WHEN channel_num = 260
  THEN 'RACK US RSWH'
  WHEN channel_num = 261
  THEN 'RACK CA RSWH'
  WHEN channel_num = 210
  THEN 'RACK STORES'
  ELSE NULL
  END AS cluster_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE channel_num NOT IN (580);


--COLLECT STATS     COLUMN(channel_country, channel_num,banner, cluster_name)     ON STORE_LKUP


--N category_store_channel_cluster lookup


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup_nord
AS
SELECT DISTINCT store_num,
 channel_country,
 channel_num,
 TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel_label,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'FP'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'OP'
  ELSE NULL
  END AS banner,
  CASE
  WHEN channel_num = 111
  THEN 'NORDSTROM_CANADA_STORES'
  WHEN channel_num = 120
  THEN 'NORDSTROM_ONLINE'
  WHEN channel_num = 121
  THEN 'NORDSTROM_CANADA_ONLINE'
  WHEN channel_num = 110
  THEN 'NORDSTROM_STORES'
  WHEN channel_num = 310
  THEN 'NORD US RSWH'
  WHEN channel_num = 311
  THEN 'NORD CA RSWH'
  ELSE NULL
  END AS cluster_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE channel_num IN (110, 120, 111, 121, 310, 311);


--COLLECT STATS 	COLUMN(channel_country ,channel_num ,banner, cluster_name)     , COLUMN(store_num) 	ON STORE_LKUP_NORD


--NR category_store_channel_cluster lookup


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup_rack
AS
SELECT DISTINCT *
FROM (SELECT st.store_num,
    st.channel_country,
    st.channel_num,
    TRIM(FORMAT('%11d', st.channel_num) || ', ' || st.channel_desc) AS channel_label,
     CASE
     WHEN LOWER(st.channel_brand) = LOWER('NORDSTROM')
     THEN 'FP'
     WHEN LOWER(st.channel_brand) = LOWER('NORDSTROM_RACK')
     THEN 'OP'
     ELSE NULL
     END AS banner,
     CASE
     WHEN st.channel_num = 210 AND LOWER(grp.peer_group_desc) = LOWER('BEST')
     THEN 'BRAND'
     WHEN st.channel_num = 210 AND LOWER(grp.peer_group_desc) = LOWER('BETTER')
     THEN 'HYBRID'
     WHEN st.channel_num = 210 AND LOWER(grp.peer_group_desc) = LOWER('GOOD')
     THEN 'PRICE'
     ELSE grp.peer_group_desc
     END AS cluster_name
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_peer_group_dim AS grp ON st.store_num = grp.store_num
   WHERE LOWER(grp.peer_group_type_code) IN (LOWER('OCP'))
    AND st.channel_num = 210
   UNION ALL
   SELECT store_num,
    channel_country,
    channel_num,
    TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel_label,
     CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN 'FP'
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN 'OP'
     ELSE NULL
     END AS banner,
     CASE
     WHEN channel_num = 250
     THEN 'RACK_ONLINE'
     WHEN channel_num = 211
     THEN 'RACK_CANADA_STORES'
     WHEN channel_num = 260
     THEN 'RACK US RSWH'
     WHEN channel_num = 261
     THEN 'RACK CA RSWH'
     ELSE NULL
     END AS cluster_name
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st
   WHERE channel_num IN (260, 261, 211, 250)) AS T1;


--COLLECT STATS 	COLUMN(channel_country, channel_num, banner, cluster_name) 	, COLUMN(store_num)     ON STORE_LKUP_RACK


--department lookup


CREATE TEMPORARY TABLE IF NOT EXISTS dept_lkup_ccp
AS
SELECT DISTINCT dept_num,
 division_num,
 subdivision_num,
 TRIM(FORMAT('%11d', dept_num) || ', ' || dept_name) AS dept_label,
 TRIM(FORMAT('%11d', division_num) || ', ' || division_name) AS division_label,
 TRIM(FORMAT('%11d', subdivision_num) || ', ' || subdivision_name) AS subdivision_label
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim
WHERE dept_num IN (SELECT CAST(TRUNC(CAST(department_number AS FLOAT64)) AS INTEGER) AS department_number
   FROM (SELECT DISTINCT department_number
     FROM `{{params.gcp_project_id}}`.t2dl_das_apt_reporting.merch_assortment_category_cluster_plan_fact) AS t0);


--COLLECT STATS     COLUMN(dept_num)     ON DEPT_LKUP_CCP




CREATE TEMPORARY TABLE IF NOT EXISTS ly_date_lkup
AS
SELECT DISTINCT month_idnt,
 month_label,
 month_454_label,
 month_start_day_date,
 month_end_day_date,
 week_idnt,
 week_start_day_date,
 week_end_day_date,
 week_label,
 week_num_of_fiscal_month,
 quarter_label,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_end_day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 365 * 2 DAY) AND CURRENT_DATE('PST8PDT')
 AND fiscal_year_num <> 2020;


--COLLECT STATS 	COLUMN (MONTH_START_DAY_DATE, MONTH_END_DAY_DATE, WEEK_START_DAY_DATE, WEEK_END_DAY_DATE)     , COLUMN (WEEK_IDNT) 	ON LY_DATE_LKUP


-- category lookup


CREATE TEMPORARY TABLE IF NOT EXISTS cat_lkup
AS
SELECT DISTINCT dept_idnt AS dept_num,
 sku_idnt,
 channel_country,
 quantrix_category AS category,
 fanatics_ind,
 fp_supplier_group,
 op_supplier_group
FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy;


--COLLECT STATS 	COLUMN(category)     , COLUMN (dept_num ,sku_idnt, channel_country)     , COLUMN(sku_idnt)     , COLUMN(channel_country)     , COLUMN(fanatics_ind) 	ON CAT_LKUP


-- N receipted price band
CREATE TEMPORARY TABLE IF NOT EXISTS receipt_price_band_nord
AS
SELECT a.rms_sku_num AS sku_idnt,
 b.channel_country,
 MAX((a.receipts_regular_retail + a.receipts_clearance_retail + a.receipts_crossdock_regular_retail + a.receipts_crossdock_clearance_retail
     ) / (a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
     )) AS aur
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
 INNER JOIN store_lkup_nord AS b ON a.store_num = b.store_num
 INNER JOIN dept_lkup_ccp AS dept ON a.department_num = dept.dept_num
WHERE a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
    > 0
GROUP BY sku_idnt,
 b.channel_country;


-- NR receipted price band


CREATE TEMPORARY TABLE IF NOT EXISTS receipt_price_band_rack
AS
SELECT sku_idnt,
 channel_country,
 MAX(aur) AS aur
FROM (SELECT a.rms_sku_num AS sku_idnt,
    b.channel_country,
    MAX((a.receipts_regular_retail + a.receipts_clearance_retail + a.receipts_crossdock_regular_retail + a.receipts_crossdock_clearance_retail
        ) / (a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
        )) AS aur
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
    INNER JOIN store_lkup_rack AS b ON a.store_num = b.store_num
    INNER JOIN dept_lkup_ccp AS dept ON a.department_num = dept.dept_num
   WHERE a.receipts_regular_units + a.receipts_clearance_units + a.receipts_crossdock_regular_units + a.receipts_crossdock_clearance_units
       > 0
   GROUP BY sku_idnt,
    b.channel_country
   UNION ALL
   SELECT tsf.rms_sku_num AS sku_idnt,
    b0.channel_country,
    MAX(tsf.packandhold_transfer_in_retail / tsf.packandhold_transfer_in_units) AS aur
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS tsf
    INNER JOIN store_lkup_rack AS b0 ON tsf.store_num = b0.store_num
    INNER JOIN dept_lkup_ccp AS dept0 ON tsf.department_num = dept0.dept_num
   WHERE tsf.packandhold_transfer_in_units > 0
   GROUP BY sku_idnt,
    b0.channel_country
   UNION ALL
   SELECT tsf0.rms_sku_num AS sku_idnt,
    b1.channel_country,
    MAX(tsf0.racking_transfer_in_retail / tsf0.racking_transfer_in_units) AS aur
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS tsf0
    INNER JOIN store_lkup_rack AS b1 ON tsf0.store_num = b1.store_num
    INNER JOIN dept_lkup_ccp AS dept1 ON tsf0.department_num = dept1.dept_num
   WHERE tsf0.racking_transfer_in_units > 0
   GROUP BY sku_idnt,
    b1.channel_country) AS TBL
GROUP BY sku_idnt,
 channel_country;


--COLLECT STATS 	COLUMN(sku_idnt, channel_country)     ON RECEIPT_PRICE_BAND_RACK


/* Create Category/Price Band Table at Channel/Cluster Grain */


-- 1. Sales
--Rack
CREATE TEMPORARY TABLE IF NOT EXISTS priceband_sales
AS
-- Nord
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 sls.month_num AS month_idnt,
 sls.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 sls.department_num,
 sls.dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 sls.sales_cost_curr_code AS currency,
 SUM(sls.net_sales_tot_retl) AS net_sls_r,
 SUM(sls.net_sales_tot_cost) AS net_sls_c,
 SUM(sls.net_sales_tot_units) AS net_sls_units,
 SUM(sls.gross_sales_tot_retl) AS gross_sls_r,
 SUM(sls.gross_sales_tot_units) AS gross_sls_units,
  CASE
  WHEN st.channel_num IN (110, 111, 210, 211)
  THEN SUM(sls.gross_sales_tot_retl)
  ELSE 0
  END AS demand_ttl_r,
  CASE
  WHEN st.channel_num IN (110, 111, 210, 211)
  THEN SUM(sls.gross_sales_tot_units)
  ELSE 0
  END AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS sls
 INNER JOIN ly_date_lkup AS lyd ON sls.week_num = lyd.week_idnt
 INNER JOIN store_lkup_nord AS st ON sls.store_num = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(sls.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(sls.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 sls.department_num,
 sls.dropship_ind,
 quantrix_category,
 price_band,
 currency
UNION ALL
SELECT st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 sls0.month_num AS month_idnt,
 sls0.week_num AS week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 sls0.department_num,
 sls0.dropship_ind,
 cat0.category AS quantrix_category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 sls0.sales_cost_curr_code AS currency,
 SUM(sls0.net_sales_tot_retl) AS net_sls_r,
 SUM(sls0.net_sales_tot_cost) AS net_sls_c,
 SUM(sls0.net_sales_tot_units) AS net_sls_units,
 SUM(sls0.gross_sales_tot_retl) AS gross_sls_r,
 SUM(sls0.gross_sales_tot_units) AS gross_sls_units,
  CASE
  WHEN st0.channel_num IN (110, 111, 210, 211)
  THEN SUM(sls0.gross_sales_tot_retl)
  ELSE 0
  END AS demand_ttl_r,
  CASE
  WHEN st0.channel_num IN (110, 111, 210, 211)
  THEN SUM(sls0.gross_sales_tot_units)
  ELSE 0
  END AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS sls0
 INNER JOIN ly_date_lkup AS lyd0 ON sls0.week_num = lyd0.week_idnt
 INNER JOIN store_lkup_rack AS st0 ON sls0.store_num = st0.store_num
 LEFT JOIN cat_lkup AS cat0 ON LOWER(sls0.rms_sku_num) = LOWER(cat0.sku_idnt) AND LOWER(st0.channel_country) = LOWER(cat0
    .channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(sls0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
WHERE cat0.fanatics_ind = 0
GROUP BY st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 month_idnt,
 week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 sls0.department_num,
 sls0.dropship_ind,
 quantrix_category,
 price_band,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)     , COLUMN(channel_num)     , COLUMN(week_idnt)     , COLUMN(department_num)     , COLUMN(quantrix_category)     , COLUMN(price_band)     ON PRICEBAND_SALES


-- 2. Demand
--Rack


CREATE TEMPORARY TABLE IF NOT EXISTS priceband_demand
AS
-- Nord
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 dmd.month_num AS month_idnt,
 dmd.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 dmd.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 SUBSTR(CASE
   WHEN LOWER(st.channel_country) = LOWER('US')
   THEN 'USD'
   WHEN LOWER(st.channel_country) = LOWER('CA')
   THEN 'CAD'
   ELSE NULL
   END, 1, 40) AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
  CASE
  WHEN st.channel_num NOT IN (110, 111, 210, 211)
  THEN SUM(dmd.demand_tot_amt)
  ELSE 0
  END AS demand_ttl_r,
  CASE
  WHEN st.channel_num NOT IN (110, 111, 210, 211)
  THEN SUM(dmd.demand_tot_qty)
  ELSE 0
  END AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw AS dmd
 INNER JOIN ly_date_lkup AS lyd ON dmd.week_num = lyd.week_idnt
 INNER JOIN store_lkup_nord AS st ON dmd.store_num = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(dmd.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(dmd.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 dmd.department_num,
 dropship_ind,
 quantrix_category,
 price_band,
 currency
UNION ALL
SELECT st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 dmd0.month_num AS month_idnt,
 dmd0.week_num AS week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 dmd0.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat0.category AS quantrix_category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 SUBSTR(CASE
   WHEN LOWER(st0.channel_country) = LOWER('US')
   THEN 'USD'
   WHEN LOWER(st0.channel_country) = LOWER('CA')
   THEN 'CAD'
   ELSE NULL
   END, 1, 40) AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
  CASE
  WHEN st0.channel_num NOT IN (110, 111, 210, 211)
  THEN SUM(dmd0.demand_tot_amt)
  ELSE 0
  END AS demand_ttl_r,
  CASE
  WHEN st0.channel_num NOT IN (110, 111, 210, 211)
  THEN SUM(dmd0.demand_tot_qty)
  ELSE 0
  END AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw AS dmd0
 INNER JOIN ly_date_lkup AS lyd0 ON dmd0.week_num = lyd0.week_idnt
 INNER JOIN store_lkup_rack AS st0 ON dmd0.store_num = st0.store_num
 LEFT JOIN cat_lkup AS cat0 ON LOWER(dmd0.rms_sku_num) = LOWER(cat0.sku_idnt) AND LOWER(st0.channel_country) = LOWER(cat0
    .channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(dmd0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
WHERE cat0.fanatics_ind = 0
GROUP BY st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 month_idnt,
 week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 dmd0.department_num,
 dropship_ind,
 quantrix_category,
 price_band,
 currency;


--COLLECT STATS     COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)     , COLUMN(channel_num)     , COLUMN(week_idnt)     , COLUMN(department_num)     , COLUMN(quantrix_category)     , COLUMN(price_band)     ON PRICEBAND_DEMAND


-- 3.A. Inventory FP
CREATE TEMPORARY TABLE IF NOT EXISTS priceband_inventory_fp
AS
-- Nord
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 inv.month_num AS month_idnt,
 inv.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 inv.inv_cost_currency_code AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(inv.eoh_total_cost + inv.eoh_in_transit_total_cost) AS eoh_ttl_c,
 SUM(inv.eoh_total_units + inv.eoh_in_transit_total_units) AS eoh_ttl_units,
 SUM(inv.boh_total_cost + inv.boh_in_transit_total_cost) AS boh_ttl_c,
 SUM(inv.boh_total_units + inv.boh_in_transit_total_units) AS boh_ttl_units,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_cost + inv.boh_in_transit_total_cost
   ELSE NULL
   END) AS beginofmonth_bop_c,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_units + inv.boh_in_transit_total_units
   ELSE NULL
   END) AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS inv
 INNER JOIN ly_date_lkup AS lyd ON inv.week_num = lyd.week_idnt
 INNER JOIN store_lkup_nord AS st ON CAST(inv.store_num AS FLOAT64) = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(inv.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(inv.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 dropship_ind,
 quantrix_category,
 price_band,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(price_band) 	ON PRICEBAND_INVENTORY_FP


-- 3.B. Inventory OP
CREATE TEMPORARY TABLE IF NOT EXISTS priceband_inventory_op
AS
-- Rack
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 inv.month_num AS month_idnt,
 inv.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 10
  THEN '$0-10'
  WHEN pb.aur <= 15
  THEN '$10-15'
  WHEN pb.aur <= 20
  THEN '$15-20'
  WHEN pb.aur <= 25
  THEN '$20-25'
  WHEN pb.aur <= 30
  THEN '$25-30'
  WHEN pb.aur <= 40
  THEN '$30-40'
  WHEN pb.aur <= 50
  THEN '$40-50'
  WHEN pb.aur <= 60
  THEN '$50-60'
  WHEN pb.aur <= 80
  THEN '$60-80'
  WHEN pb.aur <= 100
  THEN '$80-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 inv.inv_cost_currency_code AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(inv.eoh_total_cost + inv.eoh_in_transit_total_cost) AS eoh_ttl_c,
 SUM(inv.eoh_total_units + inv.eoh_in_transit_total_units) AS eoh_ttl_units,
 SUM(inv.boh_total_cost + inv.boh_in_transit_total_cost) AS boh_ttl_c,
 SUM(inv.boh_total_units + inv.boh_in_transit_total_units) AS boh_ttl_units,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_cost + inv.boh_in_transit_total_cost
   ELSE NULL
   END) AS beginofmonth_bop_c,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_units + inv.boh_in_transit_total_units
   ELSE NULL
   END) AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS inv
 INNER JOIN ly_date_lkup AS lyd ON inv.week_num = lyd.week_idnt
 INNER JOIN store_lkup_rack AS st ON CAST(inv.store_num AS FLOAT64) = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(inv.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
 LEFT JOIN receipt_price_band_rack AS pb ON LOWER(inv.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 dropship_ind,
 quantrix_category,
 price_band,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(price_band) 	ON PRICEBAND_INVENTORY_
-- 4. Receipts
-- Rack
CREATE TEMPORARY TABLE IF NOT EXISTS priceband_receipts
AS
-- Nord
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 rpt.month_num AS month_idnt,
 rpt.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 rpt.department_num,
 rpt.dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 rpt.receipts_cost_currency_code AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 SUM(rpt.receipts_regular_cost + rpt.receipts_clearance_cost + rpt.receipts_crossdock_regular_cost + rpt.receipts_crossdock_clearance_cost
   ) AS ttl_porcpt_c,
 SUM(rpt.receipts_regular_units + rpt.receipts_clearance_units + rpt.receipts_crossdock_regular_units + rpt.receipts_crossdock_clearance_units
   ) AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS rpt
 INNER JOIN ly_date_lkup AS lyd ON rpt.week_num = lyd.week_idnt
 INNER JOIN store_lkup_nord AS st ON rpt.store_num = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(rpt.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(rpt.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 rpt.department_num,
 rpt.dropship_ind,
 quantrix_category,
 price_band,
 currency
UNION ALL
SELECT st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 rpt0.month_num AS month_idnt,
 rpt0.week_num AS week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 rpt0.department_num,
 rpt0.dropship_ind,
 cat0.category AS quantrix_category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 rpt0.receipts_cost_currency_code AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 SUM(rpt0.receipts_regular_cost + rpt0.receipts_clearance_cost + rpt0.receipts_crossdock_regular_cost + rpt0.receipts_crossdock_clearance_cost
   ) AS ttl_porcpt_c,
 SUM(rpt0.receipts_regular_units + rpt0.receipts_clearance_units + rpt0.receipts_crossdock_regular_units + rpt0.receipts_crossdock_clearance_units
   ) AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS rpt0
 INNER JOIN ly_date_lkup AS lyd0 ON rpt0.week_num = lyd0.week_idnt
 INNER JOIN store_lkup_rack AS st0 ON rpt0.store_num = st0.store_num
 LEFT JOIN cat_lkup AS cat0 ON LOWER(rpt0.rms_sku_num) = LOWER(cat0.sku_idnt) AND LOWER(st0.channel_country) = LOWER(cat0
    .channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(rpt0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
WHERE cat0.fanatics_ind = 0
GROUP BY st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 month_idnt,
 week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 rpt0.department_num,
 rpt0.dropship_ind,
 quantrix_category,
 price_band,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(price_band) 	ON PRICEBAND_RECEIPTS


-- 5. Transfers
-- Rack
CREATE TEMPORARY TABLE IF NOT EXISTS priceband_transfers
AS
-- Nord
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 tsf.month_num AS month_idnt,
 tsf.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 tsf.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN pb.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb.aur <= 25
  THEN '$0-25'
  WHEN pb.aur <= 50
  THEN '$25-50'
  WHEN pb.aur <= 100
  THEN '$50-100'
  WHEN pb.aur <= 150
  THEN '$100-150'
  WHEN pb.aur <= 200
  THEN '$150-200'
  WHEN pb.aur <= 300
  THEN '$200-300'
  WHEN pb.aur <= 500
  THEN '$300-500'
  WHEN pb.aur <= 700
  THEN '$500-700'
  WHEN pb.aur <= 1000
  THEN '$700-1000'
  WHEN pb.aur <= 1500
  THEN '$1000-1500'
  WHEN pb.aur <= 1000000000
  THEN 'Over $1500'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 SUBSTR(CASE
   WHEN LOWER(st.channel_country) = LOWER('US')
   THEN 'USD'
   WHEN LOWER(st.channel_country) = LOWER('CA')
   THEN 'CAD'
   ELSE NULL
   END, 1, 40) AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 SUM(tsf.packandhold_transfer_in_cost) AS pah_tsfr_in_c,
 SUM(tsf.packandhold_transfer_in_units) AS pah_tsfr_in_u,
 SUM(tsf.reservestock_transfer_in_cost) AS rs_stk_tsfr_in_c,
 SUM(tsf.reservestock_transfer_in_units) AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS tsf
 INNER JOIN ly_date_lkup AS lyd ON tsf.week_num = lyd.week_idnt
 INNER JOIN store_lkup_nord AS st ON tsf.store_num = st.store_num
 LEFT JOIN cat_lkup AS cat ON LOWER(tsf.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
 LEFT JOIN receipt_price_band_nord AS pb ON LOWER(tsf.rms_sku_num) = LOWER(pb.sku_idnt) AND LOWER(st.channel_country) =
   LOWER(pb.channel_country)
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 tsf.department_num,
 dropship_ind,
 quantrix_category,
 price_band,
 currency
UNION ALL
SELECT st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 tsf0.month_num AS month_idnt,
 tsf0.week_num AS week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 tsf0.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat0.category AS quantrix_category,
  CASE
  WHEN pb0.aur <= 0.01
  THEN 'NO RECEIPT FOUND'
  WHEN pb0.aur <= 10
  THEN '$0-10'
  WHEN pb0.aur <= 15
  THEN '$10-15'
  WHEN pb0.aur <= 20
  THEN '$15-20'
  WHEN pb0.aur <= 25
  THEN '$20-25'
  WHEN pb0.aur <= 30
  THEN '$25-30'
  WHEN pb0.aur <= 40
  THEN '$30-40'
  WHEN pb0.aur <= 50
  THEN '$40-50'
  WHEN pb0.aur <= 60
  THEN '$50-60'
  WHEN pb0.aur <= 80
  THEN '$60-80'
  WHEN pb0.aur <= 100
  THEN '$80-100'
  WHEN pb0.aur <= 150
  THEN '$100-150'
  WHEN pb0.aur <= 200
  THEN '$150-200'
  WHEN pb0.aur <= 1000000000
  THEN 'Over $200'
  ELSE 'NO RECEIPT FOUND'
  END AS price_band,
 SUBSTR(CASE
   WHEN LOWER(st0.channel_country) = LOWER('US')
   THEN 'USD'
   WHEN LOWER(st0.channel_country) = LOWER('CA')
   THEN 'CAD'
   ELSE NULL
   END, 1, 40) AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 SUM(tsf0.packandhold_transfer_in_cost) AS pah_tsfr_in_c,
 SUM(tsf0.packandhold_transfer_in_units) AS pah_tsfr_in_u,
 SUM(tsf0.reservestock_transfer_in_cost) AS rs_stk_tsfr_in_c,
 SUM(tsf0.reservestock_transfer_in_units) AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS tsf0
 INNER JOIN ly_date_lkup AS lyd0 ON tsf0.week_num = lyd0.week_idnt
 INNER JOIN store_lkup_rack AS st0 ON tsf0.store_num = st0.store_num
 LEFT JOIN cat_lkup AS cat0 ON LOWER(tsf0.rms_sku_num) = LOWER(cat0.sku_idnt) AND LOWER(st0.channel_country) = LOWER(cat0
    .channel_country)
 LEFT JOIN receipt_price_band_rack AS pb0 ON LOWER(tsf0.rms_sku_num) = LOWER(pb0.sku_idnt) AND LOWER(st0.channel_country
    ) = LOWER(pb0.channel_country)
WHERE cat0.fanatics_ind = 0
GROUP BY st0.channel_num,
 st0.cluster_name,
 st0.channel_country,
 st0.banner,
 month_idnt,
 week_idnt,
 lyd0.month_start_day_date,
 lyd0.month_end_day_date,
 lyd0.week_start_day_date,
 lyd0.week_end_day_date,
 tsf0.department_num,
 dropship_ind,
 quantrix_category,
 price_band,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(price_band) 	ON PRICEBAND_TRANSFERS


-- Union all & insert into final category/priceband table


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.category_priceband_cost_ly{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.category_priceband_cost_ly{{params.env_suffix}}
(SELECT *
 FROM (SELECT *
    FROM priceband_sales
    UNION ALL
    SELECT *
    FROM priceband_demand
    UNION ALL
    SELECT *
    FROM priceband_inventory_fp
    UNION ALL
    SELECT *
    FROM priceband_inventory_op
    UNION ALL
    SELECT *
    FROM priceband_receipts
    UNION ALL
    SELECT *
    FROM priceband_transfers) AS t);


--COLLECT STATS     PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, channel_num, banner, channel_country, department_num, quantrix_category, price_band) 	, COLUMN (week_idnt, quantrix_category, price_band) 	ON {environment_schema}.category_priceband_cost_ly{env_suffix}


/* Create Category/Supplier Table at Channel/Cluster Grain */


-- 1. Sales


CREATE TEMPORARY TABLE IF NOT EXISTS supplier_sales
AS
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 sls.month_num AS month_idnt,
 sls.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 sls.department_num,
 sls.dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN LOWER(st.banner) = LOWER('FP')
  THEN COALESCE(cat.fp_supplier_group, 'OTHER')
  WHEN LOWER(st.banner) = LOWER('OP')
  THEN COALESCE(cat.op_supplier_group, 'OTHER')
  ELSE 'OTHER'
  END AS supplier_group,
 sls.sales_cost_curr_code AS currency,
 SUM(sls.net_sales_tot_retl) AS net_sls_r,
 SUM(sls.net_sales_tot_cost) AS net_sls_c,
 SUM(sls.net_sales_tot_units) AS net_sls_units,
 SUM(sls.gross_sales_tot_retl) AS gross_sls_r,
 SUM(sls.gross_sales_tot_units) AS gross_sls_units,
  CASE
  WHEN st.channel_num IN (110, 111, 210, 211)
  THEN SUM(sls.gross_sales_tot_retl)
  ELSE 0
  END AS demand_ttl_r,
  CASE
  WHEN st.channel_num IN (110, 111, 210, 211)
  THEN SUM(sls.gross_sales_tot_units)
  ELSE 0
  END AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS sls
 INNER JOIN ly_date_lkup AS lyd ON sls.week_num = lyd.week_idnt
 INNER JOIN store_lkup AS st ON sls.store_num = st.store_num
 INNER JOIN cat_lkup AS cat ON LOWER(sls.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 sls.department_num,
 sls.dropship_ind,
 quantrix_category,
 supplier_group,
 currency;


--COLLECT STATS     COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)     , COLUMN(channel_num)     , COLUMN(week_idnt)     , COLUMN(department_num)     , COLUMN(quantrix_category)     , COLUMN(supplier_group)     , COLUMN(dropship_ind)     ON SUPPLIER_SALES


-- 2. Demand


CREATE TEMPORARY TABLE IF NOT EXISTS supplier_demand
AS
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 dmd.month_num AS month_idnt,
 dmd.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 dmd.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN LOWER(st.banner) = LOWER('FP')
  THEN COALESCE(cat.fp_supplier_group, 'OTHER')
  WHEN LOWER(st.banner) = LOWER('OP')
  THEN COALESCE(cat.op_supplier_group, 'OTHER')
  ELSE 'OTHER'
  END AS supplier_group,
 SUBSTR(CASE
   WHEN LOWER(st.channel_country) = LOWER('US')
   THEN 'USD'
   WHEN LOWER(st.channel_country) = LOWER('CA')
   THEN 'CAD'
   ELSE NULL
   END, 1, 40) AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
  CASE
  WHEN st.channel_num NOT IN (110, 111, 210, 211)
  THEN SUM(dmd.demand_tot_amt)
  ELSE 0
  END AS demand_ttl_r,
  CASE
  WHEN st.channel_num NOT IN (110, 111, 210, 211)
  THEN SUM(dmd.demand_tot_qty)
  ELSE 0
  END AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw AS dmd
 INNER JOIN ly_date_lkup AS lyd ON dmd.week_num = lyd.week_idnt
 INNER JOIN store_lkup AS st ON dmd.store_num = st.store_num
 INNER JOIN cat_lkup AS cat ON LOWER(dmd.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 dmd.department_num,
 dropship_ind,
 quantrix_category,
 supplier_group,
 currency;


--COLLECT STATS     COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)     , COLUMN(channel_num)     , COLUMN(week_idnt)     , COLUMN(department_num)     , COLUMN(quantrix_category)     , COLUMN(supplier_group)     , COLUMN(dropship_ind)     ON SUPPLIER_DEMAND


-- 3. Inventory FP


CREATE TEMPORARY TABLE IF NOT EXISTS supplier_inventory_fp
AS
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 inv.month_num AS month_idnt,
 inv.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN LOWER(st.banner) = LOWER('FP')
  THEN COALESCE(cat.fp_supplier_group, 'OTHER')
  WHEN LOWER(st.banner) = LOWER('OP')
  THEN COALESCE(cat.op_supplier_group, 'OTHER')
  ELSE 'OTHER'
  END AS supplier_group,
 inv.inv_cost_currency_code AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(inv.eoh_total_cost + inv.eoh_in_transit_total_cost) AS eoh_ttl_c,
 SUM(inv.eoh_total_units + inv.eoh_in_transit_total_units) AS eoh_ttl_units,
 SUM(inv.boh_total_cost + inv.boh_in_transit_total_cost) AS boh_ttl_c,
 SUM(inv.boh_total_units + inv.boh_in_transit_total_units) AS boh_ttl_units,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_cost + inv.boh_in_transit_total_cost
   ELSE NULL
   END) AS beginofmonth_bop_c,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_units + inv.boh_in_transit_total_units
   ELSE NULL
   END) AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS inv
 INNER JOIN ly_date_lkup AS lyd ON inv.week_num = lyd.week_idnt
 INNER JOIN store_lkup AS st ON CAST(inv.store_num AS FLOAT64) = st.store_num AND LOWER(st.banner) = LOWER('FP')
 INNER JOIN cat_lkup AS cat ON LOWER(inv.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 dropship_ind,
 quantrix_category,
 supplier_group,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(supplier_group) 	, COLUMN(dropship_ind) 	ON SUPPLIER_INVENTORY_FP


-- 3. Inventory OP


CREATE TEMPORARY TABLE IF NOT EXISTS supplier_inventory_op
AS
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 inv.month_num AS month_idnt,
 inv.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN LOWER(st.banner) = LOWER('FP')
  THEN COALESCE(cat.fp_supplier_group, 'OTHER')
  WHEN LOWER(st.banner) = LOWER('OP')
  THEN COALESCE(cat.op_supplier_group, 'OTHER')
  ELSE 'OTHER'
  END AS supplier_group,
 inv.inv_cost_currency_code AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 SUM(inv.eoh_total_cost + inv.eoh_in_transit_total_cost) AS eoh_ttl_c,
 SUM(inv.eoh_total_units + inv.eoh_in_transit_total_units) AS eoh_ttl_units,
 SUM(inv.boh_total_cost + inv.boh_in_transit_total_cost) AS boh_ttl_c,
 SUM(inv.boh_total_units + inv.boh_in_transit_total_units) AS boh_ttl_units,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_cost + inv.boh_in_transit_total_cost
   ELSE NULL
   END) AS beginofmonth_bop_c,
 SUM(CASE
   WHEN lyd.week_num_of_fiscal_month = 1
   THEN inv.boh_total_units + inv.boh_in_transit_total_units
   ELSE NULL
   END) AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS inv
 INNER JOIN ly_date_lkup AS lyd ON inv.week_num = lyd.week_idnt
 INNER JOIN store_lkup AS st ON CAST(inv.store_num AS FLOAT64) = st.store_num AND LOWER(st.banner) = LOWER('OP')
 INNER JOIN cat_lkup AS cat ON LOWER(inv.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 inv.department_num,
 dropship_ind,
 quantrix_category,
 supplier_group,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(supplier_group) 	, COLUMN(dropship_ind) 	ON SUPPLIER_INVENTORY_OP


-- 4. Receipts


CREATE TEMPORARY TABLE IF NOT EXISTS supplier_receipts
AS
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 rpt.month_num AS month_idnt,
 rpt.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 rpt.department_num,
 rpt.dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN LOWER(st.banner) = LOWER('FP')
  THEN COALESCE(cat.fp_supplier_group, 'OTHER')
  WHEN LOWER(st.banner) = LOWER('OP')
  THEN COALESCE(cat.op_supplier_group, 'OTHER')
  ELSE 'OTHER'
  END AS supplier_group,
 rpt.receipts_cost_currency_code AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_unit,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 SUM(rpt.receipts_regular_cost + rpt.receipts_clearance_cost + rpt.receipts_crossdock_regular_cost + rpt.receipts_crossdock_clearance_cost
   ) AS ttl_porcpt_c,
 SUM(rpt.receipts_regular_units + rpt.receipts_clearance_units + rpt.receipts_crossdock_regular_units + rpt.receipts_crossdock_clearance_units
   ) AS ttl_porcpt_c_units,
 0 AS pah_tsfr_in_c,
 0 AS pah_tsfr_in_u,
 0 AS rs_stk_tsfr_in_c,
 0 AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS rpt
 INNER JOIN ly_date_lkup AS lyd ON rpt.week_num = lyd.week_idnt
 INNER JOIN store_lkup AS st ON rpt.store_num = st.store_num
 INNER JOIN cat_lkup AS cat ON LOWER(rpt.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 rpt.department_num,
 rpt.dropship_ind,
 quantrix_category,
 supplier_group,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(supplier_group) 	, COLUMN(dropship_ind) 	ON SUPPLIER_RECEIPTS


-- 5. Transfers


CREATE TEMPORARY TABLE IF NOT EXISTS supplier_transfers
AS
SELECT st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 tsf.month_num AS month_idnt,
 tsf.week_num AS week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 tsf.department_num,
 RPAD(NULL, 1, ' ') AS dropship_ind,
 cat.category AS quantrix_category,
  CASE
  WHEN LOWER(st.banner) = LOWER('FP')
  THEN COALESCE(cat.fp_supplier_group, 'OTHER')
  WHEN LOWER(st.banner) = LOWER('OP')
  THEN COALESCE(cat.op_supplier_group, 'OTHER')
  ELSE 'OTHER'
  END AS supplier_group,
 SUBSTR(CASE
   WHEN LOWER(st.channel_country) = LOWER('US')
   THEN 'USD'
   WHEN LOWER(st.channel_country) = LOWER('CA')
   THEN 'CAD'
   ELSE NULL
   END, 1, 40) AS currency,
 0 AS net_sls_r,
 0 AS net_sls_c,
 0 AS net_sls_units,
 0 AS gross_sls_r,
 0 AS gross_sls_units,
 0 AS demand_ttl_r,
 0 AS demand_ttl_units,
 0 AS eoh_ttl_c,
 0 AS eoh_ttl_units,
 0 AS boh_ttl_c,
 0 AS boh_ttl_units,
 0 AS beginofmonth_bop_c,
 0 AS beginofmonth_bop_u,
 0 AS ttl_porcpt_c,
 0 AS ttl_porcpt_c_units,
 SUM(tsf.packandhold_transfer_in_cost) AS pah_tsfr_in_c,
 SUM(tsf.packandhold_transfer_in_units) AS pah_tsfr_in_u,
 SUM(tsf.reservestock_transfer_in_cost) AS rs_stk_tsfr_in_c,
 SUM(tsf.reservestock_transfer_in_units) AS rs_stk_tsfr_in_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS tsf
 INNER JOIN ly_date_lkup AS lyd ON tsf.week_num = lyd.week_idnt
 INNER JOIN store_lkup AS st ON tsf.store_num = st.store_num
 INNER JOIN cat_lkup AS cat ON LOWER(tsf.rms_sku_num) = LOWER(cat.sku_idnt) AND LOWER(st.channel_country) = LOWER(cat.channel_country
    )
WHERE cat.fanatics_ind = 0
GROUP BY st.channel_num,
 st.cluster_name,
 st.channel_country,
 st.banner,
 month_idnt,
 week_idnt,
 lyd.month_start_day_date,
 lyd.month_end_day_date,
 lyd.week_start_day_date,
 lyd.week_end_day_date,
 tsf.department_num,
 dropship_ind,
 quantrix_category,
 supplier_group,
 currency;


--COLLECT STATS 	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) 	, COLUMN(channel_num) 	, COLUMN(week_idnt) 	, COLUMN(department_num) 	, COLUMN(quantrix_category) 	, COLUMN(supplier_group) 	, COLUMN(dropship_ind) 	ON SUPPLIER_TRANSFERS


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.category_suppliergroup_cost_ly{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.category_suppliergroup_cost_ly{{params.env_suffix}}
(SELECT *
 FROM (SELECT *
    FROM supplier_sales
    UNION ALL
    SELECT *
    FROM supplier_demand
    UNION ALL
    SELECT *
    FROM supplier_inventory_fp
    UNION ALL
    SELECT *
    FROM supplier_inventory_op
    UNION ALL
    SELECT *
    FROM supplier_receipts
    UNION ALL
    SELECT *
    FROM supplier_transfers) AS t);


--COLLECT STATS     PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, channel_num, banner, channel_country, department_num, quantrix_category, supplier_group)     ON {environment_schema}.category_suppliergroup_cost_ly{env_suffix}