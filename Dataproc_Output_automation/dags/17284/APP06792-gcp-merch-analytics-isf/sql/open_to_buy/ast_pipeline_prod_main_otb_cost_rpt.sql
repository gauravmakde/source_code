--This script builds out the dataset for OTB Cost Reporting
--Data is at Banner/Dept/Month and is for current a future
--Author:  Isaac Wallick
--Date Created:8/30/2022
--Last Edited:3/13/2024


--lets create a store mapping for country and banner we only need open stores for everything but OO



CREATE TEMPORARY TABLE IF NOT EXISTS otb_loc_base
AS
SELECT store_num,
 channel_num,
 channel_desc,
 channel_brand,
 channel_country,
   FORMAT('%11d', channel_num) || ',' || channel_desc AS channel,
  CASE
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Rack'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Rack'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(channel_brand) IN (LOWER('Nordstrom'), LOWER('Nordstrom_Rack'))
 AND channel_num IN (110, 120, 140, 210, 220, 240, 250, 260, 310, 930, 940, 990, 111, 121, 211, 221, 261, 311)
 AND LOWER(store_name) NOT LIKE LOWER('CLSD%')
 AND LOWER(store_name) NOT LIKE LOWER('CLOSED%')
GROUP BY store_num,
 channel_brand,
 channel_country,
 channel_num,
 channel_desc;


--COLLECT STATS      PRIMARY INDEX (STORE_NUM)      ,COLUMN(STORE_NUM)      ON otb_loc_base


CREATE TEMPORARY TABLE IF NOT EXISTS otb_loc_oo
AS
SELECT store_num,
 channel_num,
 channel_desc,
 channel_brand,
 channel_country,
   FORMAT('%11d', channel_num) || ',' || channel_desc AS channel,
  CASE
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Rack'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Rack'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(channel_brand) IN (LOWER('Nordstrom'), LOWER('Nordstrom_Rack'))
 AND channel_num IN (110, 120, 140, 210, 220, 240, 250, 260, 310, 930, 940, 990, 111, 121, 211, 221, 261, 311)
GROUP BY store_num,
 channel_brand,
 channel_country,
 channel_num,
 channel_desc;


--COLLECT STATS      PRIMARY INDEX (STORE_NUM)      ,COLUMN(STORE_NUM)      ON otb_loc_oo


CREATE TEMPORARY TABLE IF NOT EXISTS otb_date
AS
SELECT week_idnt,
 month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
    'FY' || SUBSTR(SUBSTR(CAST(fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || quarter_abrv AS quarter,
 fiscal_year_num,
  CASE
  WHEN CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date
  THEN 1
  ELSE 0
  END AS curr_month_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE CASE
   WHEN CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date
   THEN 1
   ELSE 0
   END = 1
 OR day_date >= CURRENT_DATE('PST8PDT')
GROUP BY week_idnt,
 month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 quarter,
 fiscal_year_num,
 curr_month_flag;


--COLLECT STATS      PRIMARY INDEX (WEEK_IDNT)      ,COLUMN(WEEK_IDNT)      ,COLUMN(MONTH_IDNT)      ,COLUMN(QUARTER_IDNT)      ,COLUMN(FISCAL_YEAR_NUM)      ON otb_date


CREATE TEMPORARY TABLE IF NOT EXISTS otb_mtd
AS
SELECT b.week_idnt,
 a.month_idnt,
 b.month_label,
 b.month_454_label,
     FORMAT('%11d', b.fiscal_year_num) || '' || FORMAT('%4d', b.fiscal_month_num) || ' ' || b.month_abrv AS month_id,
 b.quarter_idnt,
 b.quarter_label,
    'FY' || SUBSTR(SUBSTR(CAST(b.fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || b.quarter_abrv AS quarter,
 b.half_label,
 b.fiscal_year_num
FROM (SELECT MAX(month_idnt) AS month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE week_start_day_date <= CURRENT_DATE('PST8PDT')) AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.month_idnt = b.month_idnt
WHERE b.week_start_day_date <= CURRENT_DATE('PST8PDT')
GROUP BY b.week_idnt,
 a.month_idnt,
 b.month_label,
 b.month_454_label,
 month_id,
 b.quarter_idnt,
 b.quarter_label,
 quarter,
 b.half_label,
 b.fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (WEEK_IDNT)      ,COLUMN(WEEK_IDNT)      ,COLUMN(MONTH_IDNT)      ON otb_mtd


CREATE TEMPORARY TABLE IF NOT EXISTS otb_current_wtd_rcpts
AS
SELECT dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num,
 SUM(rp_rcpts_active_wtd_c0) AS ty_rcpts_rp_active_wtd_c,
 SUM(non_rp_rcpts_active_wtd_c0) AS ty_rcpts_nrp_active_wtd_c,
 SUM(rcpts_inactive_wtd_c0) AS ty_rcpts_inactive_wtd_c
FROM (SELECT a.department_id AS dept_id,
    CASE
    WHEN LOWER(a.banner_id) = LOWER('1')
    THEN 'US NORDSTROM'
    WHEN LOWER(a.banner_id) = LOWER('2')
    THEN 'CA NORDSTROM'
    WHEN LOWER(a.banner_id) = LOWER('3')
    THEN 'US RACK'
    WHEN LOWER(a.banner_id) = LOWER('4')
    THEN 'CA RACK'
    ELSE FORMAT('%4d', 0)
    END AS banner,
    CASE
    WHEN LOWER(a.banner_id) IN (LOWER('1'), LOWER('3'))
    THEN 'US'
    WHEN LOWER(a.banner_id) IN (LOWER('2'), LOWER('4'))
    THEN 'CA'
    ELSE FORMAT('%4d', 0)
    END AS country,
   b.month_idnt,
   b.month_label,
   b.quarter,
   b.fiscal_year_num,
   CAST(a.rp_rcpts_active_wtd_c AS FLOAT64) AS rp_rcpts_active_wtd_c0,
   CAST(a.non_rp_rcpts_active_wtd_c AS FLOAT64) AS non_rp_rcpts_active_wtd_c0,
   CAST(a.rcpts_inactive_wtd_c AS FLOAT64) AS rcpts_inactive_wtd_c0
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_mfp_cost_poreceipts_wtd_vw AS a
   INNER JOIN otb_date AS b ON a.week_id = b.week_idnt
  WHERE LOWER(a.fulfilment_id) = LOWER('3')
  GROUP BY a.week_id,
   b.month_idnt,
   b.month_label,
   b.quarter,
   b.fiscal_year_num,
   dept_id,
   a.rp_rcpts_active_wtd_c,
   a.non_rp_rcpts_active_wtd_c,
   a.rcpts_inactive_wtd_c,
   banner,
   country) AS t2
GROUP BY dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_current_wtd_rcpts


CREATE TEMPORARY TABLE IF NOT EXISTS otb_mtd_rcpts
AS
SELECT dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num,
 SUM(ty_rp_receipts_active_cost_amt) AS ty_rcpts_rp_active_wtd_c,
 SUM(ty_non_rp_receipts_active_cost_amt) AS ty_rcpts_nrp_active_wtd_c,
 SUM(ty_receipts_inactive_cost_amt) AS ty_rcpts_inactive_wtd_c
FROM (SELECT a.dept_num AS dept_id,
    CASE
    WHEN a.banner_country_num = 1
    THEN 'US NORDSTROM'
    WHEN a.banner_country_num = 2
    THEN 'CA NORDSTROM'
    WHEN a.banner_country_num = 3
    THEN 'US RACK'
    WHEN a.banner_country_num = 4
    THEN 'CA RACK'
    ELSE FORMAT('%4d', 0)
    END AS banner,
    CASE
    WHEN a.banner_country_num IN (1, 3)
    THEN 'US'
    WHEN a.banner_country_num IN (2, 4)
    THEN 'CA'
    ELSE FORMAT('%4d', 0)
    END AS country,
   b.month_idnt,
   b.month_label,
   b.quarter,
   b.fiscal_year_num,
   a.ty_rp_receipts_active_cost_amt,
   a.ty_non_rp_receipts_active_cost_amt,
   a.ty_receipts_inactive_cost_amt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_banner_country_fact AS a
   INNER JOIN otb_mtd AS b ON a.week_num = b.week_idnt
  WHERE a.fulfill_type_num = 3
  GROUP BY a.week_num,
   b.month_idnt,
   b.month_label,
   b.quarter,
   b.fiscal_year_num,
   dept_id,
   a.ty_rp_receipts_active_cost_amt,
   a.ty_non_rp_receipts_active_cost_amt,
   a.ty_receipts_inactive_cost_amt,
   banner,
   country) AS c
GROUP BY dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_mtd_rcpts


CREATE TEMPORARY TABLE IF NOT EXISTS otb_wtd_rcpts
AS
SELECT dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num,
 SUM(ty_rcpts_rp_active_wtd_c) AS ty_rcpts_rp_active_wtd_c,
 SUM(ty_rcpts_nrp_active_wtd_c) AS ty_rcpts_nrp_active_wtd_c,
 SUM(ty_rcpts_inactive_wtd_c) AS ty_rcpts_inactive_wtd_c
FROM (SELECT *
   FROM otb_current_wtd_rcpts
   UNION ALL
   SELECT *
   FROM otb_mtd_rcpts) AS a
GROUP BY dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_wtd_rcpts


CREATE TEMPORARY TABLE IF NOT EXISTS otb_ty
AS
SELECT dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num,
 SUM(cp_receipts_reserve_cost_amt) AS cp_rcpts_reserve_c,
 SUM(cp_receipts_active_cost_amt) AS cp_rcpts_active_c,
 SUM(cp_receipts_inactive_cost_amt) AS cp_rcpts_inactive_c
FROM (SELECT a.dept_num AS dept_id,
   a.week_num AS week_id,
   b.month_idnt,
   b.month_label,
   b.quarter,
   b.fiscal_year_num,
    CASE
    WHEN a.banner_country_num = 1
    THEN 'US Nordstrom'
    WHEN a.banner_country_num = 2
    THEN 'CA Nordstrom'
    WHEN a.banner_country_num = 3
    THEN 'US Rack'
    WHEN a.banner_country_num = 4
    THEN 'CA Rack'
    ELSE FORMAT('%4d', 0)
    END AS banner,
    CASE
    WHEN a.banner_country_num IN (1, 3)
    THEN 'US'
    WHEN a.banner_country_num IN (2, 4)
    THEN 'CA'
    ELSE FORMAT('%4d', 0)
    END AS country,
   a.cp_receipts_reserve_cost_amt,
   a.cp_receipts_active_cost_amt,
   a.cp_receipts_inactive_cost_amt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_banner_country_fact AS a
   INNER JOIN otb_date AS b ON a.week_num = b.week_idnt
  WHERE a.fulfill_type_num = 3
  GROUP BY dept_id,
   week_id,
   b.month_idnt,
   b.month_label,
   b.quarter,
   b.fiscal_year_num,
   banner,
   country,
   a.cp_receipts_reserve_cost_amt,
   a.cp_receipts_active_cost_amt,
   a.cp_receipts_inactive_cost_amt) AS c
GROUP BY dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_ty


CREATE TEMPORARY TABLE IF NOT EXISTS otb_rp
AS
SELECT a.dept_idnt AS dept_id,
 b.month_idnt,
 b.month_label,
 b.quarter,
 b.fiscal_year_num,
  CASE
  WHEN a.banner_id = 1
  THEN 'US Nordstrom'
  WHEN a.banner_id = 2
  THEN 'CA Nordstrom'
  WHEN a.banner_id = 3
  THEN 'US Rack'
  WHEN a.banner_id = 4
  THEN 'CA Rack'
  ELSE FORMAT('%4d', 0)
  END AS banner,
  CASE
  WHEN a.banner_id IN (1, 3)
  THEN 'US'
  WHEN a.banner_id IN (2, 4)
  THEN 'CA'
  ELSE FORMAT('%4d', 0)
  END AS country,
 SUM(a.rp_antspnd_c) AS ty_rp_anticipated_spend_c,
 SUM(a.rp_antspnd_u) AS ty_rp_anticipated_spend_u
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_current AS a
 INNER JOIN otb_date AS b ON a.week_idnt = b.week_idnt
WHERE a.ft_id = 3
GROUP BY dept_id,
 b.month_idnt,
 b.month_label,
 b.quarter,
 b.fiscal_year_num,
 banner,
 country;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_rp


CREATE TEMPORARY TABLE IF NOT EXISTS otb_cm
AS
SELECT dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num,
 SUM(`A121810`) AS ty_inactive_cm_c,
 SUM(`A121811`) AS ty_active_cm_c,
 SUM(`A121812`) AS nrp_cm_c,
 SUM(`A121813`) AS rp_cm_c
FROM (SELECT a.dept_id,
   b.banner,
   b.channel_country AS country,
   c.month_idnt,
   c.month_label,
   c.quarter,
   c.fiscal_year_num,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('RKPACKHOLD')) AND LOWER(b.banner) IN (LOWER('US NORDSTROM'), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('RKPACKHOLD')) AND LOWER(b.banner) IN (LOWER('CA NORDSTROM'), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121810`,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER'), LOWER('REPLNSHMNT')) AND LOWER(b
       .banner) IN (LOWER('US NORDSTROM'), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER'), LOWER('REPLNSHMNT')) AND LOWER(b
       .banner) IN (LOWER('CA NORDSTROM'), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121811`,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER')) AND LOWER(b.banner) IN (LOWER('US NORDSTROM'
        ), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER')) AND LOWER(b.banner) IN (LOWER('CA NORDSTROM'
        ), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121812`,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('REPLNSHMNT')) AND LOWER(b.banner) IN (LOWER('US NORDSTROM'), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('REPLNSHMNT')) AND LOWER(b.banner) IN (LOWER('CA NORDSTROM'), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121813`
  FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.ab_cm_orders_current AS a
   INNER JOIN otb_loc_base AS b ON a.org_id = b.channel_num
   INNER JOIN otb_date AS c ON CAST(a.fiscal_month_id AS FLOAT64) = c.month_idnt
  GROUP BY a.dept_id,
   a.style_id,
   a.style_group_id,
   a.class_name,
   a.subclass_name,
   b.banner,
   b.channel_country,
   a.supp_id,
   a.fiscal_month_id,
   a.otb_eow_date,
   a.vpn,
   a.nrf_color_code,
   a.supp_color,
   a.plan_type,
   a.ttl_cost_us,
   a.ttl_cost_ca,
   a.cm_dollars,
   c.month_idnt,
   c.month_label,
   c.quarter,
   c.fiscal_year_num,
   a.po_key,
   a.plan_key) AS t1
GROUP BY dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,BANNER,COUNTRY,month_label)      ,COLUMN(DEPT_ID)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_cm


CREATE TEMPORARY TABLE IF NOT EXISTS otb_oo_base
AS
SELECT a.purchase_order_number,
 a.rms_sku_num,
 a.rms_casepack_num,
 b.channel_num,
 b.channel_desc,
 b.channel_country,
 b.banner,
 a.store_num,
  CASE
  WHEN a.week_num < (SELECT week_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE('PST8PDT'))
  THEN (SELECT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'))
  ELSE a.week_num
  END AS week_num,
 a.order_type,
 a.latest_approval_date,
 a.quantity_open,
 a.total_estimated_landing_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS a
 INNER JOIN otb_loc_oo AS b ON a.store_num = b.store_num
WHERE a.quantity_open > 0
 AND (LOWER(a.status) = LOWER('CLOSED') AND a.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY) OR LOWER(a.status
     ) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
 AND a.first_approval_date IS NOT NULL;


--COLLECT STATS      PRIMARY INDEX (RMS_SKU_NUM,STORE_NUM)      ,COLUMN(RMS_SKU_NUM)      ,COLUMN(STORE_NUM)      ON otb_oo_base


CREATE TEMPORARY TABLE IF NOT EXISTS otb_oo_hierarchy
AS
SELECT b.rms_sku_num,
 b.channel_country,
 a.dept_num,
 a.dept_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS a
 INNER JOIN (SELECT DISTINCT rms_sku_num,
   channel_country
  FROM otb_oo_base) AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(a.channel_country) = LOWER(b.channel_country
    )
GROUP BY b.rms_sku_num,
 b.channel_country,
 a.dept_num,
 a.dept_desc;


--COLLECT STATS      PRIMARY INDEX (RMS_SKU_NUM)      ,COLUMN(RMS_SKU_NUM)      ON otb_oo_hierarchy


CREATE TEMPORARY TABLE IF NOT EXISTS otb_oo_final
AS
SELECT dept_num,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num,
 SUM(`A121810`) AS ty_rcpts_oo_inactive_c,
 SUM(`A121811`) AS ty_rcpts_oo_active_rp_c,
 SUM(`A121812`) AS ty_rcpts_oo_active_nrp_c
FROM (SELECT b.dept_num,
   a.banner,
   a.channel_country AS country,
   d.month_idnt,
   d.month_label,
   d.quarter,
   d.fiscal_year_num,
    CASE
    WHEN a.channel_num IN (930, 220, 221)
    THEN a.total_estimated_landing_cost
    ELSE 0
    END AS `A121810`,
    CASE
    WHEN LOWER(a.order_type) IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER')) AND a.channel_num NOT IN (930, 220
       , 221)
    THEN a.total_estimated_landing_cost
    ELSE 0
    END AS `A121811`,
    CASE
    WHEN a.channel_num NOT IN (930, 220, 221) AND LOWER(a.order_type) NOT IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER'
        ))
    THEN a.total_estimated_landing_cost
    ELSE 0
    END AS `A121812`
  FROM otb_oo_base AS a
   INNER JOIN otb_oo_hierarchy AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(a.channel_country) = LOWER(b
      .channel_country)
   INNER JOIN otb_date AS d ON a.week_num = d.week_idnt
  GROUP BY a.purchase_order_number,
   a.rms_sku_num,
   a.rms_casepack_num,
   b.dept_num,
   b.dept_desc,
   a.channel_num,
   a.channel_desc,
   a.store_num,
   a.banner,
   a.channel_country,
   a.week_num,
   d.month_idnt,
   d.month_label,
   d.quarter,
   d.fiscal_year_num,
   a.order_type,
   a.latest_approval_date,
   a.quantity_open,
   a.total_estimated_landing_cost) AS t1
GROUP BY dept_num,
 banner,
 country,
 month_idnt,
 month_label,
 quarter,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX(DEPT_NUM,BANNER,COUNTRY,MONTH_LABEL)      ,COLUMN(DEPT_NUM)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_oo_final


CREATE TEMPORARY TABLE IF NOT EXISTS otb_final_stg
AS
SELECT dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(cp_rcpts_reserve_c) AS cp_rcpts_reserve_c,
 SUM(cp_rcpts_active_c) AS cp_rcpts_active_c,
 SUM(cp_rcpts_active_c - cp_rcpts_reserve_c) AS cp_rcpts_less_reserve_c,
 SUM(ty_rcpts_rp_active_wtd_c + ty_rcpts_nrp_active_wtd_c) AS ty_npr_rp_mtd_rcpts_c,
 SUM(ty_rcpts_oo_active_rp_c + ty_rcpts_oo_active_nrp_c) AS ty_npr_rp_oo_rcpts_c,
 SUM(ty_rcpts_nrp_active_wtd_c + ty_rcpts_oo_active_nrp_c + ty_active_cm_c) AS ty_nrp_rcpts_mtd_oo_cm_c,
 SUM(ty_rcpts_nrp_active_wtd_c) AS ty_nrp_rcpts_mtd_c,
 SUM(ty_rcpts_oo_active_nrp_c) AS ty_nrp_oo_c,
 SUM(ty_active_cm_c) AS ty_nrp_cm_c,
 SUM(nrp_cm_c) AS nrp_cm_c,
 SUM(rp_cm_c) AS rp_cm_c,
 SUM(ty_rp_anticipated_spend_c) AS ty_rp_anticipated_spend_c,
 SUM(ty_rcpts_rp_active_wtd_c + ty_rcpts_oo_active_rp_c) AS ty_rp_rcpts_mtd_oo_c,
 SUM(ty_rcpts_rp_active_wtd_c) AS ty_rp_rcpts_mtd_c,
 SUM(ty_rcpts_oo_active_rp_c) AS ty_rp_oo_c,
 SUM(cp_rcpts_inactive_c) AS inactive_cp_rcpts_c,
 SUM(ty_rcpts_inactive_wtd_c + ty_rcpts_oo_inactive_c + ty_inactive_cm_c) AS inactive_ty_mtd_oo_cm_rcpts_c,
 SUM(ty_rcpts_inactive_wtd_c) AS inactive_ty_mtd_c,
 SUM(ty_rcpts_oo_inactive_c) AS inactive_ty_oo_c,
 SUM(ty_inactive_cm_c) AS inactive_ty_cm_c
FROM (SELECT cast(trunc(dept_id) as integer) AS dept_id,
--CAST(dept_id AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(quarter, 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    CAST(cp_rcpts_reserve_c AS NUMERIC) AS cp_rcpts_reserve_c,
    CAST(cp_rcpts_active_c AS NUMERIC) AS cp_rcpts_active_c,
    CAST(cp_rcpts_inactive_c AS NUMERIC) AS cp_rcpts_inactive_c,
    0 AS ty_rcpts_rp_active_wtd_c,
    0 AS ty_rcpts_nrp_active_wtd_c,
    0 AS ty_rcpts_inactive_wtd_c,
    0 AS ty_rp_anticipated_spend_c,
    0 AS ty_inactive_cm_c,
    0 AS ty_active_cm_c,
    0 AS nrp_cm_c,
    0 AS rp_cm_c,
    0 AS ty_rcpts_oo_inactive_c,
    0 AS ty_rcpts_oo_active_rp_c,
    0 AS ty_rcpts_oo_active_nrp_c
   FROM otb_ty
   UNION ALL
   SELECT cast(trunc(dept_id) as integer) AS dept_id,
   --CAST(dept_id AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(quarter, 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS cp_rcpts_reserve_c,
    0 AS cp_rcpts_active_c,
    0 AS cp_rcpts_inactive_c,
    ROUND(CAST(ty_rcpts_rp_active_wtd_c AS NUMERIC), 4) AS ty_rcpts_rp_active_wtd_c,
    ROUND(CAST(ty_rcpts_nrp_active_wtd_c AS NUMERIC), 4) AS ty_rcpts_nrp_active_wtd_c,
    ROUND(CAST(ty_rcpts_inactive_wtd_c AS NUMERIC), 4) AS ty_rcpts_inactive_wtd_c,
    0 AS ty_rp_anticipated_spend_c,
    0 AS ty_inactive_cm_c,
    0 AS ty_active_cm_c,
    0 AS nrp_cm_c,
    0 AS rp_cm_c,
    0 AS ty_rcpts_oo_inactive_c,
    0 AS ty_rcpts_oo_active_rp_c,
    0 AS ty_rcpts_oo_active_nrp_c
   FROM otb_wtd_rcpts
   UNION ALL
   SELECT cast(trunc(dept_id) as integer) AS dept_id,
   --CAST(dept_id AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(quarter, 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS cp_rcpts_reserve_c,
    0 AS cp_rcpts_active_c,
    0 AS cp_rcpts_inactive_c,
    0 AS ty_rcpts_rp_active_wtd_c,
    0 AS ty_rcpts_nrp_active_wtd_c,
    0 AS ty_rcpts_inactive_wtd_c,
    CAST(ty_rp_anticipated_spend_c AS NUMERIC) AS ty_rp_anticipated_spend_c,
    0 AS ty_inactive_cm_c,
    0 AS ty_active_cm_c,
    0 AS nrp_cm_c,
    0 AS rp_cm_c,
    0 AS ty_rcpts_oo_inactive_c,
    0 AS ty_rcpts_oo_active_rp_c,
    0 AS ty_rcpts_oo_active_nrp_c
   FROM otb_rp
   UNION ALL
   SELECT cast(trunc(dept_id) as integer) AS dept_id,
   --CAST(dept_id AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(quarter, 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS cp_rcpts_reserve_c,
    0 AS cp_rcpts_active_c,
    0 AS cp_rcpts_inactive_c,
    0 AS ty_rcpts_rp_active_wtd_c,
    0 AS ty_rcpts_nrp_active_wtd_c,
    0 AS ty_rcpts_inactive_wtd_c,
    0 AS ty_rp_anticipated_spend_c,
    CAST(ty_inactive_cm_c AS NUMERIC) AS ty_inactive_cm_c,
    CAST(ty_active_cm_c AS NUMERIC) AS ty_active_cm_c,
    CAST(nrp_cm_c AS NUMERIC) AS nrp_cm_c,
    CAST(rp_cm_c AS NUMERIC) AS rp_cm_c,
    0 AS ty_rcpts_oo_inactive_c,
    0 AS ty_rcpts_oo_active_rp_c,
    0 AS ty_rcpts_oo_active_nrp_c
   FROM otb_cm
   UNION ALL
   SELECT cast(trunc(dept_num) as integer) AS dept_id,
   --CAST(dept_num AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(quarter, 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS cp_rcpts_reserve_c,
    0 AS cp_rcpts_active_c,
    0 AS cp_rcpts_inactive_c,
    0 AS ty_rcpts_rp_active_wtd_c,
    0 AS ty_rcpts_nrp_active_wtd_c,
    0 AS ty_rcpts_inactive_wtd_c,
    0 AS ty_rp_anticipated_spend_c,
    0 AS ty_inactive_cm_c,
    0 AS ty_active_cm_c,
    0 AS nrp_cm_c,
    0 AS rp_cm_c,
    CAST(ty_rcpts_oo_inactive_c AS NUMERIC) AS ty_rcpts_oo_inactive_c,
    CAST(ty_rcpts_oo_active_rp_c AS NUMERIC) AS ty_rcpts_oo_active_rp_c,
    CAST(ty_rcpts_oo_active_nrp_c AS NUMERIC) AS ty_rcpts_oo_active_nrp_c
   FROM otb_oo_final) AS a
GROUP BY dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX(DEPT_ID,BANNER,COUNTRY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(BANNER)      ,COLUMN(COUNTRY)      ,COLUMN(MONTH_LABEL)      ON otb_final_stg


TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.otb_cost_current;


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.otb_cost_current
(SELECT TRIM(FORMAT('%11d', b.division_num) || ',' || b.division_name) AS division,
  TRIM(FORMAT('%11d', b.subdivision_num) || ',' || b.subdivision_name) AS subdivision,
  TRIM(FORMAT('%11d', a.dept_id) || ',' || b.dept_name) AS department,
  a.banner,
  a.country,
  a.month_idnt,
  a.month_label AS month,
  a.quarter_idnt AS quarter,
  a.fiscal_year_num AS year,
  b.active_store_ind AS `active vs inactive`,
  a.cp_rcpts_reserve_c,
  a.cp_rcpts_active_c,
  a.cp_rcpts_less_reserve_c AS `cp rcpts less reserve c`,
  a.ty_npr_rp_mtd_rcpts_c AS `ty npr rp mtd rcpts c`,
  a.ty_npr_rp_oo_rcpts_c AS `ty npr rp oo rcpts c`,
  a.ty_nrp_rcpts_mtd_oo_cm_c AS `ty nrp rcpts mtd oo cm c`,
  a.ty_nrp_rcpts_mtd_c AS `ty nrp rcpts mtd c`,
  a.ty_nrp_oo_c AS `ty nrp oo c`,
  a.ty_nrp_cm_c AS `ty nrp cm c`,
  a.nrp_cm_c AS `nrp cm c`,
  a.rp_cm_c AS `rp cm c`,
  a.ty_rp_anticipated_spend_c AS `ty rp anticipated spend c`,
  a.ty_rp_rcpts_mtd_oo_c AS `ty rp rcpts mtd oo c`,
  a.ty_rp_rcpts_mtd_c AS `ty rp rcpts mtd c`,
  a.ty_rp_oo_c AS `ty rp oo c`,
  a.inactive_cp_rcpts_c AS `inactive cp rcpts c`,
  a.inactive_ty_mtd_oo_cm_rcpts_c AS `inactive ty mtd oo cm rcpts c`,
  a.inactive_ty_mtd_c AS `inactive ty mtd c`,
  a.inactive_ty_oo_c AS `inactive ty oo c`,
  a.inactive_ty_cm_c AS `inactive ty cm c`,
  CAST(CURRENT_DATE('PST8PDT') AS STRING) AS process_dt
 FROM otb_final_stg AS a
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS b ON a.dept_id = b.dept_num
 WHERE a.cp_rcpts_reserve_c <> 0
  OR a.cp_rcpts_active_c <> 0
  OR a.cp_rcpts_less_reserve_c <> 0
  OR a.ty_npr_rp_mtd_rcpts_c <> 0
  OR a.ty_npr_rp_oo_rcpts_c <> 0
  OR a.ty_nrp_rcpts_mtd_oo_cm_c <> 0
  OR a.ty_nrp_rcpts_mtd_c <> 0
  OR a.ty_nrp_oo_c <> 0
  OR a.ty_nrp_cm_c <> 0
  OR a.nrp_cm_c <> 0
  OR a.rp_cm_c <> 0
  OR a.ty_rp_anticipated_spend_c <> 0
  OR a.ty_rp_rcpts_mtd_oo_c <> 0
  OR a.ty_rp_rcpts_mtd_c <> 0
  OR a.ty_rp_oo_c <> 0
  OR a.inactive_cp_rcpts_c <> 0
  OR a.inactive_ty_mtd_oo_cm_rcpts_c <> 0
  OR a.inactive_ty_mtd_c <> 0
  OR a.inactive_ty_oo_c <> 0
  OR a.inactive_ty_cm_c <> 0);


--COLLECT STATS      PRIMARY INDEX(Department,Banner, "Month" )      ,COLUMN(Department)      ,COLUMN(Banner)      ,COLUMN("Month")      ON `{{params.gcp_project_id}}`.t2dl_DAS_OPEN_TO_BUY.OTB_COST_CURRENT