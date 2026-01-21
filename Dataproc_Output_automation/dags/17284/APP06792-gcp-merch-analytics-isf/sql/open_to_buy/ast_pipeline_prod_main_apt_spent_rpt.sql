/*This SQL builds the back end data that is used in the APT Spend report
Includes Plan, rcpts, OO, and CM data
Plan data is from APT not MFP
Author: Isaac J Wallick
DAG:ast_prod_main_apt_spent_rpt
Date Created:1/6/23
Updated: 2/16/23
*/



CREATE TEMPORARY TABLE IF NOT EXISTS sc_cluster_map
CLUSTER BY chnl_idnt, cluster_name, selling_country, selling_brand
AS
SELECT selling_country,
 selling_brand,
 cluster_name,
  CASE
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('NORDSTROM_CANADA_STORES'))
  THEN 111
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('NORDSTROM_CANADA_ONLINE'))
  THEN 121
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('NORDSTROM_STORES'))
  THEN 110
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('NORDSTROM_ONLINE'))
  THEN 120
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('RACK_ONLINE'))
  THEN 250
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('RACK_CANADA_STORES'))
  THEN 211
  WHEN LOWER(RTRIM(cluster_name)) IN (LOWER(RTRIM('RACK STORES')), LOWER(RTRIM('PRICE')), LOWER(RTRIM('HYBRID')), LOWER(RTRIM('BRAND'
      )))
  THEN 210
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('NORD CA RSWH'))
  THEN 311
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('NORD US RSWH'))
  THEN 310
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('RACK CA RSWH'))
  THEN 261
  WHEN LOWER(RTRIM(cluster_name)) = LOWER(RTRIM('RACK US RSWH'))
  THEN 260
  ELSE NULL
  END AS chnl_idnt
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_reporting.merch_assortment_supplier_cluster_plan_fact
GROUP BY selling_country,
 selling_brand,
 cluster_name,
 chnl_idnt;


--COLLECT STATS 	PRIMARY INDEX (chnl_idnt, cluster_name,SELLING_COUNTRY,SELLING_BRAND) 	,COLUMN(chnl_idnt) 	,COLUMN(cluster_name) 	,COLUMN(SELLING_COUNTRY) 	,COLUMN(SELLING_BRAND) ON sc_cluster_map 


--drop table sc_date


--lets create a week to month mapping


CREATE TEMPORARY TABLE IF NOT EXISTS sc_date
CLUSTER BY week_idnt
AS
SELECT week_idnt,
 month_idnt,
 month_label,
 month_454_label,
     FORMAT('%11d', fiscal_year_num) || '' || FORMAT('%11d', fiscal_month_num) || ' ' || month_abrv AS month_id,
 quarter_idnt,
 quarter_label,
    'FY' || SUBSTR(SUBSTR(CAST(fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || quarter_abrv AS quarter,
 half_label,
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
 month_454_label,
 month_id,
 quarter_idnt,
 quarter_label,
 quarter,
 half_label,
 fiscal_year_num,
 curr_month_flag;


--COLLECT STATS      PRIMARY INDEX (WEEK_IDNT)      ,COLUMN(WEEK_IDNT)      ,COLUMN(MONTH_IDNT)      ,COLUMN(QUARTER_IDNT)      ,COLUMN(FISCAL_YEAR_NUM)      ON sc_date


--Lets current week so we can get MTD rcpts


CREATE TEMPORARY TABLE IF NOT EXISTS sc_wtd_date
CLUSTER BY week_idnt
AS
SELECT b.week_idnt,
 a.month_idnt,
 b.month_label,
 b.month_454_label,
     FORMAT('%11d', b.fiscal_year_num) || '' || FORMAT('%11d', b.fiscal_month_num) || ' ' || b.month_abrv AS month_id,
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


--COLLECT STATS      PRIMARY INDEX (WEEK_IDNT)      ,COLUMN(WEEK_IDNT)      ON sc_wtd_date


--Lets collect supp attributes    


--DROP TABLE sc_supp_gp


CREATE TEMPORARY TABLE IF NOT EXISTS sc_supp_gp
CLUSTER BY dept_num, supplier_group
AS
SELECT dept_num,
 supplier_num,
 supplier_group,
  CASE
  WHEN LOWER(RTRIM(banner)) = LOWER(RTRIM('FP'))
  THEN 'NORDSTROM'
  ELSE 'NORDSTROM_RACK'
  END AS selling_brand,
 buy_planner,
 preferred_partner_desc,
 areas_of_responsibility,
 is_npg,
 diversity_group,
 nord_to_rack_transfer_rate
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.supp_dept_map_dim AS a
GROUP BY dept_num,
 supplier_num,
 supplier_group,
 selling_brand,
 buy_planner,
 preferred_partner_desc,
 areas_of_responsibility,
 is_npg,
 diversity_group,
 nord_to_rack_transfer_rate;


--COLLECT STATS      PRIMARY INDEX(DEPT_NUM,SUPPLIER_GROUP)      ,COLUMN(DEPT_NUM)      ,COLUMN(SUPPLIER_GROUP) 	ON sc_supp_gp


--lets collect category attributes


CREATE TEMPORARY TABLE IF NOT EXISTS sc_cattr
CLUSTER BY dept_num, category
AS
SELECT dept_num,
 class_num,
 sbclass_num,
 category,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS a
GROUP BY dept_num,
 class_num,
 sbclass_num,
 category,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map;


--COLLECT STATS      PRIMARY INDEX(dept_num,CATEGORY)      ,COLUMN(dept_num)      ,COLUMN(CATEGORY)      ON sc_cattr


--Lets get the current and future plan data    


--drop table sc_plan_base


CREATE TEMPORARY TABLE IF NOT EXISTS sc_plan_base
CLUSTER BY department_number, supplier_group, category, month_id
AS
SELECT t8.selling_country,
 t8.selling_brand,
 t8.chnl_idnt,
 t8.supplier_group,
 t8.alt_inv_model,
 t8.category,
 t8.department_number,
 t8.month_id,
 t8.month_label,
 t8.half_label,
 t8.quarter,
 t8.fiscal_year_num,
 t8.category_planner_1,
 t8.category_planner_2,
 t8.category_group,
 t8.seasonal_designation,
 t8.rack_merch_zone,
 t8.is_activewear,
 t8.channel_category_roles_1,
 t8.channel_category_roles_2,
 t8.bargainista_dept_map,
 t8.buy_planner,
 t8.preferred_partner_desc,
 t8.areas_of_responsibility,
 t8.is_npg,
 t8.diversity_group,
 t8.nord_to_rack_transfer_rate,
 SUM(t8.replenishment_receipts_less_reserve_cost_amount) AS rp_plan_rcpt_c,
 SUM(t8.replenishment_receipts_less_reserve_retail_amount) AS rp_plan_rcpt_r,
 SUM(t8.replenishment_receipts_less_reserve_units) AS rp_plan_rcpt_u,
 SUM(t8.nonreplenishment_receipts_less_reserve_cost_amount) AS nrp_plan_rcpt_c,
 SUM(t8.nonreplenishment_receipts_less_reserve_retail_amount) AS nrp_plan_rcpt_r,
 SUM(t8.nonreplenishment_receipts_less_reserve_units) AS nrp_plan_rcpt_u,
 SUM(t8.`A121834`) AS ttl_plan_rcpt_less_reserve_c,
 SUM(t8.`A121835`) AS ttl_plan_rcpt_less_reserve_r,
 SUM(t8.`A121836`) AS ttl_plan_rcpt_less_reserve_u
FROM (SELECT f.selling_country,
   f.selling_brand,
   f.chnl_idnt,
   COALESCE(f.supplier_group, 'OTHER') AS supplier_group,
   'NON-FANATICS' AS alt_inv_model,
   f.category,
   f.department_number,
   TRIM(f.month_id) AS month_id,
   TRIM(f.month_label) AS month_label,
   TRIM(f.half_label) AS half_label,
   TRIM(f.quarter) AS quarter,
   TRIM(FORMAT('%11d', f.fiscal_year_num)) AS fiscal_year_num,
   f.category_planner_1,
   f.category_planner_2,
   f.category_group,
   f.seasonal_designation,
   f.rack_merch_zone,
   f.is_activewear,
   f.channel_category_roles_1,
   f.channel_category_roles_2,
   f.bargainista_dept_map,
   g.buy_planner,
   g.preferred_partner_desc,
   g.areas_of_responsibility,
   g.is_npg,
   g.diversity_group,
   g.nord_to_rack_transfer_rate,
   f.replenishment_receipts_less_reserve_cost_amount,
   f.replenishment_receipts_less_reserve_retail_amount,
   f.replenishment_receipts_less_reserve_units,
   f.nonreplenishment_receipts_less_reserve_cost_amount,
   f.nonreplenishment_receipts_less_reserve_retail_amount,
   f.nonreplenishment_receipts_less_reserve_units,
    f.replenishment_receipts_less_reserve_cost_amount + f.nonreplenishment_receipts_less_reserve_cost_amount AS `A121834`,
    f.replenishment_receipts_less_reserve_retail_amount + f.nonreplenishment_receipts_less_reserve_retail_amount AS `A121835`
   ,
    f.replenishment_receipts_less_reserve_units + f.nonreplenishment_receipts_less_reserve_units AS `A121836`
  FROM (SELECT d.selling_country,
     d.selling_brand,
     d.chnl_idnt,
     d.cluster_name,
     d.supplier_group,
     d.category,
     e.category_planner_1,
     e.category_planner_2,
     e.category_group,
     e.seasonal_designation,
     e.rack_merch_zone,
     e.is_activewear,
     e.channel_category_roles_1,
     e.channel_category_roles_2,
     e.bargainista_dept_map,
     d.department_number,
     d.month_id,
     d.month_label,
     d.half_label,
     d.quarter,
     d.fiscal_year_num,
     d.replenishment_receipts_cost_amount,
     d.replenishment_receipts_retail_amount,
     d.replenishment_receipts_units,
     d.replenishment_receipts_less_reserve_cost_amount,
     d.replenishment_receipts_less_reserve_retail_amount,
     d.replenishment_receipts_less_reserve_units,
     d.nonreplenishment_receipts_less_reserve_cost_amount,
     d.nonreplenishment_receipts_less_reserve_retail_amount,
     d.nonreplenishment_receipts_less_reserve_units,
     d.nonreplenishment_receipts_cost_amount,
     d.nonreplenishment_receipts_retail_amount,
     d.nonreplenishment_receipts_units
    FROM (SELECT a.selling_country,
       a.selling_brand,
       b.chnl_idnt,
       a.cluster_name,
       a.supplier_group,
       a.category,
       a.department_number,
       c.month_id,
       c.month_label,
       c.half_label,
       c.quarter,
       c.fiscal_year_num,
       a.replenishment_receipts_cost_amount,
       a.replenishment_receipts_retail_amount,
       a.replenishment_receipts_units,
       a.replenishment_receipts_less_reserve_cost_amount,
       a.replenishment_receipts_less_reserve_retail_amount,
       a.replenishment_receipts_less_reserve_units,
       a.nonreplenishment_receipts_less_reserve_cost_amount,
       a.nonreplenishment_receipts_less_reserve_retail_amount,
       a.nonreplenishment_receipts_less_reserve_units,
       a.nonreplenishment_receipts_cost_amount,
       a.nonreplenishment_receipts_retail_amount,
       a.nonreplenishment_receipts_units
      FROM `{{params.gcp_project_id}}`.t2dl_das_apt_reporting.merch_assortment_supplier_cluster_plan_fact AS a
       INNER JOIN sc_date AS c ON LOWER(RTRIM(a.month_id)) = LOWER(RTRIM(c.month_454_label))
       INNER JOIN sc_cluster_map AS b ON LOWER(RTRIM(a.cluster_name)) = LOWER(RTRIM(b.cluster_name))
      WHERE LOWER(RTRIM(a.alternate_inventory_model)) = LOWER(RTRIM('OWN'))
      GROUP BY a.selling_country,
       a.selling_brand,
       a.cluster_name,
       a.category,
       a.supplier_group,
       a.department_number,
       a.replenishment_receipts_units,
       a.replenishment_receipts_retail_amount,
       a.replenishment_receipts_cost_amount,
       a.replenishment_receipts_less_reserve_units,
       a.replenishment_receipts_less_reserve_retail_amount,
       a.replenishment_receipts_less_reserve_cost_amount,
       a.nonreplenishment_receipts_units,
       a.nonreplenishment_receipts_retail_amount,
       a.nonreplenishment_receipts_cost_amount,
       a.nonreplenishment_receipts_less_reserve_units,
       a.nonreplenishment_receipts_less_reserve_retail_amount,
       a.nonreplenishment_receipts_less_reserve_cost_amount,
       c.month_label,
       c.month_id,
       c.quarter,
       c.half_label,
       c.fiscal_year_num,
       b.chnl_idnt) AS d
     LEFT JOIN sc_cattr AS e ON CAST(RTRIM(d.department_number) AS FLOAT64) = e.dept_num AND LOWER(RTRIM(d.category)) =
       LOWER(RTRIM(e.category))
    GROUP BY d.selling_country,
     d.selling_brand,
     d.chnl_idnt,
     d.cluster_name,
     d.supplier_group,
     d.category,
     d.department_number,
     d.month_id,
     d.month_label,
     d.half_label,
     d.quarter,
     d.fiscal_year_num,
     d.replenishment_receipts_cost_amount,
     d.replenishment_receipts_retail_amount,
     d.replenishment_receipts_units,
     d.replenishment_receipts_less_reserve_cost_amount,
     d.replenishment_receipts_less_reserve_retail_amount,
     d.replenishment_receipts_less_reserve_units,
     d.nonreplenishment_receipts_less_reserve_cost_amount,
     d.nonreplenishment_receipts_less_reserve_retail_amount,
     d.nonreplenishment_receipts_less_reserve_units,
     d.nonreplenishment_receipts_cost_amount,
     d.nonreplenishment_receipts_retail_amount,
     d.nonreplenishment_receipts_units,
     e.category_planner_1,
     e.category_planner_2,
     e.category_group,
     e.seasonal_designation,
     e.rack_merch_zone,
     e.is_activewear,
     e.channel_category_roles_1,
     e.channel_category_roles_2,
     e.bargainista_dept_map) AS f
   LEFT JOIN sc_supp_gp AS g ON LOWER(RTRIM(f.department_number)) = LOWER(RTRIM(g.dept_num)) AND LOWER(RTRIM(f.supplier_group
        )) = LOWER(RTRIM(g.supplier_group)) AND LOWER(RTRIM(f.selling_brand)) = LOWER(RTRIM(g.selling_brand))
  GROUP BY f.selling_country,
   f.selling_brand,
   f.chnl_idnt,
   supplier_group,
   alt_inv_model,
   f.category,
   f.department_number,
   f.month_id,
   f.month_label,
   f.half_label,
   f.quarter,
   f.fiscal_year_num,
   f.category_planner_1,
   f.category_planner_2,
   f.category_group,
   f.seasonal_designation,
   f.rack_merch_zone,
   f.is_activewear,
   f.channel_category_roles_1,
   f.channel_category_roles_2,
   f.bargainista_dept_map,
   g.buy_planner,
   g.preferred_partner_desc,
   g.areas_of_responsibility,
   g.is_npg,
   g.diversity_group,
   g.nord_to_rack_transfer_rate,
   f.replenishment_receipts_cost_amount,
   f.replenishment_receipts_retail_amount,
   f.replenishment_receipts_units,
   f.nonreplenishment_receipts_cost_amount,
   f.nonreplenishment_receipts_retail_amount,
   f.nonreplenishment_receipts_units,
   f.replenishment_receipts_less_reserve_cost_amount,
   f.replenishment_receipts_less_reserve_retail_amount,
   f.replenishment_receipts_less_reserve_units,
   f.nonreplenishment_receipts_less_reserve_cost_amount,
   f.nonreplenishment_receipts_less_reserve_retail_amount,
   f.nonreplenishment_receipts_less_reserve_units) AS t8
GROUP BY t8.selling_country,
 t8.selling_brand,
 t8.chnl_idnt,
 t8.supplier_group,
 t8.alt_inv_model,
 t8.category,
 t8.department_number,
 t8.month_id,
 t8.month_label,
 t8.half_label,
 t8.quarter,
 t8.fiscal_year_num,
 t8.category_planner_1,
 t8.category_planner_2,
 t8.category_group,
 t8.seasonal_designation,
 t8.rack_merch_zone,
 t8.is_activewear,
 t8.channel_category_roles_1,
 t8.channel_category_roles_2,
 t8.bargainista_dept_map,
 t8.buy_planner,
 t8.preferred_partner_desc,
 t8.areas_of_responsibility,
 t8.is_npg,
 t8.diversity_group,
 t8.nord_to_rack_transfer_rate;


--COLLECT STATS      PRIMARY INDEX(DEPARTMENT_NUMBER,SUPPLIER_GROUP,CATEGORY,MONTH_ID)      ,COLUMN(DEPARTMENT_NUMBER)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_ID)      ON sc_plan_base


--Lets bring in wtd rcpts at supp/cat


-- drop table sc_wtd_rcpts;


--where a.DEPARTMENT_NUM = '882'


--AND b.MONTH_LABEL = '2022 JAN'


--AND a.DROPSHIP_IND = 'N'


--AND a.RP_IND = 'Y'


CREATE TEMPORARY TABLE IF NOT EXISTS sc_wtd_rcpts
CLUSTER BY department_num, supplier_group, category, month_label
AS
SELECT g.department_num,
 g.supplier_group,
 g.alt_inv_model,
 g.selling_country,
 g.selling_brand,
 g.chnl_idnt,
 TRIM(FORMAT('%11d', g.year_num)) AS year_num,
 TRIM(g.month_id) AS month_id,
 TRIM(g.month_label) AS month_label,
 TRIM(g.half_label) AS half_label,
 TRIM(g.quarter) AS quarter,
 COALESCE(h.category, i.category, 'OTHER') AS category,
 g.buy_planner,
 g.preferred_partner_desc,
 g.areas_of_responsibility,
 g.is_npg,
 g.diversity_group,
 g.nord_to_rack_transfer_rate,
 COALESCE(h.category_planner_1, i.category_planner_1, 'OTHER') AS category_planner_1,
 COALESCE(h.category_planner_2, i.category_planner_2, 'OTHER') AS category_planner_2,
 COALESCE(h.category_group, i.category_group, 'OTHER') AS category_group,
 COALESCE(h.seasonal_designation, i.seasonal_designation, 'OTHER') AS seasonal_designation,
 COALESCE(h.rack_merch_zone, i.rack_merch_zone, 'OTHER') AS rack_merch_zone,
 COALESCE(h.is_activewear, i.is_activewear, 'OTHER') AS is_activewear,
 COALESCE(h.channel_category_roles_1, i.channel_category_roles_1, 'OTHER') AS channel_category_roles_1,
 COALESCE(h.channel_category_roles_2, i.channel_category_roles_2, 'OTHER') AS channel_category_roles_2,
 COALESCE(h.bargainista_dept_map, i.bargainista_dept_map, 'OTHER') AS bargainista_dept_map,
 SUM(g.rp_rcpts_mtd_c) AS rp_rcpts_mtd_c,
 SUM(g.rp_rcpts_mtd_r) AS rp_rcpts_mtd_r,
 SUM(g.rp_rcpts_mtd_u) AS rp_rcpts_mtd_u,
 SUM(g.nrp_rcpts_mtd_c) AS nrp_rcpts_mtd_c,
 SUM(g.nrp_rcpts_mtd_r) AS nrp_rcpts_mtd_r,
 SUM(g.nrp_rcpts_mtd_u) AS nrp_rcpts_mtd_u
FROM (SELECT d.department_num,
   d.supplier_num,
   d.supplier_name,
   d.selling_country,
   d.selling_brand,
   d.year_num,
   d.month_id,
   d.month_label,
   d.half_label,
   d.quarter,
   d.rp_ind,
   d.npg_ind,
   d.chnl_idnt,
   d.class_num,
   d.subclass_num,
   COALESCE(e.supplier_group, 'OTHER') AS supplier_group,
    CASE
    WHEN LOWER(RTRIM(f.payto_vendor_num)) = LOWER(RTRIM('5179609'))
    THEN 'FANATICS ONLY'
    ELSE 'NON-FANATICS'
    END AS alt_inv_model,
   e.buy_planner,
   e.preferred_partner_desc,
   e.areas_of_responsibility,
   e.is_npg,
   e.diversity_group,
   e.nord_to_rack_transfer_rate,
   SUM(CASE
     WHEN LOWER(RTRIM(d.rp_ind)) = LOWER(RTRIM('Y')) AND LOWER(RTRIM(d.dropship_ind)) = LOWER(RTRIM('N')) AND d.channel_num
        NOT IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER))
     THEN d.receipts_regular_cost + d.receipts_clearance_cost + d.receipts_crossdock_regular_cost + d.receipts_crossdock_clearance_cost
      
     ELSE 0
     END) AS rp_rcpts_mtd_c,
   SUM(CASE
     WHEN LOWER(RTRIM(d.rp_ind)) = LOWER(RTRIM('Y')) AND LOWER(RTRIM(d.dropship_ind)) = LOWER(RTRIM('N')) AND d.channel_num
        NOT IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER))
     THEN d.receipts_regular_retail + d.receipts_clearance_retail + d.receipts_crossdock_regular_retail + d.receipts_crossdock_clearance_retail
      
     ELSE 0
     END) AS rp_rcpts_mtd_r,
   SUM(CASE
     WHEN LOWER(RTRIM(d.rp_ind)) = LOWER(RTRIM('Y')) AND LOWER(RTRIM(d.dropship_ind)) = LOWER(RTRIM('N')) AND d.channel_num
        NOT IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER))
     THEN d.receipts_regular_units + d.receipts_clearance_units + d.receipts_crossdock_regular_units + d.receipts_crossdock_clearance_units
      
     ELSE 0
     END) AS rp_rcpts_mtd_u,
   SUM(CASE
     WHEN LOWER(RTRIM(d.rp_ind)) = LOWER(RTRIM('N')) AND LOWER(RTRIM(d.dropship_ind)) = LOWER(RTRIM('N')) AND d.channel_num
        NOT IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER))
     THEN d.receipts_regular_cost + d.receipts_clearance_cost + d.receipts_crossdock_regular_cost + d.receipts_crossdock_clearance_cost
      
     ELSE 0
     END) AS nrp_rcpts_mtd_c,
   SUM(CASE
     WHEN LOWER(RTRIM(d.rp_ind)) = LOWER(RTRIM('N')) AND LOWER(RTRIM(d.dropship_ind)) = LOWER(RTRIM('N')) AND d.channel_num
        NOT IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER))
     THEN d.receipts_regular_retail + d.receipts_clearance_retail + d.receipts_crossdock_regular_retail + d.receipts_crossdock_clearance_retail
      
     ELSE 0
     END) AS nrp_rcpts_mtd_r,
   SUM(CASE
     WHEN LOWER(RTRIM(d.rp_ind)) = LOWER(RTRIM('N')) AND LOWER(RTRIM(d.dropship_ind)) = LOWER(RTRIM('N')) AND d.channel_num
        NOT IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER))
     THEN d.receipts_regular_units + d.receipts_clearance_units + d.receipts_crossdock_regular_units + d.receipts_crossdock_clearance_units
      
     ELSE 0
     END) AS nrp_rcpts_mtd_u
  FROM (SELECT a.rms_sku_num,
     a.department_num,
     a.department_desc,
     a.class_num,
     a.subclass_num,
     a.division_num,
     a.division_desc,
     a.subdivision_num,
     a.supplier_num,
     a.supplier_name,
     a.store_num,
     a.channel_num,
     a.week_num,
     a.year_num,
     b.month_id,
     b.month_label,
     b.half_label,
     b.quarter,
     a.month_num,
     a.dropship_ind,
     a.rp_ind,
     a.npg_ind,
     c.chnl_idnt,
     c.cluster_name,
     c.selling_brand,
     c.selling_country,
     a.receipts_regular_cost,
     a.receipts_regular_retail,
     a.receipts_regular_units,
     a.receipts_clearance_cost,
     a.receipts_clearance_retail,
     a.receipts_clearance_units,
     a.receipts_crossdock_regular_cost,
     a.receipts_crossdock_regular_retail,
     a.receipts_crossdock_regular_units,
     a.receipts_crossdock_clearance_cost,
     a.receipts_crossdock_clearance_retail,
     a.receipts_crossdock_clearance_units
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
     INNER JOIN sc_wtd_date AS b ON a.week_num = b.week_idnt
     INNER JOIN sc_cluster_map AS c ON a.channel_num = c.chnl_idnt
    GROUP BY a.rms_sku_num,
     a.department_num,
     a.department_desc,
     a.class_num,
     a.subclass_num,
     a.division_num,
     a.division_desc,
     a.subdivision_num,
     a.supplier_num,
     a.supplier_name,
     a.store_num,
     a.channel_num,
     a.week_num,
     a.year_num,
     b.month_id,
     b.month_label,
     b.half_label,
     b.quarter,
     a.month_num,
     a.dropship_ind,
     a.rp_ind,
     a.npg_ind,
     c.chnl_idnt,
     c.cluster_name,
     c.selling_brand,
     c.selling_country,
     a.receipts_regular_cost,
     a.receipts_regular_retail,
     a.receipts_regular_units,
     a.receipts_clearance_cost,
     a.receipts_clearance_retail,
     a.receipts_clearance_units,
     a.receipts_crossdock_regular_cost,
     a.receipts_crossdock_regular_retail,
     a.receipts_crossdock_regular_units,
     a.receipts_crossdock_clearance_cost,
     a.receipts_crossdock_clearance_retail,
     a.receipts_crossdock_clearance_units) AS d
   LEFT JOIN sc_supp_gp AS e ON d.department_num = CAST(RTRIM(e.dept_num) AS FLOAT64) AND LOWER(RTRIM(d.supplier_num)) =
      LOWER(RTRIM(e.supplier_num)) AND LOWER(RTRIM(d.selling_brand)) = LOWER(RTRIM(e.selling_brand))
   LEFT JOIN (SELECT DISTINCT payto_vendor_num,
     order_from_vendor_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim
    WHERE LOWER(RTRIM(payto_vendor_num)) IN (LOWER(RTRIM('5179609')))) AS f ON LOWER(RTRIM(d.supplier_num)) = LOWER(RTRIM(f
      .order_from_vendor_num))
  GROUP BY d.department_num,
   d.supplier_num,
   d.supplier_name,
   d.selling_country,
   d.selling_brand,
   d.year_num,
   d.month_id,
   d.month_label,
   d.half_label,
   d.quarter,
   d.rp_ind,
   d.npg_ind,
   d.chnl_idnt,
   d.class_num,
   d.subclass_num,
   supplier_group,
   alt_inv_model,
   e.buy_planner,
   e.preferred_partner_desc,
   e.areas_of_responsibility,
   e.is_npg,
   e.diversity_group,
   e.nord_to_rack_transfer_rate) AS g
 LEFT JOIN sc_cattr AS h ON g.department_num = h.dept_num AND g.class_num = CAST(RTRIM(h.class_num) AS FLOAT64) AND g.subclass_num
    = CAST(RTRIM(h.sbclass_num) AS FLOAT64)
 LEFT JOIN sc_cattr AS i ON g.department_num = i.dept_num AND g.class_num = CAST(RTRIM(i.class_num) AS FLOAT64) AND CAST(RTRIM(i.sbclass_num) AS FLOAT64)
   = - 1
GROUP BY g.department_num,
 g.supplier_group,
 g.alt_inv_model,
 g.selling_country,
 g.selling_brand,
 g.chnl_idnt,
 year_num,
 month_id,
 month_label,
 half_label,
 quarter,
 category,
 g.buy_planner,
 g.preferred_partner_desc,
 g.areas_of_responsibility,
 g.is_npg,
 g.diversity_group,
 g.nord_to_rack_transfer_rate,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map;


--COLLECT STATS      PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPARTMENT_NUM)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON sc_wtd_rcpts


--drop table sc_oo_final


--lets add back in the hierarchy to the base data and SUM up inactive/active rcpts as well as rp nrp


--Inactive channels 930,220 and 221


--where a.month_num = '202302'


--and a.DEPARTMENT_NUM = '882'


CREATE TEMPORARY TABLE IF NOT EXISTS sc_oo_final
CLUSTER BY department_num, supplier_group, category, month_label
AS
SELECT g.department_num,
 g.channel_num,
 g.selling_country,
 g.selling_brand,
 TRIM(g.month_label) AS month_label,
 TRIM(g.month_id) AS month_id,
 TRIM(g.quarter) AS quarter,
 TRIM(g.half_label) AS half_label,
 TRIM(FORMAT('%11d', g.fiscal_year_num)) AS fiscal_year_num,
 g.alt_inv_model,
 COALESCE(h.category, i.category, 'OTHER') AS category,
 g.supplier_group,
 COALESCE(h.category_planner_1, i.category_planner_1, 'OTHER') AS category_planner_1,
 COALESCE(h.category_planner_2, i.category_planner_2, 'OTHER') AS category_planner_2,
 COALESCE(h.category_group, i.category_group, 'OTHER') AS category_group,
 COALESCE(h.seasonal_designation, i.seasonal_designation, 'OTHER') AS seasonal_designation,
 COALESCE(h.rack_merch_zone, i.rack_merch_zone, 'OTHER') AS rack_merch_zone,
 COALESCE(h.is_activewear, i.is_activewear, 'OTHER') AS is_activewear,
 COALESCE(h.channel_category_roles_1, i.channel_category_roles_1, 'OTHER') AS channel_category_roles_1,
 COALESCE(h.channel_category_roles_2, i.channel_category_roles_2, 'OTHER') AS channel_category_roles_2,
 COALESCE(h.bargainista_dept_map, i.bargainista_dept_map, 'OTHER') AS bargainista_dept_map,
 g.buy_planner,
 g.preferred_partner_desc,
 g.areas_of_responsibility,
 g.is_npg,
 g.diversity_group,
 g.nord_to_rack_transfer_rate,
 SUM(g.rp_oo_active_cost) AS rp_oo_c,
 SUM(g.rp_oo_active_retail) AS rp_oo_r,
 SUM(g.rp_oo_active_units) AS rp_oo_u,
 SUM(g.non_rp_oo_active_cost) AS nrp_oo_c,
 SUM(g.non_rp_oo_active_retail) AS nrp_oo_r,
 SUM(g.non_rp_oo_active_units) AS nrp_oo_u
FROM (SELECT d.department_num,
   d.division_num,
   d.subdivision_desc,
   d.class_num,
   d.subclass_num,
   COALESCE(e.supplier_group, 'OTHER') AS supplier_group,
    CASE
    WHEN LOWER(RTRIM(f.payto_vendor_num)) = LOWER(RTRIM('5179609'))
    THEN 'FANATICS ONLY'
    ELSE 'NON-FANATICS'
    END AS alt_inv_model,
   d.week_num,
   d.month_num,
   d.month_label,
   d.month_id,
   d.quarter,
   d.half_label,
   d.fiscal_year_num,
   d.channel_num,
   d.selling_country,
   d.selling_brand,
   e.buy_planner,
   e.preferred_partner_desc,
   e.areas_of_responsibility,
   e.is_npg,
   e.diversity_group,
   e.nord_to_rack_transfer_rate,
   SUM(d.rp_oo_active_cost) AS rp_oo_active_cost,
   SUM(d.rp_oo_active_retail) AS rp_oo_active_retail,
   SUM(d.rp_oo_active_units) AS rp_oo_active_units,
   SUM(d.non_rp_oo_active_cost) AS non_rp_oo_active_cost,
   SUM(d.non_rp_oo_active_retail) AS non_rp_oo_active_retail,
   SUM(d.non_rp_oo_active_units) AS non_rp_oo_active_units
  FROM (SELECT a.rms_sku_num,
     a.purchase_order_number,
     a.department_num,
     a.division_num,
     a.subdivision_desc,
     a.class_num,
     a.subclass_num,
     a.supplier_num,
     a.week_num,
     a.month_num,
     a.month_label,
     b.month_id,
     b.quarter,
     b.half_label,
     b.fiscal_year_num,
     a.store_num,
     a.channel_num,
     c.selling_country,
     c.selling_brand,
     a.rp_oo_active_cost,
     a.rp_oo_active_retail,
     a.rp_oo_active_units,
     a.non_rp_oo_active_cost,
     a.non_rp_oo_active_retail,
     a.non_rp_oo_active_units
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_apt_on_order_insight_fact_vw AS a
     INNER JOIN sc_date AS b ON a.week_num = b.week_idnt
     INNER JOIN sc_cluster_map AS c ON a.channel_num = c.chnl_idnt) AS d
   LEFT JOIN sc_supp_gp AS e ON d.department_num = CAST(RTRIM(e.dept_num) AS FLOAT64) AND LOWER(RTRIM(d.supplier_num)) =
      LOWER(RTRIM(e.supplier_num)) AND LOWER(RTRIM(d.selling_brand)) = LOWER(RTRIM(e.selling_brand))
   LEFT JOIN (SELECT DISTINCT payto_vendor_num,
     order_from_vendor_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim
    WHERE LOWER(RTRIM(payto_vendor_num)) IN (LOWER(RTRIM('5179609')))) AS f ON LOWER(RTRIM(d.supplier_num)) = LOWER(RTRIM(f
      .order_from_vendor_num))
  GROUP BY d.department_num,
   d.division_num,
   d.subdivision_desc,
   d.class_num,
   d.subclass_num,
   supplier_group,
   alt_inv_model,
   d.week_num,
   d.month_num,
   d.month_label,
   d.month_id,
   d.quarter,
   d.half_label,
   d.fiscal_year_num,
   d.channel_num,
   d.selling_country,
   d.selling_brand,
   e.buy_planner,
   e.preferred_partner_desc,
   e.areas_of_responsibility,
   e.is_npg,
   e.diversity_group,
   e.nord_to_rack_transfer_rate) AS g
 LEFT JOIN sc_cattr AS h ON g.department_num = h.dept_num AND g.class_num = CAST(RTRIM(h.class_num) AS FLOAT64) AND g.subclass_num
    = CAST(RTRIM(h.sbclass_num) AS FLOAT64)
 LEFT JOIN sc_cattr AS i ON g.department_num = i.dept_num AND g.class_num = CAST(RTRIM(i.class_num) AS FLOAT64) AND CAST(RTRIM(i.sbclass_num) AS FLOAT64)
   = - 1
GROUP BY g.department_num,
 g.channel_num,
 g.selling_country,
 g.selling_brand,
 month_label,
 month_id,
 quarter,
 half_label,
 fiscal_year_num,
 g.alt_inv_model,
 category,
 g.supplier_group,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map,
 g.buy_planner,
 g.preferred_partner_desc,
 g.areas_of_responsibility,
 g.is_npg,
 g.diversity_group,
 g.nord_to_rack_transfer_rate;


--COLLECT STATS      PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPARTMENT_NUM)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON sc_oo_final


--drop table sc_cm


--bring in cm


--T3DL_ACE_ASSORTMENT.AB_CM_ORDERS_CURRENT_TEST a


--t2dl_das_open_to_buy.AB_CM_ORDERS_CURRENT a


--where a.DEPT_ID = '882'


CREATE TEMPORARY TABLE IF NOT EXISTS sc_cm
CLUSTER BY dept_id, supplier_group, category, month_label
AS
SELECT g.dept_id,
 g.selling_country,
 g.selling_brand,
 g.org_id,
 TRIM(g.month_id) AS month_id,
 TRIM(g.month_label) AS month_label,
 TRIM(g.quarter) AS quarter,
 TRIM(g.half_label) AS half_label,
 TRIM(FORMAT('%11d', g.fiscal_year_num)) AS fiscal_year_num,
 COALESCE(h.category, i.category, 'OTHER') AS category,
 g.supplier_group,
 g.alt_inv_model,
 g.buy_planner,
 g.preferred_partner_desc,
 g.areas_of_responsibility,
 g.is_npg,
 g.diversity_group,
 g.nord_to_rack_transfer_rate,
 COALESCE(h.category_planner_1, i.category_planner_1, 'OTHER') AS category_planner_1,
 COALESCE(h.category_planner_2, i.category_planner_2, 'OTHER') AS category_planner_2,
 COALESCE(h.category_group, i.category_group, 'OTHER') AS category_group,
 COALESCE(h.seasonal_designation, i.seasonal_designation, 'OTHER') AS seasonal_designation,
 COALESCE(h.rack_merch_zone, i.rack_merch_zone, 'OTHER') AS rack_merch_zone,
 COALESCE(h.is_activewear, i.is_activewear, 'OTHER') AS is_activewear,
 COALESCE(h.channel_category_roles_1, i.channel_category_roles_1, 'OTHER') AS channel_category_roles_1,
 COALESCE(h.channel_category_roles_2, i.channel_category_roles_2, 'OTHER') AS channel_category_roles_2,
 COALESCE(h.bargainista_dept_map, i.bargainista_dept_map, 'OTHER') AS bargainista_dept_map,
 SUM(g.nrp_cm_c) AS nrp_cm_c,
 SUM(g.nrp_cm_r) AS nrp_cm_r,
 SUM(g.nrp_cm_u) AS nrp_cm_u,
 SUM(g.rp_cm_c) AS rp_cm_c,
 SUM(g.rp_cm_r) AS rp_cm_r,
 SUM(g.rp_cm_u) AS rp_cm_u
FROM (SELECT d.dept_id,
   d.selling_country,
   d.selling_brand,
   d.month_id,
   d.month_label,
   d.quarter,
   d.fiscal_year_num,
   d.half_label,
   d.class_id,
   d.subclass_id,
   COALESCE(e.supplier_group, 'OTHER') AS supplier_group,
    CASE
    WHEN LOWER(RTRIM(f.payto_vendor_num)) = LOWER(RTRIM('5179609'))
    THEN 'FANATICS ONLY'
    ELSE 'NON-FANATICS'
    END AS alt_inv_model,
   d.org_id,
   e.buy_planner,
   e.preferred_partner_desc,
   e.areas_of_responsibility,
   e.is_npg,
   e.diversity_group,
   e.nord_to_rack_transfer_rate,
   SUM(CASE
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('BACKUP')), LOWER(RTRIM('FASHION')), LOWER(RTRIM('NRPREORDER'))) AND
       LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('US')))
     THEN d.ttl_cost_us
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('BACKUP')), LOWER(RTRIM('FASHION')), LOWER(RTRIM('NRPREORDER'))) AND
       LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('CA')))
     THEN d.ttl_cost_ca
     ELSE 0
     END) AS nrp_cm_c,
   SUM(CASE
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('BACKUP')), LOWER(RTRIM('FASHION')), LOWER(RTRIM('NRPREORDER'))) AND
       LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('US')))
     THEN d.ttl_rtl_us
     ELSE 0
     END) AS nrp_cm_r,
   SUM(CASE
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('BACKUP')), LOWER(RTRIM('FASHION')), LOWER(RTRIM('NRPREORDER')))
     THEN d.rcpt_units
     ELSE 0
     END) AS nrp_cm_u,
   SUM(CASE
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('REPLNSHMNT'))) AND LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('US'
          )))
     THEN d.ttl_cost_us
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('REPLNSHMNT'))) AND LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('CA'
          )))
     THEN d.ttl_cost_ca
     ELSE 0
     END) AS rp_cm_c,
   SUM(CASE
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('REPLNSHMNT'))) AND LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('US'
          )))
     THEN d.ttl_rtl_us
     ELSE 0
     END) AS rp_cm_r,
   SUM(CASE
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('REPLNSHMNT')))
     THEN d.rcpt_units
     ELSE 0
     END) AS rp_cm_u
  FROM (SELECT a.dept_id,
     a.style_id,
     a.style_group_id,
     a.class_id,
     a.subclass_id,
     a.supp_id,
     a.supp_name,
     a.fiscal_month_id,
     a.otb_eow_date,
     a.org_id,
     a.vpn,
     a.nrf_color_code,
     a.supp_color,
     a.plan_type,
     a.ttl_cost_us,
     a.ttl_cost_ca,
     a.ttl_rtl_us,
     a.rcpt_units,
     a.cm_dollars,
     c.month_id,
     c.month_label,
     c.quarter,
     c.half_label,
     c.fiscal_year_num,
     a.po_key,
     a.plan_key,
     b.selling_brand,
     b.selling_country
    FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.ab_cm_orders_current AS a
     INNER JOIN sc_cluster_map AS b ON a.org_id = b.chnl_idnt
     INNER JOIN sc_date AS c ON CAST(RTRIM(a.fiscal_month_id) AS FLOAT64) = c.month_idnt
    GROUP BY a.dept_id,
     a.style_id,
     a.style_group_id,
     a.class_id,
     a.subclass_id,
     a.supp_id,
     a.supp_name,
     a.fiscal_month_id,
     a.otb_eow_date,
     a.org_id,
     a.vpn,
     a.nrf_color_code,
     a.supp_color,
     a.plan_type,
     a.ttl_cost_us,
     a.ttl_cost_ca,
     a.ttl_rtl_us,
     a.rcpt_units,
     a.cm_dollars,
     c.month_id,
     c.month_label,
     c.quarter,
     c.half_label,
     c.fiscal_year_num,
     a.po_key,
     a.plan_key,
     b.selling_brand,
     b.selling_country) AS d
   LEFT JOIN sc_supp_gp AS e ON d.dept_id = CAST(RTRIM(e.dept_num) AS FLOAT64) AND LOWER(RTRIM(d.supp_id)) = LOWER(RTRIM(e
        .supplier_num)) AND LOWER(RTRIM(d.selling_brand)) = LOWER(RTRIM(e.selling_brand))
   LEFT JOIN (SELECT DISTINCT payto_vendor_num,
     order_from_vendor_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim
    WHERE LOWER(RTRIM(payto_vendor_num)) IN (LOWER(RTRIM('5179609')))) AS f ON LOWER(RTRIM(d.supp_id)) = LOWER(RTRIM(f.order_from_vendor_num
      ))
  GROUP BY d.dept_id,
   d.selling_country,
   d.selling_brand,
   d.month_id,
   d.month_label,
   d.quarter,
   d.fiscal_year_num,
   d.half_label,
   d.class_id,
   d.subclass_id,
   supplier_group,
   alt_inv_model,
   d.org_id,
   e.buy_planner,
   e.preferred_partner_desc,
   e.areas_of_responsibility,
   e.is_npg,
   e.diversity_group,
   e.nord_to_rack_transfer_rate) AS g
 LEFT JOIN sc_cattr AS h ON g.dept_id = h.dept_num AND g.class_id = CAST(RTRIM(h.class_num) AS FLOAT64) AND g.subclass_id
    = CAST(RTRIM(h.sbclass_num) AS FLOAT64)
 LEFT JOIN sc_cattr AS i ON g.dept_id = i.dept_num AND g.class_id = CAST(RTRIM(i.class_num) AS FLOAT64) AND CAST(RTRIM(i.sbclass_num) AS FLOAT64)
   = - 1
GROUP BY g.dept_id,
 g.selling_country,
 g.selling_brand,
 g.org_id,
 month_id,
 month_label,
 quarter,
 half_label,
 fiscal_year_num,
 category,
 g.supplier_group,
 g.alt_inv_model,
 g.buy_planner,
 g.preferred_partner_desc,
 g.areas_of_responsibility,
 g.is_npg,
 g.diversity_group,
 g.nord_to_rack_transfer_rate,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON sc_cm


--where Department = '3 HOSIERY'


CREATE TEMPORARY TABLE IF NOT EXISTS sc_rp
CLUSTER BY dept_id, supplier_group, category, month_label
AS
SELECT f.dept_id,
 f.channel,
 f.selling_country,
 f.selling_brand,
 TRIM(f.month_id) AS month_id,
 TRIM(f.month_label) AS month_label,
 TRIM(f.half_label) AS half_label,
 TRIM(f.quarter) AS quarter,
 TRIM(FORMAT('%11d', f.fiscal_year_num)) AS fiscal_year_num,
 COALESCE(g.category, h.category, 'OTHER') AS category,
 f.supplier_group,
 f.alt_inv_model,
 f.buy_planner,
 f.preferred_partner_desc,
 f.areas_of_responsibility,
 f.is_npg,
 f.diversity_group,
 f.nord_to_rack_transfer_rate,
 COALESCE(g.category_planner_1, h.category_planner_1, 'OTHER') AS category_planner_1,
 COALESCE(g.category_planner_2, h.category_planner_2, 'OTHER') AS category_planner_2,
 COALESCE(g.category_group, h.category_group, 'OTHER') AS category_group,
 COALESCE(g.seasonal_designation, h.seasonal_designation, 'OTHER') AS seasonal_designation,
 COALESCE(g.rack_merch_zone, h.rack_merch_zone, 'OTHER') AS rack_merch_zone,
 COALESCE(g.is_activewear, h.is_activewear, 'OTHER') AS is_activewear,
 COALESCE(g.channel_category_roles_1, h.channel_category_roles_1, 'OTHER') AS channel_category_roles_1,
 COALESCE(g.channel_category_roles_2, h.channel_category_roles_2, 'OTHER') AS channel_category_roles_2,
 COALESCE(g.bargainista_dept_map, h.bargainista_dept_map, 'OTHER') AS bargainista_dept_map,
 SUM(f.rp_ant_spd_c) AS rp_ant_spd_c,
 SUM(f.rp_ant_spd_u) AS rp_ant_spd_u,
 SUM(f.rp_ant_spd_r) AS rp_ant_spd_r
FROM (SELECT c.selling_country,
   c.selling_brand,
   c.channel,
   COALESCE(d.supplier_group, 'OTHER') AS supplier_group,
    CASE
    WHEN LOWER(RTRIM(e.payto_vendor_num)) = LOWER(RTRIM('5179609'))
    THEN 'FANATICS ONLY'
    ELSE 'NON-FANATICS'
    END AS alt_inv_model,
   d.buy_planner,
   d.preferred_partner_desc,
   d.areas_of_responsibility,
   d.is_npg,
   d.diversity_group,
   d.nord_to_rack_transfer_rate,
   c.dept_id,
   c.class_id,
   c.subclass_id,
   c.month_id,
   c.month_label,
   c.half_label,
   c.quarter,
   c.fiscal_year_num,
   SUM(c.rp_ant_spd_c) AS rp_ant_spd_c,
   SUM(c.rp_ant_spd_u) AS rp_ant_spd_u,
   SUM(c.rp_ant_spd_r) AS rp_ant_spd_r
  FROM (SELECT 'US' AS selling_country,
     a.channel_brand AS selling_brand,
     TRIM(SUBSTR(a.channel_number, 1, STRPOS(LOWER(a.channel_number), LOWER(' ')) - 1)) AS channel,
     TRIM(SUBSTR(a.department, 1, STRPOS(LOWER(a.department), LOWER(' ')) - 1)) AS dept_id,
     TRIM(SUBSTR(a.class, 1, STRPOS(LOWER(a.class), LOWER(' ')) - 1)) AS class_id,
     TRIM(SUBSTR(a.subclass, 1, STRPOS(LOWER(a.subclass), LOWER(' ')) - 1)) AS subclass_id,
     a.supplier_idnt,
     TRIM(b.month_id) AS month_id,
     b.month_label,
     b.half_label,
     b.quarter,
     b.fiscal_year_num,
     SUM(a.rp_ant_spd_c) AS rp_ant_spd_c,
     SUM(a.rp_ant_spd_u) AS rp_ant_spd_u,
     SUM(a.rp_ant_spd_r) AS rp_ant_spd_r
    FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_ant_spend_report AS a
     INNER JOIN sc_date AS b ON CAST(RTRIM(a.week_idnt) AS FLOAT64) = b.week_idnt
    GROUP BY selling_country,
     selling_brand,
     channel,
     dept_id,
     class_id,
     subclass_id,
     a.supplier_idnt,
     month_id,
     b.month_label,
     b.half_label,
     b.quarter,
     b.fiscal_year_num) AS c
   LEFT JOIN sc_supp_gp AS d ON LOWER(RTRIM(c.dept_id)) = LOWER(RTRIM(d.dept_num)) AND LOWER(RTRIM(c.supplier_idnt)) =
      LOWER(RTRIM(d.supplier_num)) AND LOWER(RTRIM(c.selling_brand)) = LOWER(RTRIM(d.selling_brand))
   LEFT JOIN (SELECT DISTINCT payto_vendor_num,
     order_from_vendor_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim
    WHERE LOWER(RTRIM(payto_vendor_num)) IN (LOWER(RTRIM('5179609')))) AS e ON LOWER(RTRIM(c.supplier_idnt)) = LOWER(RTRIM(e
      .order_from_vendor_num))
  GROUP BY c.selling_country,
   c.selling_brand,
   c.channel,
   supplier_group,
   alt_inv_model,
   d.buy_planner,
   d.preferred_partner_desc,
   d.areas_of_responsibility,
   d.is_npg,
   d.diversity_group,
   d.nord_to_rack_transfer_rate,
   c.dept_id,
   c.class_id,
   c.subclass_id,
   c.month_id,
   c.month_label,
   c.half_label,
   c.quarter,
   c.fiscal_year_num) AS f
 LEFT JOIN sc_cattr AS g ON CAST(RTRIM(f.dept_id) AS FLOAT64) = g.dept_num AND LOWER(RTRIM(f.class_id)) = LOWER(RTRIM(g
      .class_num)) AND LOWER(RTRIM(f.subclass_id)) = LOWER(RTRIM(g.sbclass_num))
 LEFT JOIN sc_cattr AS h ON CAST(RTRIM(f.dept_id) AS FLOAT64) = h.dept_num AND LOWER(RTRIM(f.class_id)) = LOWER(RTRIM(h
      .class_num)) AND CAST(RTRIM(h.sbclass_num) AS FLOAT64) = - 1
GROUP BY f.dept_id,
 f.channel,
 f.selling_country,
 f.selling_brand,
 month_id,
 month_label,
 half_label,
 quarter,
 fiscal_year_num,
 category,
 f.supplier_group,
 f.alt_inv_model,
 f.buy_planner,
 f.preferred_partner_desc,
 f.areas_of_responsibility,
 f.is_npg,
 f.diversity_group,
 f.nord_to_rack_transfer_rate,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map;


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON sc_rp


--lets bring all the data together   


--WHERE DEPARTMENT_NUMBER = '882'


CREATE TEMPORARY TABLE IF NOT EXISTS sc_final
CLUSTER BY department_number, supplier_group, category, month_label
AS
SELECT selling_country,
 selling_brand,
 channel,
 supplier_group,
 category,
 department_number,
 alt_inv_model,
 month_id,
 month_label,
 half,
 quarter,
 fiscal_year_num,
 buy_planner,
 preferred_partner_desc,
 areas_of_resposibility,
 npg_ind,
 diversity_group,
 nord_to_rack_transfer_rate,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map,
 SUM(rp_plan_rcpt_c) AS rp_plan_rcpt_c,
 SUM(rp_plan_rcpt_r) AS rp_plan_rcpt_r,
 SUM(rp_plan_rcpt_u) AS rp_plan_rcpt_u,
 SUM(nrp_plan_rcpt_c) AS nrp_plan_rcpt_c,
 SUM(nrp_plan_rcpt_r) AS nrp_plan_rcpt_r,
 SUM(nrp_plan_rcpt_u) AS nrp_plan_rcpt_u,
 SUM(ttl_plan_rcpt_less_reserve_c) AS ttl_plan_rcpt_less_reserve_c,
 SUM(ttl_plan_rcpt_less_reserve_r) AS ttl_plan_rcpt_less_reserve_r,
 SUM(ttl_plan_rcpt_less_reserve_u) AS ttl_plan_rcpt_less_reserve_u,
 SUM(rp_rcpts_mtd_c) AS rp_rcpts_mtd_c,
 SUM(rp_rcpts_mtd_r) AS rp_rcpts_mtd_r,
 SUM(rp_rcpts_mtd_u) AS rp_rcpts_mtd_u,
 SUM(nrp_rcpts_mtd_c) AS nrp_rcpts_mtd_c,
 SUM(nrp_rcpts_mtd_r) AS nrp_rcpts_mtd_r,
 SUM(nrp_rcpts_mtd_u) AS nrp_rcpts_mtd_u,
 SUM(rp_oo_c) AS rp_oo_c,
 SUM(rp_oo_r) AS rp_oo_r,
 SUM(rp_oo_u) AS rp_oo_u,
 SUM(nrp_oo_c) AS nrp_oo_c,
 SUM(nrp_oo_r) AS nrp_oo_r,
 SUM(nrp_oo_u) AS nrp_oo_u,
 SUM(nrp_cm_c) AS nrp_cm_c,
 SUM(nrp_cm_r) AS nrp_cm_r,
 SUM(nrp_cm_u) AS nrp_cm_u,
 SUM(rp_cm_c) AS rp_cm_c,
 SUM(rp_cm_r) AS rp_cm_r,
 SUM(rp_cm_u) AS rp_cm_u,
 SUM(rp_ant_spd_c) AS rp_ant_spd_c,
 SUM(rp_ant_spd_r) AS rp_ant_spd_r,
 SUM(rp_ant_spd_u) AS rp_ant_spd_u
FROM (SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(CAST(chnl_idnt AS STRING), 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    CAST(department_number AS INTEGER) AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(fiscal_year_num, 1, 10) AS fiscal_year_num,
    SUBSTR(buy_planner, 1, 100) AS buy_planner,
    SUBSTR(preferred_partner_desc, 1, 100) AS preferred_partner_desc,
    SUBSTR(areas_of_responsibility, 1, 100) AS areas_of_resposibility,
    SUBSTR(is_npg, 1, 2) AS npg_ind,
    SUBSTR(diversity_group, 1, 100) AS diversity_group,
    SUBSTR(nord_to_rack_transfer_rate, 1, 100) AS nord_to_rack_transfer_rate,
    SUBSTR(category_planner_1, 1, 50) AS category_planner_1,
    SUBSTR(category_planner_2, 1, 50) AS category_planner_2,
    SUBSTR(category_group, 1, 50) AS category_group,
    SUBSTR(seasonal_designation, 1, 50) AS seasonal_designation,
    SUBSTR(rack_merch_zone, 1, 50) AS rack_merch_zone,
    SUBSTR(is_activewear, 1, 50) AS is_activewear,
    SUBSTR(channel_category_roles_1, 1, 50) AS channel_category_roles_1,
    SUBSTR(channel_category_roles_2, 1, 50) AS channel_category_roles_2,
    SUBSTR(bargainista_dept_map, 1, 50) AS bargainista_dept_map,
    ROUND(CAST(rp_plan_rcpt_c AS NUMERIC), 4) AS rp_plan_rcpt_c,
    ROUND(CAST(rp_plan_rcpt_r AS NUMERIC), 4) AS rp_plan_rcpt_r,
    CAST(rp_plan_rcpt_u AS NUMERIC) AS rp_plan_rcpt_u,
    ROUND(CAST(nrp_plan_rcpt_c AS NUMERIC), 4) AS nrp_plan_rcpt_c,
    ROUND(CAST(nrp_plan_rcpt_r AS NUMERIC), 4) AS nrp_plan_rcpt_r,
    CAST(nrp_plan_rcpt_u AS NUMERIC) AS nrp_plan_rcpt_u,
    ROUND(CAST(ttl_plan_rcpt_less_reserve_c AS NUMERIC), 4) AS ttl_plan_rcpt_less_reserve_c,
    ROUND(CAST(ttl_plan_rcpt_less_reserve_r AS NUMERIC), 4) AS ttl_plan_rcpt_less_reserve_r,
    CAST(ttl_plan_rcpt_less_reserve_u AS NUMERIC) AS ttl_plan_rcpt_less_reserve_u,
    0 AS rp_rcpts_mtd_c,
    0 AS rp_rcpts_mtd_r,
    0 AS rp_rcpts_mtd_u,
    0 AS nrp_rcpts_mtd_c,
    0 AS nrp_rcpts_mtd_r,
    0 AS nrp_rcpts_mtd_u,
    0 AS rp_oo_c,
    0 AS rp_oo_r,
    0 AS rp_oo_u,
    0 AS nrp_oo_c,
    0 AS nrp_oo_r,
    0 AS nrp_oo_u,
    0 AS nrp_cm_c,
    0 AS nrp_cm_r,
    0 AS nrp_cm_u,
    0 AS rp_cm_c,
    0 AS rp_cm_r,
    0 AS rp_cm_u,
    0 AS rp_ant_spd_c,
    0 AS rp_ant_spd_r,
    0 AS rp_ant_spd_u
   FROM sc_plan_base
   UNION ALL
   SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(CAST(chnl_idnt AS STRING), 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    CAST(department_num AS INTEGER) AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(year_num, 1, 10) AS fiscal_year_num,
    SUBSTR(buy_planner, 1, 100) AS buy_planner,
    SUBSTR(preferred_partner_desc, 1, 100) AS preferred_partner_desc,
    SUBSTR(areas_of_responsibility, 1, 100) AS areas_of_resposibility,
    SUBSTR(is_npg, 1, 2) AS npg_ind,
    SUBSTR(diversity_group, 1, 100) AS diversity_group,
    SUBSTR(nord_to_rack_transfer_rate, 1, 100) AS nord_to_rack_transfer_rate,
    SUBSTR(category_planner_1, 1, 50) AS category_planner_1,
    SUBSTR(category_planner_2, 1, 50) AS category_planner_2,
    SUBSTR(category_group, 1, 50) AS category_group,
    SUBSTR(seasonal_designation, 1, 50) AS seasonal_designation,
    SUBSTR(rack_merch_zone, 1, 50) AS rack_merch_zone,
    SUBSTR(is_activewear, 1, 50) AS is_activewear,
    SUBSTR(channel_category_roles_1, 1, 50) AS channel_category_roles_1,
    SUBSTR(channel_category_roles_2, 1, 50) AS channel_category_roles_2,
    SUBSTR(bargainista_dept_map, 1, 50) AS bargainista_dept_map,
    0 AS rp_plan_rcpt_c,
    0 AS rp_plan_rcpt_r,
    0 AS rp_plan_rcpt_u,
    0 AS nrp_plan_rcpt_c,
    0 AS nrp_plan_rcpt_r,
    0 AS nrp_plan_rcpt_u,
    0 AS ttl_plan_rcpt_less_reserve_c,
    0 AS ttl_plan_rcpt_less_reserve_r,
    0 AS ttl_plan_rcpt_less_reserve_u,
    CAST(rp_rcpts_mtd_c AS NUMERIC) AS rp_rcpts_mtd_c,
    CAST(rp_rcpts_mtd_r AS NUMERIC) AS rp_rcpts_mtd_r,
    CAST(rp_rcpts_mtd_u AS NUMERIC) AS rp_rcpts_mtd_u,
    CAST(nrp_rcpts_mtd_c AS NUMERIC) AS nrp_rcpts_mtd_c,
    CAST(nrp_rcpts_mtd_r AS NUMERIC) AS nrp_rcpts_mtd_r,
    CAST(nrp_rcpts_mtd_u AS NUMERIC) AS nrp_rcpts_mtd_u,
    0 AS rp_oo_c,
    0 AS rp_oo_r,
    0 AS rp_oo_u,
    0 AS nrp_oo_c,
    0 AS nrp_oo_r,
    0 AS nrp_oo_u,
    0 AS nrp_cm_c,
    0 AS nrp_cm_r,
    0 AS nrp_cm_u,
    0 AS rp_cm_c,
    0 AS rp_cm_r,
    0 AS rp_cm_u,
    0 AS rp_ant_spd_c,
    0 AS rp_ant_spd_r,
    0 AS rp_ant_spd_u
   FROM sc_wtd_rcpts
   UNION ALL
   SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(CAST(channel_num AS STRING), 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    CAST(department_num AS INTEGER) AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(fiscal_year_num, 1, 10) AS fiscal_year_num,
    SUBSTR(buy_planner, 1, 100) AS buy_planner,
    SUBSTR(preferred_partner_desc, 1, 100) AS preferred_partner_desc,
    SUBSTR(areas_of_responsibility, 1, 100) AS areas_of_resposibility,
    SUBSTR(is_npg, 1, 2) AS npg_ind,
    SUBSTR(diversity_group, 1, 100) AS diversity_group,
    SUBSTR(nord_to_rack_transfer_rate, 1, 100) AS nord_to_rack_transfer_rate,
    SUBSTR(category_planner_1, 1, 50) AS category_planner_1,
    SUBSTR(category_planner_2, 1, 50) AS category_planner_2,
    SUBSTR(category_group, 1, 50) AS category_group,
    SUBSTR(seasonal_designation, 1, 50) AS seasonal_designation,
    SUBSTR(rack_merch_zone, 1, 50) AS rack_merch_zone,
    SUBSTR(is_activewear, 1, 50) AS is_activewear,
    SUBSTR(channel_category_roles_1, 1, 50) AS channel_category_roles_1,
    SUBSTR(channel_category_roles_2, 1, 50) AS channel_category_roles_2,
    SUBSTR(bargainista_dept_map, 1, 50) AS bargainista_dept_map,
    0 AS rp_plan_rcpt_c,
    0 AS rp_plan_rcpt_r,
    0 AS rp_plan_rcpt_u,
    0 AS nrp_plan_rcpt_c,
    0 AS nrp_plan_rcpt_r,
    0 AS nrp_plan_rcpt_u,
    0 AS ttl_plan_rcpt_less_reserve_c,
    0 AS ttl_plan_rcpt_less_reserve_r,
    0 AS ttl_plan_rcpt_less_reserve_u,
    0 AS rp_rcpts_mtd_c,
    0 AS rp_rcpts_mtd_r,
    0 AS rp_rcpts_mtd_u,
    0 AS nrp_rcpts_mtd_c,
    0 AS nrp_rcpts_mtd_r,
    0 AS nrp_rcpts_mtd_u,
    CAST(rp_oo_c AS NUMERIC) AS rp_oo_c,
    CAST(rp_oo_r AS NUMERIC) AS rp_oo_r,
    CAST(rp_oo_u AS NUMERIC) AS rp_oo_u,
    CAST(nrp_oo_c AS NUMERIC) AS nrp_oo_c,
    CAST(nrp_oo_r AS NUMERIC) AS nrp_oo_r,
    CAST(nrp_oo_u AS NUMERIC) AS nrp_oo_u,
    0 AS nrp_cm_c,
    0 AS nrp_cm_r,
    0 AS nrp_cm_u,
    0 AS rp_cm_c,
    0 AS rp_cm_r,
    0 AS rp_cm_u,
    0 AS rp_ant_spd_c,
    0 AS rp_ant_spd_r,
    0 AS rp_ant_spd_u
   FROM sc_oo_final
   UNION ALL
   SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(CAST(org_id AS STRING), 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    CAST(trunc(dept_id AS INTEGER)) AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(fiscal_year_num, 1, 10) AS fiscal_year_num,
    SUBSTR(buy_planner, 1, 100) AS buy_planner,
    SUBSTR(preferred_partner_desc, 1, 100) AS preferred_partner_desc,
    SUBSTR(areas_of_responsibility, 1, 100) AS areas_of_resposibility,
    SUBSTR(is_npg, 1, 2) AS npg_ind,
    SUBSTR(diversity_group, 1, 100) AS diversity_group,
    SUBSTR(nord_to_rack_transfer_rate, 1, 100) AS nord_to_rack_transfer_rate,
    SUBSTR(category_planner_1, 1, 50) AS category_planner_1,
    SUBSTR(category_planner_2, 1, 50) AS category_planner_2,
    SUBSTR(category_group, 1, 50) AS category_group,
    SUBSTR(seasonal_designation, 1, 50) AS seasonal_designation,
    SUBSTR(rack_merch_zone, 1, 50) AS rack_merch_zone,
    SUBSTR(is_activewear, 1, 50) AS is_activewear,
    SUBSTR(channel_category_roles_1, 1, 50) AS channel_category_roles_1,
    SUBSTR(channel_category_roles_2, 1, 50) AS channel_category_roles_2,
    SUBSTR(bargainista_dept_map, 1, 50) AS bargainista_dept_map,
    0 AS rp_plan_rcpt_c,
    0 AS rp_plan_rcpt_r,
    0 AS rp_plan_rcpt_u,
    0 AS nrp_plan_rcpt_c,
    0 AS nrp_plan_rcpt_r,
    0 AS nrp_plan_rcpt_u,
    0 AS ttl_plan_rcpt_less_reserve_c,
    0 AS ttl_plan_rcpt_less_reserve_r,
    0 AS ttl_plan_rcpt_less_reserve_u,
    0 AS rp_rcpts_mtd_c,
    0 AS rp_rcpts_mtd_r,
    0 AS rp_rcpts_mtd_u,
    0 AS nrp_rcpts_mtd_c,
    0 AS nrp_rcpts_mtd_r,
    0 AS nrp_rcpts_mtd_u,
    0 AS rp_oo_c,
    0 AS rp_oo_r,
    0 AS rp_oo_u,
    0 AS nrp_oo_c,
    0 AS nrp_oo_r,
    0 AS nrp_oo_u,
    CAST(nrp_cm_c AS NUMERIC) AS nrp_cm_c,
    CAST(nrp_cm_r AS NUMERIC) AS nrp_cm_r,
    CAST(nrp_cm_u AS NUMERIC) AS nrp_cm_u,
    CAST(rp_cm_c AS NUMERIC) AS rp_cm_c,
    CAST(rp_cm_r AS NUMERIC) AS rp_cm_r,
    CAST(rp_cm_u AS NUMERIC) AS rp_cm_u,
    0 AS rp_ant_spd_c,
    0 AS rp_ant_spd_r,
    0 AS rp_ant_spd_u
   FROM sc_cm
   UNION ALL
   SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(channel, 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    CAST(trunc(dept_id AS INTEGER)) AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(fiscal_year_num, 1, 10) AS fiscal_year_num,
    SUBSTR(buy_planner, 1, 100) AS buy_planner,
    SUBSTR(preferred_partner_desc, 1, 100) AS preferred_partner_desc,
    SUBSTR(areas_of_responsibility, 1, 100) AS areas_of_resposibility,
    SUBSTR(is_npg, 1, 2) AS npg_ind,
    SUBSTR(diversity_group, 1, 100) AS diversity_group,
    SUBSTR(nord_to_rack_transfer_rate, 1, 100) AS nord_to_rack_transfer_rate,
    SUBSTR(category_planner_1, 1, 50) AS category_planner_1,
    SUBSTR(category_planner_2, 1, 50) AS category_planner_2,
    SUBSTR(category_group, 1, 50) AS category_group,
    SUBSTR(seasonal_designation, 1, 50) AS seasonal_designation,
    SUBSTR(rack_merch_zone, 1, 50) AS rack_merch_zone,
    SUBSTR(is_activewear, 1, 50) AS is_activewear,
    SUBSTR(channel_category_roles_1, 1, 50) AS channel_category_roles_1,
    SUBSTR(channel_category_roles_2, 1, 50) AS channel_category_roles_2,
    SUBSTR(bargainista_dept_map, 1, 50) AS bargainista_dept_map,
    0 AS rp_plan_rcpt_c,
    0 AS rp_plan_rcpt_r,
    0 AS rp_plan_rcpt_u,
    0 AS nrp_plan_rcpt_c,
    0 AS nrp_plan_rcpt_r,
    0 AS nrp_plan_rcpt_u,
    0 AS ttl_plan_rcpt_less_reserve_c,
    0 AS ttl_plan_rcpt_less_reserve_r,
    0 AS ttl_plan_rcpt_less_reserve_u,
    0 AS rp_rcpts_mtd_c,
    0 AS rp_rcpts_mtd_r,
    0 AS rp_rcpts_mtd_u,
    0 AS nrp_rcpts_mtd_c,
    0 AS nrp_rcpts_mtd_r,
    0 AS nrp_rcpts_mtd_u,
    0 AS rp_oo_c,
    0 AS rp_oo_r,
    0 AS rp_oo_u,
    0 AS nrp_oo_c,
    0 AS nrp_oo_r,
    0 AS nrp_oo_u,
    0 AS nrp_cm_c,
    0 AS nrp_cm_r,
    0 AS nrp_cm_u,
    0 AS rp_cm_c,
    0 AS rp_cm_r,
    0 AS rp_cm_u,
    CAST(rp_ant_spd_c AS NUMERIC) AS rp_ant_spd_c,
    CAST(rp_ant_spd_r AS NUMERIC) AS rp_ant_spd_r,
    CAST(rp_ant_spd_u AS NUMERIC) AS rp_ant_spd_u
   FROM sc_rp) AS a
GROUP BY selling_country,
 selling_brand,
 channel,
 supplier_group,
 category,
 department_number,
 alt_inv_model,
 month_id,
 month_label,
 half,
 quarter,
 fiscal_year_num,
 buy_planner,
 preferred_partner_desc,
 areas_of_resposibility,
 npg_ind,
 diversity_group,
 nord_to_rack_transfer_rate,
 category_planner_1,
 category_planner_2,
 category_group,
 seasonal_designation,
 rack_merch_zone,
 is_activewear,
 channel_category_roles_1,
 channel_category_roles_2,
 bargainista_dept_map;


--COLLECT STATS      PRIMARY INDEX (DEPARTMENT_NUMBER,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPARTMENT_NUMBER)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON sc_final


TRUNCATE TABLE t2dl_das_open_to_buy.apt_supp_cat_spend_current;


--DELETE FROM `{{params.gcp_project_id}}`.T2DL_DAS_OPEN_TO_BUY.APT_SUPP_CAT_SPEND_CURRENT;   


--INSERT INTO `{{params.gcp_project_id}}`.T2DL_DAS_OPEN_TO_BUY.APT_SUPP_CAT_SPEND_CURRENT 


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.apt_supp_cat_spend_current
(SELECT a.selling_country,
  a.selling_brand,
  TRIM(a.channel || ',' || c.channel_desc) AS channel,
  a.supplier_group,
  a.category,
  TRIM(FORMAT('%11d', b.division_num) || ',' || b.division_name) AS division,
  TRIM(FORMAT('%11d', b.subdivision_num) || ',' || b.subdivision_name) AS subdivision,
  TRIM(FORMAT('%11d', a.department_number) || ',' || b.dept_name) AS department,
  b.active_store_ind AS active_division,
  a.alt_inv_model,
  TRIM(a.month_id) AS month_id,
  a.month_label,
  a.half,
  a.quarter,
  a.fiscal_year_num,
  a.buy_planner,
  a.preferred_partner_desc,
  a.areas_of_resposibility,
  a.npg_ind,
   CASE
   WHEN a.rp_plan_rcpt_c > 0
   THEN 'Y'
   ELSE 'N'
   END AS rp_ind,
  a.diversity_group,
  a.nord_to_rack_transfer_rate,
  a.category_planner_1,
  a.category_planner_2,
  a.category_group,
  a.seasonal_designation,
  a.rack_merch_zone,
  a.is_activewear,
  a.channel_category_roles_1,
  a.channel_category_roles_2,
  a.bargainista_dept_map,
  a.rp_plan_rcpt_c,
  a.rp_plan_rcpt_r,
  a.rp_plan_rcpt_u,
  a.nrp_plan_rcpt_c,
  a.nrp_plan_rcpt_r,
  a.nrp_plan_rcpt_u,
  a.ttl_plan_rcpt_less_reserve_c,
  a.ttl_plan_rcpt_less_reserve_r,
  a.ttl_plan_rcpt_less_reserve_u,
  a.rp_rcpts_mtd_c,
  a.rp_rcpts_mtd_r,
  a.rp_rcpts_mtd_u,
  a.nrp_rcpts_mtd_c,
  a.nrp_rcpts_mtd_r,
  a.nrp_rcpts_mtd_u,
  a.rp_oo_c,
  a.rp_oo_r,
  a.rp_oo_u,
  a.nrp_oo_c,
  a.nrp_oo_r,
  a.nrp_oo_u,
  a.nrp_cm_c,
  a.nrp_cm_r,
  a.nrp_cm_u,
  a.rp_cm_c,
  a.rp_cm_r,
  a.rp_cm_u,
  a.rp_ant_spd_c,
  a.rp_ant_spd_r,
  a.rp_ant_spd_u,
  CAST(CURRENT_DATE('PST8PDT') AS STRING) AS process_dt
 FROM sc_final AS a
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS b ON a.department_number = b.dept_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS c ON CAST(RTRIM(a.channel) AS FLOAT64) = c.channel_num
 WHERE a.rp_plan_rcpt_c <> 0
  OR a.rp_plan_rcpt_u <> 0
  OR a.nrp_plan_rcpt_c <> 0
  OR a.nrp_plan_rcpt_u <> 0
  OR a.ttl_plan_rcpt_less_reserve_c <> 0
  OR a.ttl_plan_rcpt_less_reserve_u <> 0
  OR a.rp_rcpts_mtd_c <> 0
  OR a.rp_rcpts_mtd_u <> 0
  OR a.nrp_rcpts_mtd_c <> 0
  OR a.nrp_rcpts_mtd_u <> 0
  OR a.rp_oo_c <> 0
  OR a.rp_oo_u <> 0
  OR a.nrp_oo_c <> 0
  OR a.nrp_oo_u <> 0
  OR a.rp_cm_c <> 0
  OR a.rp_cm_u <> 0
  OR a.nrp_cm_c <> 0
  OR a.nrp_cm_u <> 0
  OR a.rp_ant_spd_c <> 0
  OR a.rp_ant_spd_r <> 0
  OR a.rp_ant_spd_u <> 0
 GROUP BY a.selling_country,
  a.selling_brand,
  channel,
  a.supplier_group,
  a.category,
  division,
  subdivision,
  department,
  active_division,
  a.alt_inv_model,
  month_id,
  a.month_label,
  a.half,
  a.quarter,
  a.fiscal_year_num,
  a.buy_planner,
  a.preferred_partner_desc,
  a.areas_of_resposibility,
  a.npg_ind,
  rp_ind,
  a.diversity_group,
  a.nord_to_rack_transfer_rate,
  a.category_planner_1,
  a.category_planner_2,
  a.category_group,
  a.seasonal_designation,
  a.rack_merch_zone,
  a.is_activewear,
  a.channel_category_roles_1,
  a.channel_category_roles_2,
  a.bargainista_dept_map,
  a.rp_plan_rcpt_c,
  a.rp_plan_rcpt_r,
  a.rp_plan_rcpt_u,
  a.nrp_plan_rcpt_c,
  a.nrp_plan_rcpt_r,
  a.nrp_plan_rcpt_u,
  a.ttl_plan_rcpt_less_reserve_c,
  a.ttl_plan_rcpt_less_reserve_r,
  a.ttl_plan_rcpt_less_reserve_u,
  a.rp_rcpts_mtd_c,
  a.rp_rcpts_mtd_r,
  a.rp_rcpts_mtd_u,
  a.nrp_rcpts_mtd_c,
  a.nrp_rcpts_mtd_r,
  a.nrp_rcpts_mtd_u,
  a.rp_oo_c,
  a.rp_oo_r,
  a.rp_oo_u,
  a.nrp_oo_c,
  a.nrp_oo_r,
  a.nrp_oo_u,
  a.nrp_cm_c,
  a.nrp_cm_r,
  a.nrp_cm_u,
  a.rp_cm_c,
  a.rp_cm_r,
  a.rp_cm_u,
  a.rp_ant_spd_c,
  a.rp_ant_spd_r,
  a.rp_ant_spd_u);