--WHERE CHANNEL_BRAND in ('Nordstrom','Nordstrom_Rack')


CREATE TEMPORARY TABLE IF NOT EXISTS ia_cluster_map
CLUSTER BY channel_idnt, selling_country, selling_brand
AS
SELECT channel_idnt,
 channel_desc,
 selling_brand,
 selling_country,
 channel,
 selling_brand AS banner
FROM (SELECT channel_num AS channel_idnt,
   channel_desc,
   channel_brand AS selling_brand,
   channel_country AS selling_country,
   TRIM(FORMAT('%11d', channel_num) || ',' || channel_desc) AS channel
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
  WHERE channel_num IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER))
  GROUP BY store_num,
   channel_brand,
   channel_country,
   channel_num,
   channel_desc) AS a
GROUP BY channel_idnt,
 channel_desc,
 selling_brand,
 selling_country,
 channel;


--COLLECT STATS 	PRIMARY INDEX (CHANNEL_IDNT,SELLING_COUNTRY,SELLING_BRAND) 	,COLUMN(CHANNEL_IDNT) 	,COLUMN(SELLING_COUNTRY) 	,COLUMN(SELLING_BRAND) ON ia_cluster_map 


--drop table sc_date


--lets create a week to month mapping


CREATE TEMPORARY TABLE IF NOT EXISTS ia_date
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


--COLLECT STATS      PRIMARY INDEX (WEEK_IDNT)      ,COLUMN(WEEK_IDNT)      ,COLUMN(MONTH_IDNT)      ,COLUMN(QUARTER_IDNT)      ,COLUMN(FISCAL_YEAR_NUM)      ON ia_date


--Lets current week so we can get MTD rcpts


CREATE TEMPORARY TABLE IF NOT EXISTS ia_wtd_date
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


--COLLECT STATS      PRIMARY INDEX (WEEK_IDNT)      ,COLUMN(WEEK_IDNT)      ON ia_wtd_date


--Lets collect supp attributes    


--DROP TABLE sc_supp_gp


CREATE TEMPORARY TABLE IF NOT EXISTS ia_supp_gp
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


--COLLECT STATS      PRIMARY INDEX(DEPT_NUM,SUPPLIER_GROUP)      ,COLUMN(DEPT_NUM)      ,COLUMN(SUPPLIER_GROUP) 	ON ia_supp_gp


--lets collect category attributes


CREATE TEMPORARY TABLE IF NOT EXISTS ia_cattr
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


--COLLECT STATS      PRIMARY INDEX(dept_num,CATEGORY)      ,COLUMN(dept_num)      ,COLUMN(CATEGORY)      ON ia_cattr


--Lets bring in wtd rcpts at supp/cat


--drop table sc_wtd_rcpts


--,sum(g.NRP_RCPTS_MTD_C) as NRP_RCPTS_MTD_C


--,sum(g.RP_RCPTS_MTD_U) as RP_RCPTS_MTD_U


--,sum(g.NRP_RCPTS_MTD_U) as NRP_RCPTS_MTD_U


--,SUM(CASE WHEN d.RP_IND = 'N' AND d.DROPSHIP_IND ='N' AND d.CHANNEL_NUM IN ('930','220','221') THEN d.RECEIPTS_REGULAR_COST+d.RECEIPTS_CLEARANCE_COST+d.RECEIPTS_CROSSDOCK_REGULAR_COST+d.RECEIPTS_CROSSDOCK_CLEARANCE_COST ELSE 0


--	END) AS NRP_RCPTS_MTD_C


--,SUM(CASE WHEN d.RP_IND = 'Y' AND d.DROPSHIP_IND ='N' AND d.CHANNEL_NUM IN ('930','220','221') THEN d.RECEIPTS_REGULAR_UNITS+d.RECEIPTS_CLEARANCE_UNITS+d.RECEIPTS_CROSSDOCK_REGULAR_UNITS+d.RECEIPTS_CROSSDOCK_CLEARANCE_UNITS ELSE 0


--END) AS RP_RCPTS_MTD_U


--,SUM(CASE WHEN d.RP_IND = 'N' AND d.DROPSHIP_IND ='N' AND d.CHANNEL_NUM IN ('930','220','221') THEN d.RECEIPTS_REGULAR_UNITS+d.RECEIPTS_CLEARANCE_UNITS+d.RECEIPTS_CROSSDOCK_REGULAR_UNITS+d.RECEIPTS_CROSSDOCK_CLEARANCE_UNITS ELSE 0


--END) AS NRP_RCPTS_MTD_U	


--,c.CLUSTER_NAME


--where a.DEPARTMENT_NUM = '882'


--AND b.MONTH_LABEL = '2022 JAN'


--AND a.DROPSHIP_IND = 'N'


--AND a.RP_IND = 'Y'


CREATE TEMPORARY TABLE IF NOT EXISTS ia_wtd_rcpts
CLUSTER BY department_num, supplier_group, category, month_label
AS
SELECT g.department_num,
 g.supplier_group,
 g.alt_inv_model,
 g.selling_country,
 g.selling_brand,
 g.channel_idnt,
 g.year_num,
 g.month_id,
 g.month_label,
 g.half_label,
 g.quarter,
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
 SUM(g.inactive_rcpts_mtd_c) AS inactive_rcpts_mtd_c
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
   d.channel_idnt,
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
     WHEN LOWER(RTRIM(d.dropship_ind)) = LOWER(RTRIM('N')) AND d.channel_num IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER)
        , CAST(RTRIM('221') AS INTEGER))
     THEN d.receipts_regular_cost + d.receipts_clearance_cost + d.receipts_crossdock_regular_cost + d.receipts_crossdock_clearance_cost
      
     ELSE 0
     END) AS inactive_rcpts_mtd_c
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
     c.channel_idnt,
     c.selling_brand,
     c.selling_country,
     a.receipts_regular_cost,
     a.receipts_regular_units,
     a.receipts_clearance_cost,
     a.receipts_clearance_units,
     a.receipts_crossdock_regular_cost,
     a.receipts_crossdock_regular_units,
     a.receipts_crossdock_clearance_cost,
     a.receipts_crossdock_clearance_units
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
     INNER JOIN ia_wtd_date AS b ON a.week_num = b.week_idnt
     INNER JOIN ia_cluster_map AS c ON a.channel_num = c.channel_idnt
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
     c.channel_idnt,
     c.selling_brand,
     c.selling_country,
     a.receipts_regular_cost,
     a.receipts_regular_units,
     a.receipts_clearance_cost,
     a.receipts_clearance_units,
     a.receipts_crossdock_regular_cost,
     a.receipts_crossdock_regular_units,
     a.receipts_crossdock_clearance_cost,
     a.receipts_crossdock_clearance_units) AS d
   LEFT JOIN ia_supp_gp AS e ON d.department_num = CAST(RTRIM(e.dept_num) AS FLOAT64) AND LOWER(RTRIM(d.supplier_num)) =
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
   d.channel_idnt,
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
 LEFT JOIN ia_cattr AS h ON g.department_num = h.dept_num AND g.class_num = CAST(RTRIM(h.class_num) AS FLOAT64) AND g.subclass_num
    = CAST(RTRIM(h.sbclass_num) AS FLOAT64)
 LEFT JOIN ia_cattr AS i ON g.department_num = i.dept_num AND g.class_num = CAST(RTRIM(i.class_num) AS FLOAT64) AND CAST(RTRIM(i.sbclass_num) AS FLOAT64)
   = - 1
GROUP BY g.department_num,
 g.supplier_group,
 g.alt_inv_model,
 g.selling_country,
 g.selling_brand,
 g.channel_idnt,
 g.year_num,
 g.month_id,
 g.month_label,
 g.half_label,
 g.quarter,
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


--COLLECT STATS      PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPARTMENT_NUM)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON ia_wtd_rcpts


--drop table sc_oo_final


--lets add back in the hierarchy to the base data and sum up inactive/active rcpts as well as rp nrp


--Inactive channels 930,220 and 221


--,sum(g.RP_OO_ACTIVE_UNITS) as RP_OO_U


--,sum(g.NON_RP_OO_ACTIVE_COST) as NRP_OO_C


--,sum(g.NON_RP_OO_ACTIVE_UNITS) as NRP_OO_U


--where a.month_num = '202302'


--and a.DEPARTMENT_NUM = '882'


CREATE TEMPORARY TABLE IF NOT EXISTS ia_oo_final
CLUSTER BY department_num, supplier_group, category, month_label
AS
SELECT g.department_num,
 g.channel_num,
 g.selling_country,
 g.selling_brand,
 g.month_label,
 g.month_id,
 g.quarter,
 g.half_label,
 g.fiscal_year_num,
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
 SUM(g.oo_inactive_cost) AS inactive_oo_c
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
   SUM(d.oo_inactive_cost) AS oo_inactive_cost
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
     a.rp_ind,
     a.oo_inactive_cost
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_apt_on_order_insight_fact_vw AS a
     INNER JOIN ia_date AS b ON a.week_num = b.week_idnt
     INNER JOIN ia_cluster_map AS c ON a.channel_num = c.channel_idnt
    WHERE a.channel_num IN (CAST(RTRIM('930') AS INTEGER), CAST(RTRIM('220') AS INTEGER), CAST(RTRIM('221') AS INTEGER)
       )) AS d
   LEFT JOIN ia_supp_gp AS e ON d.department_num = CAST(RTRIM(e.dept_num) AS FLOAT64) AND LOWER(RTRIM(d.supplier_num)) =
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
 LEFT JOIN ia_cattr AS h ON g.department_num = h.dept_num AND g.class_num = CAST(RTRIM(h.class_num) AS FLOAT64) AND g.subclass_num
    = CAST(RTRIM(h.sbclass_num) AS FLOAT64)
 LEFT JOIN ia_cattr AS i ON g.department_num = i.dept_num AND g.class_num = CAST(RTRIM(i.class_num) AS FLOAT64) AND CAST(RTRIM(i.sbclass_num) AS FLOAT64)
   = - 1
GROUP BY g.department_num,
 g.channel_num,
 g.selling_country,
 g.selling_brand,
 g.month_label,
 g.month_id,
 g.quarter,
 g.half_label,
 g.fiscal_year_num,
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


--COLLECT STATS      PRIMARY INDEX(DEPARTMENT_NUM,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPARTMENT_NUM)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON ia_oo_final


--,SUM(CASE 


--	WHEN d.PLAN_TYPE in ('REPLNSHMNT') AND d.SELLING_COUNTRY in ('US') THEN d.TTL_COST_US


--	WHEN d.PLAN_TYPE in ('REPLNSHMNT') AND d.SELLING_COUNTRY in ('CA') THEN d.TTL_COST_CA  


--	ELSE 0 END) AS RP_CM_C


--,SUM(CASE 


--	WHEN d.PLAN_TYPE in ('BACKUP','FASHION','NRPREORDER') THEN d.RCPT_UNITS 


--	ELSE 0 END) AS NRP_CM_U


--,SUM(CASE 


--	WHEN d.PLAN_TYPE in ('REPLNSHMNT') THEN d.RCPT_UNITS  


--	ELSE 0 END) AS RP_CM_U


--where a.DEPT_ID = '882'


CREATE TEMPORARY TABLE IF NOT EXISTS ia_cm
CLUSTER BY dept_id, supplier_group, category, month_label
AS
SELECT g.dept_id,
 g.selling_country,
 g.selling_brand,
 g.org_id,
 g.month_id,
 g.month_label,
 g.quarter,
 g.half_label,
 g.fiscal_year_num,
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
 SUM(g.inactive_cm_c) AS inactive_cm_c
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
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('RKPACKHOLD'))) AND LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('US'
          )))
     THEN d.ttl_cost_us
     WHEN LOWER(RTRIM(d.plan_type)) IN (LOWER(RTRIM('RKPACKHOLD'))) AND LOWER(RTRIM(d.selling_country)) IN (LOWER(RTRIM('CA'
          )))
     THEN d.ttl_cost_ca
     ELSE 0
     END) AS inactive_cm_c
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
     INNER JOIN ia_cluster_map AS b ON a.org_id = b.channel_idnt
     INNER JOIN ia_date AS c ON CAST(RTRIM(a.fiscal_month_id) AS FLOAT64) = c.month_idnt
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
   LEFT JOIN ia_supp_gp AS e ON d.dept_id = CAST(RTRIM(e.dept_num) AS FLOAT64) AND LOWER(RTRIM(d.supp_id)) = LOWER(RTRIM(e
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
 LEFT JOIN ia_cattr AS h ON g.dept_id = h.dept_num AND g.class_id = CAST(RTRIM(h.class_num) AS FLOAT64) AND g.subclass_id
    = CAST(RTRIM(h.sbclass_num) AS FLOAT64)
 LEFT JOIN ia_cattr AS i ON g.dept_id = i.dept_num AND g.class_id = CAST(RTRIM(i.class_num) AS FLOAT64) AND CAST(RTRIM(i.sbclass_num) AS FLOAT64)
   = - 1
GROUP BY g.dept_id,
 g.selling_country,
 g.selling_brand,
 g.org_id,
 g.month_id,
 g.month_label,
 g.quarter,
 g.half_label,
 g.fiscal_year_num,
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


--COLLECT STATS      PRIMARY INDEX (DEPT_ID,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPT_ID)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON ia_cm


--lets bring all the data together   


--WHERE DEPARTMENT_NUMBER = '882'


CREATE TEMPORARY TABLE IF NOT EXISTS ia_final
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
 SUM(inactive_rcpts_mtd_c) AS inactive_rcpts_mtd_c,
 SUM(inactive_oo_c) AS inactive_oo_c,
 SUM(inactive_cm_c) AS inactive_cm_c
FROM (SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(CAST(channel_idnt AS STRING), 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    CAST(department_num AS INTEGER) AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(CAST(year_num AS STRING), 1, 10) AS fiscal_year_num,
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
    CAST(inactive_rcpts_mtd_c AS NUMERIC) AS inactive_rcpts_mtd_c,
    0 AS inactive_oo_c,
    0 AS inactive_cm_c
   FROM ia_wtd_rcpts
   UNION ALL
   SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(CAST(channel_num AS STRING), 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    department_num AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
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
    0 AS inactive_rcpts_mtd_c,
    CAST(inactive_oo_c AS NUMERIC) AS inactive_oo_c,
    0 AS inactive_cm_c
   FROM ia_oo_final
   UNION ALL
   SELECT SUBSTR(selling_country, 1, 2) AS selling_country,
    SUBSTR(selling_brand, 1, 15) AS selling_brand,
    SUBSTR(CAST(org_id AS STRING), 1, 5) AS channel,
    SUBSTR(supplier_group, 1, 100) AS supplier_group,
    SUBSTR(category, 1, 50) AS category,
    CAST(dept_id AS INTEGER) AS department_number,
    SUBSTR(alt_inv_model, 1, 15) AS alt_inv_model,
    SUBSTR(month_id, 1, 25) AS month_id,
    SUBSTR(month_label, 1, 15) AS month_label,
    SUBSTR(half_label, 1, 20) AS half,
    SUBSTR(quarter, 1, 10) AS quarter,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
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
    0 AS inactive_rcpts_mtd_c,
    0 AS inactive_oo_c,
    CAST(inactive_cm_c AS NUMERIC) AS inactive_cm_c
   FROM ia_cm) AS a
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


--COLLECT STATS      PRIMARY INDEX (DEPARTMENT_NUMBER,SUPPLIER_GROUP,CATEGORY,MONTH_LABEL)      ,COLUMN(DEPARTMENT_NUMBER)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ,COLUMN(MONTH_LABEL)      ON ia_final


TRUNCATE TABLE t2dl_das_open_to_buy.apt_inactive_current;


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.apt_inactive_current
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
  a.inactive_rcpts_mtd_c,
  a.inactive_oo_c,
  a.inactive_cm_c,
  CAST(CURRENT_DATE('PST8PDT') AS STRING) AS process_dt
 FROM ia_final AS a
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS b ON a.department_number = b.dept_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS c ON CAST(RTRIM(a.channel) AS FLOAT64) = c.channel_num
 WHERE a.inactive_rcpts_mtd_c <> 0
  OR a.inactive_oo_c <> 0
  OR a.inactive_cm_c <> 0
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
  a.inactive_rcpts_mtd_c,
  a.inactive_oo_c,
  a.inactive_cm_c,
  process_dt);


--COLLECT STATS      PRIMARY INDEX(Department,SELLING_BRAND, MONTH_ID,SUPPLIER_GROUP,CATEGORY)      ,COLUMN(Department)      ,COLUMN(SELLING_BRAND)      ,COLUMN(MONTH_ID)      ,COLUMN(SUPPLIER_GROUP)      ,COLUMN(CATEGORY)      ON T2DL_DAS_OPEN_TO_BUY.APT_INACTIVE_CURRENT