/*
Purpose:        Inserts data in table `apt_plan_version_month_fact` for APT Plan Version monthly table
Variable(s):    {t2dl_das_apt_cost_reporting} T2DL_DAS_APT_COST_REPORTING (prod) or T3DL_ACE_ASSORTMENT
                {} '' or '_dev' table suffix for prod testing 
Author(s):      Sara Scott
*/


--grab channel_info



CREATE TEMPORARY TABLE IF NOT EXISTS sc_cluster_map
AS
SELECT DISTINCT 
 selling_country,
 selling_brand,
 cluster_name,
  CASE
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_CANADA_STORES')
  THEN 111
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_CANADA_ONLINE')
  THEN 121
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_STORES')
  THEN 110
  WHEN LOWER(cluster_name) = LOWER('NORDSTROM_ONLINE')
  THEN 120
  WHEN LOWER(cluster_name) = LOWER('RACK_ONLINE')
  THEN 250
  WHEN LOWER(cluster_name) = LOWER('RACK_CANADA_STORES')
  THEN 211
  WHEN LOWER(cluster_name) IN (LOWER('RACK STORES'), LOWER('PRICE'), LOWER('HYBRID'), LOWER('BRAND'))
  THEN 210
  WHEN LOWER(cluster_name) = LOWER('NORD CA RSWH')
  THEN 311
  WHEN LOWER(cluster_name) = LOWER('NORD US RSWH')
  THEN 310
  WHEN LOWER(cluster_name) = LOWER('RACK CA RSWH')
  THEN 261
  WHEN LOWER(cluster_name) = LOWER('RACK US RSWH')
  THEN 260
  ELSE NULL
  END AS chnl_idnt
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_history
GROUP BY selling_country,
 selling_brand,
 cluster_name,
 chnl_idnt;


--COLLECT STATS 	PRIMARY INDEX (chnl_idnt, cluster_name,SELLING_COUNTRY,SELLING_BRAND) 	,COLUMN(chnl_idnt) 	,COLUMN(cluster_name) 	,COLUMN(SELLING_COUNTRY) 	,COLUMN(SELLING_BRAND) ON sc_cluster_map 


--lets create a week to month mapping


CREATE TEMPORARY TABLE IF NOT EXISTS sc_date
AS
SELECT month_idnt,
 month_label,
 month_454_label,
     FORMAT('%11d', fiscal_year_num) || '' || FORMAT('%11d', fiscal_month_num) || ' ' || month_abrv AS month_id,
 quarter_idnt,
 quarter_label,
    'FY' || SUBSTR(SUBSTR(CAST(fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || quarter_abrv AS quarter,
 half_label,
 fiscal_year_num,
 COUNT(fiscal_week_num) AS week_count
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt >= 202210
GROUP BY month_idnt,
 month_label,
 month_454_label,
 month_id,
 quarter_idnt,
 quarter_label,
 quarter,
 half_label,
 fiscal_year_num;


--COLLECT STATS      PRIMARY INDEX (month_idnt)      ,COLUMN(month_idnt)      ,COLUMN(quarter_idnt)      ,COLUMN(fiscal_year_num)      ON sc_date


--lets collect category attributes


CREATE TEMPORARY TABLE IF NOT EXISTS sc_cattr
AS
SELECT dept_num,
 category,
 category_group
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS a
GROUP BY dept_num,
 category,
 category_group;


--COLLECT STATS      PRIMARY INDEX(dept_num,CATEGORY)      ,COLUMN(dept_num)      ,COLUMN(CATEGORY)      ON sc_cattr


--grabbing hierarchy info


-- subdivision


CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy
AS
SELECT DISTINCT CAST(trunc(div_num) AS INTEGER) AS div_idnt,
 div_desc,
 CAST(trunc(grp_num) AS INTEGER) AS subdiv_idnt,
 grp_desc AS subdiv_desc,
 CAST(trunc(dept_num) AS INTEGER) AS dept_idnt,
 dept_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku;


--Lets get the current and future plan data    


--drop table sc_plan


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_plan_version_month_fact{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_plan_version_month_fact{{params.env_suffix}}
(SELECT selling_country,
  selling_brand,
  chnl_idnt,
  cluster_name,
  supplier_group,
  category,
  category_group,
  dept_idnt,
  dept_desc,
  div_idnt,
  div_desc,
  subdiv_idnt,
  subdiv_desc,
  snapshot_plan_month_idnt,
  snapshot_month_id,
  snapshot_month_label,
  month_id,
  month_label,
  half_label,
  quarter,
  fiscal_year_num,
  alternate_inventory_model,
  SUM(net_sales_retail_amount) AS net_sales_retail_amount,
  SUM(net_sales_cost_amount) AS net_sales_cost_amount,
  SUM(net_sales_units) AS net_sales_units,
  SUM(average_inventory_retail_amount) AS average_inventory_retail_amount,
  SUM(average_inventory_cost_amount) AS average_inventory_cost_amount,
  SUM(average_inventory_units) AS average_inventory_units,
  SUM(beginning_of_period_inventory_retail_amount) AS beginning_of_period_inventory_retail_amount,
  SUM(beginning_of_period_inventory_cost_amount) AS beginning_of_period_inventory_cost_amount,
  SUM(beginning_of_period_inventory_units) AS beginning_of_period_inventory_units,
  SUM(replenishment_receipts_retail_amount) AS replenishment_receipts_retail_amount,
  SUM(replenishment_receipts_cost_amount) AS replenishment_receipts_cost_amount,
  SUM(replenishment_receipts_units) AS replenishment_receipts_units,
  SUM(nonreplenishment_receipts_retail_amount) AS nonreplenishment_receipts_retail_amount,
  SUM(nonreplenishment_receipts_cost_amount) AS nonreplenishment_receipts_cost_amount,
  SUM(nonreplenishment_receipts_units) AS nonreplenishment_receipts_units,
  SUM(ttl_plan_rcpt_less_reserve_retail_amount) AS ttl_plan_rcpt_less_reserve_retail_amount,
  SUM(ttl_plan_rcpt_less_reserve_cost_amount) AS ttl_plan_rcpt_less_reserve_cost_amount,
  SUM(ttl_plan_rcpt_less_reserve_units) AS ttl_plan_rcpt_less_reserve_units
 FROM (SELECT a.selling_country,
    a.selling_brand,
    b.chnl_idnt,
    a.cluster_name,
    a.supplier_group,
    a.category,
    g.category_group,
    a.dept_idnt,
    h.dept_desc,
    h.div_idnt,
    h.div_desc,
    h.subdiv_idnt,
    h.subdiv_desc,
    a.snapshot_plan_month_idnt,
    s.month_id AS snapshot_month_id,
    s.month_label AS snapshot_month_label,
    c.month_id,
    c.month_label,
    c.half_label,
    c.quarter,
    c.fiscal_year_num,
    a.alternate_inventory_model,
    a.net_sales_retail_amount,
    a.net_sales_cost_amount,
    a.net_sales_units,
    a.average_inventory_retail_amount,
    a.average_inventory_cost_amount,
    a.average_inventory_units,
    a.beginning_of_period_inventory_retail_amount,
    a.beginning_of_period_inventory_cost_amount,
    a.beginning_of_period_inventory_units,
    a.replenishment_receipts_retail_amount,
    a.replenishment_receipts_cost_amount,
    a.replenishment_receipts_units,
    a.nonreplenishment_receipts_retail_amount,
    a.nonreplenishment_receipts_cost_amount,
    a.nonreplenishment_receipts_units,
     a.replenishment_receipts_less_reserve_retail_amount + a.nonreplenishment_receipts_less_reserve_retail_amount AS
    ttl_plan_rcpt_less_reserve_retail_amount,
     a.replenishment_receipts_less_reserve_cost_amount + a.nonreplenishment_receipts_less_reserve_cost_amount AS ttl_plan_rcpt_less_reserve_cost_amount
    ,
     a.replenishment_receipts_less_reserve_units + a.nonreplenishment_receipts_less_reserve_units AS ttl_plan_rcpt_less_reserve_units
   FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_history AS a
    INNER JOIN sc_date AS c 
    ON LOWER(a.month_id) = LOWER(c.month_454_label)
    INNER JOIN sc_date AS s 
    ON a.snapshot_plan_month_idnt = s.month_idnt
    INNER JOIN sc_cluster_map AS b 
    ON LOWER(a.cluster_name) = LOWER(b.cluster_name)
    INNER JOIN hierarchy AS h 
    ON a.dept_idnt = h.dept_idnt
    INNER JOIN sc_cattr AS g 
    ON a.dept_idnt = g.dept_num 
    AND LOWER(a.category) = LOWER(g.category)
   GROUP BY a.selling_country,
    a.selling_brand,
    b.chnl_idnt,
    a.cluster_name,
    a.supplier_group,
    a.category,
    g.category_group,
    a.dept_idnt,
    h.dept_desc,
    h.div_idnt,
    h.div_desc,
    h.subdiv_idnt,
    h.subdiv_desc,
    a.snapshot_plan_month_idnt,
    snapshot_month_id,
    snapshot_month_label,
    c.month_id,
    c.month_label,
    c.half_label,
    c.quarter,
    c.fiscal_year_num,
    a.alternate_inventory_model,
    a.net_sales_retail_amount,
    a.net_sales_cost_amount,
    a.net_sales_units,
    a.average_inventory_retail_amount,
    a.average_inventory_cost_amount,
    a.average_inventory_units,
    a.beginning_of_period_inventory_retail_amount,
    a.beginning_of_period_inventory_cost_amount,
    a.beginning_of_period_inventory_units,
    a.replenishment_receipts_retail_amount,
    a.replenishment_receipts_cost_amount,
    a.replenishment_receipts_units,
    a.replenishment_receipts_less_reserve_retail_amount,
    a.replenishment_receipts_less_reserve_cost_amount,
    a.replenishment_receipts_less_reserve_units,
    a.nonreplenishment_receipts_less_reserve_retail_amount,
    a.nonreplenishment_receipts_less_reserve_cost_amount,
    a.nonreplenishment_receipts_less_reserve_units,
    a.nonreplenishment_receipts_retail_amount,
    a.nonreplenishment_receipts_cost_amount,
    a.nonreplenishment_receipts_units) AS t1
 GROUP BY selling_country,
  selling_brand,
  chnl_idnt,
  cluster_name,
  supplier_group,
  category,
  category_group,
  dept_idnt,
  dept_desc,
  div_idnt,
  div_desc,
  subdiv_idnt,
  subdiv_desc,
  snapshot_plan_month_idnt,
  snapshot_month_id,
  snapshot_month_label,
  month_id,
  month_label,
  half_label,
  quarter,
  fiscal_year_num,
  alternate_inventory_model);


--COLLECT STATS      PRIMARY INDEX(dept_idnt,supplier_group,category,month_id)      ,COLUMN(dept_idnt)      ,COLUMN(supplier_group)      ,COLUMN(category)      ,COLUMN(month_id)      ON t2dl_das_apt_cost_reporting.apt_plan_version_month_fact