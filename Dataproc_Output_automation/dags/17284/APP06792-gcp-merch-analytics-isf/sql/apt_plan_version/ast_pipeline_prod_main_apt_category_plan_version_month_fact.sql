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
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt >= 202211
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


CREATE TEMPORARY TABLE IF NOT EXISTS sc_cattr
AS
SELECT dept_num,
 category,
 category_group
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS a
GROUP BY dept_num,
 category,
 category_group;


--COLLECT STATS      PRIMARY INDEX(dept_num,CATEGORY)      ,COLUMN(dept_num)      ,COLUMN(CATEGORY)      ON sc_cattr


CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy
AS
SELECT DISTINCT CAST(trunc(div_num) AS INTEGER) AS div_idnt,
 div_desc,
 CAST(trunc(grp_num) AS INTEGER) AS subdiv_idnt,
 grp_desc AS subdiv_desc,
 CAST(trunc(dept_num) AS INTEGER) AS dept_idnt,
 dept_desc
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku;


CREATE TEMPORARY TABLE IF NOT EXISTS full_data
AS
SELECT a.country AS selling_country,
 a.banner AS selling_brand,
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
 0 AS average_inventory_retail_amount,
 0 AS average_inventory_cost_amount,
 0 AS average_inventory_units,
 0 AS beginning_of_period_inventory_retail_amount,
 0 AS beginning_of_period_inventory_cost_amount,
 0 AS beginning_of_period_inventory_units,
 0 AS receipts_retail_amount,
 0 AS receipts_cost_amount,
 0 AS receipts_units,
 0 AS receipts_less_reserve_retail_amount,
 0 AS receipts_less_reserve_cost_amount,
 0 AS receipts_less_reserve_units
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_category_cluster_plan_history AS a
 INNER JOIN sc_date AS c ON a.fiscal_month_idnt = c.month_idnt
 INNER JOIN sc_date AS s ON a.snapshot_plan_month_idnt = s.month_idnt
 INNER JOIN hierarchy AS h ON CAST(a.dept_idnt AS FLOAT64) = h.dept_idnt
 INNER JOIN sc_cattr AS g ON CAST(a.dept_idnt AS FLOAT64) = g.dept_num AND LOWER(a.category) = LOWER(g.category)
GROUP BY selling_country,
 selling_brand,
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
 average_inventory_retail_amount,
 average_inventory_cost_amount,
 average_inventory_units,
 beginning_of_period_inventory_retail_amount,
 beginning_of_period_inventory_cost_amount,
 beginning_of_period_inventory_units,
 receipts_retail_amount,
 receipts_cost_amount,
 receipts_units,
 receipts_less_reserve_retail_amount,
 receipts_less_reserve_cost_amount,
 receipts_less_reserve_units
UNION ALL
SELECT a0.selling_country,
 a0.selling_brand,
 a0.category,
 g0.category_group,
 a0.dept_idnt,
 h0.dept_desc,
 h0.div_idnt,
 h0.div_desc,
 h0.subdiv_idnt,
 h0.subdiv_desc,
 a0.snapshot_plan_month_idnt,
 s0.month_id AS snapshot_month_id,
 s0.month_label AS snapshot_month_label,
 c0.month_id,
 c0.month_label,
 c0.half_label,
 c0.quarter,
 c0.fiscal_year_num,
 a0.alternate_inventory_model,
 0 AS net_sales_retail_amount,
 0 AS net_sales_cost_amount,
 0 AS net_sales_units,
 a0.average_inventory_retail_amount,
 a0.average_inventory_cost_amount,
 a0.average_inventory_units,
 a0.beginning_of_period_inventory_retail_amount,
 a0.beginning_of_period_inventory_cost_amount,
 a0.beginning_of_period_inventory_units,
 a0.receipts_retail_amount,
 a0.receipts_cost_amount,
 a0.receipts_units,
 a0.receipts_less_reserve_retail_amount,
 a0.receipts_less_reserve_cost_amount,
 a0.receipts_less_reserve_units
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_category_country_plan_history AS a0
 INNER JOIN sc_date AS c0 ON a0.fiscal_month_idnt = c0.month_idnt
 INNER JOIN sc_date AS s0 ON a0.snapshot_plan_month_idnt = s0.month_idnt
 INNER JOIN hierarchy AS h0 ON CAST(a0.dept_idnt AS FLOAT64) = h0.dept_idnt
 INNER JOIN sc_cattr AS g0 ON CAST(a0.dept_idnt AS FLOAT64) = g0.dept_num AND LOWER(a0.category) = LOWER(g0.category)
GROUP BY a0.selling_country,
 a0.selling_brand,
 a0.category,
 g0.category_group,
 a0.dept_idnt,
 h0.dept_desc,
 h0.div_idnt,
 h0.div_desc,
 h0.subdiv_idnt,
 h0.subdiv_desc,
 a0.snapshot_plan_month_idnt,
 snapshot_month_id,
 snapshot_month_label,
 c0.month_id,
 c0.month_label,
 c0.half_label,
 c0.quarter,
 c0.fiscal_year_num,
 a0.alternate_inventory_model,
 net_sales_retail_amount,
 a0.average_inventory_retail_amount,
 a0.average_inventory_cost_amount,
 a0.average_inventory_units,
 a0.beginning_of_period_inventory_retail_amount,
 a0.beginning_of_period_inventory_cost_amount,
 a0.beginning_of_period_inventory_units,
 a0.receipts_retail_amount,
 a0.receipts_cost_amount,
 a0.receipts_units,
 a0.receipts_less_reserve_retail_amount,
 a0.receipts_less_reserve_cost_amount,
 a0.receipts_less_reserve_units,
 net_sales_cost_amount,
 net_sales_units;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_category_plan_version_month_fact{{params.env_suffix}};

 INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.apt_category_plan_version_month_fact{{params.env_suffix}}
SELECT
    d.selling_country,
    d.selling_brand,
    d.category,
    d.category_group,
    cast(trunc(cast(d.dept_idnt as float64)) as Int64),
    d.dept_desc,
    d.div_idnt,
    d.div_desc,
    d.subdiv_idnt,
    d.subdiv_desc,
    d.snapshot_plan_month_idnt,
    d.snapshot_month_id,
    d.snapshot_month_label,
    d.month_id,
    d.month_label,
    d.half_label,
    d.quarter,
    d.fiscal_year_num,
    d.alternate_inventory_model,
    SUM(d.net_sales_retail_amount) AS net_sales_retail_amount,
    SUM(d.net_sales_cost_amount) AS net_sales_cost_amount,
    SUM(d.net_sales_units) AS net_sales_units,
    SUM(d.average_inventory_retail_amount) AS average_inventory_retail_amount,
    SUM(d.average_inventory_cost_amount) AS average_inventory_cost_amount,
    SUM(d.average_inventory_units) AS average_inventory_units,
    SUM(d.beginning_of_period_inventory_retail_amount) AS beginning_of_period_inventory_retail_amount,
    SUM(d.beginning_of_period_inventory_cost_amount) AS beginning_of_period_inventory_cost_amount,
    SUM(d.beginning_of_period_inventory_units) AS beginning_of_period_inventory_units,
    SUM(d.receipts_retail_amount) AS rcpt_retail_amount,
    SUM(d.receipts_cost_amount) AS rcpt_cost_amount,
    SUM(d.receipts_units) AS rcpt_units,
    SUM(d.receipts_less_reserve_retail_amount) AS rcpt_less_reserve_retail_amount,
    SUM(d.receipts_less_reserve_cost_amount) AS rcpt_less_reserve_cost_amount,
    SUM(d.receipts_less_reserve_units) AS rcpt_less_reserve_units
FROM 
    `full_data` d
GROUP BY 
    d.selling_country,
    d.selling_brand,
    d.category,
    d.category_group,
    d.dept_idnt,
    d.dept_desc,
    d.div_idnt,
    d.div_desc,
    d.subdiv_idnt,
    d.subdiv_desc,
    d.snapshot_plan_month_idnt,
    d.snapshot_month_id,
    d.snapshot_month_label,
    d.month_id,
    d.month_label,
    d.half_label,
    d.quarter,
    d.fiscal_year_num,
    d.alternate_inventory_model;
