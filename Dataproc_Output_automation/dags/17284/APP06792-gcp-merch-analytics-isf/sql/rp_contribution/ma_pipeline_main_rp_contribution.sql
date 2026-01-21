

BEGIN

CREATE TEMPORARY TABLE IF NOT EXISTS week_dim_lkup
CLUSTER BY week_idnt
AS
SELECT DISTINCT r.week_idnt,
 d.week_idnt AS week_idnt_true,
 r.week_num_of_fiscal_month,
 r.week_454_label,
 r.month_abrv,
 r.month_idnt,
 r.month_454_label,
 r.fiscal_month_num,
 r.month_desc,
 r.quarter_desc,
 r.fiscal_year_num,
 r.ty_ly_lly_ind AS ty_ly_ind,
 r.week_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS r
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS d ON r.day_date = d.day_date
WHERE r.fiscal_year_num BETWEEN (SELECT MAX(fiscal_year_num) - 2
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
   WHERE week_end_day_date < CURRENT_DATE) AND (SELECT MAX(fiscal_year_num)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
   WHERE week_end_day_date < CURRENT_DATE)
 AND r.week_idnt <= (SELECT MAX(week_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
   WHERE week_end_day_date < CURRENT_DATE);







CREATE TEMPORARY TABLE IF NOT EXISTS store_dim_lkup
CLUSTER BY store_num, channel_num
AS
SELECT DISTINCT store_num,
 channel_num,
 channel_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE LOWER(store_country_code) = LOWER('US')
 AND LOWER(store_abbrev_name) <> LOWER('CLOSED')
 AND channel_num NOT IN (140, 930, 990)
 AND channel_num IS NOT NULL;





CREATE TEMPORARY TABLE IF NOT EXISTS dept_class_dim_lkup
CLUSTER BY dept_num, class_num
AS
SELECT DISTINCT dept_num,
 dept_desc,
 division_num,
 division_desc,
 subdivision_num,
 subdivision_desc,
 class_num,
 class_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_product_subclass_dim_vw
WHERE LOWER(UPPER(dept_desc)) NOT LIKE LOWER('%INACTIVE%')
 AND LOWER(UPPER(division_desc)) NOT LIKE LOWER('%INACTIVE%')
 AND LOWER(UPPER(subdivision_desc)) NOT LIKE LOWER('%INACTIVE%');





CREATE TEMPORARY TABLE IF NOT EXISTS vendor_dim_lkup
CLUSTER BY vendor_num
AS
SELECT DISTINCT vendor_num,
 vendor_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim;





CREATE TEMPORARY TABLE IF NOT EXISTS rp_contribution_fact
CLUSTER BY week_num, channel_num, dept_num, class_num
AS
SELECT d.week_idnt AS week_num,
 f.channel_num,
 f.dept_num,
 f.store_num,
 f.supp_num,
 f.class_num,
 f.npg_ind,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_eoh_total_retail_amt_ty
   ELSE 0
   END) AS rpl_eoh_total_retail,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_eoh_total_retail_amt_ty
   ELSE 0
   END) AS nrp_eoh_total_retail,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_eoh_total_cost_amt_ty
   ELSE 0
   END) AS rpl_eoh_total_cost,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_eoh_total_cost_amt_ty
   ELSE 0
   END) AS nrp_eoh_total_cost,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_eoh_total_units_ty
   ELSE 0
   END) AS rpl_eoh_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_eoh_total_units_ty
   ELSE 0
   END) AS nrp_eoh_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_eoh_in_transit_total_retail_amt_ty
   ELSE 0
   END) AS rpl_inventory_eoh_in_transit_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_eoh_in_transit_total_retail_amt_ty
   ELSE 0
   END) AS nrp_inventory_eoh_in_transit_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_eoh_in_transit_total_cost_amt_ty
   ELSE 0
   END) AS rpl_inventory_eoh_in_transit_total_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_eoh_in_transit_total_cost_amt_ty
   ELSE 0
   END) AS nrp_inventory_eoh_in_transit_total_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_eoh_in_transit_total_units_ty
   ELSE 0
   END) AS rpl_inventory_eoh_in_transit_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_eoh_in_transit_total_units_ty
   ELSE 0
   END) AS nrp_inventory_eoh_in_transit_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.jwn_operational_gmv_total_retail_amt_ty
   ELSE 0
   END) AS rpl_jwn_operational_gmv_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.jwn_operational_gmv_total_retail_amt_ty
   ELSE 0
   END) AS nrp_jwn_operational_gmv_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.jwn_operational_gmv_total_cost_amt_ty
   ELSE 0
   END) AS rpl_jwn_operational_gmv_total_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.jwn_operational_gmv_total_cost_amt_ty
   ELSE 0
   END) AS nrp_jwn_operational_gmv_total_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.jwn_operational_gmv_total_units_ty
   ELSE 0
   END) AS rpl_jwn_operational_gmv_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.jwn_operational_gmv_total_units_ty
   ELSE 0
   END) AS nrp_jwn_operational_gmv_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.receipts_total_retail_amt_ty
   ELSE 0
   END) AS rpl_receipts_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.receipts_total_retail_amt_ty
   ELSE 0
   END) AS nrp_receipts_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.receipts_total_cost_amt_ty
   ELSE 0
   END) AS rpl_receipts_total_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.receipts_total_cost_amt_ty
   ELSE 0
   END) AS nrp_receipts_total_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.receipts_total_units_ty
   ELSE 0
   END) AS rpl_receipts_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.receipts_total_units_ty
   ELSE 0
   END) AS nrp_receipts_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.receipts_po_retail_amt_ty
   ELSE 0
   END) AS rpl_receipts_po_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.receipts_po_retail_amt_ty
   ELSE 0
   END) AS nrp_receipts_po_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.receipts_po_cost_amt_ty
   ELSE 0
   END) AS rpl_receipts_po_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.receipts_po_cost_amt_ty
   ELSE 0
   END) AS nrp_receipts_po_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.receipts_po_units_ty
   ELSE 0
   END) AS rpl_receipts_po_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.receipts_po_units_ty
   ELSE 0
   END) AS nrp_receipts_po_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_boh_total_units_ty
   ELSE 0
   END) AS rpl_inventory_boh_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_boh_total_units_ty
   ELSE 0
   END) AS nrp_inventory_boh_total_units,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_boh_total_retail_amt_ty
   ELSE 0
   END) AS rpl_inventory_boh_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_boh_total_retail_amt_ty
   ELSE 0
   END) AS nrp_inventory_boh_total_retail_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('Y')
   THEN f.inventory_boh_total_cost_amt_ty
   ELSE 0
   END) AS rpl_inventory_boh_total_cost_amt,
 SUM(CASE
   WHEN LOWER(f.rp_ind) = LOWER('N')
   THEN f.inventory_boh_total_cost_amt_ty
   ELSE 0
   END) AS nrp_inventory_boh_total_cost_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transaction_sbclass_store_week_agg_fact AS f
 INNER JOIN week_dim_lkup AS d ON f.week_num = d.week_idnt_true
GROUP BY week_num,
 f.channel_num,
 f.dept_num,
 f.store_num,
 f.supp_num,
 f.class_num,
 f.npg_ind;





CREATE TEMPORARY TABLE IF NOT EXISTS rp_contribution_fact_dim
CLUSTER BY week_num, channel_num, department_num, division_num
AS
SELECT f.week_num,
 f.dept_num AS department_num,
 dc.dept_desc AS department_desc,
 dc.division_num,
 dc.division_desc,
 dc.subdivision_num,
 dc.subdivision_desc,
 f.supp_num AS supplier_num,
 vd.vendor_name AS supplier_name,
 f.class_num,
 dc.class_desc,
 f.npg_ind,
 orgstore.channel_num,
 orgstore.channel_desc,
 SUM(f.rpl_eoh_total_retail) AS rpl_eoh_total_retail,
 SUM(f.nrp_eoh_total_retail) AS nrp_eoh_total_retail,
 SUM(f.rpl_eoh_total_cost) AS rpl_eoh_total_cost,
 SUM(f.nrp_eoh_total_cost) AS nrp_eoh_total_cost,
 SUM(f.rpl_eoh_total_units) AS rpl_eoh_total_units,
 SUM(f.nrp_eoh_total_units) AS nrp_eoh_total_units,
 SUM(f.rpl_inventory_eoh_in_transit_total_retail_amt) AS rpl_inventory_eoh_in_transit_total_retail_amt,
 SUM(f.nrp_inventory_eoh_in_transit_total_retail_amt) AS nrp_inventory_eoh_in_transit_total_retail_amt,
 SUM(f.rpl_inventory_eoh_in_transit_total_cost_amt) AS rpl_inventory_eoh_in_transit_total_cost_amt,
 SUM(f.nrp_inventory_eoh_in_transit_total_cost_amt) AS nrp_inventory_eoh_in_transit_total_cost_amt,
 SUM(f.rpl_inventory_eoh_in_transit_total_units) AS rpl_inventory_eoh_in_transit_total_units,
 SUM(f.nrp_inventory_eoh_in_transit_total_units) AS nrp_inventory_eoh_in_transit_total_units,
 SUM(f.rpl_jwn_operational_gmv_total_retail_amt) AS rpl_jwn_operational_gmv_total_retail_amt,
 SUM(f.nrp_jwn_operational_gmv_total_retail_amt) AS nrp_jwn_operational_gmv_total_retail_amt,
 SUM(f.rpl_jwn_operational_gmv_total_cost_amt) AS rpl_jwn_operational_gmv_total_cost_amt,
 SUM(f.nrp_jwn_operational_gmv_total_cost_amt) AS nrp_jwn_operational_gmv_total_cost_amt,
 SUM(f.rpl_jwn_operational_gmv_total_units) AS rpl_jwn_operational_gmv_total_units,
 SUM(f.nrp_jwn_operational_gmv_total_units) AS nrp_jwn_operational_gmv_total_units,
 SUM(f.rpl_receipts_total_retail_amt) AS rpl_receipts_total_retail_amt,
 SUM(f.nrp_receipts_total_retail_amt) AS nrp_receipts_total_retail_amt,
 SUM(f.rpl_receipts_total_cost_amt) AS rpl_receipts_total_cost_amt,
 SUM(f.nrp_receipts_total_cost_amt) AS nrp_receipts_total_cost_amt,
 SUM(f.rpl_receipts_total_units) AS rpl_receipts_total_units,
 SUM(f.nrp_receipts_total_units) AS nrp_receipts_total_units,
 SUM(f.rpl_receipts_po_retail_amt) AS rpl_receipts_po_retail_amt,
 SUM(f.nrp_receipts_po_retail_amt) AS nrp_receipts_po_retail_amt,
 SUM(f.rpl_receipts_po_cost_amt) AS rpl_receipts_po_cost_amt,
 SUM(f.nrp_receipts_po_cost_amt) AS nrp_receipts_po_cost_amt,
 SUM(f.rpl_receipts_po_units) AS rpl_receipts_po_units,
 SUM(f.nrp_receipts_po_units) AS nrp_receipts_po_units,
 SUM(f.rpl_inventory_boh_total_units) AS rpl_inventory_boh_total_units,
 SUM(f.nrp_inventory_boh_total_units) AS nrp_inventory_boh_total_units,
 SUM(f.rpl_inventory_boh_total_retail_amt) AS rpl_inventory_boh_total_retail_amt,
 SUM(f.nrp_inventory_boh_total_retail_amt) AS nrp_inventory_boh_total_retail_amt,
 SUM(f.rpl_inventory_boh_total_cost_amt) AS rpl_inventory_boh_total_cost_amt,
 SUM(f.nrp_inventory_boh_total_cost_amt) AS nrp_inventory_boh_total_cost_amt
FROM rp_contribution_fact AS f
 INNER JOIN store_dim_lkup AS orgstore ON f.store_num = orgstore.store_num
 INNER JOIN dept_class_dim_lkup AS dc ON f.dept_num = dc.dept_num AND f.class_num = dc.class_num
 INNER JOIN vendor_dim_lkup AS vd ON LOWER(vd.vendor_num) = LOWER(f.supp_num)
GROUP BY f.week_num,
 department_num,
 department_desc,
 dc.division_num,
 dc.division_desc,
 dc.subdivision_num,
 dc.subdivision_desc,
 supplier_num,
 supplier_name,
 f.class_num,
 dc.class_desc,
 f.npg_ind,
 orgstore.channel_num,
 orgstore.channel_desc;





TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.rp_contribution_rollup;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.rp_contribution_rollup (week_num, week_end_day_date,
 week_num_of_fiscal_month, week_454_label, month_abrv, month_idnt, month_454_label, fiscal_month_num, month_desc,
 quarter_desc, fiscal_year_num, ty_ly_ind, channel_num, channel_desc, division_num, division_desc, subdivision_num,
 subdivision_desc, department_num, department_desc, class_num, class_desc, supplier_num, supplier_name, npg_ind,
 rpl_eoh_total_retail, nrp_eoh_total_retail, rpl_eoh_total_cost, nrp_eoh_total_cost, rpl_eoh_total_units,
 nrp_eoh_total_units, rpl_inventory_eoh_in_transit_total_retail_amt, nrp_inventory_eoh_in_transit_total_retail_amt,
 rpl_inventory_eoh_in_transit_total_cost_amt, nrp_inventory_eoh_in_transit_total_cost_amt,
 rpl_inventory_eoh_in_transit_total_units, nrp_inventory_eoh_in_transit_total_units,
 rpl_jwn_operational_gmv_total_retail_amt, nrp_jwn_operational_gmv_total_retail_amt,
 rpl_jwn_operational_gmv_total_cost_amt, nrp_jwn_operational_gmv_total_cost_amt, rpl_jwn_operational_gmv_total_units,
 nrp_jwn_operational_gmv_total_units, rpl_receipts_total_retail_amt, nrp_receipts_total_retail_amt,
 rpl_receipts_total_cost_amt, nrp_receipts_total_cost_amt, rpl_receipts_total_units, nrp_receipts_total_units,
 rpl_receipts_po_retail_amt, nrp_receipts_po_retail_amt, rpl_receipts_po_cost_amt, nrp_receipts_po_cost_amt,
 rpl_receipts_po_units, nrp_receipts_po_units, rpl_inventory_boh_total_units, nrp_inventory_boh_total_units,
 rpl_inventory_boh_total_retail_amt, nrp_inventory_boh_total_retail_amt, rpl_inventory_boh_total_cost_amt,
 nrp_inventory_boh_total_cost_amt, dw_sys_load_tmstp)
(SELECT r.week_num,
  d.week_end_day_date,
  d.week_num_of_fiscal_month,
  d.week_454_label,
  d.month_abrv,
  d.month_idnt,
  d.month_454_label,
  d.fiscal_month_num,
  d.month_desc,
  d.quarter_desc,
  d.fiscal_year_num,
  d.ty_ly_ind,
  r.channel_num,
  r.channel_desc,
  r.division_num,
  r.division_desc,
  r.subdivision_num,
  r.subdivision_desc,
  r.department_num,
  r.department_desc,
  r.class_num,
  r.class_desc,
  r.supplier_num,
  r.supplier_name,
  r.npg_ind,
  CAST(r.rpl_eoh_total_retail AS BIGNUMERIC) AS rpl_eoh_total_retail,
  CAST(r.nrp_eoh_total_retail AS BIGNUMERIC) AS nrp_eoh_total_retail,
  CAST(r.rpl_eoh_total_cost AS BIGNUMERIC) AS rpl_eoh_total_cost,
  CAST(r.nrp_eoh_total_cost AS BIGNUMERIC) AS nrp_eoh_total_cost,
  r.rpl_eoh_total_units,
  r.nrp_eoh_total_units,
  r.rpl_inventory_eoh_in_transit_total_retail_amt,
  r.nrp_inventory_eoh_in_transit_total_retail_amt,
  r.rpl_inventory_eoh_in_transit_total_cost_amt,
  r.nrp_inventory_eoh_in_transit_total_cost_amt,
  r.rpl_inventory_eoh_in_transit_total_units,
  r.nrp_inventory_eoh_in_transit_total_units,
  r.rpl_jwn_operational_gmv_total_retail_amt,
  r.nrp_jwn_operational_gmv_total_retail_amt,
  r.rpl_jwn_operational_gmv_total_cost_amt,
  r.nrp_jwn_operational_gmv_total_cost_amt,
  r.rpl_jwn_operational_gmv_total_units,
  r.nrp_jwn_operational_gmv_total_units,
  r.rpl_receipts_total_retail_amt,
  r.nrp_receipts_total_retail_amt,
  r.rpl_receipts_total_cost_amt,
  r.nrp_receipts_total_cost_amt,
  r.rpl_receipts_total_units,
  r.nrp_receipts_total_units,
  r.rpl_receipts_po_retail_amt,
  r.nrp_receipts_po_retail_amt,
  r.rpl_receipts_total_cost_amt AS rpl_receipts_total_cost_amt51,
  r.nrp_receipts_total_cost_amt AS nrp_receipts_total_cost_amt52,
  r.rpl_receipts_po_units,
  r.nrp_receipts_po_units,
  r.rpl_inventory_boh_total_units,
  r.nrp_inventory_boh_total_units,
  r.rpl_inventory_boh_total_retail_amt,
  r.nrp_inventory_boh_total_retail_amt,
  r.rpl_inventory_boh_total_cost_amt,
  r.nrp_inventory_boh_total_cost_amt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM rp_contribution_fact_dim AS r
  INNER JOIN week_dim_lkup AS d ON r.week_num = d.week_idnt);




END