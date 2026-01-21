-- take channel with most records in case of dups


CREATE TEMPORARY TABLE IF NOT EXISTS po_dist
AS
SELECT t4.purchase_order_number,
 t4.rms_sku_num,
 t4.banner
FROM (SELECT dist.purchase_order_num AS purchase_order_number,
   dist.rms_sku_num,
   st.channel_brand AS banner,
   COUNT(*) AS A372081672
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS dist
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st ON dist.distribute_location_id = st.store_num
   INNER JOIN (SELECT DISTINCT purchase_order_number
    FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_sku_cancels) AS c ON LOWER(c.purchase_order_number) = LOWER(dist.purchase_order_num)
  GROUP BY purchase_order_number,
   dist.rms_sku_num,
   banner
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY purchase_order_number, dist.rms_sku_num ORDER BY A372081672 DESC)) = 1) AS t4;





CREATE TEMPORARY TABLE IF NOT EXISTS po_sku_base
AS
SELECT a.purchase_order_number,
 e.banner,
 a.rms_sku_num,
 d.div_idnt,
 d.div_desc,
 d.grp_idnt,
 d.grp_desc,
 d.dept_idnt,
 d.dept_desc,
   TRIM(FORMAT('%11d', d.div_idnt)) || ': ' || d.div_desc AS division,
   TRIM(FORMAT('%11d', d.grp_idnt)) || ': ' || d.grp_desc AS subdivision,
   TRIM(FORMAT('%11d', d.dept_idnt)) || ': ' || d.dept_desc AS department,
 d.vendor_name,
 a.status,
 a.npg_ind,
 a.po_type,
 a.purchase_type,
  CASE
  WHEN LOWER(a.order_type) IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER'))
  THEN 'RP'
  ELSE 'NRP'
  END AS rp_ind,
 f.month_idnt AS otb_month_idnt,
 f.month_label AS otb_month_label,
 c.comments,
 c.internal_po_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_fact AS a
 INNER JOIN (SELECT DISTINCT purchase_order_number
  FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_sku_cancels) AS b ON LOWER(a.purchase_order_number) = LOWER(b.purchase_order_number)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS c ON LOWER(a.purchase_order_number) = LOWER(c.purchase_order_number
   )
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS d ON LOWER(a.rms_sku_num) = LOWER(d.sku_idnt) AND LOWER(d.channel_country
    ) = LOWER('US')
 INNER JOIN po_dist AS e ON LOWER(a.purchase_order_number) = LOWER(e.purchase_order_number) AND LOWER(a.rms_sku_num) =
   LOWER(e.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS f ON a.otb_eow_date = f.day_date;





CREATE TEMPORARY TABLE IF NOT EXISTS po_cost
AS
SELECT a.purchase_order_number,
 a.banner,
 a.rms_sku_num,
 a.div_idnt,
 a.div_desc,
 a.grp_idnt,
 a.grp_desc,
 a.dept_idnt,
 a.dept_desc,
 a.division,
 a.subdivision,
 a.department,
 a.vendor_name,
 a.status,
 a.npg_ind,
 a.po_type,
 a.purchase_type,
 a.rp_ind,
 a.otb_month_idnt,
 a.otb_month_label,
 a.comments,
 a.internal_po_ind,
 b.quantity_ordered AS ttl_ordered_qty,
 SUM(b.unit_cost * b.quantity_ordered + b.total_expenses_per_unit * b.quantity_ordered + b.total_duty_per_unit * b.quantity_ordered
    ) AS ttl_ordered_c
FROM po_sku_base AS a
 INNER JOIN (SELECT purchase_order_number,
   rms_sku_num,
   AVG(unit_cost) AS unit_cost,
   AVG(total_expenses_per_unit) AS total_expenses_per_unit,
   AVG(total_duty_per_unit) AS total_duty_per_unit,
   SUM(COALESCE(quantity_ordered, 0)) AS quantity_ordered
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_fact AS po_fct
  GROUP BY purchase_order_number,
   rms_sku_num) AS b ON LOWER(a.purchase_order_number) = LOWER(b.purchase_order_number) AND LOWER(a.rms_sku_num) = LOWER(b
    .rms_sku_num)
GROUP BY a.purchase_order_number,
 a.banner,
 a.rms_sku_num,
 a.div_idnt,
 a.div_desc,
 a.grp_idnt,
 a.grp_desc,
 a.dept_idnt,
 a.dept_desc,
 a.division,
 a.subdivision,
 a.department,
 a.vendor_name,
 a.status,
 a.npg_ind,
 a.po_type,
 a.purchase_type,
 a.rp_ind,
 a.otb_month_idnt,
 a.otb_month_label,
 a.comments,
 a.internal_po_ind,
 ttl_ordered_qty;




TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_cancels_otb_month;





INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_cancels_otb_month
SELECT a.purchase_order_number,
  a.banner,
  a.div_idnt,
  a.div_desc,
  a.grp_idnt,
  a.grp_desc,
  a.dept_idnt,
  a.dept_desc,
  a.division,
  a.subdivision,
  a.department,
  a.vendor_name,
  a.status,
  a.npg_ind,
  a.po_type,
  a.purchase_type,
  a.rp_ind,
  a.otb_month_idnt,
  a.otb_month_label,
  a.comments,
  a.internal_po_ind,
  SUM(a.ttl_ordered_qty) AS ttl_ordered_qty,
  CAST(SUM(a.ttl_ordered_c) AS FLOAT64) AS ttl_ordered_c,
  SUM(b.buyer_cancel_u) AS buyer_cancel_u,
  SUM(b.buyer_cancel_c) AS buyer_cancel_c,
  SUM(b.supplier_cancel_u) AS supplier_cancel_u,
  SUM(b.supplier_cancel_c) AS supplier_cancel_c,
  SUM(b.other_cancel_u) AS other_cancel_u,
  SUM(b.other_cancel_c) AS other_cancel_c,
  SUM(b.invalid_cancel_u) AS invalid_cancel_u,
  SUM(b.invalid_cancel_c) AS invalid_cancel_c,
  SUM(b.true_cancel_u) AS true_supplier_cancel_u,
  SUM(b.true_cancel_c) AS true_supplier_cancel_c,
  CAST(CURRENT_DATE('PST8PDT') AS STRING) AS process_dt
 FROM po_cost AS a
  LEFT JOIN (SELECT purchase_order_number,
    rms_sku_num,
    SUM(buyer_cancel_u) AS buyer_cancel_u,
    SUM(buyer_cancel_c) AS buyer_cancel_c,
    SUM(supplier_cancel_u) AS supplier_cancel_u,
    SUM(supplier_cancel_c) AS supplier_cancel_c,
    SUM(other_cancel_u) AS other_cancel_u,
    SUM(other_cancel_c) AS other_cancel_c,
    SUM(invalid_cancel_u) AS invalid_cancel_u,
    SUM(invalid_cancel_c) AS invalid_cancel_c,
    SUM(true_cancel_u) AS true_cancel_u,
    SUM(true_cancel_c) AS true_cancel_c
   FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_sku_cancels
   GROUP BY purchase_order_number,
    rms_sku_num) AS b ON LOWER(a.purchase_order_number) = LOWER(b.purchase_order_number) AND LOWER(a.rms_sku_num) =
    LOWER(b.rms_sku_num)
 GROUP BY a.purchase_order_number,
  a.banner,
  a.div_idnt,
  a.div_desc,
  a.grp_idnt,
  a.grp_desc,
  a.dept_idnt,
  a.dept_desc,
  a.division,
  a.subdivision,
  a.department,
  a.vendor_name,
  a.status,
  a.npg_ind,
  a.po_type,
  a.purchase_type,
  a.rp_ind,
  a.otb_month_idnt,
  a.otb_month_label,
  a.comments,
  a.internal_po_ind,
  process_dt
 EXCEPT DISTINCT
 SELECT *
 FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_cancels_otb_month;


TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_cancels_action_month;


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_cancels_action_month
SELECT banner,
  purchase_order_number,
  div_idnt,
  div_desc,
  grp_idnt,
  grp_desc,
  dept_idnt,
  dept_desc,
  division,
  subdivision,
  department,
  vendor_name,
  npg_ind,
  rp_ind,
  po_type,
  purchase_type,
  comments,
  internal_po_ind,
  otb_month_idnt,
  otb_month_label,
  month_idnt,
  action_month_label,
  SUM(supplier_cancel_u) AS supplier_cancel_u,
  SUM(supplier_cancel_c) AS supplier_cancel_c,
  SUM(buyer_cancel_u) AS buyer_cancel_u,
  SUM(buyer_cancel_c) AS buyer_cancel_c,
  SUM(other_cancel_u) AS other_cancel_u,
  SUM(other_cancel_c) AS other_cancel_c,
  SUM(invalid_cancel_u) AS invalid_cancel_u,
  SUM(invalid_cancel_c) AS invalid_cancel_c,
  SUM(true_cancel_u) AS true_supplier_cancel_u,
  SUM(true_cancel_c) AS true_supplier_cancel_c,
  CAST(CURRENT_DATE('PST8PDT') AS STRING) AS process_dt
 FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_sku_cancels AS a
 GROUP BY banner,
  purchase_order_number,
  otb_month_idnt,
  otb_month_label,
  month_idnt,
  action_month_label,
  div_idnt,
  div_desc,
  grp_idnt,
  grp_desc,
  dept_idnt,
  dept_desc,
  division,
  subdivision,
  department,
  vendor_name,
  npg_ind,
  rp_ind,
  po_type,
  purchase_type,
  comments,
  internal_po_ind
 EXCEPT DISTINCT
 SELECT *
 FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_cancels_action_month; 