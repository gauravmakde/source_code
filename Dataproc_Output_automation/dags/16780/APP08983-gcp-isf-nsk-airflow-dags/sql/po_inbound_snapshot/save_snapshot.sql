--LOCK TABLE T2DL_DAS_Phase_Zero.PO_INBOUND_SNAPSHOT_2023 FOR ACCESS



INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_phase_zero.po_inbound_snapshot_2023 (snapshot_date, purchase_order_num, rms_sku_num, store_num,
 country, banner, channel_num, channel_desc, div_num, div_desc, subdiv_num, subdiv_desc, dept_num, dept_desc,
 dropship_ind, rp_flag, npg_flag, inactive_ind, order_status, snap_wk_idnt, wk_idnt, wk_desc, wk_idnt_nb, wk_desc_nb,
 wk_idnt_na, wk_desc_na, wk_idnt_fr, wk_desc_fr, wk_idnt_lr, wk_desc_lr, mth_idnt, mth_desc, mth_idnt_nb, mth_desc_nb,
 mth_idnt_na, mth_desc_na, mth_idnt_fr, mth_desc_fr, mth_idnt_lr, mth_desc_lr, orig_not_before_dt, not_before_dt,
 orig_not_after_dt, not_after_dt, first_received_date, last_received_date, expected_ship_date, otb_eow_dt,
 f_vasn_sku_qty, f_vasn_sku_retail_amt, f_vasn_sku_cost_amt, f_ordered_qty, f_ordered_retail_amt, f_ordered_cost_amt,
 f_received_qty, f_received_retail_amt, f_received_cost_amt, f_canceled_qty, f_canceled_retail_amt, f_canceled_cost_amt
 , open_oo_qty, open_oo_retail_amt, open_oo_cost_amt, shipped_in_transit_qty, shipped_in_transit_retail_amt,
 shipped_in_transit_cost_amt)
(SELECT CURRENT_DATE('PST8PDT'),
  po_inbound_tableau_extract_fact.purchase_order_num,
  po_inbound_tableau_extract_fact.rms_sku_num,
  po_inbound_tableau_extract_fact.store_num,
   CASE
   WHEN LOWER(po_inbound_tableau_extract_fact.banner_country) IN (LOWER('FP CA'), LOWER('OP CA'))
   THEN 'CA'
   WHEN LOWER(po_inbound_tableau_extract_fact.banner_country) IN (LOWER('FP US'), LOWER('OP US'))
   THEN 'US'
   ELSE NULL
   END AS country,
   CASE
   WHEN LOWER(po_inbound_tableau_extract_fact.banner_country) IN (LOWER('FP US'), LOWER('FP CA'))
   THEN 'Nordstrom'
   WHEN LOWER(po_inbound_tableau_extract_fact.banner_country) IN (LOWER('OP US'), LOWER('OP CA'))
   THEN 'Nordstrom Rack'
   ELSE NULL
   END AS banner,
  po_inbound_tableau_extract_fact.channel_num,
  po_inbound_tableau_extract_fact.channel_desc,
  po_inbound_tableau_extract_fact.div_num,
  po_inbound_tableau_extract_fact.div_desc,
  po_inbound_tableau_extract_fact.grp_num,
  po_inbound_tableau_extract_fact.grp_desc,
  po_inbound_tableau_extract_fact.dept_num,
  po_inbound_tableau_extract_fact.dept_desc,
  po_inbound_tableau_extract_fact.dropship_ind,
  po_inbound_tableau_extract_fact.rp_flag,
  po_inbound_tableau_extract_fact.npg_flag,
   CASE
   WHEN LOWER(SUBSTR(CAST(po_inbound_tableau_extract_fact.channel_num AS STRING) || '.', 1, 3)) IN (LOWER('930'), LOWER('220'
      ), LOWER('221'))
   THEN 'Y'
   ELSE 'N'
   END AS inactive_ind,
  po_inbound_tableau_extract_fact.order_status,
  cal.week_idnt AS snap_wk_idnt,
  po_inbound_tableau_extract_fact.wk_idnt,
  po_inbound_tableau_extract_fact.wk_desc,
  po_inbound_tableau_extract_fact.wk_idnt_nb,
  po_inbound_tableau_extract_fact.wk_desc_nb,
  po_inbound_tableau_extract_fact.wk_idnt_na,
  po_inbound_tableau_extract_fact.wk_desc_na,
  po_inbound_tableau_extract_fact.wk_idnt_fr,
  po_inbound_tableau_extract_fact.wk_desc_fr,
  po_inbound_tableau_extract_fact.wk_idnt_lr,
  po_inbound_tableau_extract_fact.wk_desc_lr,
  po_inbound_tableau_extract_fact.mth_idnt,
  po_inbound_tableau_extract_fact.mth_desc,
  po_inbound_tableau_extract_fact.mth_idnt_nb,
  po_inbound_tableau_extract_fact.mth_desc_nb,
  po_inbound_tableau_extract_fact.mth_idnt_na,
  po_inbound_tableau_extract_fact.mth_desc_na,
  po_inbound_tableau_extract_fact.mth_idnt_fr,
  po_inbound_tableau_extract_fact.mth_desc_fr,
  po_inbound_tableau_extract_fact.mth_idnt_lr,
  po_inbound_tableau_extract_fact.mth_desc_lr,
  po_inbound_tableau_extract_fact.orig_not_before_dt,
  po_inbound_tableau_extract_fact.not_before_dt,
  po_inbound_tableau_extract_fact.orig_not_after_dt,
  po_inbound_tableau_extract_fact.not_after_dt,
  po_inbound_tableau_extract_fact.first_received_date,
  po_inbound_tableau_extract_fact.last_received_date,
  po_inbound_tableau_extract_fact.expected_ship_date,
  po_inbound_tableau_extract_fact.otb_eow_dt,
  po_inbound_tableau_extract_fact.f_vasn_sku_qty,
  po_inbound_tableau_extract_fact.f_vasn_sku_retail_amt,
  po_inbound_tableau_extract_fact.f_vasn_sku_cost_amt,
  po_inbound_tableau_extract_fact.f_ordered_qty,
  po_inbound_tableau_extract_fact.f_ordered_retail_amt,
  po_inbound_tableau_extract_fact.f_ordered_cost_amt,
  po_inbound_tableau_extract_fact.f_received_qty,
  po_inbound_tableau_extract_fact.f_received_retail_amt,
  po_inbound_tableau_extract_fact.f_received_cost_amt,
  po_inbound_tableau_extract_fact.f_canceled_qty,
  po_inbound_tableau_extract_fact.f_canceled_retail_amt,
  po_inbound_tableau_extract_fact.f_canceled_cost_amt,
  CAST(CASE
    WHEN po_inbound_tableau_extract_fact.f_ordered_qty > po_inbound_tableau_extract_fact.f_received_qty
    THEN po_inbound_tableau_extract_fact.f_ordered_qty - po_inbound_tableau_extract_fact.f_received_qty
    ELSE 0
    END AS NUMERIC) AS open_oo_qty,
  CAST(CASE
    WHEN po_inbound_tableau_extract_fact.f_ordered_retail_amt > po_inbound_tableau_extract_fact.f_received_retail_amt
    THEN po_inbound_tableau_extract_fact.f_ordered_retail_amt - po_inbound_tableau_extract_fact.f_received_retail_amt
    ELSE 0
    END AS NUMERIC) AS open_oo_retail_amt,
  CAST(CASE
    WHEN po_inbound_tableau_extract_fact.f_ordered_cost_amt > po_inbound_tableau_extract_fact.f_received_cost_amt
    THEN po_inbound_tableau_extract_fact.f_ordered_cost_amt - po_inbound_tableau_extract_fact.f_received_cost_amt
    ELSE 0
    END AS NUMERIC) AS open_oo_cost_amt,
  CAST(CASE
    WHEN po_inbound_tableau_extract_fact.f_vasn_sku_qty > 0
    THEN po_inbound_tableau_extract_fact.f_vasn_sku_qty - po_inbound_tableau_extract_fact.f_received_qty
    ELSE 0
    END AS NUMERIC) AS shipped_in_transit_qty,
  CAST(CASE
    WHEN po_inbound_tableau_extract_fact.f_vasn_sku_retail_amt > 0
    THEN po_inbound_tableau_extract_fact.f_vasn_sku_retail_amt - po_inbound_tableau_extract_fact.f_received_retail_amt
    ELSE 0
    END AS NUMERIC) AS shipped_in_transit_retail_amt,
  CAST(CASE
    WHEN po_inbound_tableau_extract_fact.f_vasn_sku_cost_amt > 0
    THEN po_inbound_tableau_extract_fact.f_vasn_sku_cost_amt - po_inbound_tableau_extract_fact.f_received_cost_amt
    ELSE 0
    END AS NUMERIC) AS shipped_in_transit_cost_amt
 FROM `{{params.gcp_project_id}}`.t2dl_das_phase_zero.po_inbound_tableau_extract_fact
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal ON cal.day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)
 WHERE po_inbound_tableau_extract_fact.otb_eow_dt BETWEEN DATE '2022-10-30' AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 730 DAY
     )));