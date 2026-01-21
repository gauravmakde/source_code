-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=merch_double_booked_on_order_load;
-- Task_Name=merch_on_order_load_stage_fact_load;'
-- FOR SESSION VOLATILE;
-- ET;
------------------------------------------------------------
-- 1. populate MERCH_PO_XREF_ON_ORDER_GTT
---------------------------------------------------------

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_xref_on_order_gtt
(SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_po_sku_on_order_fact
 WHERE LOWER(xref_po_ind) = LOWER('Y'));
------------------------------------------------------------
--2. populate MERCH_PO_PARENT_ON_ORDER_GTT
------------------------------------------------------------
 
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_on_order_gtt
(SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_po_sku_on_order_fact
 WHERE LOWER(parent_po_ind) = LOWER('Y')
  AND (qty_open > 0 AND (LOWER(status) = LOWER('CLOSED') AND end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY) OR
        LOWER(status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) OR LOWER(status) = LOWER('CLOSED') AND otb_eow_date >=
      DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY)));

----------------------------------------------------------------------------------
-- 3. clear MERCH_DOUBLE_BOOKED_INVALID_CANCEL_ON_ORDER_FACT
-- 4. insert xref po data into MERCH_DOUBLE_BOOKED_INVALID_CANCEL_ON_ORDER_FACT
-- 5. insert parent po data into MERCH_DOUBLE_BOOKED_INVALID_CANCEL_ON_ORDER_FACT
-----------------------------------------------------------------------------------

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_double_booked_invalid_cancel_on_order_fact;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_double_booked_invalid_cancel_on_order_fact (purchase_order_num, rms_sku_num,
 parent_po_channel, supp_num, supp_name, dept_num, dept_desc, division_num, division_desc, subdivision_num,
 subdivision_desc, banner_num, otb_eow_date, otb_month, last_cancel_month_idnt, last_cancel_month_label,
 parent_po_week_num, parent_po_num, xref_po_ind, parent_po_ind, parent_inbound_on_order_units,
 xref_inbound_on_order_units, parent_outbound_receipt_units, xref_outbound_receipt_units, parent_po_xref_receipts_units
 , parent_po_xref_receipts_retail_amt, parent_po_xref_receipts_cost_amt, parent_po_xref_open_oo_units,
 parent_po_xref_open_oo_retail_amt, parent_po_xref_open_oo_cost_amt, parent_invalid_cancel_units,
 parent_invalid_buyer_cancel_units, parent_invalid_supplier_cancel_units, parent_invalid_auto_cancel_units,
 parent_invalid_other_cancel_units, parent_invalid_cancel_cost_amt, parent_invalid_buyer_cancel_cost_amt,
 parent_invalid_supplier_cancel_cost_amt, parent_invalid_auto_cancel_cost_amt, parent_invalid_other_cancel_cost_amt,
 dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT xref_sub.purchase_order_number AS purchase_order_num,
  moodbw.rms_sku_num,
  moodbw.channel_num AS parent_po_channel,
  moodbw.supp_num,
  moodbw.supp_name,
  moodbw.dept_num,
  moodbw.dept_desc,
  moodbw.division_num,
  moodbw.division_desc,
  moodbw.subdivision_num,
  moodbw.subdivision_desc,
  moodbw.banner_num,
  moodbw.otb_eow_date,
  moodbw.otb_month,
  moodbw.last_cancel_month_idnt,
  moodbw.last_cancel_month_label,
  xref_sub.week_num AS parent_po_week_num,
  xref_sub.parent_po_num,
  xref_sub.xref_po_ind,
  xref_sub.parent_po_ind,
  SUM(xref_sub.total_parent_on_order_units) AS parent_inbound_on_order_units,
  SUM(xref_sub.xref_on_order_units) AS xref_inbound_on_order_units,
  SUM(xref_sub.total_parent_received_units) AS parent_outbound_receipt_units,
  SUM(xref_sub.xref_received_units) AS xref_outbound_receipt_units,
  SUM(xref_sub.parent_po_xref_receipts_units) AS parent_po_xref_receipts_units,
  CAST(SUM(xref_sub.parent_po_xref_receipts_retail_amt) AS NUMERIC) AS parent_po_xref_receipts_retail_amt,
  SUM(xref_sub.parent_po_xref_receipts_cost_amt) AS parent_po_xref_receipts_cost_amt,
  SUM(xref_sub.parent_po_xref_open_oo_units) AS parent_po_xref_open_oo_units,
  CAST(SUM(xref_sub.parent_po_xref_open_oo_retail_amt) AS NUMERIC) AS parent_po_xref_open_oo_retail_amt,
  SUM(xref_sub.parent_po_xref_open_oo_cost_amt) AS parent_po_xref_open_oo_cost_amt,
  0 AS parent_invalid_cancel_units,
  0 AS parent_invalid_buyer_cancel_units,
  0 AS parent_invalid_supplier_cancel_units,
  0 AS parent_invalid_auto_cancel_units,
  0 AS parent_invalid_other_cancel_units,
  CAST(0.0000 AS NUMERIC) AS parent_invalid_cancel_cost_amt,
  CAST(0.0000 AS NUMERIC) AS parent_invalid_buyer_cancel_cost_amt,
  CAST(0.0000 AS NUMERIC) AS parent_invalid_supplier_cancel_cost_amt,
  CAST(0.0000 AS NUMERIC) AS parent_invalid_auto_cancel_cost_amt,
  CAST(0.0000 AS NUMERIC) AS parent_invalid_other_cancel_cost_amt,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz

 FROM (SELECT x.purchase_order_number,
    x.cancel_reason,
    x.xref_po_ind,
    x.parent_po_ind,
    x.parent_po_num,
    x.rms_sku_num,
    x.dept_num,
    p.week_num,
    x.qty_received AS xref_received_units,
    p.total_qty_received AS total_parent_received_units,
    x.total_qty_received AS total_xref_received_units,
    x.qty_open AS xref_on_order_units,
    p.total_qty_open AS total_parent_on_order_units,
    x.total_qty_open AS total_xref_on_order_units,
     CASE
     WHEN p.total_qty_open > x.qty_received
     THEN x.qty_received
     WHEN p.total_qty_open <= x.qty_received
     THEN p.total_qty_open
     ELSE 0
     END AS parent_po_xref_receipts_units,
    GREATEST(x.unit_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt) AS greater_elc,
      CASE
      WHEN p.total_qty_open > x.qty_received
      THEN x.qty_received
      WHEN p.total_qty_open <= x.qty_received
      THEN p.total_qty_open
      ELSE 0
      END * GREATEST(x.unit_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt) AS
    parent_po_xref_receipts_cost_amt,
     CASE
     WHEN p.total_qty_open > x.qty_received
     THEN x.qty_received * x.unit_anticipated_retail_amt
     WHEN p.total_qty_open <= x.qty_received
     THEN p.total_qty_open * p.unit_anticipated_retail_amt
     ELSE 0
     END AS parent_po_xref_receipts_retail_amt,
     p.total_qty_open - x.qty_received AS potential_parent_po_xref_onorder_units,
     CASE
     WHEN p.total_qty_open - x.qty_received > 0 AND x.qty_open > 0
     THEN CASE
      WHEN p.total_qty_open - x.qty_received <= x.qty_open
      THEN p.total_qty_open - x.qty_received
      ELSE x.qty_open
      END
     ELSE 0
     END AS parent_po_xref_open_oo_units,
      CASE
      WHEN p.total_qty_open - x.qty_received > 0 AND x.qty_open > 0
      THEN CASE
       WHEN p.total_qty_open - x.qty_received <= x.qty_open
       THEN p.total_qty_open - x.qty_received
       ELSE x.qty_open
       END
      ELSE 0
      END * GREATEST(x.unit_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt) AS
    parent_po_xref_open_oo_cost_amt,
     CASE
     WHEN p.total_qty_open - x.qty_received > 0 AND x.qty_open > 0
     THEN CASE
      WHEN p.total_qty_open - x.qty_received <= x.qty_open
      THEN (p.total_qty_open - x.qty_received) * p.unit_anticipated_retail_amt
      ELSE x.qty_open * x.unit_anticipated_retail_amt
      END
     ELSE 0
     END AS parent_po_xref_open_oo_retail_amt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_xref_on_order_gtt AS x
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_on_order_gtt AS p ON LOWER(x.parent_po_num) = LOWER(p.purchase_order_number)
     AND LOWER(x.rms_sku_num) = LOWER(p.rms_sku_num)
   WHERE p.qty_open > 0
    AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY) OR LOWER(p.status
        ) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))) AS xref_sub
  INNER JOIN (SELECT DISTINCT mdbwrk.purchase_order_number,
    mdbwrk.rms_sku_num,
    mdbwrk.supp_num,
    mdbwrk.dept_num,
    dd.dept_name AS dept_desc,
    vd.vendor_name AS supp_name,
    mdbwrk.channel_num,
    dd.division_num,
    dd.division_name AS division_desc,
    dd.subdivision_num,
    dd.subdivision_name AS subdivision_desc,
    bnr.banner_id AS banner_num,
    mdbwrk.otb_eow_date,
    cal.fiscal_month_num AS otb_month,
    ivcmnt.last_cancel_month_idnt,
    ivcmnt.last_cancel_month_label
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_double_booked_wrk AS mdbwrk
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_dim AS vd ON LOWER(vd.vendor_num) = LOWER(mdbwrk.supp_num)
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim AS dd ON mdbwrk.dept_num = dd.dept_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS bnr ON mdbwrk.channel_num = bnr.channel_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal ON mdbwrk.otb_eow_date = cal.day_date
    INNER JOIN (SELECT posf.purchase_order_number,
      COALESCE(pack.rms_sku_num, posf.rms_sku_num) AS rms_sku_num,
      MAX(CAST(posf.latest_item_canceled_tmstp_pacific AS DATE)) AS last_cancel_date,
      MAX(cal0.month_idnt) AS last_cancel_month_idnt,
      MAX(cal0.month_label) AS last_cancel_month_label
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_shiplocation_fact AS posf
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS sd ON CAST(posf.ship_location_id AS FLOAT64) = sd.store_num
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal0 ON COALESCE(CAST(posf.latest_item_canceled_tmstp_pacific AS DATE)
        , DATE '4444-04-04') = cal0.day_date
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.casepack_sku_xref AS pack ON posf.epm_sku_num = pack.epm_casepack_num AND LOWER(sd.store_country_code
         ) = LOWER(pack.channel_country)
     GROUP BY posf.purchase_order_number,
      rms_sku_num) AS ivcmnt ON LOWER(ivcmnt.purchase_order_number) = LOWER(mdbwrk.purchase_order_number) AND LOWER(ivcmnt
       .rms_sku_num) = LOWER(mdbwrk.rms_sku_num)
   WHERE cast(mdbwrk.channel_num as string) IN {{params.planningchannelcode_all}}
    AND mdbwrk.first_approval_date IS NOT NULL) AS moodbw ON LOWER(xref_sub.parent_po_num) = LOWER(moodbw.purchase_order_number
     ) AND LOWER(xref_sub.rms_sku_num) = LOWER(moodbw.rms_sku_num)
 GROUP BY purchase_order_num,
  moodbw.rms_sku_num,
  parent_po_channel,
  moodbw.supp_num,
  moodbw.supp_name,
  moodbw.dept_num,
  moodbw.dept_desc,
  moodbw.division_num,
  moodbw.division_desc,
  moodbw.subdivision_num,
  moodbw.subdivision_desc,
  moodbw.banner_num,
  moodbw.otb_eow_date,
  moodbw.otb_month,
  moodbw.last_cancel_month_idnt,
  moodbw.last_cancel_month_label,
  parent_po_week_num,
  xref_sub.parent_po_num,
  xref_sub.xref_po_ind,
  xref_sub.parent_po_ind,
  dw_batch_date,
  dw_sys_load_tmstp);
  
  
  INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_double_booked_invalid_cancel_on_order_fact (purchase_order_num, rms_sku_num,
 parent_po_channel, supp_num, supp_name, dept_num, dept_desc, division_num, division_desc, subdivision_num,
 subdivision_desc, banner_num, otb_eow_date, otb_month, last_cancel_month_idnt, last_cancel_month_label,
 parent_po_week_num, parent_po_num, xref_po_ind, parent_po_ind, parent_inbound_on_order_units,
 xref_inbound_on_order_units, parent_outbound_receipt_units, xref_outbound_receipt_units, parent_po_xref_receipts_units
 , parent_po_xref_receipts_retail_amt, parent_po_xref_receipts_cost_amt, parent_po_xref_open_oo_units,
 parent_po_xref_open_oo_retail_amt, parent_po_xref_open_oo_cost_amt, parent_invalid_cancel_units,
 parent_invalid_buyer_cancel_units, parent_invalid_supplier_cancel_units, parent_invalid_auto_cancel_units,
 parent_invalid_other_cancel_units, parent_invalid_cancel_cost_amt, parent_invalid_buyer_cancel_cost_amt,
 parent_invalid_supplier_cancel_cost_amt, parent_invalid_auto_cancel_cost_amt, parent_invalid_other_cancel_cost_amt,
 dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT parent_sub.purchase_order_number AS purchase_order_num,
  moodbw.rms_sku_num,
  moodbw.channel_num AS parent_po_channel,
  moodbw.supp_num,
  moodbw.supp_name,
  moodbw.dept_num,
  moodbw.dept_desc,
  moodbw.division_num,
  moodbw.division_desc,
  moodbw.subdivision_num,
  moodbw.subdivision_desc,
  moodbw.banner_num,
  moodbw.otb_eow_date,
  moodbw.otb_month,
  moodbw.last_cancel_month_idnt AS month_idnt,
  moodbw.last_cancel_month_label AS month_label,
  parent_sub.week_num AS parent_po_week_num,
  parent_sub.parent_po_num,
  parent_sub.xref_po_ind,
  parent_sub.parent_po_ind,
  SUM(parent_sub.total_parent_on_order_units) AS parent_inbound_on_order_units,
  SUM(parent_sub.xref_on_order_units) AS xref_inbound_on_order_units,
  SUM(parent_sub.total_parent_received_units) AS parent_outbound_receipt_units,
  SUM(parent_sub.xref_received_units) AS xref_outbound_receipt_units,
  SUM(parent_sub.parent_po_xref_receipts_units) AS parent_po_xref_receipts_units,
  CAST(SUM(parent_sub.parent_po_xref_receipts_retail_amt) AS NUMERIC) AS parent_po_xref_receipts_retail_amt,
  CAST(SUM(parent_sub.parent_po_xref_receipts_cost_amt) AS NUMERIC) AS parent_po_xref_receipts_cost_amt,
  SUM(parent_sub.parent_po_xref_open_oo_units) AS parent_po_xref_open_oo_units,
  CAST(SUM(parent_sub.parent_po_xref_open_oo_retail_amt) AS NUMERIC) AS parent_po_xref_open_oo_retail_amt,
  CAST(SUM(parent_sub.parent_po_xref_open_oo_cost_amt) AS NUMERIC) AS parent_po_xref_open_oo_cost_amt,
  SUM(parent_sub.parent_invalid_cancel_units) AS parent_invalid_cancel_units,
  SUM(parent_sub.parent_invalid_buyer_cancel_units) AS parent_invalid_buyer_cancel_units,
  SUM(parent_sub.parent_invalid_supplier_cancel_units) AS parent_invalid_supplier_cancel_units,
  SUM(parent_sub.parent_invalid_auto_cancel_units) AS parent_invalid_auto_cancel_units,
  SUM(parent_sub.parent_invalid_other_cancel_units) AS parent_invalid_other_cancel_units,
  CAST(SUM(parent_sub.parent_invalid_cancel_cost_amt) AS NUMERIC) AS parent_invalid_cancel_cost_amt,
  CAST(SUM(parent_sub.parent_invalid_buyer_cancel_cost_amt) AS NUMERIC) AS parent_invalid_buyer_cancel_cost_amt,
  CAST(SUM(parent_sub.parent_invalid_supplier_cancel_cost_amt) AS NUMERIC) AS parent_invalid_supplier_cancel_cost_amt,
  CAST(SUM(parent_sub.parent_invalid_auto_cancel_cost_amt) AS NUMERIC) AS parent_invalid_auto_cancel_cost_amt,
  CAST(SUM(parent_sub.parent_invalid_other_cancel_cost_amt) AS NUMERIC) AS parent_invalid_other_cancel_cost_amt,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS  dw_sys_load_tmstp_tz

 FROM (SELECT p.purchase_order_number,
    p.xref_po_ind,
    p.parent_po_ind,
    p.parent_po_num,
    p.rms_sku_num,
    p.dept_num,
    p.week_num,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
           45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
     THEN x.total_qty_open
     ELSE 0
     END AS xref_on_order_units,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
           45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
     THEN x.total_qty_received
     ELSE 0
     END AS xref_received_units,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
           45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
     THEN p.total_qty_open
     ELSE 0
     END AS total_parent_on_order_units,
    x.total_qty_open AS total_xref_on_order_units,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
           45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
     THEN p.total_qty_received
     ELSE 0
     END AS total_parent_received_units,
    x.total_qty_received AS total_xref_received_units,
    x.qty_canceled AS xref_canceled_units,
    x.total_qty_canceled AS total_xref_canceled_units,
    p.total_qty_canceled AS total_parent_canceled_units,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
            45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END > x.total_qty_received
     THEN x.total_qty_received
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
            45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <= x.total_qty_received
     THEN CASE
      WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
            45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
      THEN p.total_qty_open
      ELSE 0
      END
     ELSE 0
     END AS parent_po_xref_receipts_units,
     CASE
     WHEN CASE
       WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
       THEN p.total_qty_open
       ELSE 0
       END = x.total_qty_received
     THEN p.unit_estimated_landing_cost_amt
     WHEN CASE
       WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
       THEN p.total_qty_open
       ELSE 0
       END <> x.total_qty_received
     THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
     ELSE 0
     END AS greater_elc,
      CASE
      WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
         WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
               INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
         THEN p.total_qty_open
         ELSE 0
         END > x.total_qty_received
      THEN x.total_qty_received
      WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
         WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
               INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
         THEN p.total_qty_open
         ELSE 0
         END <= x.total_qty_received
      THEN CASE
       WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
       THEN p.total_qty_open
       ELSE 0
       END
      ELSE 0
      END * CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END = x.total_qty_received
      THEN p.unit_estimated_landing_cost_amt
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <> x.total_qty_received
      THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
      ELSE 0
      END AS parent_po_xref_receipts_cost_amt,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
            45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END > x.total_qty_received
     THEN x.total_qty_received * x.unit_anticipated_retail_amt
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
            45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <= x.total_qty_received
     THEN CASE
       WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
       THEN p.total_qty_open
       ELSE 0
       END * p.unit_anticipated_retail_amt
     ELSE 0
     END AS parent_po_xref_receipts_retail_amt,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
           45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
     THEN CASE
       WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
       THEN p.total_qty_open
       ELSE 0
       END - x.total_qty_received
     ELSE 0
     END AS potential_parent_po_xref_onorder_units,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
         WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
               INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
         THEN CASE
           WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                 INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
           THEN p.total_qty_open
           ELSE 0
           END - x.total_qty_received
         ELSE 0
         END > 0 AND x.total_qty_open > 0
     THEN CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN CASE
          WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
          THEN p.total_qty_open
          ELSE 0
          END - x.total_qty_received
        ELSE 0
        END <= x.total_qty_open
      THEN CASE
       WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
       THEN CASE
         WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
               INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
         THEN p.total_qty_open
         ELSE 0
         END - x.total_qty_received
       ELSE 0
       END
      ELSE x.total_qty_open
      END
     ELSE 0
     END AS parent_po_xref_open_oo_units,
      CASE
      WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
              45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
          WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
          THEN CASE
            WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                  INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
            THEN p.total_qty_open
            ELSE 0
            END - x.total_qty_received
          ELSE 0
          END > 0 AND x.total_qty_open > 0
      THEN CASE
       WHEN CASE
         WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
               INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
         THEN CASE
           WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                 INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
           THEN p.total_qty_open
           ELSE 0
           END - x.total_qty_received
         ELSE 0
         END <= x.total_qty_open
       THEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN CASE
          WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
          THEN p.total_qty_open
          ELSE 0
          END - x.total_qty_received
        ELSE 0
        END
       ELSE x.total_qty_open
       END
      ELSE 0
      END * CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END = x.total_qty_received
      THEN p.unit_estimated_landing_cost_amt
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <> x.total_qty_received
      THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
      ELSE 0
      END AS parent_po_xref_open_oo_cost_amt,
     CASE
     WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL
             45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))) AND CASE
         WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
               INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
         THEN CASE
           WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                 INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
           THEN p.total_qty_open
           ELSE 0
           END - x.total_qty_received
         ELSE 0
         END > 0 AND x.total_qty_open > 0
     THEN CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN CASE
          WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
          THEN p.total_qty_open
          ELSE 0
          END - x.total_qty_received
        ELSE 0
        END <= x.total_qty_open
      THEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN CASE
          WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
                INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
          THEN p.total_qty_open
          ELSE 0
          END - x.total_qty_received
        ELSE 0
        END * p.unit_anticipated_retail_amt
      ELSE x.total_qty_open * x.unit_anticipated_retail_amt
      END
     ELSE 0
     END AS parent_po_xref_open_oo_retail_amt,
     CASE
     WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
          ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled > x
       .total_qty_received
     THEN x.total_qty_received
     WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
          ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled <=
       x.total_qty_received
     THEN p.total_qty_canceled
     ELSE 0
     END AS parent_invalid_cancel_units,
     CASE
     WHEN LOWER(p.cancel_reason) = LOWER('BUYER_CANCELED')
     THEN CASE
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
        x.total_qty_received
      THEN x.total_qty_received
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled <=
        x.total_qty_received
      THEN p.total_qty_canceled
      ELSE 0
      END
     ELSE 0
     END AS parent_invalid_buyer_cancel_units,
     CASE
     WHEN LOWER(p.cancel_reason) = LOWER('VENDOR_CANCELED')
     THEN CASE
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
        x.total_qty_received
      THEN x.total_qty_received
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled <=
        x.total_qty_received
      THEN p.total_qty_canceled
      ELSE 0
      END
     ELSE 0
     END AS parent_invalid_supplier_cancel_units,
     CASE
     WHEN LOWER(p.cancel_reason) = LOWER('AUTOMATIC')
     THEN CASE
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
        x.total_qty_received
      THEN x.total_qty_received
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled <=
        x.total_qty_received
      THEN p.total_qty_canceled
      ELSE 0
      END
     ELSE 0
     END AS parent_invalid_auto_cancel_units,
     CASE
     WHEN LOWER(p.cancel_reason) = LOWER('NOT_SPECIFIED')
     THEN CASE
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
        x.total_qty_received
      THEN x.total_qty_received
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled <=
        x.total_qty_received
      THEN p.total_qty_canceled
      ELSE 0
      END
     ELSE 0
     END AS parent_invalid_other_cancel_units,
      CASE
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
        x.total_qty_received
      THEN x.total_qty_received
      WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
           ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled <=
        x.total_qty_received
      THEN p.total_qty_canceled
      ELSE 0
      END * CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END = x.total_qty_received
      THEN p.unit_estimated_landing_cost_amt
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <> x.total_qty_received
      THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
      ELSE 0
      END AS parent_invalid_cancel_cost_amt,
      CASE
      WHEN LOWER(p.cancel_reason) = LOWER('BUYER_CANCELED')
      THEN CASE
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
         x.total_qty_received
       THEN x.total_qty_received
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled
         <= x.total_qty_received
       THEN p.total_qty_canceled
       ELSE 0
       END
      ELSE 0
      END * CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END = x.total_qty_received
      THEN p.unit_estimated_landing_cost_amt
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <> x.total_qty_received
      THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
      ELSE 0
      END AS parent_invalid_buyer_cancel_cost_amt,
      CASE
      WHEN LOWER(p.cancel_reason) = LOWER('VENDOR_CANCELED')
      THEN CASE
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
         x.total_qty_received
       THEN x.total_qty_received
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled
         <= x.total_qty_received
       THEN p.total_qty_canceled
       ELSE 0
       END
      ELSE 0
      END * CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END = x.total_qty_received
      THEN p.unit_estimated_landing_cost_amt
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <> x.total_qty_received
      THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
      ELSE 0
      END AS parent_invalid_supplier_cancel_cost_amt,
      CASE
      WHEN LOWER(p.cancel_reason) = LOWER('AUTOMATIC')
      THEN CASE
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
         x.total_qty_received
       THEN x.total_qty_received
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled
         <= x.total_qty_received
       THEN p.total_qty_canceled
       ELSE 0
       END
      ELSE 0
      END * CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END = x.total_qty_received
      THEN p.unit_estimated_landing_cost_amt
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <> x.total_qty_received
      THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
      ELSE 0
      END AS parent_invalid_auto_cancel_cost_amt,
      CASE
      WHEN LOWER(p.cancel_reason) = LOWER('NOT_SPECIFIED')
      THEN CASE
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled >
         x.total_qty_received
       THEN x.total_qty_received
       WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
            ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled
         <= x.total_qty_received
       THEN p.total_qty_canceled
       ELSE 0
       END
      ELSE 0
      END * CASE
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END = x.total_qty_received
      THEN p.unit_estimated_landing_cost_amt
      WHEN CASE
        WHEN p.qty_open > 0 AND (LOWER(p.status) = LOWER('CLOSED') AND p.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'),
              INTERVAL 45 DAY) OR LOWER(p.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
        THEN p.total_qty_open
        ELSE 0
        END <> x.total_qty_received
      THEN GREATEST(x.max_estimated_landing_cost_amt, p.unit_estimated_landing_cost_amt)
      ELSE 0
      END AS parent_invalid_other_cancel_cost_amt,
    ROW_NUMBER() OVER (PARTITION BY p.purchase_order_number, p.rms_sku_num, p.xref_po_ind, p.parent_po_ind, p.parent_po_num
       ORDER BY p.purchase_order_number, p.rms_sku_num, p.xref_po_ind, p.parent_po_ind, p.parent_po_num, CASE
        WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
             ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled
          > x.total_qty_received
        THEN x.total_qty_received
        WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND p.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT')
             ,INTERVAL 395 DAY) AND x.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY) AND p.total_qty_canceled
          <= x.total_qty_received
        THEN p.total_qty_canceled
        ELSE 0
        END DESC) AS r
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_on_order_gtt AS p
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_xref_on_order_gtt AS x ON LOWER(x.parent_po_num) = LOWER(p.purchase_order_number)
     AND LOWER(x.rms_sku_num) = LOWER(p.rms_sku_num)) AS parent_sub
  INNER JOIN (SELECT DISTINCT mdbwrk.purchase_order_number,
    mdbwrk.rms_sku_num,
    mdbwrk.supp_num,
    mdbwrk.dept_num,
    dd.dept_name AS dept_desc,
    vd.vendor_name AS supp_name,
    mdbwrk.channel_num,
    dd.division_num,
    dd.division_name AS division_desc,
    dd.subdivision_num,
    dd.subdivision_name AS subdivision_desc,
    bnr.banner_id AS banner_num,
    mdbwrk.otb_eow_date,
    cal.fiscal_month_num AS otb_month,
    ivcmnt.last_cancel_month_idnt,
    ivcmnt.last_cancel_month_label
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_double_booked_wrk AS mdbwrk
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_dim AS vd ON LOWER(vd.vendor_num) = LOWER(mdbwrk.supp_num)
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim AS dd ON mdbwrk.dept_num = dd.dept_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS bnr ON mdbwrk.channel_num = bnr.channel_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal ON mdbwrk.otb_eow_date = cal.day_date
    INNER JOIN (SELECT posf.purchase_order_number,
      COALESCE(pack.rms_sku_num, posf.rms_sku_num) AS rms_sku_num,
      MAX(CAST(posf.latest_item_canceled_tmstp_pacific AS DATE)) AS last_cancel_date,
      MAX(cal0.month_idnt) AS last_cancel_month_idnt,
      MAX(cal0.month_label) AS last_cancel_month_label
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_shiplocation_fact AS posf
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS sd ON CAST(posf.ship_location_id AS FLOAT64) = sd.store_num
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal0 ON COALESCE(CAST(posf.latest_item_canceled_tmstp_pacific AS DATE)
        , DATE '4444-04-04') = cal0.day_date
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.casepack_sku_xref AS pack ON posf.epm_sku_num = pack.epm_casepack_num AND LOWER(sd.store_country_code
         ) = LOWER(pack.channel_country)
     GROUP BY posf.purchase_order_number,
      rms_sku_num) AS ivcmnt ON LOWER(ivcmnt.purchase_order_number) = LOWER(mdbwrk.purchase_order_number) AND LOWER(ivcmnt
       .rms_sku_num) = LOWER(mdbwrk.rms_sku_num)
   WHERE cast(mdbwrk.channel_num as string) IN {{params.planningchannelcode_all}}
    AND mdbwrk.first_approval_date IS NOT NULL) AS moodbw ON LOWER(parent_sub.purchase_order_number) = LOWER(moodbw.purchase_order_number
     ) AND LOWER(parent_sub.rms_sku_num) = LOWER(moodbw.rms_sku_num)
 WHERE parent_sub.r = 1
 GROUP BY purchase_order_num,
  moodbw.rms_sku_num,
  parent_po_channel,
  moodbw.supp_num,
  moodbw.supp_name,
  moodbw.dept_num,
  moodbw.dept_desc,
  moodbw.division_num,
  moodbw.division_desc,
  moodbw.subdivision_num,
  moodbw.subdivision_desc,
  moodbw.banner_num,
  moodbw.otb_eow_date,
  moodbw.otb_month,
  month_idnt,
  month_label,
  parent_po_week_num,
  parent_sub.parent_po_num,
  parent_sub.xref_po_ind,
  parent_sub.parent_po_ind,
  dw_batch_date,
  dw_sys_load_tmstp);

--   ET;
-- SET QUERY_BAND = NONE FOR SESSION;
-- ET;