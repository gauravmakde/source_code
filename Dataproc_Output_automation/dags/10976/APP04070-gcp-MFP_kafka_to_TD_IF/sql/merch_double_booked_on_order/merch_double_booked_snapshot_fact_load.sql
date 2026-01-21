
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_double_booked_on_order_load_otb_run;
---Task_Name=merch_double_booked_snapshot_fact_load;'*/

BEGIN TRANSACTION;

BEGIN
SET ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_double_booked_invalid_cancel_on_order_snapshot_fact AS mdbicsf
WHERE snapshot_week_num = (SELECT week_idnt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd
        WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_double_booked_invalid_cancel_on_order_snapshot_fact (snapshot_week_num, purchase_order_num
 , rms_sku_num, parent_po_channel, supp_num, supp_name, dept_num, dept_desc, division_num, division_desc,
 subdivision_num, subdivision_desc, banner_num, otb_eow_date, otb_month, last_cancel_month_idnt, last_cancel_month_label
 , parent_po_week_num, parent_po_num, xref_po_ind, parent_po_ind, parent_inbound_on_order_units,
 xref_inbound_on_order_units, parent_outbound_receipt_units, xref_outbound_receipt_units, parent_po_xref_receipts_units
 , parent_po_xref_receipts_retail_amt, parent_po_xref_receipts_cost_amt, parent_po_xref_open_oo_units,
 parent_po_xref_open_oo_retail_amt, parent_po_xref_open_oo_cost_amt, parent_invalid_cancel_units,
 parent_invalid_buyer_cancel_units, parent_invalid_supplier_cancel_units, parent_invalid_auto_cancel_units,
 parent_invalid_other_cancel_units, parent_invalid_cancel_cost_amt, parent_invalid_buyer_cancel_cost_amt,
 parent_invalid_supplier_cancel_cost_amt, parent_invalid_auto_cancel_cost_amt, parent_invalid_other_cancel_cost_amt,
 dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT dcd.week_idnt AS snapshot_week_num,
  mdbicf.purchase_order_num,
  mdbicf.rms_sku_num,
  mdbicf.parent_po_channel,
  mdbicf.supp_num,
  mdbicf.supp_name,
  mdbicf.dept_num,
  mdbicf.dept_desc,
  mdbicf.division_num,
  mdbicf.division_desc,
  mdbicf.subdivision_num,
  mdbicf.subdivision_desc,
  mdbicf.banner_num,
  mdbicf.otb_eow_date,
  mdbicf.otb_month,
  mdbicf.last_cancel_month_idnt,
  mdbicf.last_cancel_month_label,
  mdbicf.parent_po_week_num,
  mdbicf.parent_po_num,
  mdbicf.xref_po_ind,
  mdbicf.parent_po_ind,
  mdbicf.parent_inbound_on_order_units,
  mdbicf.xref_inbound_on_order_units,
  mdbicf.parent_outbound_receipt_units,
  mdbicf.xref_outbound_receipt_units,
  mdbicf.parent_po_xref_receipts_units,
  mdbicf.parent_po_xref_receipts_retail_amt,
  mdbicf.parent_po_xref_receipts_cost_amt,
  mdbicf.parent_po_xref_open_oo_units,
  mdbicf.parent_po_xref_open_oo_retail_amt,
  mdbicf.parent_po_xref_open_oo_cost_amt,
  mdbicf.parent_invalid_cancel_units,
  mdbicf.parent_invalid_buyer_cancel_units,
  mdbicf.parent_invalid_supplier_cancel_units,
  mdbicf.parent_invalid_auto_cancel_units,
  mdbicf.parent_invalid_other_cancel_units,
  mdbicf.parent_invalid_cancel_cost_amt,
  mdbicf.parent_invalid_buyer_cancel_cost_amt,
  mdbicf.parent_invalid_supplier_cancel_cost_amt,
  mdbicf.parent_invalid_auto_cancel_cost_amt,
  mdbicf.parent_invalid_other_cancel_cost_amt,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_double_booked_invalid_cancel_on_order_fact AS mdbicf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd ON dcd.day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY));


COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
END;