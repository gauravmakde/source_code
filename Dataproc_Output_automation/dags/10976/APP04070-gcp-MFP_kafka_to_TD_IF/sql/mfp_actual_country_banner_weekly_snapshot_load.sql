


/* SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_actual_country_banner_weekly_snapshot_load;
Task_Name=mfp_actual_country_banner_t1_load_last_fiscal_week_snapshot;'
FOR SESSION VOLATILE;*/

-- ET;

-- clear current snapshot if it exists



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.mfp_cost_actual_banner_country_snapshot_fact AS tgt
WHERE snapshot_week_id = (SELECT week_idnt AS snapshot_week_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
  WHERE day_date < CURRENT_DATE('PST8PDT')
   AND day_num_of_fiscal_week = 7
  QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1);

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.mfp_cost_actual_banner_country_snapshot_fact
(
    SNAPSHOT_WEEK_ID,
    DEPT_NUM,
    WEEK_NUM,
    BANNER_COUNTRY_NUM,
    FULFILL_TYPE_NUM,
    TY_BEGINNING_OF_PERIOD_ACTIVE_COST_AMT,
    TY_BEGINNING_OF_PERIOD_ACTIVE_QTY,
    TY_BEGINNING_OF_PERIOD_INACTIVE_COST_AMT,
    TY_BEGINNING_OF_PERIOD_INACTIVE_QTY,
    TY_ENDING_OF_PERIOD_ACTIVE_COST_AMT,
    TY_ENDING_OF_PERIOD_ACTIVE_QTY,
    TY_ENDING_OF_PERIOD_INACTIVE_COST_AMT,
    TY_ENDING_OF_PERIOD_INACTIVE_QTY,
    TY_TRANSFER_IN_ACTIVE_COST_AMT,
    TY_TRANSFER_IN_ACTIVE_QTY,
    TY_TRANSFER_OUT_ACTIVE_COST_AMT,
    TY_TRANSFER_OUT_ACTIVE_QTY,
    TY_TRANSFER_IN_INACTIVE_COST_AMT,
    TY_TRANSFER_IN_INACTIVE_QTY,
    TY_TRANSFER_OUT_INACTIVE_COST_AMT,
    TY_TRANSFER_OUT_INACTIVE_QTY,
    TY_TRANSFER_IN_OTHER_COST_AMT,
    TY_TRANSFER_IN_OTHER_QTY,
    TY_TRANSFER_OUT_OTHER_COST_AMT,
    TY_TRANSFER_OUT_OTHER_QTY,
    TY_TRANSFER_IN_RACKING_COST_AMT,
    TY_TRANSFER_IN_RACKING_QTY,
    TY_TRANSFER_OUT_RACKING_COST_AMT,
    TY_TRANSFER_OUT_RACKING_QTY,
    TY_MARK_OUT_OF_STOCK_ACTIVE_COST_AMT,
    TY_MARK_OUT_OF_STOCK_ACTIVE_QTY,
    TY_MARK_OUT_OF_STOCK_INACTIVE_COST_AMT,
    TY_MARK_OUT_OF_STOCK_INACTIVE_QTY,
    TY_RECEIPTS_ACTIVE_COST_AMT,
    TY_RECEIPTS_ACTIVE_QTY,
    TY_RECEIPTS_INACTIVE_COST_AMT,
    TY_RECEIPTS_INACTIVE_QTY,
    TY_RP_RECEIPTS_ACTIVE_COST_AMT,
    TY_NON_RP_RECEIPTS_ACTIVE_COST_AMT,
    TY_RECLASS_IN_COST_AMT,
    TY_RECLASS_IN_QTY,
    TY_RECLASS_OUT_COST_AMT,
    TY_RECLASS_OUT_QTY,
    TY_RETURN_TO_VENDOR_ACTIVE_COST_AMT,
    TY_RETURN_TO_VENDOR_ACTIVE_QTY,
    TY_RETURN_TO_VENDOR_INACTIVE_COST_AMT,
    TY_RETURN_TO_VENDOR_INACTIVE_QTY,
    TY_VENDOR_FUNDS_COST_AMT,
    TY_DISCOUNT_TERMS_COST_AMT,
    TY_MERCH_MARGIN_RETAIL_AMT,
    TY_SHRINK_BANNER_COST_AMT,
    TY_SHRINK_BANNER_QTY,
    TY_OTHER_INVENTORY_ADJ_COST,
    TY_MERCH_MARGIN_CHARGES_COST,
    DW_BATCH_DATE,
    DW_SYS_LOAD_TMSTP
)
SELECT dcd.snapshot_week_id,
 fct.dept_num,
 fct.week_num,
 fct.banner_country_num,
 fct.fulfill_type_num,
 fct.ty_beginning_of_period_active_cost_amt,
 fct.ty_beginning_of_period_active_qty,
 fct.ty_beginning_of_period_inactive_cost_amt,
 fct.ty_beginning_of_period_inactive_qty,
 fct.ty_ending_of_period_active_cost_amt,
 fct.ty_ending_of_period_active_qty,
 fct.ty_ending_of_period_inactive_cost_amt,
 fct.ty_ending_of_period_inactive_qty,
 fct.ty_transfer_in_active_cost_amt,
 fct.ty_transfer_in_active_qty,
 fct.ty_transfer_out_active_cost_amt,
 fct.ty_transfer_out_active_qty,
 fct.ty_transfer_in_inactive_cost_amt,
 fct.ty_transfer_in_inactive_qty,
 fct.ty_transfer_out_inactive_cost_amt,
 fct.ty_transfer_out_inactive_qty,
 fct.ty_transfer_in_other_cost_amt,
 fct.ty_transfer_in_other_qty,
 fct.ty_transfer_out_other_cost_amt,
 fct.ty_transfer_out_other_qty,
 fct.ty_transfer_in_racking_cost_amt,
 fct.ty_transfer_in_racking_qty,
 fct.ty_transfer_out_racking_cost_amt,
 fct.ty_transfer_out_racking_qty,
 fct.ty_mark_out_of_stock_active_cost_amt,
 fct.ty_mark_out_of_stock_active_qty,
 fct.ty_mark_out_of_stock_inactive_cost_amt,
 fct.ty_mark_out_of_stock_inactive_qty,
 fct.ty_receipts_active_cost_amt,
 fct.ty_receipts_active_qty,
 fct.ty_receipts_inactive_cost_amt,
 fct.ty_receipts_inactive_qty,
 fct.ty_rp_receipts_active_cost_amt,
 fct.ty_non_rp_receipts_active_cost_amt,
 fct.ty_reclass_in_cost_amt,
 fct.ty_reclass_in_qty,
 fct.ty_reclass_out_cost_amt,
 fct.ty_reclass_out_qty,
 fct.ty_return_to_vendor_active_cost_amt,
 fct.ty_return_to_vendor_active_qty,
 fct.ty_return_to_vendor_inactive_cost_amt,
 fct.ty_return_to_vendor_inactive_qty,
 fct.ty_vendor_funds_cost_amt,
 fct.ty_discount_terms_cost_amt,
 fct.ty_merch_margin_retail_amt,
 fct.ty_shrink_banner_cost_amt,
 fct.ty_shrink_banner_qty,
 fct.ty_other_inventory_adj_cost,
 fct.ty_merch_margin_charges_cost,
 fct.dw_batch_date,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_plan_actual_banner_country_fact AS fct
 INNER JOIN (SELECT week_idnt AS snapshot_week_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
  WHERE day_date < CURRENT_DATE('PST8PDT')
   AND day_num_of_fiscal_week = 7
  QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1) AS dcd ON TRUE
WHERE fct.week_num <= (SELECT last_completed_fiscal_week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
   WHERE LOWER(RTRIM(interface_code)) = LOWER(RTRIM('MFP_BANR_BLEND_WKLY')));


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.mfp_cost_actual_banner_country_snapshot_fact AS sf
WHERE snapshot_week_id <= (SELECT retention_week_end_idnt
  FROM (SELECT month_start_day_date,
     month_end_day_date,
     month_end_week_idnt,
     MIN(month_end_week_idnt) OVER (ORDER BY month_idnt ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS
     retention_week_end_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_month_cal_454_vw) AS a
  WHERE CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date);


--COLLECT STATS ON PRD_NAP_FCT.MFP_COST_ACTUAL_BANNER_COUNTRY_SNAPSHOT_FACT