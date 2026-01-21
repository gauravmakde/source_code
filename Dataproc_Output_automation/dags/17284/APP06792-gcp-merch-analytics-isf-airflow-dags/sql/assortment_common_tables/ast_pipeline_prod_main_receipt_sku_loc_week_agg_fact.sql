CREATE TEMPORARY TABLE IF NOT EXISTS dates
AS
SELECT week_idnt,
 week_start_day_date,
 week_end_day_date,
 MIN(week_start_day_date) OVER (ORDER BY week_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
 window_start_date,
 MAX(week_end_day_date) OVER (ORDER BY week_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
 window_end_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE day_date BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE))
GROUP BY week_idnt,
 week_start_day_date,
 week_end_day_date;



DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.receipt_sku_loc_week_agg_fact{{params.env_suffix}}
WHERE week_start_day_date >= (SELECT DISTINCT window_start_date
   FROM dates) AND week_end_day_date <= (SELECT DISTINCT window_end_date
   FROM dates);

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.receipt_sku_loc_week_agg_fact{{params.env_suffix}}
(SELECT sku_idnt,
  week_num,
  week_start_day_date,
  week_end_day_date,
  month_num AS mnth_idnt,
  channel_num,
  store_num,
  ds_ind,
  rp_ind,
  npg_ind,
  ss_ind,
  gwp_ind,
   CASE
   WHEN SUM(receipts_clearance_units) + SUM(receipts_crossdock_clearance_units) > 0
   THEN 'C'
   ELSE 'R'
   END AS price_type,
  SUM(receipts_regular_units + receipts_clearance_units + receipts_crossdock_regular_units +
    receipts_crossdock_clearance_units) AS receipt_tot_units,
  CAST(SUM(receipts_regular_retail + receipts_clearance_retail + receipts_crossdock_regular_retail + receipts_crossdock_clearance_retail) AS NUMERIC)
  AS receipt_tot_retail,
  CAST(SUM(receipts_regular_cost + receipts_clearance_cost + receipts_crossdock_regular_cost + receipts_crossdock_clearance_cost) AS NUMERIC)
  AS receipt_tot_cost,
  SUM(CASE
    WHEN LOWER(ds_ind) = LOWER('N')
    THEN receipts_regular_units + receipts_clearance_units + receipts_crossdock_regular_units +
     receipts_crossdock_clearance_units
    ELSE 0
    END) AS receipt_po_units,
  CAST(SUM(CASE
     WHEN LOWER(ds_ind) = LOWER('N')
     THEN receipts_regular_retail + receipts_clearance_retail + receipts_crossdock_regular_retail + receipts_crossdock_clearance_retail
     ELSE 0
     END) AS NUMERIC) AS receipt_po_retail,
  CAST(SUM(CASE
     WHEN LOWER(ds_ind) = LOWER('N')
     THEN receipts_regular_cost + receipts_clearance_cost + receipts_crossdock_regular_cost + receipts_crossdock_clearance_cost
     ELSE 0
     END) AS NUMERIC) AS receipt_po_cost,
  SUM(CASE
    WHEN LOWER(ds_ind) = LOWER('Y')
    THEN receipts_regular_units + receipts_clearance_units + receipts_crossdock_regular_units +
     receipts_crossdock_clearance_units
    ELSE 0
    END) AS receipt_ds_units,
  CAST(SUM(CASE
     WHEN LOWER(ds_ind) = LOWER('Y')
     THEN receipts_regular_retail + receipts_clearance_retail + receipts_crossdock_regular_retail + receipts_crossdock_clearance_retail
     ELSE 0
     END) AS NUMERIC) AS receipt_ds_retail,
  CAST(SUM(CASE
     WHEN LOWER(ds_ind) = LOWER('Y')
     THEN receipts_regular_cost + receipts_clearance_cost + receipts_crossdock_regular_cost + receipts_crossdock_clearance_cost
     ELSE 0
     END) AS NUMERIC) AS receipt_ds_cost,
  SUM(receipt_rsk_units) AS receipt_rsk_units,
  SUM(receipt_rsk_retail) AS receipt_rsk_retail,
  SUM(receipt_rsk_cost) AS receipt_rsk_cost,
  SUM(receipt_pah_units) AS receipt_pah_units,
  SUM(receipt_pah_retail) AS receipt_pah_retail,
  SUM(receipt_pah_cost) AS receipt_pah_cost,
  SUM(receipt_flx_units) AS receipt_flx_units,
  SUM(receipt_flx_retail) AS receipt_flx_retail,
  SUM(receipt_flx_cost) AS receipt_flx_cost
 FROM (SELECT rcpt.rms_sku_num AS sku_idnt,
     rcpt.week_num,
     dates.week_start_day_date,
     dates.week_end_day_date,
     rcpt.month_num,
     rcpt.channel_num,
     rcpt.store_num,
     rcpt.dropship_ind AS ds_ind,
     rcpt.rp_ind,
     rcpt.npg_ind,
     rcpt.smart_sample_ind AS ss_ind,
     rcpt.gift_with_purchase_ind AS gwp_ind,
     SUM(rcpt.receipts_regular_units) AS receipts_regular_units,
     SUM(rcpt.receipts_regular_retail) AS receipts_regular_retail,
     SUM(rcpt.receipts_regular_cost) AS receipts_regular_cost,
     SUM(rcpt.receipts_clearance_units) AS receipts_clearance_units,
     SUM(rcpt.receipts_clearance_retail) AS receipts_clearance_retail,
     SUM(rcpt.receipts_clearance_cost) AS receipts_clearance_cost,
     SUM(rcpt.receipts_crossdock_regular_units) AS receipts_crossdock_regular_units,
     SUM(rcpt.receipts_crossdock_regular_retail) AS receipts_crossdock_regular_retail,
     SUM(rcpt.receipts_crossdock_regular_cost) AS receipts_crossdock_regular_cost,
     SUM(rcpt.receipts_crossdock_clearance_units) AS receipts_crossdock_clearance_units,
     SUM(rcpt.receipts_crossdock_clearance_retail) AS receipts_crossdock_clearance_retail,
     SUM(rcpt.receipts_crossdock_clearance_cost) AS receipts_crossdock_clearance_cost,
     0 AS receipt_rsk_units,
     0 AS receipt_rsk_retail,
     0 AS receipt_rsk_cost,
     0 AS receipt_pah_units,
     0 AS receipt_pah_retail,
     0 AS receipt_pah_cost,
     0 AS receipt_flx_units,
     0 AS receipt_flx_retail,
     0 AS receipt_flx_cost
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS rcpt
     INNER JOIN dates ON rcpt.week_num = dates.week_idnt
    GROUP BY sku_idnt,
     rcpt.week_num,
     dates.week_start_day_date,
     dates.week_end_day_date,
     rcpt.month_num,
     rcpt.channel_num,
     rcpt.store_num,
     ds_ind,
     rcpt.rp_ind,
     rcpt.npg_ind,
     ss_ind,
     gwp_ind
    UNION ALL
    SELECT rcpt0.rms_sku_num AS sku_idnt,
     rcpt0.week_num,
     dates0.week_start_day_date,
     dates0.week_end_day_date,
     rcpt0.month_num,
     rcpt0.channel_num,
     rcpt0.store_num,
     'N' AS ds_ind,
     rcpt0.rp_ind,
     rcpt0.npg_ind,
     rcpt0.smart_sample_ind AS ss_ind,
     rcpt0.gift_with_purchase_ind AS gwp_ind,
     0 AS receipts_regular_units,
     0 AS receipts_regular_retail,
     0 AS receipts_regular_cost,
     0 AS receipts_clearance_units,
     0 AS receipts_clearance_retail,
     0 AS receipts_clearance_cost,
     0 AS receipts_crossdock_regular_units,
     0 AS receipts_crossdock_regular_retail,
     0 AS receipts_crossdock_regular_cost,
     0 AS receipts_crossdock_clearance_units,
     0 AS receipts_crossdock_clearance_retail,
     0 AS receipts_crossdock_clearance_cost,
     SUM(rcpt0.reservestock_transfer_in_units) AS receipt_rsk_units,
     SUM(rcpt0.reservestock_transfer_in_retail) AS receipt_rsk_retail,
     SUM(rcpt0.reservestock_transfer_in_cost) AS receipt_rsk_cost,
     SUM(rcpt0.packandhold_transfer_in_units) AS receipt_pah_units,
     SUM(rcpt0.packandhold_transfer_in_retail) AS receipt_pah_retail,
     SUM(rcpt0.packandhold_transfer_in_cost) AS receipt_pah_cost,
     SUM(rcpt0.racking_transfer_in_units) AS receipt_flx_units,
     SUM(rcpt0.racking_transfer_in_retail) AS receipt_flx_retail,
     SUM(rcpt0.racking_transfer_in_cost) AS receipt_flx_cost
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS rcpt0
     INNER JOIN dates AS dates0 ON rcpt0.week_num = dates0.week_idnt
    GROUP BY sku_idnt,
     rcpt0.week_num,
     dates0.week_start_day_date,
     dates0.week_end_day_date,
     rcpt0.month_num,
     rcpt0.channel_num,
     rcpt0.store_num,
     ds_ind,
     rcpt0.rp_ind,
     rcpt0.npg_ind,
     ss_ind,
     gwp_ind) AS sub
 GROUP BY sku_idnt,
  week_num,
  week_start_day_date,
  week_end_day_date,
  mnth_idnt,
  channel_num,
  store_num,
  ds_ind,
  rp_ind,
  npg_ind,
  ss_ind,
  gwp_ind);

-- end

-- COLLECT STATS
--     PRIMARY INDEX (sku_idnt, week_num, store_num)
--     ,COLUMN (sku_idnt)
--     ,COLUMN (week_num)
--     ,COLUMN (store_num)
--     ,COLUMN (week_num, store_num)
--     ON {environment_schema}.receipt_sku_loc_week_agg_fact{env_suffix};
