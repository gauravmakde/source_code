
DELETE
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sp_mos_adjusted_sku_store_week_agg_fact AS mos_agg_vw
WHERE
  mos_agg_vw.week_num BETWEEN (
  SELECT
    start_rebuild_week_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
  WHERE
    LOWER(tran_vw1.INTERFACE_CODE) =LOWER('MERCH_SP_MOS_ADJ_DLY'))
  AND (
  SELECT
    end_rebuild_week_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2
  WHERE
    LOWER(tran_vw2.interface_code )=LOWER('MERCH_SP_MOS_ADJ_DLY'));



INSERT into
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_sp_mos_adjusted_sku_store_week_agg_fact
SELECT
  rms_sku_num,
  store_num,
  week_num,
  total_cost_currency_code,
  total_retail_currency_code,
  cast(rp_ind as string),
  sum(mos_active_cost) as mos_active_cost,
  sum(mos_active_regular_cost) as mos_active_regular_cost,
  sum(mos_active_clearance_cost) as mos_active_clearance_cost,
  sum(mos_active_promo_cost) as mos_active_promo_cost,
  cast(sum(mos_active_units) as int64) as mos_active_units,
  cast(sum(mos_active_regular_units)  as int64)as mos_active_regular_units,
  cast(sum(mos_active_clearance_units)  as int64)as mos_active_clearance_units,
  cast(sum(mos_active_promo_units)as int64) as mos_active_promo_units,
  cast(sum(mos_active_retail)as int64) as mos_active_retail,
  cast(sum(mos_active_regular_retail)as int64) as mos_active_regular_retail,
  cast(sum(mos_active_clearance_retail)as int64) as mos_active_clearance_retail,
  cast(sum(mos_active_promo_retail)as int64) as mos_active_promo_retail,
  cast(sum(mos_inactive_cost)as int64) as mos_inactive_cost,
  cast(sum(mos_inactive_regular_cost)as  int64)as mos_inactive_regular_cost,
  cast(sum(mos_inactive_clearance_cost)as  int64)as mos_inactive_clearance_cost,
  cast(sum(mos_inactive_promo_cost)as int64) as mos_inactive_promo_cost,
  cast(sum(mos_inactive_units)as int64) as mos_inactive_units,
  cast(sum(mos_inactive_regular_units)as int64) as mos_inactive_regular_units,
  cast(sum(mos_inactive_clearance_units)as int64) as mos_inactive_clearance_units,
  cast(sum(mos_inactive_promo_units)as int64)  as mos_inactive_promo_units,
  cast(sum(mos_inactive_retail)as  int64) as mos_inactive_retail,
  cast(sum(mos_inactive_regular_retail)as int64) as mos_inactive_regular_retail,
  cast(sum(mos_inactive_clearance_retail)as int64) as mos_inactive_clearance_retail,
  cast(sum(mos_inactive_promo_retail)as int64) as mos_inactive_promo_retail,
  cast(sum(mos_total_cost)as  int64)as mos_total_cost,
  cast(sum(mos_total_regular_cost)as  int64)as mos_total_regular_cost,
  cast(sum(mos_total_clearance_cost)as int64)as mos_total_clearance_cost,
  cast(sum(mos_total_promotion_cost)as  int64) as mos_total_promotion_cost,
  cast(sum(mos_total_units)as  int64) as mos_total_units,
  cast(sum(mos_total_regular_units)as int64)  as mos_total_regular_units,
  cast(sum(mos_total_clearance_units)as int64) as mos_total_clearance_units,
  cast(sum(mos_total_promo_units)as int64) as mos_total_promo_units,
  cast(sum(mos_total_retail)as  int64) as mos_total_retail,
  cast(sum(mos_total_regular_retail)as int64) as mos_total_regular_retail,
  cast(sum(mos_total_clearance_retail)as int64) as mos_total_clearance_retail,
  cast(sum(mos_total_promotion_retail)as int64) as mos_total_promotion_retail,
  cast(sum(shrink_active_cost)as int64) as shrink_active_cost,
  cast(sum(shrink_active_regular_cost)as  int64) as shrink_active_regular_cost,
  cast(sum(shrink_active_clearance_cost)as int64)  as shrink_active_clearance_cost,
  cast(sum(shrink_active_promo_cost)as int64) as shrink_active_promo_cost,
  cast(sum(shrink_active_units)as int64) as shrink_active_units,
  cast(sum(shrink_active_regular_units)as int64) as shrink_active_regular_units,
  cast(sum(shrink_active_clearance_units)as int64)as shrink_active_clearance_units,
  cast(sum(shrink_active_promo_units)as int64)as shrink_active_promo_units,
  cast(sum(shrink_active_retail)as  int64)as shrink_active_retail,
  cast(sum(shrink_active_regular_retail)as int64)as shrink_active_regular_retail,
  cast(sum(shrink_active_clearance_retail)as int64) as shrink_active_clearance_retail,
  cast(sum(shrink_active_promo_retail)as int64) as shrink_active_promo_retail,
  cast(sum(shrink_inactive_cost)as int64) as shrink_inactive_cost,
  cast(sum(shrink_inactive_regular_cost)as int64) as shrink_inactive_regular_cost,
  cast(sum(shrink_inactive_clearance_cost)as int64) as shrink_inactive_clearance_cost,
  cast(sum(shrink_inactive_promo_cost)as int64) as shrink_inactive_promo_cost,
  cast(sum(shrink_inactive_units)as int64) as shrink_inactive_units,
  cast(sum(shrink_inactive_regular_units)as int64) as shrink_inactive_regular_units,
  cast(sum(shrink_inactive_clearance_units)as int64) as shrink_inactive_clearance_units,
  cast(sum(shrink_inactive_promo_units)as int64) as shrink_inactive_promo_units,
  cast(sum(shrink_inactive_retail)as int64) as shrink_inactive_retail,
  cast(sum(shrink_inactive_regular_retail)as int64) as shrink_inactive_regular_retail,
  cast(sum(shrink_inactive_clearance_retail)as int64) as shrink_inactive_clearance_retail,
  cast(sum(shrink_inactive_promo_retail)as int64) as shrink_inactive_promo_retail,
  cast(sum(shrink_total_cost)as int64)as shrink_total_cost,
  cast(sum(shrink_total_regular_cost)as int64) as shrink_total_regular_cost,
  cast(sum(shrink_total_clearance_cost)as int64)as shrink_total_clearance_cost,
  cast(sum(shrink_total_promo_cost)as int64)as shrink_total_promo_cost,
  cast(sum(shrink_total_units)as int64) as shrink_total_units,
  cast(sum(shrink_total_regular_units)as  int64)as shrink_total_regular_units,
  cast(sum(shrink_total_clearance_units)as  int64)as shrink_total_clearance_units,
  cast(sum(shrink_total_promo_units)as int64)as shrink_total_promo_units,
  cast(sum(shrink_total_retail)as int64)as shrink_total_retail,
  cast(sum(shrink_total_regular_retail)as int64) as shrink_total_regular_retail,
  cast(sum(shrink_total_clearance_retail)as int64)as shrink_total_clearance_retail,
  cast(sum(shrink_total_promo_retail)as int64) as shrink_total_promo_retail,
  current_datetime('PST8PDT') as dw_sys_load_tmstp,
  current_date('PST8PDT') as dw_batch_date
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sp_mos_adjusted_columnar_vw mos
WHERE
  mos.week_num between(
  SELECT
    start_rebuild_week_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
  WHERE
    LOWER(tran_vw1.INTERFACE_CODE) =LOWER('MERCH_SP_MOS_ADJ_DLY'))
  AND (
  SELECT
    end_rebuild_week_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2
  WHERE
    LOWER(tran_vw2.interface_code) =LOWER('MERCH_SP_MOS_ADJ_DLY'))
GROUP BY
  rms_sku_num,
  store_num,
  week_num,
  total_cost_currency_code,
  total_retail_currency_code,
  rp_ind;
