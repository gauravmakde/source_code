


CREATE TEMPORARY TABLE IF NOT EXISTS aoi_exposed_delta AS
SELECT *
FROM (SELECT rms_sku_num,
    to_location_num AS location_num,
    reporting_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_in_fact_vw AS a
   WHERE (po_transfer_in_qty <> 0 OR reserve_stock_transfer_in_qty <> 0 OR pack_and_hold_transfer_in_qty <> 0 OR
       racking_transfer_in_qty <> 0)
    AND dw_batch_date = (SELECT curr_batch_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
   UNION ALL
   SELECT rms_sku_num,
    location_num,
    reporting_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
   WHERE LOWER(store_inbound_receipt_flag) = LOWER('Y')
    AND dw_batch_date = (SELECT curr_batch_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))) AS a
GROUP BY rms_sku_num,
 location_num,
 reporting_date;


CREATE TEMPORARY TABLE IF NOT EXISTS aoi_exposed_sku_loc_start_end_dt
AS
SELECT rms_sku_num,
 location_num,
 reporting_date AS start_date,
 COALESCE(DATE_SUB(MAX(reporting_date) OVER (PARTITION BY rms_sku_num, location_num ORDER BY reporting_date ROWS BETWEEN
    1 FOLLOWING AND 1 FOLLOWING), INTERVAL 1 DAY), (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))) AS end_date
FROM `aoi_exposed_delta` AS a
QUALIFY COALESCE(DATE_SUB(MAX(reporting_date) OVER (PARTITION BY rms_sku_num, location_num ORDER BY reporting_date
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), INTERVAL 1 DAY), (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))) >= DATE '2022-10-30';


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact SET
 last_transfer_exposed_date = wrk.start_date,
 dw_batch_id = (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_batch_date = (SELECT curr_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
 FROM `aoi_exposed_sku_loc_start_end_dt` AS wrk
WHERE LOWER(jwn_inventory_sku_loc_day_fact.rms_sku_num) = LOWER(wrk.rms_sku_num) AND jwn_inventory_sku_loc_day_fact.location_num
     = wrk.location_num AND COALESCE(jwn_inventory_sku_loc_day_fact.last_transfer_exposed_date, DATE '1970-01-01') <>
   wrk.start_date AND jwn_inventory_sku_loc_day_fact.reporting_date BETWEEN wrk.start_date AND wrk.end_date;


DROP TABLE IF EXISTS `aoi_exposed_sku_loc_start_end_dt`;


CREATE TEMPORARY TABLE IF NOT EXISTS aoi_exposed_sku_channel_start_end_dt
AS
SELECT rms_sku_num,
 channel_num,
 reporting_date AS start_date,
 COALESCE(DATE_SUB(MAX(reporting_date) OVER (PARTITION BY rms_sku_num, channel_num ORDER BY reporting_date ROWS BETWEEN
    1 FOLLOWING AND 1 FOLLOWING), INTERVAL 1 DAY), (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))) AS channel_end_date
FROM (SELECT DISTINCT a.rms_sku_num,
   b.channel_num,
   a.reporting_date
  FROM `aoi_exposed_delta` AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS b ON a.location_num = b.store_num) AS a
QUALIFY COALESCE(DATE_SUB(MAX(reporting_date) OVER (PARTITION BY rms_sku_num, channel_num ORDER BY reporting_date
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), INTERVAL 1 DAY), (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))) >= DATE '2022-10-30';


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact SET
 last_transfer_channel_exposed_date = wrk.start_date,
 dw_batch_id = (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_batch_date = (SELECT curr_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
 FROM `aoi_exposed_sku_channel_start_end_dt` AS wrk
WHERE LOWER(jwn_inventory_sku_loc_day_fact.rms_sku_num) = LOWER(wrk.rms_sku_num) AND jwn_inventory_sku_loc_day_fact.channel_num
     = wrk.channel_num AND jwn_inventory_sku_loc_day_fact.reporting_date BETWEEN wrk.start_date AND wrk.channel_end_date
    AND COALESCE(jwn_inventory_sku_loc_day_fact.last_transfer_channel_exposed_date, DATE '1970-01-01') <> wrk.start_date;