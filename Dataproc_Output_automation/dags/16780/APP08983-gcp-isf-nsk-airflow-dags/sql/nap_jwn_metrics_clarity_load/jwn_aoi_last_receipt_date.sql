

CREATE TEMPORARY TABLE IF NOT EXISTS delta_sku_loc_wrk
AS
(((SELECT rms_sku_num,
    location_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
    WHERE dw_sys_load_tmstp_utc >= (SELECT batch_start_tmstp_utc
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    AND dw_batch_date = (SELECT curr_batch_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    GROUP BY location_num,
    rms_sku_num
  UNION DISTINCT
  SELECT rms_sku_num,
    location_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact AS a
    WHERE dw_sys_load_tmstp_utc >= (SELECT batch_start_tmstp_utc
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    AND reporting_date >= DATE_SUB((SELECT curr_batch_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
        WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')), INTERVAL 1 DAY)
    GROUP BY rms_sku_num,
    location_num)
  UNION DISTINCT
  SELECT rms_sku_num,
    location_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_rtv_logical_fact AS a
    WHERE dw_sys_load_tmstp_utc >= (SELECT batch_start_tmstp_utc
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    AND dw_batch_date = (SELECT curr_batch_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    GROUP BY rms_sku_num,
    location_num)
  UNION DISTINCT
  SELECT rms_sku_num,
    from_location_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS a
    WHERE dw_sys_load_tmstp_utc >= (SELECT batch_start_tmstp_utc
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    AND dw_batch_date = (SELECT curr_batch_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    GROUP BY rms_sku_num,
    from_location_num)
  UNION DISTINCT
  SELECT rms_sku_num,
    to_location_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS a
    WHERE dw_sys_load_tmstp_utc >= (SELECT batch_start_tmstp_utc
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    AND dw_batch_date = (SELECT curr_batch_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    GROUP BY rms_sku_num,
    to_location_num;

CREATE TEMPORARY TABLE IF NOT EXISTS last_receipt_date_wrk
AS
  SELECT 
    a.rms_sku_num,
    a.location_num,
    a.reporting_date1 AS reporting_date,
    MAX(a.reporting_date) AS last_receipt_date
  FROM (
    SELECT 
      a.*, range_start(newrange) as reporting_date1
    FROM (
      SELECT DISTINCT 
        a.rms_sku_num,
        a.location_num,
        a.reporting_date,
        RANGE(a.reporting_date,lead(a.reporting_date+1,1,current_date+1) OVER (PARTITION BY a.rms_sku_num, a.location_num ORDER BY reporting_date)) as pa
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
      INNER JOIN delta_sku_loc_wrk AS d 
        ON LOWER(a.rms_sku_num) = LOWER(d.rms_sku_num) 
        AND a.location_num = d.location_num
      WHERE 
        a.received_qty > 0
    ) AS a   
    cross join UNNEST(generate_range_array(pa, interval 1 day)) AS newrange 
  )a
  WHERE reporting_date >= '2022-10-30'
  GROUP BY 
    a.rms_sku_num,
    a.location_num,
    a.reporting_date1
;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact SET
 last_receipt_date = last_receipt_date_wrk.last_receipt_date 
 FROM last_receipt_date_wrk
WHERE LOWER(jwn_inventory_sku_loc_day_fact.rms_sku_num) = LOWER(last_receipt_date_wrk.rms_sku_num) AND
      jwn_inventory_sku_loc_day_fact.reporting_date = last_receipt_date_wrk.reporting_date AND
     jwn_inventory_sku_loc_day_fact.location_num = last_receipt_date_wrk.location_num AND COALESCE(jwn_inventory_sku_loc_day_fact
     .last_receipt_date, DATE '1970-01-01') <> last_receipt_date_wrk.last_receipt_date AND
   jwn_inventory_sku_loc_day_fact.dw_batch_id = (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) 
    AND jwn_inventory_sku_loc_day_fact.dw_sys_load_tmstp
  >= (SELECT batch_start_tmstp_utc
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'));


DROP TABLE IF EXISTS last_receipt_date_wrk;


CREATE TEMPORARY TABLE IF NOT EXISTS last_receipt_date_wrk
AS
  SELECT 
    a.rms_sku_num,
    a.location_num,
    a.reporting_date1 AS reporting_date,
    MAX(a.reporting_date) AS last_receipt_date
  FROM (
    SELECT 
      a.*, range_start(newrange) as reporting_date1
    FROM (
      SELECT DISTINCT 
        a.rms_sku_num,
        a.location_num,
        a.reporting_date,
        RANGE(a.reporting_date,lead(a.reporting_date+1,1,current_date+1) OVER (PARTITION BY a.rms_sku_num, a.location_num ORDER BY reporting_date)) as pa
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
      WHERE 
        a.received_qty > 0
        and a.dw_batch_date=(SELECT CURR_BATCH_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_JWN_METRICS_BASE_VWS.ELT_CONTROL
                                WHERE SUBJECT_AREA_NM ='NAP_ASCP_CLARITY_LOAD')
    ) AS a   
    cross join UNNEST(generate_range_array(pa, interval 1 day)) AS newrange 
  )a
  WHERE reporting_date >= '2022-10-30'
  GROUP BY 
    a.rms_sku_num,
    a.location_num,
    a.reporting_date1
;

UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact SET
 last_receipt_date = last_receipt_date_wrk.last_receipt_date,
 dw_batch_id = (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_batch_date = (SELECT curr_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT'))  AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst()
 FROM last_receipt_date_wrk
WHERE LOWER(jwn_inventory_sku_loc_day_fact.rms_sku_num) = LOWER(last_receipt_date_wrk.rms_sku_num) AND
    jwn_inventory_sku_loc_day_fact.location_num = last_receipt_date_wrk.location_num AND jwn_inventory_sku_loc_day_fact
   .reporting_date = last_receipt_date_wrk.reporting_date AND COALESCE(jwn_inventory_sku_loc_day_fact.last_receipt_date
   , DATE '1970-01-01') <> last_receipt_date_wrk.last_receipt_date;

