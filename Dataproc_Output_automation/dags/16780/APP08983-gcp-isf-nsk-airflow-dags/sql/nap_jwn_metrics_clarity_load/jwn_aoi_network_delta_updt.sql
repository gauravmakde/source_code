--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : jwn_aoi_network_delta_updt.sql
-- Author                  : Rishi Nair
-- Description             : The Age of inventory is computed from the sku_network_last_receipt_date in the JWN_INVENTORY_SKU_LOC_DAY_FACT
-- Data Source             : JWN_PURCHASE_ORDER_RMS_RECEIPTS_FACT
-- ETL Run Frequency       : Daily
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-04-22  Rishi Nair 			Clarity prototype scripts
-- 2023-04-27  Sergii Porfiriev     FA-8598: Code Refactor - for Ongoing Delta Load in Production
-- 2023-12-07  Oleksandr Chaichenko FA-10018:Exclude negative qty from calculation last received date
--*************************************************************************************************************************************


/*Age of inventory network looks at the last receipt for a SKU in the JWN network*/
/* The Age of inventory is computed from the sku_network_last_receipt_date in the
JWN_INVENTORY_SKU_LOC_DAY_FACT*/
/*sku_network_last_receipt_date changes only if there is a receipt for a sku from the JWN_PURCHASE_ORDER_RECEIPTS_FACT*/
/* The delta can be identified by dw_batch_date in the receipts table.*/
/*A set of start and end date is created using LEAD and period below in the subquery for every SKU.
The expanded set is updated into the final table. The entire process is performed only for the SKU;s
that had any kind of receipt activity today*/


begin

CREATE TEMPORARY TABLE IF NOT EXISTS delta_sku_num_wrk (
rms_sku_num STRING
) ;


INSERT INTO delta_sku_num_wrk
(SELECT rms_sku_num
 FROM ((((SELECT rms_sku_num
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
       WHERE dw_sys_load_tmstp >= (SELECT batch_start_tmstp FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
        AND dw_batch_date = (SELECT curr_batch_date
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
          WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
       GROUP BY rms_sku_num
       UNION DISTINCT
       SELECT rms_sku_num
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact AS a
       WHERE dw_sys_load_tmstp >= (SELECT batch_start_tmstp FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
        AND reporting_date >= DATE_SUB((SELECT curr_batch_date
           FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
           WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')), INTERVAL 1 DAY)
       GROUP BY rms_sku_num)
      UNION DISTINCT
      SELECT rms_sku_num
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_rtv_logical_fact AS a
      WHERE dw_sys_load_tmstp >= (SELECT batch_start_tmstp FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
       AND dw_batch_date = (SELECT curr_batch_date
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
      GROUP BY rms_sku_num)
     UNION DISTINCT
     SELECT rms_sku_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS a
     WHERE dw_sys_load_tmstp >= (SELECT batch_start_tmstp FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
      AND dw_batch_date = (SELECT curr_batch_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
        WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
     GROUP BY rms_sku_num)
    UNION DISTINCT
    SELECT rms_sku_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS a
    WHERE dw_sys_load_tmstp >= (SELECT batch_start_tmstp FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
     AND dw_batch_date = (SELECT curr_batch_date
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    GROUP BY rms_sku_num) AS t29
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM delta_sku_num_wrk
   WHERE rms_sku_num = t29.rms_sku_num)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num)) = 1);



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact SET
 sku_network_last_receipt_date = src.last_receipt_date,
 dw_batch_id = (SELECT batch_id   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_batch_date = (SELECT curr_batch_date   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst()
 FROM (SELECT rms_sku_num,
     reporting_date1 AS reporting_date,
     MAX(reporting_date) AS last_receipt_date
    FROM (
SELECT a.* ,range_start(newrange) as reporting_date1
    FROM 
    (SELECT a.*,
       RANGE(a.reporting_date, LEAD(DATE_ADD(a.reporting_date, INTERVAL 1 DAY), 1, DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY
          )) OVER (PARTITION BY a.rms_sku_num ORDER BY a.reporting_date)) AS pa,
       
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
       INNER JOIN delta_sku_num_wrk AS d ON LOWER(a.rms_sku_num) = LOWER(d.rms_sku_num)
      WHERE a.received_qty > 0) AS a
   cross join UNNEST(generate_range_array(pa, interval 1 day)) AS newrange
  ) AS a
    WHERE reporting_date >= DATE '2022-10-30'
    GROUP BY rms_sku_num,
     reporting_date ) AS src
WHERE LOWER(jwn_inventory_sku_loc_day_fact.rms_sku_num) = LOWER(src.rms_sku_num) AND jwn_inventory_sku_loc_day_fact.reporting_date
     = src.reporting_date AND jwn_inventory_sku_loc_day_fact.dw_batch_id = (SELECT batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AND jwn_inventory_sku_loc_day_fact.dw_sys_load_tmstp
  >= (SELECT batch_start_tmstp_utc FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'));


DROP TABLE IF EXISTS delta_sku_num_wrk;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact SET
 sku_network_last_receipt_date = src.last_receipt_date,
 dw_batch_id = (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_batch_date = (SELECT curr_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')),
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst()
 FROM (SELECT rms_sku_num,
     reporting_date1 AS reporting_date,
     MAX(reporting_date) AS last_receipt_date
    FROM (
SELECT a.* ,range_start(newrange) as reporting_date1
    FROM (SELECT a.*,
       RANGE(reporting_date, LEAD(DATE_ADD(reporting_date, INTERVAL 1 DAY), 1, DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
        OVER (PARTITION BY rms_sku_num ORDER BY reporting_date)) AS pa,
       
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
      WHERE dw_batch_date = (SELECT curr_batch_date
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
       AND received_qty > 0) AS a
   cross join UNNEST(generate_range_array(pa, interval 1 day)) AS newrange

    ) AS a
    WHERE reporting_date >= DATE '2022-10-30'
    GROUP BY rms_sku_num,
     reporting_date ) AS src
WHERE LOWER(jwn_inventory_sku_loc_day_fact.rms_sku_num) = LOWER(src.rms_sku_num) 
AND COALESCE(jwn_inventory_sku_loc_day_fact.sku_network_last_receipt_date, DATE '1970-01-01') <> src.last_receipt_date 
AND jwn_inventory_sku_loc_day_fact.last_receipt_date <= src.last_receipt_date AND jwn_inventory_sku_loc_day_fact.reporting_date = src.reporting_date;

end