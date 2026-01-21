DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_on_order_load;
---Task_Name=merch_on_order_load_stage_fact_load;'*/


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_wrk
(
purchase_order_number,
external_distribution_id,
rms_sku_num,
epm_sku_num,
store_num,
ship_location_id,
rms_casepack_num,
epm_casepack_num,
order_from_vendor_id,
order_category_code,
po_type,
order_type,
purchase_type,
status,
cancel_reason,
start_ship_date,
end_ship_date,
otb_eow_date,
first_approval_date,
latest_approval_date,
first_approval_event_tmstp_pacific,
first_approval_event_tmstp_pacific_tz,
latest_approval_event_tmstp_pacific,
latest_approval_event_tmstp_pacific_tz,
total_expenses_per_unit_currency,
cross_ref_id,
written_date,
quantity_ordered,
quantity_canceled,
unit_cost_amt,
total_expenses_per_unit_amt,
total_duty_per_unit_amt,
unit_estimated_landing_cost,
pricing_start_date,
dw_batch_date,
dw_sys_load_tmstp,
dw_sys_load_tmstp_tz
)
WITH exch AS (SELECT 'CA' AS country,
 eff_date_begin,
 eff_date_end,
 exch_rate
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.consolidated_exchange_rate_dim AS r
WHERE LOWER(currcy_cd) = LOWER('CAD')
 AND CURRENT_DATE('PST8PDT') BETWEEN eff_date_begin AND eff_date_end
UNION ALL
SELECT 'US' AS country,
 DATE '2021-01-01' AS eff_date_begin,
 DATE '9999-12-31' AS eff_date_end,
 1.0 AS exch_rate)
SELECT pof.purchase_order_number,
 pof.external_distribution_id,
 COALESCE(pack.rms_sku_num, pof.rms_sku_num) AS rms_sku_num,
 COALESCE(pack.epm_sku_num, pof.epm_sku_num) AS epm_sku_num,
 CAST(TRUNC(CAST(pof.store_num AS FLOAT64)) AS INT64),
 pof.ship_location_id,
 pack.rms_casepack_num,
 pack.epm_casepack_num,
 pof.order_from_vendor_id,
  CASE
  WHEN LOWER(org_store.selling_store_ind) = LOWER('S') AND LOWER(org_ship.selling_store_ind) <> LOWER('S')
  THEN 'XD'
  WHEN LOWER(org_store.selling_store_ind) = LOWER('S') AND LOWER(pof.store_num) = LOWER(pof.ship_location_id)
  THEN 'DS'
  WHEN LOWER(org_store.selling_store_ind) <> LOWER('S') AND LOWER(org_ship.selling_store_ind) <> LOWER('S')
  THEN 'WW'
  ELSE NULL
  END AS order_category_code,
 pof.po_type,
 pof.order_type,
 pof.purchase_type,
 pof.status,
 pof.cancel_reason,
 pof.start_ship_date,
 pof.end_ship_date,
 pof.otb_eow_date,
 pof.first_approval_date,
 pof.latest_approval_date,
 pof.first_approval_event_tmstp_pacific_utc as first_approval_event_tmstp_pacific,
 pof.first_approval_event_tmstp_pacific_tz,
 pof.latest_approval_event_tmstp_pacific_utc as latest_approval_event_tmstp_pacific,
 pof.latest_approval_event_tmstp_pacific_tz,
 pof.total_expenses_per_unit_currency,
 pof.cross_ref_id,
 pof.written_date,
  CASE
  WHEN pack.epm_casepack_num IS NOT NULL
  THEN pof.quantity_ordered * pack.items_per_pack_qty
  ELSE pof.quantity_ordered
  END AS quantity_ordered,
  CASE
  WHEN pack.epm_casepack_num IS NOT NULL
  THEN pof.quantity_canceled * pack.items_per_pack_qty
  ELSE pof.quantity_canceled
  END AS quantity_canceled,
  CASE
  WHEN pack.epm_casepack_num IS NOT NULL
  THEN CASE
   WHEN COALESCE(pack.total_sku_qty, 0) = 0
   THEN 0
   ELSE pof.unit_cost_amt / pack.total_sku_qty
   END
  ELSE pof.unit_cost_amt
  END AS unit_cost_amt,
  CASE
  WHEN pack.epm_casepack_num IS NOT NULL
  THEN CASE
   WHEN COALESCE(pack.total_sku_qty, 0) = 0
   THEN 0
   ELSE pof.total_expenses_per_unit_amt / pack.total_sku_qty
   END
  ELSE pof.total_expenses_per_unit_amt
  END AS total_expenses_per_unit_amt,
  CASE
  WHEN pack.epm_casepack_num IS NOT NULL
  THEN CASE
   WHEN COALESCE(pack.total_sku_qty, 0) = 0
   THEN 0
   ELSE pof.total_duty_per_unit_amt / pack.total_sku_qty
   END
  ELSE pof.total_duty_per_unit_amt
  END AS total_duty_per_unit_amt,
  CASE
  WHEN pack.epm_casepack_num IS NOT NULL
  THEN CASE
   WHEN COALESCE(pack.total_sku_qty, 0) = 0
   THEN 0
   ELSE pof.unit_estimated_landing_cost / pack.total_sku_qty
   END
  ELSE pof.unit_estimated_landing_cost
  END AS unit_estimated_landing_cost,
  CASE
  WHEN pof.start_ship_date <= CURRENT_DATE('PST8PDT')
  THEN CURRENT_DATE('PST8PDT')
  ELSE pof.start_ship_date
  END AS pricing_start_date,
 CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz
FROM (SELECT ship.purchase_order_number,
   head.import_country,
   head.exchange_rate,
    CASE
    WHEN LOWER(ship.distribution_status) = LOWER('APPROVED') AND LOWER(ship.ship_location_id) <> LOWER(dist.distribute_location_id
        ) AND dist.purchase_order_number IS NOT NULL
    THEN COALESCE(dist.external_distribution_id, '-1')
    ELSE ship.purchase_order_number
    END AS external_distribution_id,
   ship.rms_sku_num,
   ship.epm_sku_num,
    CASE
    WHEN LOWER(ship.distribution_status) = LOWER('APPROVED') AND LOWER(ship.ship_location_id) <> LOWER(dist.distribute_location_id
        ) AND dist.purchase_order_number IS NOT NULL
    THEN SUBSTR(dist.distribute_location_id, 1, 4)
    ELSE SUBSTR(ship.ship_location_id, 1, 4)
    END AS store_num,
   SUBSTR(ship.ship_location_id, 1, 4) AS ship_location_id,
   ship.order_from_vendor_id,
   ship.po_type,
   ship.order_type,
   ship.purchase_type,
   ship.status,
   ship.cancel_reason,
   ship.start_ship_date,
   ship.end_ship_date,
   ship.otb_eow_date,
   CAST(head.first_approval_event_tmstp_pacific_utc AS DATE) AS first_approval_date,
   CAST(ship.latest_approval_event_tmstp_pacific_utc AS DATE) AS latest_approval_date,
   head.first_approval_event_tmstp_pacific_utc,
   head.first_approval_event_tmstp_pacific_tz,
   ship.latest_approval_event_tmstp_pacific_utc,
   ship.latest_approval_event_tmstp_pacific_tz,
   ship.total_expenses_per_unit_currency,
   head.cross_reference_external_id AS cross_ref_id,
   head.written_date,
    CASE
    WHEN LOWER(ship.status) IN (LOWER('CLOSED'), LOWER('APPROVED'))
    THEN CASE
     WHEN LOWER(ship.distribution_status) = LOWER('APPROVED') AND LOWER(ship.ship_location_id) <> LOWER(dist.distribute_location_id
         ) AND dist.purchase_order_number IS NOT NULL
     THEN COALESCE(dist.quantity_allocated, 0)
     ELSE COALESCE(ship.quantity_ordered, 0)
     END
    ELSE CASE
     WHEN LOWER(ship.distribution_status) = LOWER('APPROVED') AND LOWER(ship.ship_location_id) <> LOWER(dist.distribute_location_id
         ) AND dist.purchase_order_number IS NOT NULL
     THEN COALESCE(dist.approved_quantity_allocated, 0)
     ELSE COALESCE(ship.approved_quantity_ordered, 0)
     END
    END AS quantity_ordered,
    CASE
    WHEN LOWER(ship.distribution_status) = LOWER('APPROVED') AND LOWER(ship.ship_location_id) <> LOWER(dist.distribute_location_id
        ) AND dist.purchase_order_number IS NOT NULL
    THEN COALESCE(dist.quantity_canceled, 0)
    ELSE COALESCE(ship.quantity_canceled, 0)
    END AS quantity_canceled,
    CASE
    WHEN LOWER(ship.status) IN (LOWER('CLOSED'), LOWER('APPROVED'))
    THEN CASE
     WHEN LOWER(SUBSTR(TRIM(ship.unit_cost_currency), 1, 2)) = LOWER(head.import_country)
     THEN COALESCE(ship.unit_cost, 0)
     WHEN LOWER(SUBSTR(TRIM(ship.unit_cost_currency), 1, 2)) <> LOWER(head.import_country)
     THEN COALESCE(ship.unit_cost, 0) / head.exchange_rate * exch.exch_rate
     ELSE NULL
     END
    ELSE CASE
     WHEN LOWER(SUBSTR(TRIM(ship.approved_unit_cost_currency), 1, 2)) = LOWER(head.import_country)
     THEN COALESCE(ship.approved_unit_cost, 0)
     WHEN LOWER(SUBSTR(TRIM(ship.approved_unit_cost_currency), 1, 2)) <> LOWER(head.import_country)
     THEN COALESCE(ship.approved_unit_cost, 0) / head.exchange_rate * exch.exch_rate
     ELSE NULL
     END
    END AS unit_cost_amt,
    CASE
    WHEN LOWER(head.import_country) = LOWER('CA')
    THEN COALESCE(ship.total_expenses_per_unit, 0) * exch.exch_rate
    ELSE COALESCE(ship.total_expenses_per_unit, 0)
    END AS total_expenses_per_unit_amt,
   COALESCE(ship.total_duty_per_unit, 0) AS total_duty_per_unit_amt,
      CASE
      WHEN LOWER(ship.status) IN (LOWER('CLOSED'), LOWER('APPROVED'))
      THEN CASE
       WHEN LOWER(SUBSTR(TRIM(ship.unit_cost_currency), 1, 2)) = LOWER(head.import_country)
       THEN COALESCE(ship.unit_cost, 0)
       WHEN LOWER(SUBSTR(TRIM(ship.unit_cost_currency), 1, 2)) <> LOWER(head.import_country)
       THEN COALESCE(ship.unit_cost, 0) / head.exchange_rate * exch.exch_rate
       ELSE NULL
       END
      ELSE CASE
       WHEN LOWER(SUBSTR(TRIM(ship.approved_unit_cost_currency), 1, 2)) = LOWER(head.import_country)
       THEN COALESCE(ship.approved_unit_cost, 0)
       WHEN LOWER(SUBSTR(TRIM(ship.approved_unit_cost_currency), 1, 2)) <> LOWER(head.import_country)
       THEN COALESCE(ship.approved_unit_cost, 0) / head.exchange_rate * exch.exch_rate
       ELSE NULL
       END
      END + CASE
      WHEN LOWER(head.import_country) = LOWER('CA')
      THEN COALESCE(ship.total_expenses_per_unit, 0) * exch.exch_rate
      ELSE COALESCE(ship.total_expenses_per_unit, 0)
      END + COALESCE(ship.total_duty_per_unit, 0) AS unit_estimated_landing_cost
  FROM (SELECT purchase_order_number,
     distribution_status,
     ship_location_id,
     rms_sku_num,
     epm_sku_num,
     sku_num,
     order_from_vendor_id,
     po_type,
     order_type,
     purchase_type,
     status,
     cancel_reason,
     start_ship_date,
     end_ship_date,
     otb_eow_date,
     first_approval_event_tmstp_pacific_utc,
	 first_approval_event_tmstp_pacific_tz,
     latest_approval_event_tmstp_pacific_utc,
	 latest_approval_event_tmstp_pacific_tz,
     total_expenses_per_unit_currency,
     total_expenses_per_unit,
     approved_quantity_ordered,
     quantity_ordered,
     quantity_canceled,
     approved_unit_cost_currency,
     approved_unit_cost,
     total_duty_per_unit,
     epo_purchase_order_id,
     unit_cost_currency,
     unit_cost
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_fact
    WHERE LOWER(status) IN (LOWER('APPROVED'), LOWER('CLOSED'), LOWER('WORKSHEET'))
     AND start_ship_date >= DATE '2021-01-31'
     AND (LOWER(case_pack_ind) = LOWER('F') OR DATE_SUB(start_ship_date, INTERVAL 3 DAY) >= CURRENT_DATE('PST8PDT'))) AS ship
   LEFT JOIN (SELECT purchase_order_number,
     distribute_location_id,
     external_distribution_id,
     approved_quantity_allocated,
     quantity_allocated,
     quantity_canceled,
     epo_purchase_order_id,
     sku_num,
     ship_location_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_distributelocation_fact
    WHERE LOWER(distribution_status) = LOWER('APPROVED')
     AND (LOWER(case_pack_ind) = LOWER('F') OR DATE_SUB(start_ship_date, INTERVAL 3 DAY) >= CURRENT_DATE('PST8PDT'))) AS dist ON
      LOWER(ship.epo_purchase_order_id) = LOWER(dist.epo_purchase_order_id) AND LOWER(ship.sku_num) = LOWER(dist.sku_num
       ) AND LOWER(ship.ship_location_id) = LOWER(dist.ship_location_id)
   INNER JOIN (SELECT purchase_order_number,
     epo_purchase_order_id,
     include_on_order_ind,
     import_country,
     exchange_rate,
     latest_item_cost_changed_tmstp_pacific_utc,
	 latest_item_cost_changed_tmstp_pacific_tz,
     first_approval_event_tmstp_pacific_utc,
	 first_approval_event_tmstp_pacific_tz,
     cross_reference_external_id,
     written_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact
    WHERE LOWER(include_on_order_ind) = LOWER('T')) AS head 
    ON LOWER(ship.epo_purchase_order_id) = LOWER(head.epo_purchase_order_id)
   LEFT JOIN exch ON LOWER(head.import_country) = LOWER(exch.country)) AS pof
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS org_store ON org_store.store_num = CAST(pof.store_num AS FLOAT64)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS org_ship ON org_ship.store_num = CAST(pof.ship_location_id AS FLOAT64)
 LEFT JOIN (SELECT epm_casepack_num,
   channel_country,
   rms_sku_num,
   epm_sku_num,
   rms_casepack_num,
   items_per_pack_qty,
   SUM(items_per_pack_qty) OVER (PARTITION BY epm_casepack_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS total_sku_qty
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.casepack_sku_xref) AS pack ON pof.epm_sku_num = pack.epm_casepack_num AND LOWER(org_store.store_country_code
    ) = LOWER(pack.channel_country);



