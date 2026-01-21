--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : external_intransit_historical.sql
-- Author                  : Rishi Nair
-- Description             : Historical load of External intransit into JWN_EXTERNAL_IN_TRANSIT_FACT
-- Data Source             : VENDOR_ASN_FACT and JWN_PURCHASE_ORDER_RMS_RECEIPTS_FACT
-- ETL Run Frequency       : Once
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-04-22  Rishi Nair 			Clarity prototype scripts
-- 2023-04-27  Sergii Porfiriev     FA-8450: Code Refactor - for Ongoing Delta Load in Production
-- 2024-01-09  Oleksandr Chaichenko FA-11076: Added VASN table as secondary source for po_distributed_location_num.
--                                  Exposing negative  values to cumulative_external_in_transit_qty
--*************************************************************************************************************************************

/*Delta volatile table with all purchase orders wih an ASN or a receipt*/




CREATE TEMPORARY TABLE IF NOT EXISTS delta_external_intransit_wrk
AS
SELECT *
FROM (SELECT LTRIM(purchase_order_num, '0') AS purchase_order_num,
    LTRIM(rms_sku_num, '0') AS rms_sku_num
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_asn_fact
   WHERE dw_sys_updt_tmstp BETWEEN (SELECT extract_from_tmstp
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AND (SELECT extract_to_tmstp
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
   UNION ALL
   SELECT purchase_order_num,
    rms_sku_num
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact
   WHERE dw_batch_date = (SELECT curr_batch_date
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))) AS a
GROUP BY purchase_order_num,
 rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS vendor_asn_wrk
AS
SELECT LTRIM(a.purchase_order_num, '0') AS purchase_order_num,
 LTRIM(a.rms_sku_num, '0') AS rms_sku_num,
 a.carton_id AS carton_num,
 a.edi_date,
 SUM(a.vasn_sku_qty) AS vasn_sku_qty,
 MAX(a.ship_to_location_id) AS ship_to_location_id
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_asn_fact AS a
 INNER JOIN `delta_external_intransit_wrk` AS b ON LOWER(LTRIM(a.purchase_order_num, '0')) = LOWER(b.purchase_order_num
    ) AND LOWER(LTRIM(a.rms_sku_num, '0')) = LOWER(b.rms_sku_num)
WHERE a.edi_date >= DATE '2022-10-30'
GROUP BY a.purchase_order_num,
 a.rms_sku_num,
 carton_num,
 a.edi_date
QUALIFY (RANK() OVER (PARTITION BY LTRIM(a.purchase_order_num, '0'), LTRIM(a.rms_sku_num, '0'), a.carton_id ORDER BY a.edi_date
     DESC)) = 1;



DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_external_in_transit_fact AS fct
WHERE EXISTS (SELECT 1
 FROM `delta_external_intransit_wrk` AS wrk
 WHERE LOWER(purchase_order_num) = LOWER(fct.purchase_order_num)
  AND LOWER(COALESCE(CASE
      WHEN LOWER(rms_sku_num) = LOWER('NA')
      THEN NULL
      ELSE rms_sku_num
      END, 'null')) = LOWER(COALESCE(fct.rms_sku_num, 'null'))
 GROUP BY purchase_order_num);


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_external_in_transit_fact
(--CPU under 10K and historical load should complete in undr 5 mins
purchase_order_num,
rms_sku_num,
po_distributed_location_num,
in_transit_activity_date,
cumulative_external_in_transit_qty,
dw_batch_id,
dw_batch_date,
dw_sys_load_tmstp,
dw_sys_load_tmstp_tz,
dw_sys_updt_tmstp,
dw_sys_updt_tmstp_tz
)
 SELECT purchase_order_num,
  CASE
  WHEN LOWER(rms_sku_num) = LOWER('NA')
  THEN NULL
  ELSE rms_sku_num
  END AS rms_sku_num,
 0 AS po_distributed_location_num,
 in_transit_activity_date,
 any_value(SUM(sku_qty)) OVER (PARTITION BY purchase_order_num, ANY_VALUE(rms_sku_num) ORDER BY in_transit_activity_date ROWS BETWEEN
  UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_external_in_transit_qty,
  (SELECT batch_id
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
  (SELECT curr_batch_date
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
 timestamp(current_datetime('PST8PDT')) AS dw_sys_load_tmstp,
 'PST8PDT' AS dw_sys_load_tmstp_tz,
 timestamp(current_datetime('PST8PDT')) AS dw_sys_updt_tmstp,
 'PST8PDT' as dw_sys_updt_tmstp_tz
FROM (SELECT purchase_order_num,
    rms_sku_num,
    edi_date AS in_transit_activity_date,
    SUM(vasn_sku_qty) AS sku_qty
   FROM `vendor_asn_wrk`
   GROUP BY purchase_order_num,
    rms_sku_num,
    in_transit_activity_date
   UNION ALL
   SELECT vndr.purchase_order_num,
    vndr.rms_sku_num,
     CASE
     WHEN rcpt.reporting_date < vndr.edi_date
     THEN vndr.edi_date
     ELSE rcpt.reporting_date
     END AS in_transit_activity_date,
     - 1 * vndr.vasn_sku_qty AS sku_qty
   FROM (SELECT purchase_order_num,
      carton_num,
      rms_sku_num,
      reporting_date
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact
     WHERE LOWER(store_inbound_receipt_flag) <> LOWER('Y')
     GROUP BY purchase_order_num,
      carton_num,
      rms_sku_num,
      reporting_date) AS rcpt
    INNER JOIN `vendor_asn_wrk` AS vndr 
    ON LOWER(vndr.purchase_order_num) = LOWER(rcpt.purchase_order_num) 
    AND LOWER(vndr.rms_sku_num) = LOWER(rcpt.rms_sku_num) 
         AND LOWER(vndr.carton_num) = LOWER(rcpt.carton_num) 
         AND rcpt.reporting_date IS NOT NULL
   UNION ALL
   SELECT vndr.purchase_order_num,
    vndr.rms_sku_num,
     CASE
     WHEN rcpt.reporting_date < vndr.min_edi_date
     THEN vndr.min_edi_date
     ELSE rcpt.reporting_date
     END AS in_transit_activity_date,
     - 1 * rcpt.received_qty AS sku_qty
   FROM (SELECT purchase_order_num,
      rms_sku_num,
      reporting_date,
      SUM(received_qty) AS received_qty
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact
     WHERE LOWER(store_inbound_receipt_flag) = LOWER('Y')
     GROUP BY purchase_order_num,
      rms_sku_num,
      reporting_date) AS rcpt
    INNER JOIN (SELECT purchase_order_num,
      rms_sku_num,
      MIN(edi_date) AS min_edi_date
     FROM `vendor_asn_wrk`
     GROUP BY purchase_order_num,
      rms_sku_num) AS vndr ON LOWER(vndr.purchase_order_num) = LOWER(rcpt.purchase_order_num) AND LOWER(vndr.rms_sku_num
        ) = LOWER(rcpt.rms_sku_num) AND rcpt.reporting_date IS NOT NULL) AS main
GROUP BY purchase_order_num,
 rms_sku_num,
 in_transit_activity_date;



TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_stg.po_dist_location_num_wrk_stg;


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_stg.po_dist_location_num_wrk_stg
(--CPU consumed is high , wont fail but run time may vary depending on resource availability. Avg run time of 15 mins.
purchase_order_num,
rms_sku_num,
max_distribute_loc_num
)
 SELECT purchase_order_number AS purchase_order_num,
 rms_sku_num,
 MAX(max_loc_id) AS max_distribute_loc_num
FROM (SELECT a.purchase_order_number,
    a.rms_sku_num,
    MAX(CASE
      WHEN LOWER(a.status) <> LOWER('CLOSED') AND a.approved_quantity_allocated <> 0 OR LOWER(a.status) = LOWER('CLOSED'
          ) AND a.quantity_allocated <> 0
      THEN SAFE_CAST(TRUNC(CAST(a.distribute_location_id AS FLOAT64)) AS INT64)
      ELSE  0
      END) AS max_loc_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.purchase_order_distributelocation_fact AS a
    INNER JOIN `delta_external_intransit_wrk` AS b 
    ON LOWER(a.purchase_order_number) = LOWER(b.purchase_order_num) 
    AND LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num)
   WHERE a.approved_quantity_allocated <> 0
    OR a.quantity_allocated <> 0
   GROUP BY a.purchase_order_number,
    a.rms_sku_num
   UNION ALL
   SELECT a0.purchase_order_number,
    a0.rms_sku_num,
    MAX(CASE
      WHEN LOWER(a0.status) <> LOWER('CLOSED') AND a0.approved_quantity_ordered <> 0 OR LOWER(a0.status) = LOWER('CLOSED'
          ) AND a0.quantity_ordered <> 0
      THEN SAFE_CAST(TRUNC(CAST(a0.ship_location_id AS FLOAT64)) AS INT64)
      ELSE  0
      END) AS max_loc_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.purchase_order_shiplocation_fact AS a0
    INNER JOIN `delta_external_intransit_wrk` AS b0 ON LOWER(a0.purchase_order_number) = LOWER(b0.purchase_order_num)
     AND LOWER(a0.rms_sku_num) = LOWER(b0.rms_sku_num)
    INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.store_dim AS c ON CAST(a0.ship_location_id AS FLOAT64) = c.store_num
   WHERE CAST(a0.ship_location_id AS FLOAT64) = c.store_num
    AND LOWER(c.selling_store_ind) = LOWER('S')
    AND (a0.approved_quantity_ordered <> 0 OR a0.quantity_ordered <> 0)
   GROUP BY a0.purchase_order_number,
    a0.rms_sku_num) AS a
GROUP BY purchase_order_number,
 rms_sku_num;



UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_stg.po_dist_location_num_wrk_stg SET
 max_distribute_loc_num = wrk.ship_to_location_id FROM (SELECT purchase_order_num,
   rms_sku_num,
   MAX(ship_to_location_id) AS ship_to_location_id
  FROM `vendor_asn_wrk`
  GROUP BY purchase_order_num,
   rms_sku_num) AS wrk
WHERE LOWER(po_dist_location_num_wrk_stg.purchase_order_num) = LOWER(wrk.purchase_order_num) AND LOWER(po_dist_location_num_wrk_stg.rms_sku_num) = LOWER(wrk.rms_sku_num) AND po_dist_location_num_wrk_stg.max_distribute_loc_num = 0;



INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_stg.po_dist_location_num_wrk_stg
  (
  purchase_order_num,
  rms_sku_num,
  max_distribute_loc_num
  )
SELECT purchase_order_num,
 rms_sku_num,
 MAX(ship_to_location_id) AS ship_to_location_id
FROM `vendor_asn_wrk` AS wrk
WHERE NOT EXISTS (SELECT 1
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.po_dist_location_num_wrk_stg AS stg
  WHERE LOWER(wrk.purchase_order_num) = LOWER(purchase_order_num)
   AND LOWER(wrk.rms_sku_num) = LOWER(rms_sku_num))
GROUP BY purchase_order_num,
 rms_sku_num;


UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_external_in_transit_fact SET
 po_distributed_location_num = src.max_distribute_loc_num FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.po_dist_location_num_wrk_stg
  AS src
WHERE LOWER(jwn_external_in_transit_fact.purchase_order_num) = LOWER(src.purchase_order_num) AND LOWER(jwn_external_in_transit_fact
    .rms_sku_num) = LOWER(src.rms_sku_num) AND jwn_external_in_transit_fact.po_distributed_location_num <> src.max_distribute_loc_num;