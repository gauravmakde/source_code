begin 
CREATE or replace TEMPORARY TABLE  `ISF_DAG_CONTROL_LOG_WRK` AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ISF_DAG_CONTROL_LOG;


INSERT INTO `ISF_DAG_CONTROL_LOG_WRK`
SELECT SUBSTR('{{params.dag_name}}', 1, 100) AS isf_dag_nm,
 SUBSTR('Fact table load', 1, 100) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.fct_table_name}}', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 CAST(CURRENT_DATETIME ('GMT') AS string) AS metric_value,
            CAST(CURRENT_DATETIME ('GMT') AS datetime) AS metric_tmstp,
            CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS
  metric_tmstp_utc,
      'GMT' as metric_tmstp_tz,
      CURRENT_DATE('GMT') AS metric_date
UNION ALL
SELECT SUBSTR('{{params.dag_name}}', 1, 100) AS isf_dag_nm,
 SUBSTR(step_nm, 1, 100) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR(tbl_nm, 1, 100) AS tbl_nm,
 SUBSTR(metric_nm, 1, 100) AS metric_nm,
 SUBSTR(metric_value, 1, 60) AS metric_value,
 CAST(SUBSTR(metric_tmstp, 0, 19) AS DATETIME) AS metric_tmstp,
  CAST(SUBSTR(metric_tmstp, 0, 19) AS TIMESTAMP) AS metric_tmstp_utc,
 jwn_udf.default_tz_pst() as metric_tmstp_tz,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(metric_tmstp AS DATETIME)) AS DATETIME) AS DATE) AS metric_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 SUBSTR('LDG load step', 1, 100) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.ldg_table_name}}', 1, 60) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST(COUNT(*) AS STRING), 1, 60) AS metric_value,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', timestamp(current_datetime('PST8PDT'))) AS DATETIME) AS metric_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', timestamp(current_datetime('PST8PDT'))) AS TIMESTAMP) AS metric_tmstp_utc,
  jwn_udf.default_tz_pst() as metric_tmstp_tz,
 CURRENT_DATE AS metric_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_shipment_discrepancy_ldg;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('{{params.subject_area}}',  '{{params.dbenv}}_NAP_FCT',  '{{params.dag_name}}',  'ldg_to_fact',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),  '{{params.subject_area}}');


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.ascp_error_ldg (target_table, unique_key, reason, dw_batch_id, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz)
(SELECT 'PURCHASE_ORDER_SHIPMENT_DISCREPANCY_FACT',
     'ExceptionId: ' || exception_id || ', EventName: ' || event_name,
  'Revision ExceptionId or EventName are NULL',
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
   'PST8PDT' as dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
   'PST8PDT' as dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_shipment_discrepancy_ldg
 WHERE exception_id IS NULL
  OR event_name IS NULL);

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.ascp_error_ldg (target_table, unique_key, reason, dw_batch_id, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz)
(SELECT 'PURCHASE_ORDER_SHIPMENT_DISCREPANCY_FACT',
     'ExceptionId: ' || exception_id || ', EventName: ' || event_name,
  'VendorNumber is NULL',
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
   'PST8PDT' as dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
   'PST8PDT' as dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_shipment_discrepancy_ldg
 WHERE vendor_number IS NULL);

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.purchase_order_shipment_discrepancy_fact AS fact
USING (SELECT DISTINCT exception_id, vendor_number, event_name, event_time,event_time_tz, event_date_pacific, event_datetime_pacific, event_datetime_pacific_tz,external_purchase_order_id, purchase_order_id, item_detail_rms_sku_id, item_detail_upc_number, item_detail_units_in_discrepancy, cross_reference_rms_purchase_order_id, units_accepted, point_of_identification, discrepancy_identification_source_system, advanced_shipment_notice_id, vendor_bill_of_landing, freight_bill_id, receiving_location_id, actual_ship_location_id, expected_ship_locations, actual_purchase_order_id, actual_vendor_ship_date, actual_distribute_locations, units_refused, actual_pack_type, actual_vendor_number, (SELECT batch_id
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER_SHIPMENT_DISCREPANCY')) AS dw_batch_id, 
            CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_load_tmstp, 
            `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as dw_sys_load_tmstp_tz,
            CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS timestamp) AS dw_sys_updt_tmstp,
           `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as dw_sys_updt_tmstp_tz,
            (SELECT curr_batch_date
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER_SHIPMENT_DISCREPANCY')) AS dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.purchase_order_shipment_discrepancy_ldg
    WHERE event_name IS NOT NULL AND vendor_number IS NOT NULL) AS ldg
ON LOWER(ldg.event_name) = LOWER(fact.event_name) AND LOWER(ldg.exception_id) = LOWER(fact.exception_id) AND ldg.event_time = fact.event_time AND LOWER(COALESCE(ldg.actual_distribute_locations, 'N/A')) = LOWER(COALESCE(fact.actual_distribute_locations, 'N/A')) AND LOWER(COALESCE(ldg.expected_ship_locations, 'N/A')) = LOWER(COALESCE(fact.expected_ship_locations, 'N/A'))
WHEN MATCHED THEN UPDATE SET
    vendor_number = ldg.vendor_number,
    external_purchase_order_id = ldg.external_purchase_order_id,
    purchase_order_id = ldg.purchase_order_id,
    item_detail_rms_sku_id = ldg.item_detail_rms_sku_id,
    item_detail_upc_number = ldg.item_detail_upc_number,
    item_detail_units_in_discrepancy = ldg.item_detail_units_in_discrepancy,
    cross_reference_rms_purchase_order_id = ldg.cross_reference_rms_purchase_order_id,
    units_accepted = ldg.units_accepted,
    point_of_identification = ldg.point_of_identification,
    discrepancy_identification_source_system = ldg.discrepancy_identification_source_system,
    advanced_shipment_notice_id = ldg.advanced_shipment_notice_id,
    vendor_bill_of_landing = ldg.vendor_bill_of_landing,
    freight_bill_id = ldg.freight_bill_id,
    receiving_location_id = ldg.receiving_location_id,
    actual_ship_location_id = ldg.actual_ship_location_id,
    actual_purchase_order_id = ldg.actual_purchase_order_id,
    actual_vendor_ship_date = ldg.actual_vendor_ship_date,
    actual_pack_type = ldg.actual_pack_type,
    actual_vendor_number = ldg.actual_vendor_number,
    units_refused = ldg.units_refused
WHEN NOT MATCHED THEN INSERT VALUES(ldg.exception_id, ldg.vendor_number, ldg.event_name, ldg.event_time,ldg.event_time_tz, ldg.event_datetime_pacific,ldg.event_datetime_pacific_tz, ldg.event_date_pacific, ldg.external_purchase_order_id, ldg.purchase_order_id, ldg.item_detail_rms_sku_id, ldg.item_detail_upc_number, ldg.item_detail_units_in_discrepancy, ldg.cross_reference_rms_purchase_order_id, ldg.units_accepted, ldg.point_of_identification, ldg.discrepancy_identification_source_system, ldg.advanced_shipment_notice_id, ldg.vendor_bill_of_landing, ldg.freight_bill_id, ldg.receiving_location_id, ldg.actual_ship_location_id, ldg.expected_ship_locations, ldg.actual_purchase_order_id, ldg.actual_vendor_ship_date, ldg.actual_distribute_locations, ldg.units_refused, ldg.actual_pack_type, ldg.actual_vendor_number, CAST(ldg.dw_batch_id AS STRING),ldg.dw_sys_load_tmstp,ldg.dw_sys_load_tmstp_tz,ldg.dw_sys_updt_tmstp, ldg.dw_sys_updt_tmstp_tz,ldg.dw_batch_date);



CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.dq_pipeline_audit_v1('PURCHASE_ORDER_SHIPMENT_DISCREPANCY_LDG_TO_BASE',  '{{params.subject_area}}',  '{{params.dbenv}}_NAP_STG',  'PURCHASE_ORDER_SHIPMENT_DISCREPANCY_LDG',  '{{params.dbenv}}_NAP_FCT',  'PURCHASE_ORDER_SHIPMENT_DISCREPANCY_FACT',  'Count_Distinct',  0,  'T-S',  'exception_id',  'exception_id',  NULL,  NULL,  'Y');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.data_timeliness_metric_fact_ld('{{params.subject_area}}',  '{{params.dbenv}}_NAP_FCT',  '{{params.dag_name}}',  'ldg_to_fact',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),  '{{params.subject_area}}');



INSERT INTO `ISF_DAG_CONTROL_LOG_WRK`
(
isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp,
metric_tmstp_tz,
metric_date
)
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
  'Fact table load' AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  SUBSTR('{{params.fct_table_name}}', 1, 60) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS
  metric_tmstp,
  `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as metric_tmstp_tz,
  CURRENT_DATE AS metric_date);

UPDATE `ISF_DAG_CONTROL_LOG_WRK` AS wrk SET
    metric_value = TRIM(wrk.metric_value),
    metric_tmstp = wrk.metric_tmstp FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER(wrk.isf_dag_nm) AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, '')) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND isf_dag_control_log.batch_id = wrk.batch_id AND LOWER(isf_dag_control_log.step_nm) = LOWER(wrk.step_nm);

end