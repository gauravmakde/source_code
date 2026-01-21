--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : jwn_init_zero_count_dq_metrics_fact.sql
-- Author                  : Oleksandr Chaichenko
-- Description             : Provide information -  Zero count loaded DQ checks from secure schema
-- Data Source             : NAP_JWN_METRICS_BASE views
-- ETL Run Frequency       : Daily
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-06-05  Oleksandr Chaichenko     FA-8788: Code Refactor - for Ongoing Delta Load in Production
--*************************************************************************************************************************************

/*DELETE all impacted zero count metrics for current day batch*/

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact
WHERE dw_batch_date = (SELECT curr_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AND LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'
    ) AND LOWER(dq_metric_name) IN (LOWER('records_loaded'));

/*insert  metrics for current day batch */
INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact
    	(
            dq_object_name,
            dq_metric_name,
            subject_area_nm,
            dw_batch_date,
            dag_completion_timestamp,
            dq_metric_value,
            dq_metric_value_type,
            is_sensitive,
            dw_record_load_tmstp,
            dw_record_updt_tmstp
        )

    SELECT 'JWN_INVENTORY_SKU_LOC_DAY_FACT' AS dq_object_name,
 'records_loaded' AS dq_metric_name,
 'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
  (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
 NULL AS dag_completion_timestamp,
 0 AS dq_metric_value,
 'TARGET' AS dq_metric_value_type,
 'F' AS is_sensitive,
 current_datetime('PST8PDT') AS dw_record_load_tmstp,
 current_datetime('PST8PDT') AS dw_record_updt_tmstp;

INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact
    	(
            dq_object_name,
            dq_metric_name,
            subject_area_nm,
            dw_batch_date,
            dag_completion_timestamp,
            dq_metric_value,
            dq_metric_value_type,
            is_sensitive,
            dw_record_load_tmstp,
            dw_record_updt_tmstp
        )


    SELECT 'JWN_EXTERNAL_IN_TRANSIT_FACT' AS dq_object_name,
 'records_loaded' AS dq_metric_name,
 'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
  (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
 NULL AS dag_completion_timestamp,
 0 AS dq_metric_value,
 'TARGET' AS dq_metric_value_type,
 'F' AS is_sensitive,
 current_datetime('PST8PDT') AS dw_record_load_tmstp,
 current_datetime('PST8PDT') AS dw_record_updt_tmstp;

INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact
    	(
            dq_object_name,
            dq_metric_name,
            subject_area_nm,
            dw_batch_date,
            dag_completion_timestamp,
            dq_metric_value,
            dq_metric_value_type,
            is_sensitive,
            dw_record_load_tmstp,
            dw_record_updt_tmstp
        )
    SELECT 'JWN_TRANSFERS_RMS_FACT' AS dq_object_name,
 'records_loaded' AS dq_metric_name,
 'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
  (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
 NULL AS dag_completion_timestamp,
 0 AS dq_metric_value,
 'TARGET' AS dq_metric_value_type,
 'F' AS is_sensitive,
 current_datetime('PST8PDT') AS dw_record_load_tmstp,
 current_datetime('PST8PDT') AS dw_record_updt_tmstp;

INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact
    	(
            dq_object_name,
            dq_metric_name,
            subject_area_nm,
            dw_batch_date,
            dag_completion_timestamp,
            dq_metric_value,
            dq_metric_value_type,
            is_sensitive,
            dw_record_load_tmstp,
            dw_record_updt_tmstp
        )

SELECT 'JWN_PURCHASE_ORDER_RMS_RECEIPTS_FACT' AS dq_object_name,
 'records_loaded' AS dq_metric_name,
 'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
  (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
 NULL AS dag_completion_timestamp,
 0 AS dq_metric_value,
 'TARGET' AS dq_metric_value_type,
 'F' AS is_sensitive,
 current_datetime('PST8PDT') AS dw_record_load_tmstp,
 current_datetime('PST8PDT') AS dw_record_updt_tmstp;


