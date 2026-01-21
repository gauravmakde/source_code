/*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : dc_internal_transfer_shipment_receipt_fact_load.sql
-- Author            : Dmytro Utte
-- Description       : Merge DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT table with delta from staging table
-- Source topic      : inventory-distribution-center-internal-transfer-analytical-avro
-- Object model      : DistributionCenterInternalTransfer
-- ETL Run Frequency : Every hour
-- Version :         : 0.2
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-25  Utte Dmytro   FA-9925: Migrate NRLT DAG for DC_INTERNAL_TRANSFER to IsF
-- 2024-02-07  Andrii Skyba  FA-11561: Optimize NRLT DAGs
--************************************************************************************************************************************/
/***********************************************************************************
-- Load temp table with metrics
 ************************************************************************************/
CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk AS
SELECT
    isf_dag_nm,
    step_nm,
    batch_id,
    tbl_nm,
    metric_nm,
    metric_value,
    metric_tmstp_utc,
    metric_tmstp_tz,
    metric_date
FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
LIMIT
    0;

INSERT INTO
    isf_dag_control_log_wrk (
        isf_dag_nm,
        step_nm,
        batch_id,
        tbl_nm,
        metric_nm,
        metric_value,
        metric_tmstp_utc,
        metric_tmstp_tz,
        metric_date
    )
WITH
    dummy AS (
        SELECT
            'nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
            (
                SELECT
                    batch_id 
                FROM
                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
                WHERE
                    LOWER(subject_area_nm) = LOWER('NAP_ASCP_DC_INTERNAL_TRANSFER')
            ) AS batch_id,
            CAST(CURRENT_DATETIME ('GMT') AS timestamp) AS metric_tmstp,
            'GMT' as metric_tmstp_tz,
            CURRENT_DATE('GMT') AS metric_date
    )
SELECT
    isf_dag_nm,
    cast(NULL as string) AS step_nm,
    batch_id,
    SUBSTR (
        '{{params.dbenv}}_nap_fct.DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT',
        1,
        100
    ) AS tbl_nm,
    SUBSTR ('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
    SUBSTR (CAST(metric_tmstp AS STRING), 1, 60) AS metric_value,
    metric_tmstp,
    metric_tmstp_tz,
    metric_date
FROM
    dummy AS d
UNION ALL
SELECT
    isf_dag_nm,
    cast(NULL as string) AS step_nm,
    batch_id,
    SUBSTR (
        '{{params.dbenv}}_nap_stg.DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_LDG',
        1,
        100
    ) AS tbl_nm,
    SUBSTR ('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
    SUBSTR (
        (
            SELECT
                metric_value
            FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
            WHERE
                LOWER(isf_dag_nm) = LOWER(
                    'nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights'
                )
                AND LOWER(tbl_nm) = LOWER('DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_LDG')
                AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')
        ),
        1,
        60
    ) AS metric_value,
    metric_tmstp,
    metric_tmstp_tz,
    metric_date
FROM
    dummy AS d
UNION ALL
SELECT
    isf_dag_nm,
    cast(NULL as string) AS step_nm,
    batch_id,
    SUBSTR (NULL, 1, 100) AS tbl_nm,
    SUBSTR ('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
    SUBSTR (
        (
            SELECT
                metric_value
            FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
            WHERE
                LOWER(isf_dag_nm) = LOWER(
                    'nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights'
                )
                AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')
        ),
        1,
        60
    ) AS metric_value,
    metric_tmstp,
    metric_tmstp_tz,
    metric_date
FROM
    dummy AS d;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.dc_internal_transfer_shipment_receipt_fact AS fact USING (
    SELECT
        transfer_id,
        transfer_type,
        purchase_order_num,
        shipment_num,
        shipment_num_type,
        route_num,
        lane_num,
        load_num,
        carton_lpn,
        CAST(CONCAT (shipped_time, '+00:00') AS timestamp) AS shipped_time,
        '+00:00' as shipped_time_tz,
        CAST(CONCAT (receipt_time, '+00:00') AS timestamp) AS receipt_time,
        '+00:00' as receipt_time_tz,
        pallet_lpn,
        ship_from_location_id,
        NULLIF(TRIM(ship_to_location_id), '') AS ship_to_location_id,
        originating_location_id,
        destination_location_id,
        warehouse_shipment_stage,
        warehouse_receipt_stage,
        sku_id,
        sku_type,
        CASE
            WHEN LOWER(reason_codes) = LOWER('[]') THEN NULL
            ELSE reason_codes
        END AS reason_codes,
        shipment_qty,
        receipt_qty,
        (
            SELECT
                batch_id
            FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE
                LOWER(subject_area_nm) = LOWER('NAP_ASCP_DC_INTERNAL_TRANSFER')
        ) AS dw_batch_id,
        (
            SELECT
                curr_batch_date
            FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE
                LOWER(subject_area_nm) = LOWER('NAP_ASCP_DC_INTERNAL_TRANSFER')
        ) AS dw_batch_date
    FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.dc_internal_transfer_shipment_receipt_ldg QUALIFY (
            ROW_NUMBER() OVER (
                PARTITION BY
                    transfer_id,
                    transfer_type,
                    carton_lpn,
                    sku_id,
                    COALESCE(ship_from_location_id, ''),
                    COALESCE(
                        CAST(
                            FORMAT_TIMESTAMP (
                                '%F %H:%M:%E6S',
                                CAST(CONCAT (shipped_time, '+00:00') AS DATETIME)
                            ) AS DATETIME
                        ),
                        CAST(
                            FORMAT_TIMESTAMP (
                                '%F %H:%M:%E6S',
                                CAST(CONCAT (receipt_time, '+00:00') AS DATETIME)
                            ) AS DATETIME
                        )
                    )
                ORDER BY
                    CAST(
                        FORMAT_TIMESTAMP (
                            '%F %H:%M:%E6S',
                            CAST(CONCAT (shipped_time, '+00:00') AS DATETIME)
                        ) AS DATETIME
                    ) DESC
            )
        ) = 1
) AS t4 ON LOWER(fact.transfer_id) = LOWER(t4.transfer_id)
AND LOWER(fact.transfer_type) = LOWER(t4.transfer_type)
AND LOWER(fact.carton_lpn) = LOWER(t4.carton_lpn)
AND LOWER(fact.sku_id) = LOWER(t4.sku_id)
AND LOWER(COALESCE(fact.ship_from_location_id, '')) = LOWER(COALESCE(t4.ship_from_location_id, ''))
AND CAST(COALESCE(fact.shipped_time, fact.receipt_time) AS timestamp
) = t4.shipped_time WHEN MATCHED THEN
UPDATE
SET
    purchase_order_num = t4.purchase_order_num,
    shipment_num = t4.shipment_num,
    shipment_num_type = t4.shipment_num_type,
    route_num = t4.route_num,
    lane_num = t4.lane_num,
    load_num = t4.load_num,
    ship_to_location_id = t4.ship_to_location_id,
    receipt_qty = t4.receipt_qty,
    shipment_qty = t4.shipment_qty,
    original_location_id = t4.originating_location_id,
    destination_location_id = t4.destination_location_id,
    warehouse_shipment_stage = t4.warehouse_shipment_stage,
    warehouse_receipt_stage = t4.warehouse_receipt_stage,
    sku_type = t4.sku_type,
    pallet_lpn = t4.pallet_lpn,
    reason_code = t4.reason_codes,
    shipped_time = t4.shipped_time,
    shipped_time_tz = t4.shipped_time_tz,
    receipt_time = t4.receipt_time,
    receipt_time_tz = t4.receipt_time_tz,
    dw_batch_id = t4.dw_batch_id,
    dw_batch_date = t4.dw_batch_date,
    dw_sys_updt_tmstp = CAST( CURRENT_DATETIME ('PST8PDT') AS TIMESTAMP),
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST () 
    
WHEN NOT MATCHED THEN INSERT
(
        transfer_id,
        transfer_type,
        carton_lpn,
		sku_id,
        sku_type,
        purchase_order_num,
        shipment_num,
        shipment_num_type,
        route_num,
        lane_num,
        load_num,
		shipped_time,
        shipped_time_tz,
		receipt_time,
        receipt_time_tz,
        pallet_lpn,
        ship_to_location_id,
        ship_from_location_id,
        original_location_id,
        destination_location_id,
		warehouse_shipment_stage,
		warehouse_receipt_stage,
        shipment_qty,
		receipt_qty,
        reason_code,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_load_tmstp_tz,
        dw_sys_updt_tmstp,
        dw_sys_updt_tmstp_tz
    )
    VALUES(
        t4.transfer_id,
        t4.transfer_type,
        t4.carton_lpn,
        t4.sku_id,
        t4.sku_type,
        t4.purchase_order_num,
        t4.shipment_num,
        t4.shipment_num_type,
        t4.route_num,
        t4.lane_num,
        t4.load_num,
        t4.shipped_time,
        t4.shipped_time_tz,
        t4.receipt_time,
        t4.receipt_time_tz,
        t4.pallet_lpn,
        t4.ship_to_location_id,
        t4.ship_from_location_id,
        t4.originating_location_id,
        t4.destination_location_id,
        t4.warehouse_shipment_stage,
        t4.warehouse_receipt_stage,
        t4.shipment_qty,
        t4.receipt_qty,
        t4.reason_codes,
        t4.dw_batch_id,
        t4.dw_batch_date,
        dw_sys_updt_tmstp = CAST( CURRENT_DATETIME ('PST8PDT') AS TIMESTAMP),
        `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST (),
        dw_sys_updt_tmstp = CAST( CURRENT_DATETIME ('PST8PDT') AS TIMESTAMP),
        `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST ()
    );

--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT INDEX (transfer_id, transfer_type, carton_lpn, sku_id)
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1 (
    'DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_LDG_TO_BASE',
    'NAP_ASCP_DC_INTERNAL_TRANSFER',
    '{{params.dbenv}}_NAP_BASE_VWS',
    'DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_LDG',
    '{{params.dbenv}}_NAP_FCT',
    'DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT',
    'Count_Distinct',
    0,
    'T-S',
    "concat(transfer_id, ''-'', transfer_type, ''-'', carton_lpn, ''-'', sku_id)",
    "concat(transfer_id, ''-'', transfer_type, ''-'', carton_lpn, ''-'', sku_id)",
    null,
    null,
    'Y'
);

INSERT INTO
    isf_dag_control_log_wrk (
        SELECT
            'nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights' AS isf_dag_nm,
            CAST(NULL AS STRING) AS step_nm,
            (
                SELECT
                    batch_id
                FROM
                    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
                WHERE
                    LOWER(subject_area_nm) = LOWER('NAP_ASCP_DC_INTERNAL_TRANSFER')
            ) AS batch_id,
            SUBSTR (
                '{{params.dbenv}}_nap_fct.DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT',
                1,
                100
            ) AS tbl_nm,
            SUBSTR ('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
            cast(CURRENT_DATETIME ('GMT') AS STRING) AS metric_value,
            CAST( CURRENT_DATETIME ('GMT') AS TIMESTAMP) AS metric_tmstp,
            'GMT' AS metric_tmstp_tz,
            CURRENT_DATE('GMT') AS metric_date
    );

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log
SET
    metric_value = wrk.metric_value,
    metric_tmstp = wrk.metric_tmstp_utc,
    metric_tmstp_tz = wrk.metric_tmstp_tz
FROM
    isf_dag_control_log_wrk AS wrk
WHERE
    LOWER(isf_dag_control_log.isf_dag_nm) = LOWER('{{params.dag_name}}')
    AND LOWER(COALESCE(isf_dag_control_log.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, ''))
    AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm)
    AND isf_dag_control_log.batch_id = (
        SELECT
            batch_id
        FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE
            LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')
    );
