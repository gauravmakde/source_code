DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WHERE
    LOWER(RTRIM (isf_dag_nm)) = LOWER(
        RTRIM (
            'nrlt_dc_internal_transfer_lifecycle_16753_TECH_SC_NAP_insights'
        )
    )
    AND batch_id = (
        SELECT
            batch_id + 1
        FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE
            LOWER(RTRIM (subject_area_nm)) = LOWER(RTRIM ('{{params.subject_area}}'))
    );

INSERT INTO
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log (
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
WITH
    dummy AS (
        SELECT
            1 AS num
    )
SELECT
    '{{params.dag_name}}' AS isf_dag_nm,
    step_nm,
    (
        SELECT
            batch_id + 1
        FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE
            LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')
    ) AS batch_id,
    tbl_nm,
    metric_nm,
    metric_value,
    metric_tmstp,
    metric_tmstp_tz,
    CURRENT_DATE('GMT') AS metric_date
FROM
    (
        SELECT
            SUBSTR ('DAG start', 1, 100) AS step_nm,
            SUBSTR (NULL, 1, 100) AS tbl_nm,
            SUBSTR ('ISF_DAG_START_TMSTP', 1, 100) AS metric_nm,
            SUBSTR (
                CAST(CURRENT_DATETIME ('GMT') AS STRING
                ),
                1,
                60) AS metric_value,
             cast(CURRENT_DATETIME ('GMT') as timestamp)  AS metric_tmstp,
             'GMT'  AS metric_tmstp_tz,

        FROM
            dummy AS d
        UNION ALL
        SELECT
            SUBSTR ('Spark job', 1, 100) AS step_nm,
            SUBSTR (NULL, 1, 100) AS tbl_nm,
            SUBSTR ('SPARK_JOB_START_TMSTP', 1, 100) AS metric_nm,
            SUBSTR (NULL, 1, 60) AS metric_value,
            cast(NULL as timestamp) AS metric_tmstp,
            cast(NULL as string) AS metric_tmstp_tz,
        FROM
            dummy AS d
        UNION ALL
        SELECT
            SUBSTR ('Spark job', 1, 100) AS step_nm,
            SUBSTR (NULL, 1, 100) AS tbl_nm,
            SUBSTR ('SPARK_JOB_END_TMSTP', 1, 100) AS metric_nm,
            SUBSTR (NULL, 1, 60) AS metric_value,
            cast(NULL as timestamp) AS metric_tmstp,
            cast(NULL as string) AS metric_tmstp_tz,
        FROM
            dummy AS d
        UNION ALL
        SELECT
            SUBSTR ('DAG end', 1, 100) AS step_nm,
            SUBSTR (NULL, 1, 100) AS tbl_nm,
            SUBSTR ('ISF_DAG_END_TMSTP', 1, 100) AS metric_nm,
            SUBSTR (NULL, 1, 60) AS metric_value,
           cast(NULL as timestamp) AS metric_tmstp,
            cast(NULL as string) AS metric_tmstp_tz,
        FROM
            dummy AS d
        UNION ALL
        SELECT
            SUBSTR ('Spark job', 1, 100) AS step_nm,
            SUBSTR (NULL, 1, 100) AS tbl_nm,
            SUBSTR ('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
            SUBSTR (NULL, 1, 60) AS metric_value,
           cast(NULL as timestamp) AS metric_tmstp,
            cast(NULL as string) AS metric_tmstp_tz,
        FROM
            dummy AS d
        UNION ALL
        SELECT
            SUBSTR ('TPT step', 1, 100) AS step_nm,
            SUBSTR (
                '{{params.dbenv}}_nap_stg.DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_LDG',
                1,
                100
            ) AS tbl_nm,
            SUBSTR ('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
            SUBSTR (NULL, 1, 60) AS metric_value,
           cast(NULL as timestamp) AS metric_tmstp,
            cast(NULL as string) AS metric_tmstp_tz,
        FROM
            dummy AS d
        UNION ALL
        SELECT
            SUBSTR ('Fact table load', 1, 100) AS step_nm,
            SUBSTR (
                '{{params.dbenv}}_nap_fct.DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT',
                1,
                100
            ) AS tbl_nm,
            SUBSTR ('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
            SUBSTR (NULL, 1, 60) AS metric_value,
            cast(NULL as timestamp) AS metric_tmstp,
            cast(NULL as string) AS metric_tmstp_tz,
        FROM
            dummy AS d
        UNION ALL
        SELECT
            SUBSTR ('Fact table load', 1, 100) AS step_nm,
            SUBSTR (
                '{{params.dbenv}}_nap_fct.DC_INTERNAL_TRANSFER_SHIPMENT_RECEIPT_FACT',
                1,
                100
            ) AS tbl_nm,
            SUBSTR ('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
            SUBSTR (NULL, 1, 60) AS metric_value,
            cast(NULL as timestamp) AS metric_tmstp,
            cast(NULL as string) AS metric_tmstp_tz,
        FROM
            dummy AS d
    ) AS s;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}');

call `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_NRTL_CONTROL_START_LOAD ('{{params.subject_area}}');
