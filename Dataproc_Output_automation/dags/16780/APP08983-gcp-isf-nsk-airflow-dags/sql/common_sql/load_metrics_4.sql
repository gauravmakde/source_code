
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}') AND batch_id = (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log 
  WITH dummy AS (
    SELECT
        1 AS num
  )
  SELECT
      '{{params.dag_name}}' AS isf_dag_nm,
      s.step_nm,
      (
        SELECT
            batch_id
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE upper(rtrim(subject_area_nm, ' ')) = '{{params.subject_area}}'
      ) AS batch_id,
      s.tbl_nm,
      s.metric_nm,
      s.metric_value,
      s.metric_tmstp,
      s.metric_tmstp_tz,
      CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS DATE) AS metric_date
    FROM
      (
        SELECT
            'DAG start' AS step_nm,
            CAST(NULL as STRING) AS tbl_nm,
            'ISF_DAG_START_TMSTP' AS metric_nm,
            SUBSTR(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS STRING), 1, 60) AS metric_value,
            CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
            `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Spark job' AS step_nm,
            CAST(NULL as STRING) AS tbl_nm,
            'SPARK_JOB_START_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Spark job' AS step_nm,
            CAST(NULL as STRING) AS tbl_nm,
            'SPARK_JOB_END_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'DAG end' AS step_nm,
            CAST(NULL as STRING) AS tbl_nm,
            'ISF_DAG_END_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Spark job' AS step_nm,
            CAST(NULL as STRING) AS tbl_nm,
            'SPARK_PROCESSED_MESSAGE_CNT' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Spark job' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name2}}' AS tbl_nm,
            'LOADED_TO_CSV_CNT' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'TPT step' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name2}}' AS tbl_nm,
            'TPT_JOB_START_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'TPT step' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name2}}' AS tbl_nm,
            'TPT_JOB_END_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'TPT step' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name2}}' AS tbl_nm,
            'LOADED_TO_LDG_CNT' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Fact table load' AS step_nm,
            '{{params.dbenv}}_{{params.fct_table_name2}}' AS tbl_nm,
            'TERADATA_JOB_START_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Fact table load' AS step_nm,
            '{{params.dbenv}}_{{params.fct_table_name2}}' AS tbl_nm,
            'TERADATA_JOB_END_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Spark job' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name}}' AS tbl_nm,
            'LOADED_TO_CSV_CNT' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'TPT step' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name}}' AS tbl_nm,
            'TPT_JOB_START_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'TPT step' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name}}' AS tbl_nm,
            'TPT_JOB_END_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'TPT step' AS step_nm,
            '{{params.dbenv}}_{{params.ldg_table_name}}' AS tbl_nm,
            'LOADED_TO_LDG_CNT' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Fact table load' AS step_nm,
            '{{params.dbenv}}_{{params.fct_table_name}}' AS tbl_nm,
            'TERADATA_JOB_START_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
        UNION ALL
        SELECT
            'Fact table load' AS step_nm,
            '{{params.dbenv}}_{{params.fct_table_name}}' AS tbl_nm,
            'TERADATA_JOB_END_TMSTP' AS metric_nm,
            CAST(NULL as STRING) AS metric_value,
            CAST(NULL as TIMESTAMP) AS metric_tmstp,
            CAST(NULL AS STRING) as metric_tmstp_tz
          FROM
            dummy
      ) AS s
;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.processed_data_spark_log
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}');