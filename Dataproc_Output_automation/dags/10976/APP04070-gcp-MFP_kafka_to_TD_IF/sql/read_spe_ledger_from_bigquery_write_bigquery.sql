-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=mfpc_spe_ledger_kafka_to_teradata_v1;
-- Task_Name=job_spe_ledger_third_exec_04;'
-- FOR SESSION VOLATILE;

-- ET;

-- This table to Test Teradata engine.

 DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_spe_profitability_expectation_fact tgt
      WHERE EXISTS (SELECT 1 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_spe_profitability_expectation_ldg stg
      WHERE  
          tgt.supplier_group_id = stg.supplier_group_id 
      AND LOWER(CAST(tgt.year_num AS STRING)) = LOWER(stg.year_num) 
      AND LOWER(tgt.selling_country) = LOWER(stg.selling_country) 
      AND LOWER(stg.last_updated_time_in_millis) <> LOWER('last_updated_time_in_millis')   
      AND tgt.last_updated_time_in_millis < Cast(TRUNC(CAST(stg.last_updated_time_in_millis AS FLOAT64)) AS BIGINT));

 DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_spe_profitability_expectation_ldg stg
      WHERE EXISTS (SELECT 1 FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_spe_profitability_expectation_fact tgt
      WHERE 
          tgt.supplier_group_id = stg.supplier_group_id 
      AND LOWER(CAST(tgt.year_num AS STRING)) = LOWER(stg.year_num) 
      AND LOWER(tgt.selling_country) = LOWER(stg.selling_country) 
      AND LOWER(stg.last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS')
      AND Cast(TRUNC(CAST(stg.last_updated_time_in_millis AS FLOAT64)) AS BIGINT)  <= tgt.last_updated_time_in_millis);

 INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_spe_profitability_expectation_fact
        (   event_time,
            event_time_tz,
            supplier_group_id,
            year_num,
            selling_country,
            planned_profitability_expectation_percent,
            last_updated_time_in_millis,
            last_updated_time,
            dw_sys_load_tmstp ,
            initial_markup_expectation_percent,
            regular_price_sales_expectation_percent             
        )
      SELECT
            CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(event_time) AS TIMESTAMP) as event_time,
            `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(event_time)) event_time_tz,
            supplier_group_id,
            CAST(trunc(cast(year_num as float64)) AS INT64) AS year_num,
            selling_country,
            CAST(planned_profitability_expectation_percent AS NUMERIC),        
            CAST(TRUNC(CAST(last_updated_time_in_millis AS FLOAT64)) as BIGINT) as last_updated_time_in_millis,
            CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`( CAST(LAST_UPDATED_TIME_IN_MILLIS  AS BIGINT) ) AS DATETIME) as LAST_UPDATED_TIME,
            current_datetime('PST8PDT') as DW_SYS_LOAD_TMSTP,
            cast(initial_markup_expectation_percent as NUMERIC),
            CAST(regular_price_sales_expectation_percent AS NUMERIC)
       FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_spe_profitability_expectation_ldg
       WHERE LOWER(year_num) <> LOWER('year_num');
--  ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;