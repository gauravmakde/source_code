
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_return_to_vendor_kafka_to_teradata;
---Task_Name=job_rtv_third_exec_004;'*/
--ET;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RETURN_TO_VENDOR_FCT',  '{{params.dbenv}}_NAP_FCT',  'mfpc_return_to_vendor_kafka_to_teradata',  'job_rtv_third_exec_004',  0,  'LOAD_START',  'Delete delta data from fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'MERCH_NAP_RTV_DLY');


DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_return_to_vendor_fct AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_return_to_vendor_ldg AS stg
    WHERE LOWER(tgt.rtv_id) = LOWER(rtv_id) AND LOWER(last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND tgt.last_updated_time_in_millis < CAST(TRUNC(CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS FLOAT64)) AS BIGINT));


DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_return_to_vendor_ldg AS stg
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_return_to_vendor_fct AS tgt
    WHERE LOWER(stg.rtv_id) = LOWER(rtv_id) AND LOWER(stg.last_updated_time_in_millis) <> LOWER('LAST_UPDATED_TIME_IN_MILLIS') AND CAST(TRUNC(CAST(CASE WHEN stg.last_updated_time_in_millis = '' THEN '0' ELSE stg.last_updated_time_in_millis END AS FLOAT64)) AS BIGINT) <= last_updated_time_in_millis);


INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_fact}}.merch_return_to_vendor_fct (rtv_id, last_updated_time_in_millis, last_updated_time,last_updated_time_tz, event_time,event_time_tz,
 event_id, location_num, department_num, class_num, subclass_num, sku_type, sku_num, transaction_date, transaction_code
 , rp_ind, quantity, total_cost_currency_code, total_cost_amount, total_retail_currency_code, total_retail_amount,
 dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz)
(SELECT rtv_id,
  CAST(last_updated_time_in_millis AS BIGINT) AS last_updated_time_in_millis,
  CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(trunc(cast(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END as float64)) AS BIGINT)) AS TIMESTAMP) AS last_updated_time,
      `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast( CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(trunc(cast(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END as float64)) AS BIGINT)) AS TIMESTAMP) as string)) as last_updated_time_tz,
  CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(event_time) AS TIMESTAMP) AS event_time,--NORD_UDF
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(event_time) AS TIMESTAMP) as string)) as event_time_tz,
  event_id,
  CAST(TRUNC(CAST(location_num AS FLOAT64)) AS INTEGER) AS location_num,
  CAST(TRUNC(CAST(department_num AS FLOAT64)) AS INTEGER) AS department_num,
  CAST(TRUNC(CAST(class_num AS FLOAT64)) AS INTEGER) AS class_num,
  CAST(TRUNC(CAST(subclass_num AS FLOAT64)) AS INTEGER) AS subclass_num,
  sku_type,
  sku_num,
  CAST(transaction_date AS DATE) AS transaction_date,
  transaction_code,
  'N',
  CAST(TRUNC(CAST(quantity AS FLOAT64)) AS INTEGER) AS quantity,
  total_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN total_cost_units = ''
       THEN '0'
       ELSE total_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN total_cost_nanos = ''
        THEN '0'
        ELSE total_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_amount,
  total_retail_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN total_retail_units = ''
       THEN '0'
       ELSE total_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN total_retail_nanos = ''
        THEN '0'
        ELSE total_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_amount,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() AS dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() as dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_return_to_vendor_ldg
 WHERE LOWER(event_id) <> LOWER('EVENT_ID')
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY rtv_id, event_id ORDER BY CAST(CASE
         WHEN CAST(last_updated_time_in_millis AS STRING) = ''
         THEN '0'
         ELSE cast(last_updated_time_in_millis as string)
         END AS BIGINT) DESC)) = 1);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_return_to_vendor_fct  mrtvf 
SET RP_IND = 'Y'
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_rp_sku_loc_dim_hist  rp
WHERE LOWER(mrtvf.RP_IND) = LOWER('N') 
and LOWER(rp.RMS_SKU_NUM) = LOWER(mrtvf.SKU_NUM) 
and mrtvf.LOCATION_NUM = rp.LOCATION_NUM
and RANGE_CONTAINS(rp.RP_PERIOD,mrtvf.TRANSACTION_DATE)
and mrtvf.TRANSACTION_DATE between
(SELECT START_REBUILD_DATE from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw as tran_vw1
where LOWER(tran_vw1.INTERFACE_CODE) =LOWER('MERCH_NAP_RTV_DLY'))
and (SELECT END_REBUILD_DATE from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw as tran_vw2
where LOWER(tran_vw2.INTERFACE_CODE) =LOWER('MERCH_NAP_RTV_DLY'));


--ET;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_RETURN_TO_VENDOR_FCT',  '{{params.dbenv}}_NAP_FCT',  'mfpc_return_to_vendor_kafka_to_teradata',  'job_rtv_third_exec_004',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'MERCH_NAP_RTV_DLY');

/*SET QUERY_BAND = NONE FOR SESSION;*/
--ET;