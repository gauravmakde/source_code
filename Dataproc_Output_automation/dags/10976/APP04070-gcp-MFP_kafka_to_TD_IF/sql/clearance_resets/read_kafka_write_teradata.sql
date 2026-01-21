SET QUERY_BAND = 'App_ID=app08001;DAG_ID=clearance_reset_10794_DASCLM_smart_markdown;Task_Name=job_2_kafka_to_ldg;'
FOR SESSION VOLATILE;

CREATE TEMPORARY VIEW temp_clearance_resets_object_model AS
SELECT * FROM kafka_clearance_markdown_reset_object_model_avro;

CREATE TEMPORARY VIEW kafka_clearance_reset_object_model_avro_extract_columns AS
SELECT
  id AS reset_id,
  rmsSkuId AS rms_sku_num,
  rmsStyleId AS rms_style_num,
  nrfColorCode AS color_num,
  sellingCountry AS channel_country,
  sellingBrand AS channel_brand,
  sellingChannel AS selling_channel,
  effectiveTime AS effective_tmstp,
  CASE
    WHEN user.type = 'SYSTEM'   THEN user.systemId
    WHEN user.type = 'EMPLOYEE' THEN user.employee.id
    ELSE user.type
  END AS user_id,
  CASE
    WHEN user.type = 'EMPLOYEE' THEN user.employee.idType
    ELSE user.type
  END AS user_type,
  lastUpdatedTime AS last_updated_tmstp
FROM temp_clearance_resets_object_model;

INSERT INTO CLEARANCE_MARKDOWN_RESET_LDG
(
  reset_id,
  rms_sku_num,
  rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel,
  effective_tmstp,
  user_id,
  user_type,
  last_updated_tmstp
)
SELECT DISTINCT
  CAST(reset_id AS STRING),
  CAST(rms_sku_num AS STRING),
  CAST(rms_style_num AS STRING),
  CAST(color_num AS STRING),
  CAST(channel_country AS STRING),
  CAST(channel_brand AS STRING),
  CAST(selling_channel AS STRING),
  CAST(effective_tmstp AS STRING),
  CAST(user_id AS VARCHAR(100)),
  CAST(user_type AS STRING),
  CAST(last_updated_tmstp AS STRING)
FROM kafka_clearance_reset_object_model_avro_extract_columns;
