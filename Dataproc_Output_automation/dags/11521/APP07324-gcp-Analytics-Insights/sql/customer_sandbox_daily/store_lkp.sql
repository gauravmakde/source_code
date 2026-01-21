-- CREATE OR REPLACE PROCEDURE t2dl_das_strategy.input_bteq_21()
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
---Task_Name=store_lkp_build;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS store_mapping
CLUSTER BY store_num
AS
SELECT DISTINCT cli.store_num,
 cli.store_region,
 jsd.store_dma_desc,
 jsd.store_dma_code AS store_dma_num,
 cli.channel,
 cli.channel_country,
 cli.banner
FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_line_items AS cli
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.jwn_store_dim_vw AS jsd ON cli.store_num = jsd.store_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS region_mapping
CLUSTER BY store_region
AS
SELECT sm.store_region,
 MAX(sd.region_num) AS store_region_num
FROM store_mapping AS sm
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON sm.store_num = sd.store_num
GROUP BY sm.store_region;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.store_lkp;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.store_lkp
(SELECT DISTINCT COALESCE(sm.store_num, - 1) AS store_num,
    FORMAT('%11d', sm.store_num) || ' - ' || sd.store_name AS store_desc,
  sm.store_dma_desc,
  COALESCE(sm.store_dma_num, - 1) AS store_dma_num,
  COALESCE(sm.store_region, 'Unknown') AS store_region_desc,
  COALESCE(rm.store_region_num, - 1) AS store_region_num,
  COALESCE(sm.channel, 'Unknown') AS channel_desc,
  COALESCE(CAST(CASE WHEN SUBSTR(sm.channel, 0, 1) = '' THEN '0'  ELSE SUBSTR(sm.channel, 0, 1)
     END AS INTEGER), - 1) AS channel_num,
  COALESCE(sm.banner, 'Unknown') AS banner_desc,
   CASE
   WHEN LOWER(COALESCE(sm.banner, 'Unknown')) = LOWER('NORDSTROM')
   THEN 1
   WHEN LOWER(COALESCE(sm.banner, 'Unknown')) = LOWER('RACK')
   THEN 2
   ELSE 3
   END AS banner_num,
  COALESCE(sm.channel_country, 'Unknown') AS channel_country_desc,
   CASE
   WHEN LOWER(COALESCE(sm.channel_country, 'Unknown')) = LOWER('US')
   THEN 1
   WHEN LOWER(COALESCE(sm.channel_country, 'Unknown')) = LOWER('CA')
   THEN 2
   ELSE 3
   END AS channel_country_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM store_mapping AS sm
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON sm.store_num = sd.store_num
  LEFT JOIN region_mapping AS rm ON LOWER(sm.store_region) = LOWER(rm.store_region));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
