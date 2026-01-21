/*SET QUERY_BAND = 'App_ID=app04070;DAG_ID=smartmarkdown_insights_10976_tech_nap_merch;Task_Name=run_load_sales_insights;'
FOR SESSION VOLATILE;
*/
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

-- load_sale_insights.sql just executes within
-- the Airflow task timeout of 30 mins
-- so DO NOT ADD any SALES insights here
-- any additional SALES insights can be added to load_additional_sale_insights.sql
-- UPDATE AS OF 07/17/2023, the above ^ comment does not apply, and airflow task runs at 15 minutes. --

-- store param so only 1 reference
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS
(
  SELECT dw_batch_dt as last_SAT 
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
  WHERE LOWER(INTERFACE_CODE) = LOWER('SMD_INSIGHTS_WKLY')
);
 
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0; 

CREATE TEMPORARY TABLE IF NOT EXISTS targeted_data
(
  snapshot_date    DATE ,
  rms_style_num    STRING,
  color_num        STRING,
  channel_country  STRING,
  channel_brand    STRING,
  selling_channel  STRING,
  first_sales_date DATE
);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
-- get targeted data
INSERT INTO targeted_data
(
  snapshot_date,
  rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel,
  first_sales_date
)
SELECT
    cmibwf.snapshot_date,
    cmibwf.rms_style_num,
    cmibwf.color_num,
    cmibwf.channel_country,
    cmibwf.channel_brand,
    cmibwf.selling_channel,
    MIN(cmsv.business_day_date) first_sales_date_agg
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_sales_vw AS cmsv 
    ON LOWER(RTRIM(COALESCE(cmibwf.rms_style_num, 'NA'))) = LOWER(RTRIM(COALESCE(cmsv.rms_style_num, 'NA'))) 
      AND LOWER(RTRIM(COALESCE(cmibwf.color_num, 'NA'))) = LOWER(RTRIM(COALESCE(cmsv.color_num, 'NA'))) 
      AND LOWER(RTRIM(cmibwf.channel_country)) = LOWER(RTRIM(cmsv.channel_country)) 
      AND LOWER(RTRIM(cmibwf.channel_brand)) = LOWER(RTRIM(cmsv.channel_brand)) 
      AND LOWER(RTRIM(cmibwf.selling_channel)) = LOWER(RTRIM(cmsv.selling_channel))
WHERE cmibwf.snapshot_date  = (SELECT last_SAT from my_params)
GROUP BY 1,2,3,4,5,6
HAVING first_sales_date_agg IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


-- load first_sales_date & week_sales into final table (ONLINE & STORE)
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
  first_sales_date   = src.first_sales_date,
  weeks_sales        = src.weeks_sales,
  dw_sys_updt_tmstp  = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM
(
  SELECT
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    first_sales_date,
    CAST(EXTRACT(DAY FROM DATE_SUB(CAST(snapshot_date AS DATE), INTERVAL EXTRACT(DAY FROM first_sales_date) DAY )) / 7 AS INT64) AS weeks_sales
    -- (TRUNC(snapshot_date) - TRUNC(first_sales_date))/7 AS weeks_sales
  FROM targeted_data
) AS src
WHERE CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.snapshot_date = (SELECT last_SAT from my_params)
AND COALESCE(src.rms_style_num, 'NA') = COALESCE(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.rms_style_num, 'NA')
AND COALESCE(src.color_num, 'NA')     = COALESCE(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.color_num, 'NA')
AND LOWER(src.channel_country) = LOWER(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.channel_country)
AND LOWER(src.channel_brand)   = LOWER(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.channel_brand)
AND LOWER(src.selling_channel) = LOWER(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

-- OMNI
-- combination of STORE & ONLINE

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
  first_sales_date  = src.first_sales_date_agg,
  weeks_sales       = src.weeks_sales,
  dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM
(
  SELECT
    snapshot_date as snapshot_date_agg,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    MIN(first_sales_date) first_sales_date_agg,
        CAST(EXTRACT ( DAY FROM DATE_SUB(CAST(snapshot_date AS DATE), INTERVAL EXTRACT(DAY FROM MIN(first_sales_date)) DAY )) / 7 AS INT64) AS weeks_sales
  FROM targeted_data
  GROUP BY 1,2,3,4,5
) AS src
WHERE CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.snapshot_date = (SELECT last_SAT from my_params)
AND COALESCE(src.rms_style_num, 'NA') = COALESCE(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.rms_style_num, 'NA')
AND COALESCE(src.color_num, 'NA')     = COALESCE(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.color_num, 'NA')
AND lower(src.channel_country) = lower(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.channel_country)
AND lower(src.channel_brand)   = lower(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.channel_brand)
AND lower(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.selling_channel) = lower('OMNI');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;



END;
