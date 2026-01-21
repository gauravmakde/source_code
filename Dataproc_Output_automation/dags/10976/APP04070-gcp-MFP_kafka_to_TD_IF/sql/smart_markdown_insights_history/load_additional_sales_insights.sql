

-- ET;

-- load_sale_insights.sql just executes within
-- the Airflow task timeout of 30 mins
-- which seems reasonable so remaining sales insights are loaded here

-- store param so only 1 reference

CREATE TEMP TABLE my_params AS
(
    WITH RankedMonths
      AS (
           SELECT
               MONTH_START_DAY_DATE
             FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.MERCH_WEEK_CAL_454_VW AS wcal
             JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP AS ebdl
               ON wcal.MONTH_START_DAY_DATE <= ebdl.DW_BATCH_DT
            WHERE LOWER(ebdl.INTERFACE_CODE) = LOWER('CMD_WKLY')
           GROUP BY MONTH_START_DAY_DATE 
           QUALIFY RANK() OVER (ORDER BY MONTH_START_DAY_DATE DESC) = 6
         )
    SELECT
        (SELECT MONTH_START_DAY_DATE FROM RANKEDMONTHS) AS LAST_SUN_6MONS
      , etl.DW_BATCH_DT - 27 AS LAST_SUN_4WKS
      , etl.DW_BATCH_DT - 13 AS LAST_SUN_2WKS
      , etl.DW_BATCH_DT - 6  AS LAST_SUN_1WK
      , etl.DW_BATCH_DT      AS LAST_SAT
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP AS etl
     WHERE LOWER(etl.INTERFACE_CODE) = LOWER('CMD_WKLY')
);

-- required for DDL statements
-- ET;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
SET TOTAL_SALES_AMT_1WK     = src.TOTAL_SALES_AMT_1WK,
    TOTAL_SALES_UNITS_1WK   = src.TOTAL_SALES_UNITS_1WK,
    TOTAL_SALES_AMT_2WKS    = src.TOTAL_SALES_AMT_2WKS,
    TOTAL_SALES_UNITS_2WKS  = src.TOTAL_SALES_UNITS_2WKS,
    TOTAL_SALES_AMT_4WKS    = src.TOTAL_SALES_AMT_4WKS,
    TOTAL_SALES_UNITS_4WKS  = src.TOTAL_SALES_UNITS_4WKS,
    TOTAL_SALES_AMT_6MONS   = src.TOTAL_SALES_AMT_6MONS,
    TOTAL_SALES_UNITS_6MONS = src.TOTAL_SALES_UNITS_6MONS,
    DW_SYS_UPDT_TMSTP       = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    DW_SYS_UPDT_TMSTP_TZ    = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()

FROM 
     (
       SELECT
           total_sales_by_interval.RMS_STYLE_NUM
         , total_sales_by_interval.COLOR_NUM
         , total_sales_by_interval.CHANNEL_COUNTRY
         , total_sales_by_interval.CHANNEL_BRAND
         , total_sales_by_interval.SELLING_CHANNEL
         , SUM(CASE
                  WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_1wk FROM my_params) AND (SELECT last_SAT FROM my_params)
                  THEN total_sales_by_interval.TOTAL_SALES_AMT
               END) AS TOTAL_SALES_AMT_1WK
         , SUM(CASE
                  WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_1wk FROM my_params) AND (SELECT last_SAT FROM my_params)
                  THEN total_sales_by_interval.TOTAL_SALES_UNITS
               END) AS TOTAL_SALES_UNITS_1WK
         , SUM(CASE
                  WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_2wks FROM my_params) AND (SELECT last_SAT FROM my_params)
                  THEN total_sales_by_interval.TOTAL_SALES_AMT
               END) AS TOTAL_SALES_AMT_2WKS
         , SUM(CASE
                  WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_2wks FROM my_params) AND (SELECT last_SAT FROM my_params)
                  THEN total_sales_by_interval.TOTAL_SALES_UNITS
               END) AS TOTAL_SALES_UNITS_2WKS
         , SUM(CASE
                  WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_4wks FROM my_params) AND (SELECT last_SAT FROM my_params)
                  THEN total_sales_by_interval.TOTAL_SALES_AMT
               END) AS TOTAL_SALES_AMT_4WKS
         , SUM(CASE
                  WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_4wks FROM my_params) AND (SELECT last_SAT FROM my_params)
                  THEN total_sales_by_interval.TOTAL_SALES_UNITS
               END) AS TOTAL_SALES_UNITS_4WKS
         , SUM(total_sales_by_interval.TOTAL_SALES_AMT) AS TOTAL_SALES_AMT_6MONS
         , SUM(total_sales_by_interval.TOTAL_SALES_UNITS) AS TOTAL_SALES_UNITS_6MONS
         FROM (
                SELECT
                    cmibwf.RMS_STYLE_NUM
                  , cmibwf.COLOR_NUM
                  , cmibwf.CHANNEL_COUNTRY
                  , cmibwf.CHANNEL_BRAND
                  , cmibwf.SELLING_CHANNEL
                  , COALESCE(isibdf.TOTAL_SALES_AMT, 0) AS TOTAL_SALES_AMT
                  , COALESCE(isibdf.TOTAL_SALES_UNITS, 0) AS TOTAL_SALES_UNITS
                  , isibdf.METRICS_DATE
                  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_HIST_FACT AS cmibwf
                  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.INVENTORY_SALES_INSIGHTS_BY_DAY_HIST_FACT     AS isibdf
                    ON COALESCE(cmibwf.RMS_STYLE_NUM, 'NA') = COALESCE(isibdf.RMS_STYLE_NUM, 'NA')
                   AND COALESCE(cmibwf.COLOR_NUM, 'NA')     = COALESCE(isibdf.COLOR_NUM, 'NA')
                   AND LOWER(cmibwf.CHANNEL_COUNTRY) = LOWER(isibdf.CHANNEL_COUNTRY)
                   AND LOWER(cmibwf.CHANNEL_BRAND)   = LOWER(isibdf.CHANNEL_BRAND)
                   AND LOWER(cmibwf.SELLING_CHANNEL) = LOWER(isibdf.SELLING_CHANNEL)
                 WHERE cmibwf.SNAPSHOT_DATE = (SELECT last_SAT FROM my_params)
                   AND isibdf.METRICS_DATE BETWEEN (SELECT last_SUN_6mons FROM my_params) AND (SELECT last_SAT FROM my_params)
              ) AS total_sales_by_interval
       GROUP BY 1,2,3,4,5
       UNION ALL
         -- OMNI
       SELECT
           total_sales_by_interval.RMS_STYLE_NUM
         , total_sales_by_interval.COLOR_NUM
         , total_sales_by_interval.CHANNEL_COUNTRY
         , total_sales_by_interval.CHANNEL_BRAND
         , 'OMNI' as SELLING_CHANNEL
         , SUM(CASE
                   WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_1wk from my_params) AND (SELECT last_SAT from my_params)
                   THEN total_sales_by_interval.TOTAL_SALES_AMT
               END) AS TOTAL_SALES_AMT_1WK
         , SUM(CASE
                   WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_1wk from my_params) AND (SELECT last_SAT from my_params)
                   THEN total_sales_by_interval.TOTAL_SALES_UNITS
               END) AS TOTAL_SALES_UNITS_1WK
         , SUM(CASE
                   WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_2wks from my_params) AND (SELECT last_SAT from my_params)
                   THEN total_sales_by_interval.TOTAL_SALES_AMT
               END) AS TOTAL_SALES_AMT_2WKS
         , SUM(CASE
                   WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_2wks from my_params) AND (SELECT last_SAT from my_params)
                   THEN total_sales_by_interval.TOTAL_SALES_UNITS
               END) AS TOTAL_SALES_UNITS_2WKS
         , SUM(CASE
                   WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_4wks from my_params) AND (SELECT last_SAT from my_params)
                   THEN total_sales_by_interval.TOTAL_SALES_AMT
               END) AS TOTAL_SALES_AMT_4WKS
         , SUM(CASE
                   WHEN total_sales_by_interval.METRICS_DATE BETWEEN (SELECT last_SUN_4wks from my_params) AND (SELECT last_SAT from my_params)
                   THEN total_sales_by_interval.TOTAL_SALES_UNITS
               END) AS TOTAL_SALES_UNITS_4WKS
         , SUM(total_sales_by_interval.TOTAL_SALES_AMT) AS TOTAL_SALES_AMT_6MONS
         , SUM(total_sales_by_interval.TOTAL_SALES_UNITS) AS TOTAL_SALES_UNITS_6MONS
         FROM (
                SELECT
                    cmibwf.RMS_STYLE_NUM
                  , cmibwf.COLOR_NUM
                  , cmibwf.CHANNEL_COUNTRY
                  , cmibwf.CHANNEL_BRAND
                  , cmibwf.SELLING_CHANNEL
                  , COALESCE(isibdf.TOTAL_SALES_AMT, 0) AS TOTAL_SALES_AMT
                  , COALESCE(isibdf.TOTAL_SALES_UNITS, 0) AS TOTAL_SALES_UNITS
                  , isibdf.metrics_date
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_HIST_FACT AS cmibwf
                JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.INVENTORY_SALES_INSIGHTS_BY_DAY_HIST_FACT    AS isibdf
                    ON COALESCE(cmibwf.RMS_STYLE_NUM, 'NA') = COALESCE(isibdf.RMS_STYLE_NUM, 'NA')
                    AND COALESCE(cmibwf.COLOR_NUM, 'NA')     = COALESCE(isibdf.COLOR_NUM, 'NA')
                    AND LOWER(cmibwf.CHANNEL_COUNTRY) = LOWER(isibdf.CHANNEL_COUNTRY)
                    AND LOWER(cmibwf.CHANNEL_BRAND)   = LOWER(isibdf.CHANNEL_BRAND)
                    AND LOWER(cmibwf.SELLING_CHANNEL) = LOWER(isibdf.SELLING_CHANNEL)
                WHERE NOT (cmibwf.CHANNEL_COUNTRY = 'CA' AND cmibwf.CHANNEL_BRAND = 'NORDSTROM_RACK')
                AND cmibwf.SNAPSHOT_DATE = (SELECT last_SAT FROM my_params)
                AND isibdf.METRICS_DATE BETWEEN (SELECT last_SUN_6mons FROM my_params) AND (SELECT last_SAT FROM my_params)
             ) AS total_sales_by_interval 
       GROUP BY 1,2,3,4,5
     ) AS src
WHERE tgt.SNAPSHOT_DATE                 = (SELECT last_SAT FROM my_params)
  AND COALESCE(src.RMS_STYLE_NUM, 'NA') = COALESCE(tgt.RMS_STYLE_NUM, 'NA')
  AND COALESCE(src.COLOR_NUM, 'NA')     = COALESCE(tgt.COLOR_NUM, 'NA')
  AND LOWER(src.CHANNEL_COUNTRY)          = LOWER(tgt.CHANNEL_COUNTRY)
  AND LOWER(src.CHANNEL_BRAND)            = LOWER(tgt.CHANNEL_BRAND)
  AND LOWER(src.SELLING_CHANNEL)          = LOWER(tgt.SELLING_CHANNEL)
;

-- populating 0's for missing STORE/ONLINE selling channels

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact SET
 total_sales_amt_1wk = 0,
 total_sales_units_1wk = 0,
 total_sales_amt_2wks = 0,
 total_sales_units_2wks = 0,
 total_sales_amt_4wks = 0,
 total_sales_units_4wks = 0,
 total_sales_amt_6mons = 0,
 total_sales_units_6mons = 0,
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHERE snapshot_date = (SELECT last_sat FROM my_params) AND total_inv_qty = 0;

-- downstream requests total_sales_units_since_last_markdown has no NULL values

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact SET
 total_sales_units_since_last_markdown = 0
WHERE total_sales_units_since_last_markdown IS NULL 
AND snapshot_date = (SELECT last_sat FROM my_params);


-- ET;
-- ET;
