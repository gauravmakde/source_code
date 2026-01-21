begin
CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS(
WITH RankedMonths AS (
    SELECT month_start_day_date 
	FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw wcal
	JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup ebdl
	ON  wcal.month_start_day_date <= ebdl.dw_batch_dt
	WHERE ebdl.interface_code = 'SMD_INSIGHTS_WKLY'
	GROUP BY month_start_day_date 
	QUALIFY RANK() OVER (  ORDER BY month_start_day_date DESC) = 6) 
    SELECT 
        (SELECT month_start_day_date 
         FROM RankedMonths)  AS last_sun_6mons,
        DATE_SUB(dw_batch_dt, INTERVAL 27 DAY) AS last_sun_4wks,
        DATE_SUB(dw_batch_dt, INTERVAL 13 DAY) AS last_sun_2wks,
        DATE_SUB(dw_batch_dt, INTERVAL 6 DAY) AS last_sun_1wk,
        dw_batch_dt AS last_sat
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
        WHERE LOWER(interface_code) = LOWER('SMD_INSIGHTS_WKLY')
);

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact -- `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fac
SET
  total_sales_amt_1wk     = src.total_sales_amt_1wk,
  total_sales_units_1wk   = src.total_sales_units_1wk,
  total_sales_amt_2wks    = src.total_sales_amt_2wks,
  total_sales_units_2wks  = src.total_sales_units_2wks,
  total_sales_amt_4wks    = src.total_sales_amt_4wks,
  total_sales_units_4wks  = src.total_sales_units_4wks,
  total_sales_amt_6mons   = src.total_sales_amt_6mons,
  total_sales_units_6mons = src.total_sales_units_6mons,
  dw_sys_updt_tmstp       = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS STRING))
FROM
(
    SELECT total_sales_by_interval.rms_style_num
        , total_sales_by_interval.color_num
        , total_sales_by_interval.channel_country
        , total_sales_by_interval.channel_brand
        , total_sales_by_interval.selling_channel
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_1wk from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_amt
            END) AS total_sales_amt_1wk
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_1wk from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_units
            END) AS total_sales_units_1wk
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_2wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_amt
            END) AS total_sales_amt_2wks
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_2wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_units
            END) AS total_sales_units_2wks
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_4wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_amt
            END) AS total_sales_amt_4wks
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_4wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_units
            END) AS total_sales_units_4wks
        , SUM(total_sales_by_interval.total_sales_amt) AS total_sales_amt_6mons
        , SUM(total_sales_by_interval.total_sales_units) AS total_sales_units_6mons
    FROM (
      SELECT cmibwf.rms_style_num
          , cmibwf.color_num
          , cmibwf.channel_country
          , cmibwf.channel_brand
          , cmibwf.selling_channel
          , COALESCE(isibdf.total_sales_amt, 0) AS total_sales_amt
          , COALESCE(isibdf.total_sales_units, 0) AS total_sales_units
          , isibdf.metrics_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
      JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact  AS isibdf
            ON COALESCE(cmibwf.rms_style_num, 'NA') = COALESCE(isibdf.rms_style_num, 'NA')
          AND COALESCE(cmibwf.color_num, 'NA')     = COALESCE(isibdf.color_num, 'NA')
          AND lower(cmibwf.channel_country) = lower(isibdf.channel_country)
          AND lower(cmibwf.channel_brand)   = lower(isibdf.channel_brand)
          AND lower(cmibwf.selling_channel) = lower(isibdf.selling_channel)
      WHERE cmibwf.snapshot_date = (SELECT last_SAT from my_params)
      AND isibdf.metrics_date BETWEEN (SELECT last_SUN_6mons from my_params) AND (SELECT last_SAT from my_params)
    ) AS total_sales_by_interval
    GROUP BY 1,2,3,4,5
    UNION ALL
    -- OMNI
    SELECT total_sales_by_interval.rms_style_num
        , total_sales_by_interval.color_num
        , total_sales_by_interval.channel_country
        , total_sales_by_interval.channel_brand
        , 'OMNI' as selling_channel
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_1wk from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_amt
            END) AS total_sales_amt_1wk
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_1wk from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_units
            END) AS total_sales_units_1wk
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_2wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_amt
            END) AS total_sales_amt_2wks
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_2wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_units
            END) AS total_sales_units_2wks
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_4wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_amt
            END) AS total_sales_amt_4wks
        , SUM(CASE
            WHEN total_sales_by_interval.metrics_date BETWEEN (SELECT last_SUN_4wks from my_params) AND (SELECT last_SAT from my_params)
            THEN total_sales_by_interval.total_sales_units
            END) AS total_sales_units_4wks
        , SUM(total_sales_by_interval.total_sales_amt) AS total_sales_amt_6mons
        , SUM(total_sales_by_interval.total_sales_units) AS total_sales_units_6mons
    FROM (
        SELECT
            cmibwf.rms_style_num
            , cmibwf.color_num
            , cmibwf.channel_country
            , cmibwf.channel_brand
            , cmibwf.selling_channel
            , COALESCE(isibdf.total_sales_amt, 0) AS total_sales_amt
            , COALESCE(isibdf.total_sales_units, 0) AS total_sales_units
            , isibdf.metrics_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
        JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact  AS isibdf
            ON COALESCE(cmibwf.rms_style_num, 'NA') = COALESCE(isibdf.rms_style_num, 'NA')
            AND COALESCE(cmibwf.color_num, 'NA')     = COALESCE(isibdf.color_num, 'NA')
            AND lower(cmibwf.channel_country) = lower(isibdf.channel_country)
            AND lower(cmibwf.channel_brand)   = lower(isibdf.channel_brand)
            AND lower(cmibwf.selling_channel) = lower(isibdf.selling_channel)
        WHERE NOT (lower(cmibwf.channel_country) = lower('CA') AND lower(cmibwf.channel_brand) = lower('NORDSTROM_RACK'))
        AND cmibwf.snapshot_date = (SELECT last_SAT from my_params)
        AND isibdf.metrics_date BETWEEN (SELECT last_SUN_6mons from my_params) AND (SELECT last_SAT from my_params)
    ) AS total_sales_by_interval GROUP BY 1,2,3,4,5
) AS src
WHERE CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.snapshot_date = (SELECT last_SAT from my_params)
AND COALESCE(src.rms_style_num, 'NA') = COALESCE(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.rms_style_num, 'NA')
AND COALESCE(src.color_num, 'NA')     = COALESCE(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.color_num, 'NA')
AND lower(src.channel_country) = lower(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.channel_country)
AND lower(src.channel_brand)   = lower(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.channel_brand)
AND lower(src.selling_channel) = lower(CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.selling_channel);

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
  total_sales_amt_1wk     = 0,
  total_sales_units_1wk   = 0,
  total_sales_amt_2wks    = 0,
  total_sales_units_2wks  = 0,
  total_sales_amt_4wks    = 0,
  total_sales_units_4wks  = 0,
  total_sales_amt_6mons   = 0,
  total_sales_units_6mons = 0,
  dw_sys_updt_tmstp       =  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS STRING))
WHERE CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.snapshot_date = (SELECT last_SAT from my_params)
AND CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT.total_inv_qty = 0;



-- downstream requests total_sales_units_since_last_markdown has no NULL values
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET total_sales_units_since_last_markdown = 0
WHERE total_sales_units_since_last_markdown IS NULL;


end;