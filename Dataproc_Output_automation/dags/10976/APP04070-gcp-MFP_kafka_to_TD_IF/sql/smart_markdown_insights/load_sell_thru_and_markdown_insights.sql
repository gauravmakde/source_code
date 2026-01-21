-- SET QUERY_BAND = 'App_ID=app04070;DAG_ID=smartmarkdown_insights_10976_tech_nap_merch;Task_Name=run_load_sell_thru_and_markdown_insights;'
-- FOR SESSION VOLATILE;
--ET;
-- NOTE: this script has the following dependencies:
-- total_sales_units_1wk & total_sales_units_Xwks which are loaded in the sales script so this script must always be executed after load_additional_sales_insights.sql
-- regular_price_amt which is loaded in the aggregates script so this script must always be executed after load_aggregates.sql


-- store params so only 1 reference



CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS
SELECT dw_batch_dt AS last_sat
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
WHERE LOWER(interface_code) = LOWER('SMD_INSIGHTS_WKLY');
-- required for DDL statements
-- ET;

-- load SELL_THRU 1 wk, 2 wks, 4 wks & 6 mons

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact AS clearance_markdown_insights_by_week_fact0 
SET
 sell_thru_1wk = CAST(IFNULL(src.sell_thru_1_wk, 0) AS NUMERIC),
 sell_thru_2wks = CAST(IFNULL(src.sell_thru_2_wks, 0) AS NUMERIC),
 sell_thru_4wks = CAST(IFNULL(src.sell_thru_4_wks, 0) AS NUMERIC),
 sell_thru_6mons = CAST(IFNULL(src.sell_thru_6_mons, 0) AS NUMERIC),
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS
  TIMESTAMP) ,
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
  FROM (SELECT rms_style_num,
   color_num,
   channel_country,
   channel_brand,
   selling_channel,
    CASE
    WHEN ABS(total_sales_units_1wk + total_inv_qty) = 0
    THEN 0.00
    ELSE CAST(total_sales_units_1wk AS NUMERIC) / ABS(total_sales_units_1wk + total_inv_qty)
    END AS sell_thru_1_wk,
    CASE
    WHEN ABS(total_sales_units_2wks + total_inv_qty) = 0
    THEN 0.00
    ELSE CAST(total_sales_units_2wks AS NUMERIC) / ABS(total_sales_units_2wks + total_inv_qty)
    END AS sell_thru_2_wks,
    CASE
    WHEN ABS(total_sales_units_4wks + total_inv_qty) = 0
    THEN 0.00
    ELSE CAST(total_sales_units_4wks AS NUMERIC) / ABS(total_sales_units_4wks + total_inv_qty)
    END AS sell_thru_4_wks,
    CASE
    WHEN ABS(total_sales_units_6mons + total_inv_qty) = 0
    THEN 0.00
    ELSE CAST(total_sales_units_6mons AS NUMERIC) / ABS(total_sales_units_6mons + total_inv_qty)
    END AS sell_thru_6_mons
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact
  WHERE snapshot_date = (SELECT *
     FROM my_params)) AS src
WHERE clearance_markdown_insights_by_week_fact0.snapshot_date = (SELECT *
       FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact0
        .rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact0
       .color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact0.channel_country
     ) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact0.channel_brand) AND LOWER(src.selling_channel
   ) = LOWER(clearance_markdown_insights_by_week_fact0.selling_channel);

-- ensure better performance
-- COLLECT STATISTICS
--    COLUMN(snapshot_date),
--    COLUMN(rms_style_num),
--    COLUMN(color_num),
--    COLUMN(channel_country),
--    COLUMN(channel_brand),
--    COLUMN(selling_channel)
-- ON {db_env}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT;

-- -- required for DDL statements
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact SET
 sell_thru_since_last_markdown = CAST(IFNULL(src.sell_thru_since_last_markdown, 0) AS NUMERIC),
 total_sales_units_since_last_markdown = src.total_sales_units,
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS
  TIMESTAMP),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
   FROM (SELECT cmibwf.rms_style_num,
   cmibwf.color_num,
   cmibwf.channel_country,
   cmibwf.channel_brand,
   cmibwf.selling_channel,
   IFNULL(SUM(isibdf.total_sales_units), 0) AS total_sales_units,
    CASE
    WHEN ABS(IFNULL(SUM(isibdf.total_sales_units), 0) + cmibwf.total_inv_qty) = 0
    THEN 0.00
    ELSE CAST(IFNULL(SUM(isibdf.total_sales_units), 0) AS NUMERIC) / ABS(IFNULL(SUM(isibdf.total_sales_units), 0) +
       cmibwf.total_inv_qty)
    END AS sell_thru_since_last_markdown
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_style_color_vw AS cmstcw ON LOWER(COALESCE(cmibwf.rms_style_num, 'NA'
         )) = LOWER(COALESCE(cmstcw.rms_style_num, 'NA')) AND LOWER(COALESCE(CASE
          WHEN LOWER(cmibwf.color_num) IN (LOWER('000'), LOWER('0'))
          THEN '0'
          ELSE LTRIM(cmibwf.color_num, '0')
          END, 'NA')) = LOWER(COALESCE(cmstcw.color_num, 'NA')) AND LOWER(cmibwf.channel_country) = LOWER(cmstcw.channel_country
       ) AND LOWER(cmibwf.channel_brand) = LOWER(cmstcw.channel_brand)
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact AS isibdf ON LOWER(COALESCE(cmibwf.rms_style_num,
          'NA')) = LOWER(COALESCE(isibdf.rms_style_num, 'NA')) AND LOWER(COALESCE(cmibwf.color_num, 'NA')) = LOWER(COALESCE(isibdf
          .color_num, 'NA')) AND LOWER(cmibwf.channel_country) = LOWER(isibdf.channel_country) AND LOWER(cmibwf.channel_brand
       ) = LOWER(isibdf.channel_brand) AND LOWER(cmibwf.selling_channel) = LOWER(isibdf.selling_channel)
  WHERE cmibwf.snapshot_date = (SELECT *
     FROM my_params)
   AND isibdf.metrics_date BETWEEN DATE_TRUNC(DATE_ADD(CAST(cmstcw.last_clearance_markdown_date_utc AS DATE), INTERVAL 1
      WEEK), WEEK(SUNDAY)) AND (SELECT *
     FROM my_params)
  GROUP BY cmibwf.rms_style_num,
   cmibwf.color_num,
   cmibwf.channel_country,
   cmibwf.channel_brand,
   cmibwf.selling_channel,
   cmibwf.total_inv_qty) AS src
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT *
       FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact
        .rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact
       .color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country
     ) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(src.selling_channel
   ) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt (snapshot_date, rms_style_num, color_num,
 channel_country, channel_brand, selling_channel, regular_price_amt)
(SELECT cmibwf.snapshot_date,
  cmibwf.rms_style_num,
   CASE WHEN LOWER(cmibwf.color_num) IN (LOWER('0'), LOWER('000'))
   THEN '0'
   ELSE LTRIM(cmibwf.color_num, '0')
   END AS color_num,
  cmibwf.channel_country,
  cmibwf.channel_brand,
  cmibwf.selling_channel,
  cmibwf.regular_price_amt
 FROM 
 `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl 
  ON cmibwf.snapshot_date = etl.dw_batch_dt
 WHERE LOWER(etl.interface_code) = LOWER('SMD_INSIGHTS_WKLY'));


--COLLECT STATISTICS  ON TEMPORARY `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt 


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt AS tgt SET
 first_clearance_markdown_date = CAST(src.first_clearance_markdown_date_utc AS DATE),
 last_clearance_markdown_date = CAST(src.last_clearance_markdown_date_utc AS DATE),
 clearance_markdown_version = src.clearance_markdown_version,
 clearance_markdown_percent_off = CAST(src.clearance_markdown_percent_off AS NUMERIC),
 clearance_markdown_state = 'DECLARED',
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
 FROM (SELECT cmibwf.snapshot_date
   ,
   cmibwf.rms_style_num,
   cmibwf.color_num,
   cmibwf.channel_country,
   cmibwf.channel_brand,
   cmibwf.selling_channel,
   cmibwf.regular_price_amt,
   cmscv.first_clearance_markdown_date_utc,
   cmscv.last_clearance_markdown_date_utc,
   COALESCE(cmscv.clearance_markdown_version, 0) AS clearance_markdown_version,
   cmf.clearance_price_amt,
    CASE
    WHEN cmibwf.regular_price_amt IS NULL OR cmibwf.regular_price_amt = 0
    THEN NULL
    ELSE TRUNC((cmibwf.regular_price_amt - cmf.clearance_price_amt) / CAST(cmibwf.regular_price_amt AS NUMERIC), 2)
    END AS clearance_markdown_percent_off
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt AS cmibwf
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_style_color_vw AS cmscv 
   ON LOWER(cmscv.rms_style_num) = LOWER(cmibwf.rms_style_num
         ) AND LOWER(cmscv.color_num) = LOWER(cmibwf.color_num) 
         AND LOWER(cmscv.channel_country) = LOWER(cmibwf.channel_country
        ) AND LOWER(cmscv.channel_brand) = LOWER(cmibwf.channel_brand) 
        AND LOWER(cmscv.selling_channel) = LOWER(cmibwf.selling_channel
      )
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_fact_backfilled_vw AS cmf 
   ON LOWER(cmscv.rms_style_num) = LOWER(cmf.rms_style_num
          ) AND LOWER(cmscv.color_num) = LOWER(cmf.color_num) 
          AND LOWER(cmscv.channel_country) = LOWER(cmf.channel_country
         ) AND LOWER(cmscv.channel_brand) = LOWER(cmf.channel_brand) 
         AND LOWER(cmscv.selling_channel) = LOWER(cmf.selling_channel
       ) AND cmscv.last_clearance_markdown_date_utc = CAST(cmf.effective_begin_tmstp AS TIMESTAMP)
  GROUP BY cmibwf.snapshot_date,
   cmibwf.rms_style_num,
   cmibwf.color_num,
   cmibwf.channel_country,
   cmibwf.channel_brand,
   cmibwf.selling_channel,
   cmibwf.regular_price_amt,
   cmscv.first_clearance_markdown_date_utc,
   cmscv.last_clearance_markdown_date_utc,
   clearance_markdown_version,
   cmf.clearance_price_amt,
   clearance_markdown_percent_off) AS src
WHERE src.snapshot_date = tgt.snapshot_date 
AND LOWER(src.rms_style_num) = LOWER(tgt.rms_style_num) 
AND LOWER(src.color_num
     ) = LOWER(tgt.color_num) 
     AND LOWER(src.channel_country) = LOWER(tgt.channel_country) 
     AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt AS tgt SET
 future_clearance_markdown_date = CAST(src.future_clearance_markdown_date_utc AS DATE),
 future_clearance_markdown_price = src.future_clearance_markdown_price,
 future_clearance_markdown_percent_off = CAST(src.future_clearance_markdown_percent_off AS NUMERIC),
 clearance_markdown_state = 'DECLARED',
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
 FROM (SELECT cmibwf.snapshot_date,
   cmibwf.rms_style_num,
   cmibwf.color_num,
   cmibwf.channel_country,
   cmibwf.channel_brand,
   cmibwf.selling_channel,
   cmibwf.regular_price_amt,
   cmf.future_clearance_markdown_date_utc,
   cmf.future_clearance_price_amt AS future_clearance_markdown_price,
    CASE
    WHEN cmibwf.regular_price_amt IS NULL OR cmibwf.regular_price_amt = 0
    THEN NULL
		-- TRUNC & CAST are used here to maintain 2 decimal places vs rounding (.99 to 1)
    ELSE TRUNC((cmibwf.regular_price_amt - cmf.future_clearance_price_amt) / CAST(cmibwf.regular_price_amt AS NUMERIC),
     2)
    END AS future_clearance_markdown_percent_off
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt AS cmibwf
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_future_fact AS cmf ON LOWER(cmibwf.rms_style_num) = LOWER(cmf.rms_style_num
          ) AND LOWER(cmibwf.color_num) = LOWER(cmf.color_num) AND LOWER(cmibwf.channel_country) = LOWER(cmf.channel_country
         ) AND LOWER(cmibwf.channel_brand) = LOWER(cmf.channel_brand) AND LOWER(cmibwf.selling_channel) = LOWER(cmf.selling_channel
       ) AND cmibwf.snapshot_date = cmf.snapshot_date
  GROUP BY cmibwf.snapshot_date,
   cmibwf.rms_style_num,
   cmibwf.color_num,
   cmibwf.channel_country,
   cmibwf.channel_brand,
   cmibwf.selling_channel,
   cmibwf.regular_price_amt,
   cmf.future_clearance_markdown_date_utc,
   future_clearance_markdown_price) AS src
WHERE src.snapshot_date = tgt.snapshot_date AND LOWER(src.rms_style_num) = LOWER(tgt.rms_style_num) AND LOWER(src.color_num
     ) = LOWER(tgt.color_num) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) =
  LOWER(tgt.channel_brand);



-- COLLECT STATISTICS  ON TEMPORARY {db_env}_NAP_STG.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_GTT ; 
-- ET;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact AS tgt SET
 first_clearance_markdown_date = gtt.first_clearance_markdown_date,
 last_clearance_markdown_date = gtt.last_clearance_markdown_date,
 clearance_markdown_version = gtt.clearance_markdown_version,
 clearance_markdown_percent_off = gtt.clearance_markdown_percent_off,
 clearance_markdown_state = 'DECLARED',
 future_clearance_markdown_date = gtt.future_clearance_markdown_date,
 future_clearance_markdown_price = gtt.future_clearance_markdown_price,
 future_clearance_markdown_percent_off = gtt.future_clearance_markdown_percent_off,
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP)
 ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.clearance_markdown_insights_by_week_gtt AS gtt
WHERE LOWER(tgt.rms_style_num) = LOWER(gtt.rms_style_num) AND LOWER(CASE
         WHEN LOWER(tgt.color_num) IN (LOWER('0'), LOWER('000'))
         THEN '0'
         ELSE LTRIM(tgt.color_num, '0')
         END) = LOWER(gtt.color_num) AND LOWER(tgt.channel_country) = LOWER(gtt.channel_country) AND LOWER(tgt.channel_brand
      ) = LOWER(gtt.channel_brand) AND LOWER(tgt.selling_channel) = LOWER(gtt.selling_channel) AND tgt.snapshot_date =
   gtt.snapshot_date AND gtt.dw_sys_updt_tmstp IS NOT NULL;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact AS tgt
 SET
 clearance_markdown_version = 0 
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
WHERE tgt.clearance_markdown_version IS NULL 
AND tgt.snapshot_date = etl.dw_batch_dt 
AND LOWER(etl.interface_code) = LOWER('SMD_INSIGHTS_WKLY');


-- -- ensure better performance
-- COLLECT STATISTICS
--    COLUMN(snapshot_date),
--    COLUMN(rms_style_num),
--    COLUMN(color_num),
--    COLUMN(channel_country),
--    COLUMN(channel_brand),
--    COLUMN(selling_channel)
-- ON {db_env}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT;

-- required for DDL statements
-- load OMNI SELL_THRU since LAST_MARKDOWN
-- attempted using last_clearance_markdown_date from above in order to eliminate join to CLEARANCE_MARKDOWN_STYLE_COLOR_VW but no results

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact SET
 sell_thru_since_last_markdown = CAST(IFNULL(src.sell_thru_since_last_markdown_omni, 0) AS NUMERIC),
 total_sales_units_since_last_markdown = src.total_sales_units,
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS
  TIMESTAMP) 
   ,dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM (SELECT rms_style_num,
   color_num,
   channel_country,
   channel_brand,
   selling_channel,
   SUM(`A12187`) AS total_sales_units,
    CASE
    WHEN ABS(SUM(`A12187`) + SUM(`A12188`)) = 0
    THEN 0.00
    ELSE CAST(SUM(`A12187`) AS NUMERIC) / ABS(SUM(`A12187`) + SUM(`A12188`))
    END AS sell_thru_since_last_markdown_omni
  FROM (SELECT cmibwf.rms_style_num,
     cmibwf.color_num,
     cmibwf.channel_country,
     cmibwf.channel_brand,
     'OMNI' AS selling_channel,
     IFNULL(IFNULL(SUM(isibdf.total_sales_units), 0), 0) AS `A12187`,
     IFNULL(cmibwf.total_inv_qty, 0) AS `A12188`
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_style_color_vw AS cmstcw ON LOWER(COALESCE(cmibwf.rms_style_num,
           'NA')) = LOWER(COALESCE(cmstcw.rms_style_num, 'NA')) AND LOWER(COALESCE(CASE
            WHEN LOWER(cmibwf.color_num) IN (LOWER('000'), LOWER('0'))
            THEN '0'
            ELSE LTRIM(cmibwf.color_num, '0')
            END, 'NA')) = LOWER(COALESCE(cmstcw.color_num, 'NA')) AND LOWER(cmibwf.channel_country) = LOWER(cmstcw.channel_country
         ) AND LOWER(cmibwf.channel_brand) = LOWER(cmstcw.channel_brand)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact AS isibdf ON LOWER(COALESCE(cmibwf.rms_style_num,
            'NA')) = LOWER(COALESCE(isibdf.rms_style_num, 'NA')) AND LOWER(COALESCE(cmibwf.color_num, 'NA')) = LOWER(COALESCE(isibdf
            .color_num, 'NA')) AND LOWER(cmibwf.channel_country) = LOWER(isibdf.channel_country) AND LOWER(cmibwf.channel_brand
         ) = LOWER(isibdf.channel_brand) AND LOWER(cmibwf.selling_channel) = LOWER(isibdf.selling_channel)
    WHERE cmibwf.snapshot_date = (SELECT *
       FROM my_params)
     AND isibdf.metrics_date BETWEEN DATE_TRUNC(DATE_ADD(CAST(cmstcw.last_clearance_markdown_date_utc AS DATE), INTERVAL 1
        WEEK), WEEK(SUNDAY)) AND (SELECT *
       FROM my_params)
    GROUP BY cmibwf.rms_style_num,
     cmibwf.color_num,
     cmibwf.channel_country,
     cmibwf.channel_brand,
     cmibwf.selling_channel,
     cmibwf.total_inv_qty
    HAVING LOWER(cmibwf.channel_country) <> LOWER('CA') OR LOWER(cmibwf.channel_brand) <> LOWER('NORDSTROM_RACK')) AS t3
   
  GROUP BY rms_style_num,
   color_num,
   channel_country,
   channel_brand,
   selling_channel) AS src
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT *
       FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact
        .rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact
       .color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country
     ) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(src.selling_channel
   ) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact  SET
 sell_thru_since_last_markdown = 0
WHERE sell_thru_since_last_markdown IS NULL;

-- ensure better performance
-- COLLECT STATISTICS
--    COLUMN(snapshot_date),
--    COLUMN(rms_style_num),
--    COLUMN(color_num),
--    COLUMN(channel_country),
--    COLUMN(channel_brand),
--    COLUMN(selling_channel)
-- ON {db_env}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT;

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;
