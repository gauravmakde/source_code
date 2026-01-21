 /* -- SET --   QUERY_BAND = 'App_ID=APP08073; DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG; Task_Name=item_demand_forecasting_store_sales_hist;'*/
DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_store_sale_hist
WHERE
  week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}}; 


INSERT INTO
  `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_store_sale_hist

WITH store AS (
  SELECT
    store_num,
    CASE
      WHEN channel_num IN (110) THEN 'NORDSTROM'
      WHEN channel_num IN (210) THEN 'NORDSTROM_RACK'
      ELSE NULL
  END
    AS channel_brand,
    'STORE' AS selling_channel
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
  WHERE
    channel_num IN (110,210)) ,

  wk AS (
  SELECT
    DISTINCT week_idnt AS week_num,
    CAST(day_date AS DATE) AS day_date,
    CAST(week_start_day_date AS DATE) AS week_start_date
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
  WHERE
    CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND ({{params.end_date}})),


  sku AS (
  SELECT
    DISTINCT rms_sku_num,
    epm_choice_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
  WHERE
    LOWER(channel_country) = LOWER('US'))
  
      
      (
  SELECT
    t10.channel_brand,
    t10.selling_channel,
    t5.epm_choice_num,
    t2.week_num,
    t2.week_start_date,
    t10.store_num,

   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS date)
 AS last_updated_utc,
    CAST(SUM(trans.line_item_quantity) AS FLOAT64) AS demand_quantity,
    CAST(SUM(CASE
          WHEN LOWER(trans.line_item_fulfillment_type) = LOWER('StoreTake') THEN trans.line_item_quantity
          ELSE 0
      END
        ) AS FLOAT64) AS store_take_demand,
    CAST(SUM(CASE
          WHEN LOWER(trans.line_item_fulfillment_type) NOT LIKE LOWER('%DropShip') THEN trans.line_item_quantity
          ELSE 0
      END
        ) AS FLOAT64) AS owned_demand,
    CAST(SUM(CASE
          WHEN trans.employee_discount_amt > 0 THEN trans.line_item_quantity
          ELSE 0
      END
        ) AS FLOAT64) AS employee_demand,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('GMT')) AS DATETIME) AS dw_sys_load_tmstp
  FROM
    wk AS t2
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS trans
  ON
    t2.day_date = trans.tran_date
  INNER JOIN
    sku AS t5
  ON
    LOWER(trans.sku_num) = LOWER(t5.rms_sku_num)
  INNER JOIN
    store AS t10
  ON
    trans.intent_store_num = t10.store_num
  WHERE
    LOWER(trans.line_item_activity_type_desc) = LOWER('SALE')
    AND LOWER(trans.line_item_merch_nonmerch_ind) = LOWER('MERCH')
    AND trans.line_item_quantity > 0
    AND t5.epm_choice_num IS NOT NULL
  GROUP BY
    t10.channel_brand,
    t10.selling_channel,
    t5.epm_choice_num,
    t2.week_num,
    t2.week_start_date,
    t10.store_num,
    last_updated_utc,
    dw_sys_load_tmstp); 

