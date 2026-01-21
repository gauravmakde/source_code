
/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_store_sales_merch;'*/
---     FOR SESSION VOLATILE;

DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_store_sale_merch
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_store_sale_merch
WITH store AS (SELECT DISTINCT store_num,
   channel_num,
   channel_desc,
   channel_brand,
   selling_channel
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
  WHERE LOWER(channel_country) = LOWER('US')
   AND LOWER(selling_channel) <> LOWER('UNKNOWN')
   AND channel_num IN (110, 210)),
   
wk AS (SELECT DISTINCT week_idnt AS week_num,
   CAST(week_start_day_date AS DATE) AS week_start_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
  WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}}) 
  
SELECT t2.channel_brand,
   t2.selling_channel,
   h.epm_choice_num,
   s.week_num,
   t6.week_start_date,
   s.store_num,
   SUM(s.gross_sales_tot_units) AS gross_sls_ttl_u,
   SUM(s.gross_sales_tot_regular_units) AS gross_sls_ttl_reg_u,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
   INNER JOIN store AS t2 ON s.store_num = t2.store_num
   INNER JOIN wk AS t6 ON s.week_num = t6.week_num
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_in_season_margin_forecasts.inseason_hierarchy AS h 
   ON LOWER(h.sku_idnt) = LOWER(s.rms_sku_num)
  GROUP BY t2.channel_brand,
   t2.selling_channel,
   h.epm_choice_num,
   s.week_num,
   t6.week_start_date,
   s.store_num,
   dw_sys_load_tmstp;

