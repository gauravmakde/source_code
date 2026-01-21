BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_online_sales_merch;'*/
---     FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_sale_merch
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_online_sale_merch
WITH store AS (SELECT DISTINCT store_num,
   channel_num,
   channel_desc,
   channel_brand,
   selling_channel
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
  WHERE LOWER(channel_country) = LOWER('US')
   AND LOWER(selling_channel) <> LOWER('UNKNOWN')
   AND channel_num IN (120, 250)), 
   
   wk AS (SELECT DISTINCT week_idnt AS week_num,
   week_start_day_date AS week_start_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
  WHERE week_start_day_date BETWEEN {{params.start_date}} AND ({{params.end_date}})) 
  
  (SELECT t2.channel_brand,
   t2.selling_channel,
   h.epm_choice_num,
   t6.week_num,
   t6.week_start_date,
   s.store_num,
   SUM(s.gross_sales_tot_units) AS gross_sls_ttl_u,
   SUM(s.gross_sales_tot_regular_units) AS gross_sls_ttl_reg_u,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
   INNER JOIN store AS t2 ON s.store_num = t2.store_num
   INNER JOIN wk AS t6 ON s.week_num = t6.week_num
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_in_season_margin_forecasts.inseason_hierarchy AS h ON LOWER(h.sku_idnt) = LOWER(s.rms_sku_num)
  GROUP BY t2.channel_brand,
   t2.selling_channel,
   h.epm_choice_num,
   t6.week_num,
   t6.week_start_date,
   s.store_num,
   dw_sys_load_tmstp);


--COLLECT STATISTICS  COLUMN (week_start_date), COLUMN (epm_choice_num), COLUMN (channel_brand), COLUMN (selling_channel), COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel) on t2dl_das_inv_position_forecast.IDF_ONLINE_SALE_MERCH;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
