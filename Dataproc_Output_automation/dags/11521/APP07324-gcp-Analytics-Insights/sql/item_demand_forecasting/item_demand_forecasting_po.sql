
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_po;'*/
---     FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_po
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_po
WITH wk AS (SELECT DISTINCT week_idnt AS week_num,
   day_date AS day_date,
   week_start_day_date AS week_start_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
  WHERE week_start_day_date BETWEEN {{params.start_date}} AND ({{params.end_date}})), 
  
  sku AS (SELECT DISTINCT rms_sku_num,
   epm_choice_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
  WHERE LOWER(channel_country) = LOWER('US')), 
  
  store AS (SELECT store_num,
    CASE
    WHEN channel_num IN (110, 120)
    THEN 'NORDSTROM'
    WHEN channel_num IN (210, 250)
    THEN 'NORDSTROM_RACK'
    ELSE NULL
    END AS channel_brand,
    CASE
    WHEN channel_num IN (110, 210)
    THEN 'STORE'
    WHEN channel_num IN (120, 250)
    THEN 'ONLINE'
    ELSE NULL
    END AS selling_channel
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
  WHERE channel_num IN (110, 210, 120, 250)) 
  
  (SELECT t10.channel_brand,
   'ONLINE' AS selling_channel,
   t5.epm_choice_num,
   t2.week_num,
   t2.week_start_date,
   CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS DATE) AS last_updated_utc,
   SUM(CASE
     WHEN LOWER(t10.selling_channel) = LOWER('ONLINE')
     THEN alloc.allocated_qty
     ELSE 0
     END) AS allocated_qty_online,
   SUM(CASE
      WHEN LOWER(t10.selling_channel) = LOWER('ONLINE')
      THEN alloc.received_qty
      ELSE 0
      END) AS received_qty_online,
   SUM(CASE
     WHEN LOWER(t10.selling_channel) = LOWER('STORE')
     THEN alloc.allocated_qty
     ELSE 0
     END) AS allocated_qty_store,
   SUM(CASE
      WHEN LOWER(t10.selling_channel) = LOWER('STORE')
      THEN alloc.received_qty
      ELSE 0
      END) AS received_qty_store,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS alloc
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact AS pof ON LOWER(alloc.purchase_order_num) = LOWER(pof.purchase_order_num
     )
   INNER JOIN wk AS t2 ON pof.open_to_buy_endofweek_date = t2.day_date
   INNER JOIN sku AS t5 ON LOWER(alloc.rms_sku_num) = LOWER(t5.rms_sku_num)
   INNER JOIN store AS t10 ON alloc.distribute_location_id = t10.store_num
  WHERE LOWER(pof.status) <> LOWER('Worksheet')
   AND alloc.unit_cost_amt > 0
   AND alloc.distribution_id <> 0
   AND pof.start_ship_date IS NOT NULL
   AND t5.epm_choice_num IS NOT NULL
   AND alloc.unit_cost_amt IS NOT NULL
  GROUP BY t10.channel_brand,
   selling_channel,
   t5.epm_choice_num,
   t2.week_num,
   t2.week_start_date,
   last_updated_utc,
   dw_sys_load_tmstp);



--COLLECT STATISTICS  COLUMN (week_start_date), COLUMN (epm_choice_num), COLUMN (channel_brand), COLUMN (selling_channel), COLUMN (week_start_date, epm_choice_num, channel_brand, selling_channel) on t2dl_das_inv_position_forecast.IDF_PO;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
