-- SET QUERY_BAND = 'App_ID=app04070;DAG_ID=clearance_markdown_stock_quantity_history_10976_tech_nap_merch;Task_Name=run_load_inv_fact;'
-- FOR SESSION VOLATILE;

-- ET;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_stock_quantity_end_of_wk_hist_fact (rms_style_num, color_num, channel_country
 , channel_brand, selling_channel, inventory_units, inventory_cost, inventory_dollars)
(SELECT rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel,
  SUM(total_stock_on_hand_qty + total_in_transit_qty) AS inventory_units,
  AVG(weighted_average_cost) AS inventory_cost,
  SUM((total_stock_on_hand_qty + total_in_transit_qty) * regular_price_amt) AS inventory_dollars
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_hist_fact AS isibdf
 WHERE metrics_date = (SELECT dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('CMD_STOCK_QNTY_WKLY'))
  AND (LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(store_type_code) IN (LOWER('FC'), LOWER('LH'), LOWER('OC'),
       LOWER('OF'), LOWER('RK')) OR LOWER(selling_channel) = LOWER('STORE') AND LOWER(store_type_code) IN (LOWER('FL'),
       LOWER('NL'), LOWER('RK'), LOWER('SS'), LOWER('VS')))
 GROUP BY rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel
 HAVING inventory_units > 0 AND inventory_dollars > 0);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_stock_quantity_end_of_wk_hist_fact (rms_style_num, color_num, channel_country
 , channel_brand, selling_channel, inventory_units, inventory_cost, inventory_dollars)
(SELECT rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  'OMNI',
  SUM(total_stock_on_hand_qty + total_in_transit_qty) AS inventory_units,
  AVG(weighted_average_cost) AS inventory_cost,
  SUM((total_stock_on_hand_qty + total_in_transit_qty) * regular_price_amt) AS inventory_dollars
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_hist_fact AS isibdf
 WHERE metrics_date = (SELECT dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('CMD_STOCK_QNTY_WKLY'))
  AND (LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(store_type_code) IN (LOWER('FC'), LOWER('LH'), LOWER('OC'),
       LOWER('OF'), LOWER('RK')) OR LOWER(selling_channel) = LOWER('STORE') AND LOWER(store_type_code) IN (LOWER('FL'),
       LOWER('NL'), LOWER('RK'), LOWER('SS'), LOWER('VS')))
 GROUP BY rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  5
 HAVING inventory_units > 0 AND inventory_dollars > 0);

-- COLLECT STATISTICS
--     COLUMN(RMS_STYLE_NUM)
--   , COLUMN(COLOR_NUM)
--   , COLUMN(CHANNEL_COUNTRY)
--   , COLUMN(CHANNEL_BRAND)
--   , COLUMN(SELLING_CHANNEL)
--   , COLUMN(RMS_STYLE_NUM, COLOR_NUM, CHANNEL_COUNTRY, CHANNEL_BRAND, SELLING_CHANNEL)
-- ON PRD_NAP_FCT.CLEARANCE_MARKDOWN_STOCK_QUANTITY_END_OF_WK_HIST_FACT;

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;
