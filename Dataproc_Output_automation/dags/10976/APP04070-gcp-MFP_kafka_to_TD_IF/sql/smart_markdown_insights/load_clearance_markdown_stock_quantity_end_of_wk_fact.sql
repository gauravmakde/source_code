INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_stock_quantity_end_of_wk_fact (rms_style_num, color_num, channel_country,
 channel_brand, selling_channel, inventory_units, inventory_cost, inventory_dollars)
(SELECT rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel,
  SUM(total_stock_on_hand_qty + total_in_transit_qty) AS inventory_units,
  AVG(weighted_average_cost) AS inventory_cost,
  SUM((total_stock_on_hand_qty + total_in_transit_qty) * regular_price_amt) AS inventory_dollars
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact AS isibdf
 WHERE metrics_date = DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 WEEK), WEEK(SATURDAY)), INTERVAL 7 DAY)
  AND (LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(store_type_code) IN (LOWER('FC'), LOWER('LH'), LOWER('OC'),
       LOWER('OF'), LOWER('RK')) OR LOWER(selling_channel) = LOWER('STORE') AND LOWER(store_type_code) IN (LOWER('FL'),
       LOWER('NL'), LOWER('RK'), LOWER('SS'), LOWER('VS')))
 GROUP BY rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel
 HAVING inventory_units > 0 AND inventory_dollars > 0);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_stock_quantity_end_of_wk_fact (rms_style_num, color_num, channel_country,
 channel_brand, selling_channel, inventory_units, inventory_cost, inventory_dollars)
(SELECT rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  'OMNI',
  SUM(total_stock_on_hand_qty + total_in_transit_qty) AS inventory_units,
  AVG(weighted_average_cost) AS inventory_cost,
  SUM((total_stock_on_hand_qty + total_in_transit_qty) * regular_price_amt) AS inventory_dollars
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact AS isibdf
 WHERE metrics_date = DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 WEEK), WEEK(SATURDAY)), INTERVAL 7 DAY)
  AND (LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(store_type_code) IN (LOWER('FC'), LOWER('LH'), LOWER('OC'),
       LOWER('OF'), LOWER('RK')) OR LOWER(selling_channel) = LOWER('STORE') AND LOWER(store_type_code) IN (LOWER('FL'),
       LOWER('NL'), LOWER('RK'), LOWER('SS'), LOWER('VS')))
 GROUP BY rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  5
 HAVING inventory_units > 0 AND inventory_dollars > 0);