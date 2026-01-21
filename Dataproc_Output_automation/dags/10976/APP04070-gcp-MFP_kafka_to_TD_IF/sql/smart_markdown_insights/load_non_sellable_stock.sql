-- store param so only 1 reference
CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS
SELECT dw_batch_dt AS last_sat
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
WHERE LOWER(interface_code) = LOWER('SMD_INSIGHTS_WKLY');



CREATE TEMPORARY TABLE IF NOT EXISTS raw_data (
rms_style_num STRING,
color_num STRING,
channel_country STRING NOT NULL,
channel_brand STRING NOT NULL,
selling_channel STRING NOT NULL,
store_type_code STRING,
store_abbrev_name STRING,
inventory_units INTEGER,
inventory_dollars NUMERIC
) ;



INSERT INTO raw_data
(SELECT isibdf.rms_style_num,
  isibdf.color_num,
  isibdf.channel_country,
  isibdf.channel_brand,
  isibdf.selling_channel,
  isibdf.store_type_code,
  isibdf.store_abbrev_name,
  SUM(IFNULL(CASE
      WHEN isibdf.total_stock_on_hand_qty < 0
      THEN 0
      ELSE isibdf.total_stock_on_hand_qty
      END + CASE
      WHEN isibdf.total_in_transit_qty < 0
      THEN 0
      ELSE isibdf.total_in_transit_qty
      END, 0)) AS inventory_units,
  SUM(IFNULL((CASE
        WHEN isibdf.total_stock_on_hand_qty < 0
        THEN 0
        ELSE isibdf.total_stock_on_hand_qty
        END + CASE
        WHEN isibdf.total_in_transit_qty < 0
        THEN 0
        ELSE isibdf.total_in_transit_qty
        END) * isibdf.ownership_price_amt, 0)) AS inventory_dollars
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_fact AS isibdf
  INNER JOIN (`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf LEFT JOIN (SELECT last_sat AS
     A1459076419
    FROM my_params) AS t ON TRUE) 
    ON LOWER(COALESCE(isibdf.rms_style_num, 'NA')) = LOWER(COALESCE(cmibwf.rms_style_num,
          'NA')) AND LOWER(COALESCE(isibdf.color_num, 'NA')) = LOWER(COALESCE(cmibwf.color_num, 'NA')) AND LOWER(isibdf
        .channel_country) = LOWER(cmibwf.channel_country) AND LOWER(isibdf.channel_brand) = LOWER(cmibwf.channel_brand)
    AND LOWER(isibdf.selling_channel) = LOWER(cmibwf.selling_channel) AND cmibwf.snapshot_date = t.A1459076419
 WHERE isibdf.metrics_date = (SELECT last_sat
    FROM my_params)
  AND (LOWER(isibdf.store_type_code) IN (LOWER('RS'), LOWER('RR')) OR LOWER(isibdf.store_type_code) = LOWER('WH') AND
      LOWER(isibdf.store_abbrev_name) LIKE LOWER('%RK%'))
 GROUP BY isibdf.rms_style_num,
  isibdf.color_num,
  isibdf.channel_country,
  isibdf.channel_brand,
  isibdf.selling_channel,
  isibdf.store_type_code,
  isibdf.store_abbrev_name);



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact SET
    reserve_stock_inventory = 0,
    pack_and_hold_inventory = 0,
    reserve_stock_inventory_dollars = 0,
    pack_and_hold_inventory_dollars = 0
WHERE snapshot_date = (SELECT *
        FROM my_params);



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact

  SET
    reserve_stock_inventory = src.reserve_stock_inventory,
    pack_and_hold_inventory = src.pack_and_hold_inventory,
    reserve_stock_inventory_dollars = CAST(src.reserve_stock_inv_dollars AS NUMERIC),
    pack_and_hold_inventory_dollars = CAST(src.pack_and_hold_inv_dollars AS NUMERIC) 
from
 (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, SUM(CASE WHEN LOWER(store_type_code) IN (LOWER('RS'), LOWER('RR')) THEN inventory_units ELSE 0 END) AS reserve_stock_inventory, SUM(CASE WHEN LOWER(store_type_code) = LOWER('WH') AND LOWER(store_abbrev_name) LIKE LOWER('%RK%') THEN inventory_units ELSE 0 END) AS pack_and_hold_inventory, SUM(CASE WHEN LOWER(store_type_code) IN (LOWER('RS'), LOWER('RR')) THEN inventory_dollars ELSE 0 END) AS reserve_stock_inv_dollars, SUM(CASE WHEN LOWER(store_type_code) = LOWER('WH') AND LOWER(store_abbrev_name) LIKE LOWER('%RK%') THEN inventory_dollars ELSE 0 END) AS pack_and_hold_inv_dollars
    FROM raw_data
    WHERE inventory_units > 0
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src 

   
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT *
                            FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);


UPDATE 
 `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
  SET
    reserve_stock_inventory = src.reserve_stock_inventory,
    pack_and_hold_inventory = src.pack_and_hold_inventory,
    reserve_stock_inventory_dollars = CAST(src.reserve_stock_inv_dollars AS NUMERIC),
    pack_and_hold_inventory_dollars = CAST(src.pack_and_hold_inv_dollars AS NUMERIC)

 FROM

 (SELECT rms_style_num, color_num, channel_country, channel_brand, 'OMNI' AS selling_channel, SUM(CASE WHEN LOWER(store_type_code) IN (LOWER('RS'), LOWER('RR')) THEN inventory_units ELSE 0 END) AS reserve_stock_inventory, SUM(CASE WHEN LOWER(store_type_code) = LOWER('WH') AND LOWER(store_abbrev_name) LIKE LOWER('%RK%') THEN inventory_units ELSE 0 END) AS pack_and_hold_inventory, SUM(CASE WHEN LOWER(store_type_code) IN (LOWER('RS'), LOWER('RR')) THEN inventory_dollars ELSE 0 END) AS reserve_stock_inv_dollars, SUM(CASE WHEN LOWER(store_type_code) = LOWER('WH') AND LOWER(store_abbrev_name) LIKE LOWER('%RK%') THEN inventory_dollars ELSE 0 END) AS pack_and_hold_inv_dollars
    FROM raw_data
    WHERE inventory_units > 0
    GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS src 
  
    
     
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT *
                            FROM my_params) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand);



CREATE TEMPORARY TABLE IF NOT EXISTS on_order (
rms_style_num STRING,
color_num STRING,
channel_country STRING NOT NULL,
channel_brand STRING NOT NULL,
selling_channel STRING NOT NULL,
store_type_code STRING,
store_abbrev_name STRING,
on_order_units_qty INTEGER
) ;



INSERT INTO on_order
(SELECT psd.rms_style_num,
  psd.color_num,
  psdv.channel_country,
  psdv.channel_brand,
  psdv.selling_channel,
  psdv.store_type_code,
  psdv.store_abbrev_name,
  oo.quantity_open AS on_order_units_qty
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_on_order_snapshot_fact AS oo
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON oo.store_num = psdv.store_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd ON LOWER(oo.rms_sku_num) = LOWER(psd.rms_sku_num) 
  AND LOWER(psdv.channel_country) = LOWER(psd.channel_country)
  --AND period(psd.eff_begin_tmstp, psd.eff_end_tmstp) contains cast(oo.snapshot_week_date as timestamp) + interval '1' day - interval '0.001' second at time zone 'gmt'

  and RANGE_CONTAINS(RANGE(psd.eff_begin_tmstp_utc, psd.eff_end_tmstp_utc), cast(datetime(timestamp(oo.snapshot_week_date) + INTERVAL '1' DAY  - interval '0.001' second,'GMT')  as timestamp))

 WHERE oo.snapshot_week_date = (SELECT last_sat
    FROM my_params)
  AND (LOWER(psdv.store_type_code) IN (LOWER('RS'), LOWER('RR')) OR LOWER(psdv.store_type_code) = LOWER('WH') AND LOWER(psdv
       .store_abbrev_name) LIKE LOWER('%RK%')));

--COLLECT STATISTICS COLUMN(rms_style_num), COLUMN(color_num), COLUMN(channel_country), COLUMN(channel_brand), COLUMN(selling_channel) ON on_order;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact

SET
    reserve_stock_on_order_units_qty = 0,
    pack_and_hold_on_order_units_qty = 0
WHERE snapshot_date = (SELECT *
        FROM my_params);



UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
 SET
    reserve_stock_on_order_units_qty = src.reserve_stock_on_order_units_qty,
    pack_and_hold_on_order_units_qty = src.pack_and_hold_on_order_units_qty 


from (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, SUM(CASE WHEN LOWER(store_type_code) IN (LOWER('RS'), LOWER('RR')) THEN on_order_units_qty ELSE NULL END) AS reserve_stock_on_order_units_qty, SUM(CASE WHEN LOWER(store_type_code) = LOWER('WH') AND LOWER(store_abbrev_name) LIKE LOWER('%RK%') THEN on_order_units_qty ELSE NULL END) AS pack_and_hold_on_order_units_qty
    FROM on_order
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel) AS src
    
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT *
                            FROM my_params) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    reserve_stock_on_order_units_qty = omni.reserve_stock_on_order_units_qty,
    pack_and_hold_on_order_units_qty = omni.pack_and_hold_on_order_units_qty

from

 (SELECT rms_style_num, color_num, channel_country, channel_brand, SUM(CASE WHEN LOWER(store_type_code) IN (LOWER('RS'), LOWER('RR')) THEN on_order_units_qty ELSE NULL END) AS reserve_stock_on_order_units_qty, SUM(CASE WHEN LOWER(store_type_code) = LOWER('WH') AND LOWER(store_abbrev_name) LIKE LOWER('%RK%') THEN on_order_units_qty ELSE NULL END) AS pack_and_hold_on_order_units_qty
    FROM on_order
    GROUP BY rms_style_num, color_num, channel_country, channel_brand) AS omni 
    
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT *
                            FROM my_params) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) AND LOWER(omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) AND LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI');



