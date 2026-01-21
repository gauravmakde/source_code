

TRUNCATE TABLE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_expected_transfer_fact;

INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_expected_transfer_fact
(SELECT inv.rms_sku_id AS rms_sku_num,
  CAST(trunc(cast(inv.location_id as float64)) AS INTEGER) AS store_num,
  day_cal.day_idnt AS day_num,
  'N' AS rp_ind,
  COALESCE(inv.store_transfer_expected_qty, 0) AS expected_transfer_in_units,
  ROUND(CAST(COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * COALESCE(inv.store_transfer_expected_qty, 0) AS BIGNUMERIC)
   , 2) AS expected_transfer_in_cost,
   COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * COALESCE(inv.store_transfer_expected_qty
    , 0) AS expected_transfer_in_retail,
  COALESCE(inv.in_transit_qty, 0) AS inventory_in_transit_units,
  ROUND(CAST(COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * COALESCE(inv.in_transit_qty, 0) AS BIGNUMERIC)
   , 2) AS iinventory_in_transit_cost,
   COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * COALESCE(inv.in_transit_qty
    , 0) AS inventory_in_transit_retail,
  btch.dw_batch_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S',current_datetime('PST8PDT')) AS DATETIME)

 FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_logical_fact AS inv
  INNER JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS btch ON TRUE
  INNER JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS day_cal ON btch.dw_batch_dt = day_cal.day_date
  LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON CAST(inv.location_id AS FLOAT64) = psd.store_num

  LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pro ON LOWER(psd.store_country_code) = LOWER(pro.channel_country
         ) AND LOWER(psd.selling_channel) = LOWER(pro.selling_channel) AND LOWER(psd.channel_brand) = LOWER(pro.channel_brand
        ) AND LOWER(inv.rms_sku_id) = LOWER(pro.rms_sku_num) AND day_cal.day_date >= CAST(pro.eff_begin_tmstp AS DATE)
   AND day_cal.day_date < CAST(pro.eff_end_tmstp AS DATE)

  LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore ON CAST(inv.location_id AS FLOAT64) = orgstore.store_num

  LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wacd ON LOWER(inv.rms_sku_id) = LOWER(wacd.sku_num) AND
      LOWER(inv.location_id) = LOWER(wacd.location_num) AND btch.dw_batch_dt >= wacd.eff_begin_dt AND btch.dw_batch_dt <
    wacd.eff_end_dt

  LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim AS wacc ON LOWER(inv.rms_sku_id) = LOWER(wacc.sku_num)
     AND orgstore.channel_num = wacc.channel_num AND btch.dw_batch_dt >= wacc.eff_begin_dt AND btch.dw_batch_dt < wacc.eff_end_dt
    
 WHERE (COALESCE(inv.store_transfer_expected_qty, 0) > 0 OR COALESCE(inv.in_transit_qty, 0) > 0)
  AND LOWER(btch.interface_code) = LOWER('MERCH_NAP_INV_DLY')
  AND LOWER(COALESCE(inv.location_type, '-1')) NOT IN (LOWER('DS'), LOWER('DS_OP')));





UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_expected_transfer_fact   EXPTSFR 
SET RP_IND = 'Y'
from 
 `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_rp_sku_loc_dim_hist  rp ,
 `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup btch 

where exptsfr.rp_ind = 'n' 
and rp.rms_sku_num = exptsfr.rms_sku_num and rp.location_num = exptsfr.store_num 
AND  range_contains (rp.rp_period , btch.dw_batch_dt ) 
AND LOWER(INTERFACE_CODE) = LOWER('MERCH_NAP_INV_DLY') 
;


