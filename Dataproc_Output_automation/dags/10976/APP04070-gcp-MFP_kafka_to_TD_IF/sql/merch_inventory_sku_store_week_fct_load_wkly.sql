BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=inventory_apt_week_fact_load_wkly;
---Task_Name=inv_week_fct_load_02;'*/
---FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.merch_inventory_sku_store_week_fct AS inventory_agg_vw
WHERE week_num BETWEEN (SELECT week_idnt
        FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
        WHERE day_date = (SELECT extract_start_dt
                    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl
                    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY'))) AND (SELECT week_idnt
        FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
        WHERE day_date = (SELECT extract_end_dt
                    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl
                    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY')));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.merch_inventory_sku_store_week_wrk;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
--- Insert EOH data into WRK table

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.merch_inventory_sku_store_week_wrk (week_num, rms_sku_num, store_num, rp_ind,
 eoh_active_regular_units, eoh_in_transit_active_regular_units, eoh_active_regular_cost,
 eoh_in_transit_active_regular_cost, eoh_active_regular_retail, eoh_in_transit_active_regular_retail,
 eoh_inactive_regular_units, eoh_in_transit_inactive_regular_units, eoh_inactive_regular_cost,
 eoh_in_transit_inactive_regular_cost, eoh_inactive_regular_retail, eoh_in_transit_inactive_regular_retail,
 eoh_active_promo_units, eoh_in_transit_active_promo_units, eoh_active_promo_cost, eoh_in_transit_active_promo_cost,
 eoh_active_promo_retail, eoh_in_transit_active_promo_retail, eoh_inactive_promo_units,
 eoh_in_transit_inactive_promo_units, eoh_inactive_promo_cost, eoh_in_transit_inactive_promo_cost,
 eoh_inactive_promo_retail, eoh_in_transit_inactive_promo_retail, eoh_active_clearance_units,
 eoh_in_transit_active_clearance_units, eoh_active_clearance_cost, eoh_in_transit_active_clearance_cost,
 eoh_active_clearance_retail, eoh_in_transit_active_clearance_retail, eoh_inactive_clearance_units,
 eoh_in_transit_inactive_clearance_units, eoh_inactive_clearance_cost, eoh_in_transit_inactive_clearance_cost,
 eoh_inactive_clearance_retail, eoh_in_transit_inactive_clearance_retail, eoh_clearance_retail_ind, eoh_wac_avlbl_ind,
 inv_cost_currency_code, inv_retail_currency_code, dw_batch_date, dw_sys_load_tmstp)
(SELECT day_cal.week_idnt AS week_num,
  inv.rms_sku_num,
  TRIM(FORMAT('%11d', inv.store_num)) AS store_num,
  'N' AS rp_ind,
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  MAX(CASE
    WHEN LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE'
         ) AND pro.ownership_retail_price_amt IS NULL AND pro.selling_retail_price_amt IS NULL AND pro.regular_price_amt
     IS NOT NULL
    THEN 'R'
    ELSE 'C'
    END),
  MAX(CASE
    WHEN wacd.weighted_average_cost IS NULL AND wacc.weighted_average_cost IS NULL
    THEN 'N'
    ELSE 'Y'
    END),
  COALESCE(wacd.weighted_average_cost_currency_code, wacc.weighted_average_cost_currency_code) AS inv_cost_currency_code
  ,
   CASE
   WHEN pro.ownership_currency_code IS NULL AND LOWER(pro.channel_country) = LOWER('USA')
   THEN 'USD'
   WHEN pro.ownership_currency_code IS NULL AND LOWER(pro.channel_country) = LOWER('CAN')
   THEN 'CAD'
   ELSE pro.ownership_currency_code
   END AS inv_retail_currency_code,
  CURRENT_DATE AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    store_num,
    day_num,
    snapshot_date,
    week_num,
    stock_on_hand_qty,
    in_transit_qty,
    location_type,
    dw_batch_date,
    dw_sys_load_tmstp
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.merch_inventory_sku_store_day_fact AS fct
   WHERE snapshot_date IN (SELECT dcd.day_date -7
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
       INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd ON dcd.day_date BETWEEN etl.extract_start_dt AND etl.extract_end_dt
        
      WHERE LOWER(etl.interface_code) = LOWER('MERCH_NAP_INV_DLY')
       AND dcd.day_num_of_fiscal_week = 7)
    AND (stock_on_hand_qty IS NOT NULL OR in_transit_qty IS NOT NULL)) AS inv
  INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS day_cal ON inv.snapshot_date = day_cal.day_date
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore ON inv.store_num = orgstore.store_num
  LEFT JOIN (SELECT *
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wac1
   WHERE eff_end_dt >= (SELECT extract_start_dt
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY'))) AS wacd ON LOWER(inv.rms_sku_num) = LOWER(wacd.sku_num
       ) AND LOWER(TRIM(FORMAT('%11d', inv.store_num))) = LOWER(TRIM(wacd.location_num)) AND inv.snapshot_date >= wacd.eff_begin_dt
      AND inv.snapshot_date < wacd.eff_end_dt
  LEFT JOIN (SELECT *
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim AS wac2
   WHERE eff_end_dt >= (SELECT extract_start_dt
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY'))) AS wacc ON LOWER(inv.rms_sku_num) = LOWER(wacc.sku_num
       ) AND orgstore.channel_num = wacc.channel_num AND inv.snapshot_date >= wacc.eff_begin_dt AND inv.snapshot_date <
    wacc.eff_end_dt
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.config_lkup AS config ON CAST(config.config_value AS FLOAT64) = orgstore.channel_num AND
     LOWER(config.interface_code) = LOWER('MERCH_NAP_INV_DLY') AND LOWER(config.config_key) = LOWER('INACTIVE_CHANNELS'
     )
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON inv.store_num = psd.store_num
  LEFT JOIN ({{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pro LEFT JOIN (SELECT extract_start_dt AS A807964555
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY')) AS t10 ON TRUE) ON LOWER(psd.store_country_code) = LOWER(pro
        .channel_country) 
        AND LOWER(psd.selling_channel) = LOWER(pro.selling_channel) 
        AND LOWER(psd.channel_brand) = LOWER(pro.channel_brand)
        AND RANGE_CONTAINS(RANGE(pro.eff_begin_tmstp_utc ,pro.eff_end_tmstp_utc),TIMESTAMP_SUB(TIMESTAMP(inv.snapshot_date) + INTERVAL 1 DAY , INTERVAL 1 MILLISECOND)) 
       AND LOWER(inv.rms_sku_num) = LOWER(pro.rms_sku_num) AND CAST(pro.eff_end_tmstp AS DATE)
    >= t10.A807964555
 GROUP BY week_num,
  inv.rms_sku_num,
  store_num,
  inv_cost_currency_code,
  inv_retail_currency_code);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.merch_inventory_sku_store_week_wrk (week_num, rms_sku_num, store_num, rp_ind,
 eoh_clearance_retail_ind, eoh_wac_avlbl_ind, boh_active_regular_units, boh_in_transit_active_regular_units,
 boh_active_regular_cost, boh_in_transit_active_regular_cost, boh_active_regular_retail,
 boh_in_transit_active_regular_retail, boh_inactive_regular_units, boh_in_transit_inactive_regular_units,
 boh_inactive_regular_cost, boh_in_transit_inactive_regular_cost, boh_inactive_regular_retail,
 boh_in_transit_inactive_regular_retail, boh_active_promo_units, boh_in_transit_active_promo_units,
 boh_active_promo_cost, boh_in_transit_active_promo_cost, boh_active_promo_retail, boh_in_transit_active_promo_retail,
 boh_inactive_promo_units, boh_in_transit_inactive_promo_units, boh_inactive_promo_cost,
 boh_in_transit_inactive_promo_cost, boh_inactive_promo_retail, boh_in_transit_inactive_promo_retail,
 boh_active_clearance_units, boh_in_transit_active_clearance_units, boh_active_clearance_cost,
 boh_in_transit_active_clearance_cost, boh_active_clearance_retail, boh_in_transit_active_clearance_retail,
 boh_inactive_clearance_units, boh_in_transit_inactive_clearance_units, boh_inactive_clearance_cost,
 boh_in_transit_inactive_clearance_cost, boh_inactive_clearance_retail, boh_in_transit_inactive_clearance_retail,
 inv_cost_currency_code, inv_retail_currency_code, dw_batch_date, dw_sys_load_tmstp)
(SELECT day_cal.week_idnt AS week_num,
  inv.rms_sku_num,
  TRIM(FORMAT('%11d', inv.store_num)) AS store_num,
  'N' AS rp_ind,
  MAX('C'),
  MAX('N'),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('REGULAR'), LOWER('UNKNOWN'))
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('PROMOTION')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.stock_on_hand_qty, 0)
    ELSE 0
    END),
  SUM(CASE
    WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code
        , 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN COALESCE(inv.in_transit_qty, 0)
    ELSE 0
    END),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.stock_on_hand_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  CAST(SUM(CASE
     WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
     THEN COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) * CASE
       WHEN config.config_value IS NOT NULL AND LOWER(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
       THEN COALESCE(inv.in_transit_qty, 0)
       ELSE 0
       END
     ELSE 0
     END) AS BIGNUMERIC),
  COALESCE(wacd.weighted_average_cost_currency_code, wacc.weighted_average_cost_currency_code) AS inv_cost_currency_code
  ,
   CASE
   WHEN pro.ownership_currency_code IS NULL AND LOWER(pro.channel_country) = LOWER('USA')
   THEN 'USD'
   WHEN pro.ownership_currency_code IS NULL AND LOWER(pro.channel_country) = LOWER('CAN')
   THEN 'CAD'
   ELSE pro.ownership_currency_code
   END AS inv_retail_currency_code,
  CURRENT_DATE AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    store_num,
    day_num,
    snapshot_date,
    week_num,
    stock_on_hand_qty,
    in_transit_qty,
    location_type,
    dw_batch_date,
    dw_sys_load_tmstp,
    DATE_ADD(snapshot_date, INTERVAL 7 DAY) AS curr_week_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.merch_inventory_sku_store_day_fact AS fct
   WHERE snapshot_date IN (SELECT DATE_SUB(dcd.day_date, INTERVAL 7 DAY)
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
       INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd ON dcd.day_date BETWEEN etl.extract_start_dt AND etl.extract_end_dt
        
      WHERE LOWER(etl.interface_code) = LOWER('MERCH_NAP_INV_DLY')
       AND dcd.day_num_of_fiscal_week = 7)
    AND (stock_on_hand_qty IS NOT NULL OR in_transit_qty IS NOT NULL)) AS inv
  INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS day_cal ON inv.curr_week_date = day_cal.day_date
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore ON inv.store_num = orgstore.store_num
  LEFT JOIN (SELECT *
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wac1
   WHERE eff_end_dt >= (SELECT extract_start_dt -7
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY'))) AS wacd ON LOWER(inv.rms_sku_num) = LOWER(wacd.sku_num
       ) AND LOWER(TRIM(FORMAT('%11d', inv.store_num))) = LOWER(TRIM(wacd.location_num)) AND inv.snapshot_date >= wacd.eff_begin_dt
      AND inv.snapshot_date < wacd.eff_end_dt
  LEFT JOIN (SELECT *
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim AS wac2
   WHERE eff_end_dt >= (SELECT extract_start_dt -7
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY'))) AS wacc ON LOWER(inv.rms_sku_num) = LOWER(wacc.sku_num
       ) AND orgstore.channel_num = wacc.channel_num AND inv.snapshot_date >= wacc.eff_begin_dt AND inv.snapshot_date <
    wacc.eff_end_dt
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.config_lkup AS config ON CAST(config.config_value AS FLOAT64) = orgstore.channel_num AND
     LOWER(config.interface_code) = LOWER('MERCH_NAP_INV_DLY') AND LOWER(config.config_key) = LOWER('INACTIVE_CHANNELS'
     )
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON inv.store_num = psd.store_num
  LEFT JOIN ({{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pro LEFT JOIN (SELECT extract_start_dt -7 AS A807964555
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INV_DLY')) AS t10 ON TRUE) ON LOWER(psd.store_country_code) = LOWER(pro
        .channel_country) AND LOWER(psd.selling_channel) = LOWER(pro.selling_channel) AND LOWER(psd.channel_brand) =
      LOWER(pro.channel_brand) AND LOWER(inv.rms_sku_num) = LOWER(pro.rms_sku_num) AND CAST(pro.eff_end_tmstp AS DATE)
    >= t10.A807964555
 GROUP BY week_num,
  inv.rms_sku_num,
  store_num,
  inv_cost_currency_code,
  inv_retail_currency_code);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.merch_inventory_sku_store_week_fct (week_num, rms_sku_num, store_num, rp_ind,
 eoh_active_regular_units, eoh_in_transit_active_regular_units, eoh_active_regular_cost,
 eoh_in_transit_active_regular_cost, eoh_active_regular_retail, eoh_in_transit_active_regular_retail,
 eoh_inactive_regular_units, eoh_in_transit_inactive_regular_units, eoh_inactive_regular_cost,
 eoh_in_transit_inactive_regular_cost, eoh_inactive_regular_retail, eoh_in_transit_inactive_regular_retail,
 eoh_active_promo_units, eoh_in_transit_active_promo_units, eoh_active_promo_cost, eoh_in_transit_active_promo_cost,
 eoh_active_promo_retail, eoh_in_transit_active_promo_retail, eoh_inactive_promo_units,
 eoh_in_transit_inactive_promo_units, eoh_inactive_promo_cost, eoh_in_transit_inactive_promo_cost,
 eoh_inactive_promo_retail, eoh_in_transit_inactive_promo_retail, eoh_active_clearance_units,
 eoh_in_transit_active_clearance_units, eoh_active_clearance_cost, eoh_in_transit_active_clearance_cost,
 eoh_active_clearance_retail, eoh_in_transit_active_clearance_retail, eoh_inactive_clearance_units,
 eoh_in_transit_inactive_clearance_units, eoh_inactive_clearance_cost, eoh_in_transit_inactive_clearance_cost,
 eoh_inactive_clearance_retail, eoh_in_transit_inactive_clearance_retail, eoh_clearance_retail_ind, eoh_wac_avlbl_ind,
 boh_active_regular_units, boh_in_transit_active_regular_units, boh_active_regular_cost,
 boh_in_transit_active_regular_cost, boh_active_regular_retail, boh_in_transit_active_regular_retail,
 boh_inactive_regular_units, boh_in_transit_inactive_regular_units, boh_inactive_regular_cost,
 boh_in_transit_inactive_regular_cost, boh_inactive_regular_retail, boh_in_transit_inactive_regular_retail,
 boh_active_promo_units, boh_in_transit_active_promo_units, boh_active_promo_cost, boh_in_transit_active_promo_cost,
 boh_active_promo_retail, boh_in_transit_active_promo_retail, boh_inactive_promo_units,
 boh_in_transit_inactive_promo_units, boh_inactive_promo_cost, boh_in_transit_inactive_promo_cost,
 boh_inactive_promo_retail, boh_in_transit_inactive_promo_retail, boh_active_clearance_units,
 boh_in_transit_active_clearance_units, boh_active_clearance_cost, boh_in_transit_active_clearance_cost,
 boh_active_clearance_retail, boh_in_transit_active_clearance_retail, boh_inactive_clearance_units,
 boh_in_transit_inactive_clearance_units, boh_inactive_clearance_cost, boh_in_transit_inactive_clearance_cost,
 boh_inactive_clearance_retail, boh_in_transit_inactive_clearance_retail, inv_cost_currency_code,
 inv_retail_currency_code, dw_batch_date, dw_sys_load_tmstp)
(SELECT week_num,
  rms_sku_num,
  store_num,
  'N' AS rp_ind,
  COALESCE(SUM(eoh_active_regular_units), 0) AS eoh_active_regular_units,
  COALESCE(SUM(eoh_in_transit_active_regular_units), 0) AS eoh_in_transit_active_regular_units,
  COALESCE(SUM(eoh_active_regular_cost), 0) AS eoh_active_regular_cost,
  COALESCE(SUM(eoh_in_transit_active_regular_cost), 0) AS eoh_in_transit_active_regular_cost,
  COALESCE(SUM(eoh_active_regular_retail), 0) AS eoh_active_regular_retail,
  COALESCE(SUM(eoh_in_transit_active_regular_retail), 0) AS eoh_in_transit_active_regular_retail,
  COALESCE(SUM(eoh_inactive_regular_units), 0) AS eoh_inactive_regular_units,
  COALESCE(SUM(eoh_in_transit_inactive_regular_units), 0) AS eoh_in_transit_inactive_regular_units,
  COALESCE(SUM(eoh_inactive_regular_cost), 0) AS eoh_inactive_regular_cost,
  COALESCE(SUM(eoh_in_transit_inactive_regular_cost), 0) AS eoh_in_transit_inactive_regular_cost,
  COALESCE(SUM(eoh_inactive_regular_retail), 0) AS eoh_inactive_regular_retail,
  COALESCE(SUM(eoh_in_transit_inactive_regular_retail), 0) AS eoh_in_transit_inactive_regular_retail,
  COALESCE(SUM(eoh_active_promo_units), 0) AS eoh_active_promo_units,
  COALESCE(SUM(eoh_in_transit_active_promo_units), 0) AS eoh_in_transit_active_promo_units,
  COALESCE(SUM(eoh_active_promo_cost), 0) AS eoh_active_promo_cost,
  COALESCE(SUM(eoh_in_transit_active_promo_cost), 0) AS eoh_in_transit_active_promo_cost,
  COALESCE(SUM(eoh_active_promo_retail), 0) AS eoh_active_promo_retail,
  COALESCE(SUM(eoh_in_transit_active_promo_retail), 0) AS eoh_in_transit_active_promo_retail,
  COALESCE(SUM(eoh_inactive_promo_units), 0) AS eoh_inactive_promo_units,
  COALESCE(SUM(eoh_in_transit_inactive_promo_units), 0) AS eoh_in_transit_inactive_promo_units,
  COALESCE(SUM(eoh_inactive_promo_cost), 0) AS eoh_inactive_promo_cost,
  COALESCE(SUM(eoh_in_transit_inactive_promo_cost), 0) AS eoh_in_transit_inactive_promo_cost,
  COALESCE(SUM(eoh_inactive_promo_retail), 0) AS eoh_inactive_promo_retail,
  COALESCE(SUM(eoh_in_transit_inactive_promo_retail), 0) AS eoh_in_transit_inactive_promo_retail,
  COALESCE(SUM(eoh_active_clearance_units), 0) AS eoh_active_clearance_units,
  COALESCE(SUM(eoh_in_transit_active_clearance_units), 0) AS eoh_in_transit_active_clearance_units,
  COALESCE(SUM(eoh_active_clearance_cost), 0) AS eoh_active_clearance_cost,
  COALESCE(SUM(eoh_in_transit_active_clearance_cost), 0) AS eoh_in_transit_active_clearance_cost,
  COALESCE(SUM(eoh_active_clearance_retail), 0) AS eoh_active_clearance_retail,
  COALESCE(SUM(eoh_in_transit_active_clearance_retail), 0) AS eoh_in_transit_active_clearance_retail,
  COALESCE(SUM(eoh_inactive_clearance_units), 0) AS eoh_inactive_clearance_units,
  COALESCE(SUM(eoh_in_transit_inactive_clearance_units), 0) AS eoh_in_transit_inactive_clearance_units,
  COALESCE(SUM(eoh_inactive_clearance_cost), 0) AS eoh_inactive_clearance_cost,
  COALESCE(SUM(eoh_in_transit_inactive_clearance_cost), 0) AS eoh_in_transit_inactive_clearance_cost,
  COALESCE(SUM(eoh_inactive_clearance_retail), 0) AS eoh_inactive_clearance_retail,
  COALESCE(SUM(eoh_in_transit_inactive_clearance_retail), 0) AS eoh_in_transit_inactive_clearance_retail,
  MAX(eoh_clearance_retail_ind),
  MAX(eoh_wac_avlbl_ind),
  COALESCE(SUM(boh_active_regular_units), 0) AS boh_active_regular_units,
  COALESCE(SUM(boh_in_transit_active_regular_units), 0) AS boh_in_transit_active_regular_units,
  COALESCE(SUM(boh_active_regular_cost), 0) AS boh_active_regular_cost,
  COALESCE(SUM(boh_in_transit_active_regular_cost), 0) AS boh_in_transit_active_regular_cost,
  COALESCE(SUM(boh_active_regular_retail), 0) AS boh_active_regular_retail,
  COALESCE(SUM(boh_in_transit_active_regular_retail), 0) AS boh_in_transit_active_regular_retail,
  COALESCE(SUM(boh_inactive_regular_units), 0) AS boh_inactive_regular_units,
  COALESCE(SUM(boh_in_transit_inactive_regular_units), 0) AS boh_in_transit_inactive_regular_units,
  COALESCE(SUM(boh_inactive_regular_cost), 0) AS boh_inactive_regular_cost,
  COALESCE(SUM(boh_in_transit_inactive_regular_cost), 0) AS boh_in_transit_inactive_regular_cost,
  COALESCE(SUM(boh_inactive_regular_retail), 0) AS boh_inactive_regular_retail,
  COALESCE(SUM(boh_in_transit_inactive_regular_retail), 0) AS boh_in_transit_inactive_regular_retail,
  COALESCE(SUM(boh_active_promo_units), 0) AS boh_active_promo_units,
  COALESCE(SUM(boh_in_transit_active_promo_units), 0) AS boh_in_transit_active_promo_units,
  COALESCE(SUM(boh_active_promo_cost), 0) AS boh_active_promo_cost,
  COALESCE(SUM(boh_in_transit_active_promo_cost), 0) AS boh_in_transit_active_promo_cost,
  COALESCE(SUM(boh_active_promo_retail), 0) AS boh_active_promo_retail,
  COALESCE(SUM(boh_in_transit_active_promo_retail), 0) AS boh_in_transit_active_promo_retail,
  COALESCE(SUM(boh_inactive_promo_units), 0) AS boh_inactive_promo_units,
  COALESCE(SUM(boh_in_transit_inactive_promo_units), 0) AS boh_in_transit_inactive_promo_units,
  COALESCE(SUM(boh_inactive_promo_cost), 0) AS boh_inactive_promo_cost,
  COALESCE(SUM(boh_in_transit_inactive_promo_cost), 0) AS boh_in_transit_inactive_promo_cost,
  COALESCE(SUM(boh_inactive_promo_retail), 0) AS boh_inactive_promo_retail,
  COALESCE(SUM(boh_in_transit_inactive_promo_retail), 0) AS boh_in_transit_inactive_promo_retail,
  COALESCE(SUM(boh_active_clearance_units), 0) AS boh_active_clearance_units,
  COALESCE(SUM(boh_in_transit_active_clearance_units), 0) AS boh_in_transit_active_clearance_units,
  COALESCE(SUM(boh_active_clearance_cost), 0) AS boh_active_clearance_cost,
  COALESCE(SUM(boh_in_transit_active_clearance_cost), 0) AS boh_in_transit_active_clearance_cost,
  COALESCE(SUM(boh_active_clearance_retail), 0) AS boh_active_clearance_retail,
  COALESCE(SUM(boh_in_transit_active_clearance_retail), 0) AS boh_in_transit_active_clearance_retail,
  COALESCE(SUM(boh_inactive_clearance_units), 0) AS boh_inactive_clearance_units,
  COALESCE(SUM(boh_in_transit_inactive_clearance_units), 0) AS boh_in_transit_inactive_clearance_units,
  COALESCE(SUM(boh_inactive_clearance_cost), 0) AS boh_inactive_clearance_cost,
  COALESCE(SUM(boh_in_transit_inactive_clearance_cost), 0) AS boh_in_transit_inactive_clearance_cost,
  COALESCE(SUM(boh_inactive_clearance_retail), 0) AS boh_inactive_clearance_retail,
  COALESCE(SUM(boh_in_transit_inactive_clearance_retail), 0) AS boh_in_transit_inactive_clearance_retail,
  inv_cost_currency_code,
  inv_retail_currency_code,
  CURRENT_DATE AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.merch_inventory_sku_store_week_wrk
 GROUP BY week_num,
  rms_sku_num,
  store_num,
  inv_cost_currency_code,
  inv_retail_currency_code);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


UPDATE  {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.merch_inventory_sku_store_week_fct  INV 
SET RP_IND = 'Y'
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_VWS.MERCH_RP_SKU_LOC_DIM_HIST  RP 
INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DAY_CAL_454_DIM CAL
ON RANGE_CONTAINS(RP.RP_PERIOD, CAL.DAY_DATE)
WHERE LOWER(INV.RP_IND) = LOWER('N') 
AND INV.WEEK_NUM =  CAL.WEEK_IDNT  AND CAL.DAY_NUM_OF_FISCAL_WEEK = 7
AND LOWER(RP.RMS_SKU_NUM) = LOWER(INV.RMS_SKU_NUM)
AND CAST(RP.LOCATION_NUM AS STRING) = INV.STORE_NUM
AND INV.WEEK_NUM BETWEEN
(SELECT week_idnt FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim WHERE day_date = (SELECT EXTRACT_START_DT FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP ebdl
WHERE ebdl.interface_code = 'MERCH_NAP_INV_DLY'))
AND ( SELECT week_idnt FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.day_cal_454_dim WHERE day_date = (SELECT EXTRACT_END_DT FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP ebdl
WHERE ebdl.interface_code = 'MERCH_NAP_INV_DLY'));


BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.merch_inventory_sku_store_week_wrk;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
