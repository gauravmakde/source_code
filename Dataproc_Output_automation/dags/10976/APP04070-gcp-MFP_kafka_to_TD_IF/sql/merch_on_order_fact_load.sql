

DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_on_order_load;
---Task_Name=merch_on_order_load_stage_fact_load;'*/


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_fact;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_fact (purchase_order_number, external_distribution_id, rms_sku_num, epm_sku_num,
 store_num, week_num, ship_location_id, rms_casepack_num, epm_casepack_num, order_from_vendor_id, anticipated_price_type
 , order_category_code, po_type, order_type, purchase_type, status, cancel_reason, start_ship_date, end_ship_date,
 otb_eow_date, first_approval_date, latest_approval_date, first_approval_event_tmstp_pacific, first_approval_event_tmstp_pacific_tz,
 latest_approval_event_tmstp_pacific, latest_approval_event_tmstp_pacific_tz, anticipated_retail_amt, total_expenses_per_unit_currency, cross_ref_id,
 written_date, quantity_ordered, quantity_canceled, quantity_received, unit_cost_amt, total_expenses_per_unit_amt,
 total_duty_per_unit_amt, unit_estimated_landing_cost, dw_batch_date, dw_sys_load_tmstp, dw_sys_load_tmstp_tz)
(SELECT wrk.purchase_order_number,
  wrk.external_distribution_id,
  COALESCE(wrk.rms_sku_num, FORMAT('%4d', 0)) AS rms_sku_num,
  COALESCE(wrk.epm_sku_num, 0) AS epm_sku_num,
  COALESCE(wrk.store_num, 0) AS store_num,
  dcd.week_idnt AS week_num,
  wrk.ship_location_id,
  wrk.rms_casepack_num,
  wrk.epm_casepack_num,
  wrk.order_from_vendor_id,
   CASE
   WHEN CURRENT_DATE('PST8PDT') < wrk.start_ship_date
   THEN CASE
    WHEN LOWER(COALESCE(ppfd.ownership_retail_price_type_code, ppfd.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE'
      )
    THEN 'C'
    WHEN LOWER(COALESCE(ppfd.ownership_retail_price_type_code, ppfd.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION'
      )
    THEN 'P'
    WHEN LOWER(COALESCE(ppfd.ownership_retail_price_type_code, ppfd.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('UNKNOWN'
       ), LOWER('REGULAR'))
    THEN 'R'
    ELSE NULL
    END
   ELSE CASE
    WHEN LOWER(COALESCE(ppfd.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(COALESCE(ppfd.selling_retail_price_type_code, 'UNKNOWN')) = LOWER('PROMOTION')
    THEN 'P'
    WHEN LOWER(COALESCE(ppfd.selling_retail_price_type_code, 'UNKNOWN')) IN (LOWER('UNKNOWN'), LOWER('REGULAR'))
    THEN 'R'
    ELSE NULL
    END
   END AS anticipated_price_type,
  wrk.order_category_code,
  wrk.po_type,
  wrk.order_type,
  wrk.purchase_type,
  wrk.status,
  wrk.cancel_reason,
  wrk.start_ship_date,
  wrk.end_ship_date,
  wrk.otb_eow_date,
  wrk.first_approval_date,
  wrk.latest_approval_date,
  wrk.first_approval_event_tmstp_pacific,
  wrk.first_approval_event_tmstp_pacific_tz,
  wrk.latest_approval_event_tmstp_pacific,
  wrk.latest_approval_event_tmstp_pacific_tz,
  COALESCE(CASE
    WHEN CURRENT_DATE('PST8PDT') < wrk.start_ship_date
    THEN COALESCE(ppfd.ownership_retail_price_amt, ppfd.selling_retail_price_amt)
    ELSE ppfd.selling_retail_price_amt
    END, 0) AS anticipated_retail_amt,
  wrk.total_expenses_per_unit_currency,
  wrk.cross_ref_id,
  wrk.written_date,
  wrk.quantity_ordered,
  wrk.quantity_canceled,
  0 AS quantity_received,
  wrk.unit_cost_amt,
  wrk.total_expenses_per_unit_amt,
  wrk.total_duty_per_unit_amt,
  wrk.unit_estimated_landing_cost,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_wrk AS wrk
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd ON wrk.otb_eow_date = dcd.day_date
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON wrk.store_num = psdv.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS ppfd 
  ON LOWER(psdv.store_country_code) = LOWER(ppfd.channel_country) 
		AND LOWER(psdv.selling_channel) = LOWER(ppfd.selling_channel) 
		AND LOWER(psdv.channel_brand) = LOWER(ppfd.channel_brand) 
		AND LOWER(wrk.rms_sku_num) = LOWER(ppfd.rms_sku_num) 
		AND RANGE_CONTAINS(RANGE(ppfd.eff_begin_tmstp ,ppfd.eff_end_tmstp), DATETIME(CAST(wrk.PRICING_START_DATE as timestamp) + interval '1'day - interval '0.001'second) )  
		AND CAST(ppfd.eff_end_tmstp AS DATETIME) >= CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    );



MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_on_order_fact AS tgt
USING (SELECT wrk.purchase_order_number, wrk.external_distribution_id, wrk.rms_sku_num, wrk.store_num, dcd.week_idnt AS week_num, wrk.ship_location_id, SUM(receipts.receipts_units + receipts.receipts_crossdock_units) AS received_qty
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_sku_store_fact AS receipts
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_wrk AS wrk ON LOWER(receipts.poreceipt_order_number) = LOWER(wrk.external_distribution_id) AND LOWER(receipts.sku_num) = LOWER(wrk.rms_sku_num) AND receipts.store_num = wrk.store_num
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcd ON wrk.otb_eow_date = dcd.day_date
    WHERE LOWER(receipts.adjustment_type) <> LOWER('COST_ADJUSTMENT')
    GROUP BY wrk.purchase_order_number, wrk.external_distribution_id, wrk.rms_sku_num, wrk.store_num, week_num, wrk.ship_location_id) AS SRC
ON LOWER(SRC.purchase_order_number) = LOWER(tgt.purchase_order_number) AND LOWER(SRC.external_distribution_id) = LOWER(tgt.external_distribution_id) AND LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.store_num = tgt.store_num AND SRC.week_num = tgt.week_num AND LOWER(SRC.ship_location_id) = LOWER(tgt.ship_location_id)
WHEN MATCHED THEN UPDATE SET
    quantity_received = SRC.received_qty;



/*SET QUERY_BAND = NONE FOR SESSION;*/
