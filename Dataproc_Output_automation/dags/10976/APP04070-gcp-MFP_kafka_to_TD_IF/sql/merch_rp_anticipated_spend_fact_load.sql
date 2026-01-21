BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=rp_ant_spend_fact_load;
---Task_Name=merch_rp_t1_ant_spend_fact_load;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_rp_anticipated_spend_fact;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_rp_anticipated_spend_fact
(   rms_sku_num
, channel_num
, week_num
, booked_buffer_receipt_plan_units
, booked_receipt_plan_units
, booked_trunkclub_not_kept_units
, booked_usable_returns_units
, booked_other_units
, unit_retail_amount
, weighted_average_cost_amount
, on_order_total_units
, on_order_total_elc_amount
, on_order_total_retail_amount
, on_order_average_elc_amount
, on_order_average_retail_amount
, receipts_total_units
, receipts_total_cost_amount
, receipts_total_retail_amount
, receipts_average_cost_amount
, receipts_average_retail_amount
, anticipated_spend_units
, anticipated_spend_cost_amount
, anticipated_spend_retail_amount
, dw_batch_date
, dw_sys_load_tmstp
)
SELECT b01.RMS_SKU_NUM
, b01.channel_num
, b01.week_num
, b01.booked_buffer_receipt_plan_units
, b01.booked_receipt_plan_units
, b01.booked_trunkclub_not_kept_units
, b01.booked_usable_returns_units
, b01.booked_other_units
, b01.unit_retail_amount
, b01.weighted_average_cost_amount
, COALESCE(o01.ON_ORDER_TOTAL_UNIT, 0) AS on_order_total_units
, COALESCE(o01.ON_ORDER_TOTAL_ELC_AMT, 0) AS on_order_total_elc_amount
, COALESCE(o01.ON_ORDER_TOTAL_RETAIL_AMT, 0) AS on_order_total_retail_amount
, COALESCE(o01.ON_ORDER_AVG_ELC_AMT, 0) AS ON_ORDER_AVERAGE_ELC_AMOUNT
, COALESCE(o01.ON_ORDER_AVG_RETAIL_AMT, 0) AS ON_ORDER_AVERAGE_RETAIL_AMOUNT
, COALESCE(r01.RECEIPTS_TOTAL_UNITS, 0) AS RECEIPTS_TOTAL_UNITS
, COALESCE(r01.RECEIPTS_TOTAL_COST_AMT, 0) AS RECEIPTS_TOTAL_COST_AMOUNT
, COALESCE(r01.RECEIPTS_TOTAL_RETAIL_AMT, 0) AS RECEIPTS_TOTAL_RETAIL_AMOUNT
, COALESCE(r01.RECEIPTS_AVERAGE_COST_AMT, 0) AS RECEIPTS_AVERAGE_COST_AMOUNT
, COALESCE(r01.RECEIPTS_AVERAGE_RETAIL_AMT, 0) AS RECEIPTS_AVERAGE_RETAIL_AMOUNT
, (BOOKED_RECEIPT_PLAN_UNITS + BOOKED_BUFFER_RECEIPT_PLAN_UNITS - BOOKED_USABLE_RETURNS_UNITS 
    - BOOKED_TRUNKCLUB_NOT_KEPT_UNITS + COALESCE(o01.ON_ORDER_TOTAL_UNIT, 0) 
    + COALESCE(r01.RECEIPTS_TOTAL_UNITS, 0) ) AS  ANTICIPATED_SPEND_UNITS
, (  BOOKED_RECEIPT_PLAN_UNITS + BOOKED_BUFFER_RECEIPT_PLAN_UNITS - BOOKED_USABLE_RETURNS_UNITS 
    - BOOKED_TRUNKCLUB_NOT_KEPT_UNITS ) * 
    (
        CASE WHEN r01.RECEIPTS_AVERAGE_COST_AMT IS NOT NULL 
          THEN r01.RECEIPTS_AVERAGE_COST_AMT
        WHEN o01.ON_ORDER_AVG_ELC_AMT IS NOT NULL
          THEN o01.ON_ORDER_AVG_ELC_AMT
        ELSE WEIGHTED_AVERAGE_COST_AMOUNT 
        END
    ) + COALESCE(r01.RECEIPTS_TOTAL_COST_AMT, 0) + COALESCE(o01.ON_ORDER_TOTAL_ELC_AMT, 0) AS ANTICIPATED_SPEND_COST_AMOUNT
, (BOOKED_RECEIPT_PLAN_UNITS + BOOKED_BUFFER_RECEIPT_PLAN_UNITS - BOOKED_USABLE_RETURNS_UNITS 
    - BOOKED_TRUNKCLUB_NOT_KEPT_UNITS ) * 
    (
      CASE WHEN r01.RECEIPTS_AVERAGE_RETAIL_AMT IS NOT NULL 
        THEN r01.RECEIPTS_AVERAGE_RETAIL_AMT
      WHEN o01.ON_ORDER_AVG_RETAIL_AMT IS NOT NULL
        THEN o01.ON_ORDER_AVG_RETAIL_AMT
      ELSE UNIT_RETAIL_AMOUNT 
        END
    ) + COALESCE(r01.RECEIPTS_TOTAL_RETAIL_AMT, 0) + COALESCE(o01.ON_ORDER_TOTAL_RETAIL_AMT, 0) AS  ANTICIPATED_SPEND_RETAIL_AMOUNT
, CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_rp_booked_measures_wrk_vw b01
LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_rp_anticipated_spend_onorder_vw o01
  ON b01.RMS_SKU_NUM = o01.RMS_SKU_NUM
  AND b01.CHANNEL_NUM = o01.CHANNEL_NUM
  AND b01.WEEK_NUM = o01.WEEK_NUM
LEFT OUTER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_rp_anticipated_spend_receipt_vw r01
  ON b01.RMS_SKU_NUM = r01.RMS_SKU_NUM
  AND b01.CHANNEL_NUM = r01.CHANNEL_NUM
  AND b01.WEEK_NUM = r01.WEEK_NUM;


-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
