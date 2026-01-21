SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=item_ticket_load_13569_DAS_SC_OUTBOUND_sc_outbound_insights_framework;
Task_Name=main_1_load_from_stg_to_fct_table_sensor;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table that has the latest data for each worker number and worker type.
CREATE VOLATILE MULTISET TABLE ITEM_TICKET_EVENT_FACT_UNIQUE_TEMP AS (
    SELECT DISTINCT
    ticket_print_time,
    universal_unique_id,
    store_number,
    device_id,
    employee_id_type,
    employee_id,
    upc_id,
    rms_sku_id,
    ZPL_ticket_format,
    quantity,
    regular_price_currency_code,
    regular_price_amount,
    clearance_price_currency_code,
    clearance_price_amount,
    promotion_price_currency_code,
    promotion_price_amount
    FROM {db_env}_NAP_STG.ITEM_TICKET_PRINTED_LDG
) WITH DATA PRIMARY INDEX(universal_unique_id) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if worker number exist.
MERGE INTO {db_env}_NAP_BASE_VWS.ITEM_TICKET_PRINTED_FACT tgt
 USING  ITEM_TICKET_EVENT_FACT_UNIQUE_TEMP src
	ON (src.universal_unique_id = tgt.universal_unique_id)
WHEN MATCHED THEN
UPDATE
SET
      ticket_print_time = src.ticket_print_time,
      store_number = src.store_number,
      device_id = src.device_id,
      employee_id_type = src.employee_id_type,
      employee_id = src.employee_id,
      upc_id = src.upc_id,
      rms_sku_id = src.rms_sku_id,
      ZPL_ticket_format = src.ZPL_ticket_format,
      quantity = src.quantity,
      regular_price_currency_code = src.regular_price_currency_code,
      regular_price_amount = src.regular_price_amount,
      clearance_price_currency_code = src.clearance_price_currency_code,
      clearance_price_amount = src.clearance_price_amount,
      promotion_price_currency_code = src.promotion_price_currency_code,
      promotion_price_amount = src.promotion_price_amount,
      dw_batch_date = CURRENT_DATE,
      dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)

WHEN NOT MATCHED THEN
INSERT (
      ticket_print_time,
      universal_unique_id,
      store_number,
      device_id,
      employee_id_type,
      employee_id,
      upc_id,
      rms_sku_id,
      ZPL_ticket_format,
      quantity,
      regular_price_currency_code,
      regular_price_amount,
      clearance_price_currency_code,
      clearance_price_amount,
      promotion_price_currency_code,
      promotion_price_amount,
      dw_batch_date,
      dw_sys_load_tmstp
)
VALUES (
      src.ticket_print_time,
      src.universal_unique_id,
      src.store_number,
      src.device_id,
      src.employee_id_type,
      src.employee_id,
      src.upc_id,
      src.rms_sku_id,
      src.ZPL_ticket_format,
      src.quantity,
      src.regular_price_currency_code,
      src.regular_price_amount,
      src.clearance_price_currency_code,
      src.clearance_price_amount,
      src.promotion_price_currency_code,
      src.promotion_price_amount,
      CURRENT_DATE,
      CURRENT_TIMESTAMP(0)
);
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
