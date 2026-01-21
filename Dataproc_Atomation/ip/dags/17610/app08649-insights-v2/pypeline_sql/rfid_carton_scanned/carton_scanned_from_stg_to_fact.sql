CREATE
VOLATILE MULTISET TABLE #CARTON_SCANNED_TEMP AS (
  WITH ETL_BATCH_INFO AS (
    SELECT MAX(batch_id) AS dw_batch_id
         , MAX(batch_date) AS dw_batch_date
    FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
    WHERE INTERFACE_CODE = 'INVENTORY_STORE_CARTON_RFID_SCANNED_FACT'
    AND dw_sys_end_tmstp IS NULL ),
  CARTON_SCANNED_TEMP AS (
    SELECT DISTINCT
             eventTime,
             CAST(eventTime || '+00:00' AS TIMESTAMP(6)) AT 'America Pacific' as eventTimePacific,
             CAST(CAST(eventTime || '+00:00' AS TIMESTAMP(6)) AT 'America Pacific' AS DATE FORMAT 'YYYY-MM-DD') as eventDatePacific,
             kafkaTimestamp,
             storeNumber,
             cartonNumber,
             loadNumber,
             shipmentReceiptId,
             transactionId,
             sourceSystem,
             employee_id,
             employee_idType,
             cartonDetails_expectedItemQuantity_item_id,
             cartonDetails_expectedItemQuantity_item_idType,
             cartonDetails_expectedItemQuantity_quantity,
             cartonDetails_expectedItemQuantity_epcs,
             cartonDetails_isInRfidProgram,
             cartonDetails_scannedItemQuantities_item_id,
             cartonDetails_scannedItemQuantities_item_idType,
             cartonDetails_scannedItemQuantities_quantity,
             cartonDetails_scannedItemQuantities_epcs,
             dw_batch_id,
             dw_batch_date
     FROM {db_env}_NAP_STG.INVENTORY_STORE_CARTON_RFID_SCANNED_LDG
    LEFT JOIN ETL_BATCH_INFO ON 1=1
    qualify row_number() over (partition BY cartonNumber, loadNumber, storeNumber, eventTime, cartonDetails_scannedItemQuantities_item_id ORDER BY kafkaTimestamp DESC) = 1)
  SELECT * FROM CARTON_SCANNED_TEMP ldg
) WITH DATA PRIMARY INDEX(cartonNumber, loadNumber, storeNumber, eventTime, cartonDetails_scannedItemQuantities_item_id) ON COMMIT PRESERVE ROWS;
ET;
-- Merge and update:
MERGE INTO {db_env}_NAP_FCT.INVENTORY_STORE_CARTON_RFID_SCANNED_FACT tgt
    USING #CARTON_SCANNED_TEMP src
    ON (src.eventTime = tgt.event_tmstp
        AND src.storeNumber = tgt.store_number
        AND src.cartonNumber = tgt.carton_number
        AND src.loadNumber = tgt.load_number
        AND src.cartonDetails_scannedItemQuantities_item_id = tgt.scanned_item_sku_id)
    WHEN MATCHED THEN UPDATE SET
        event_tmstp_pacific = src.eventTimePacific,
        event_date_pacific = src.eventDatePacific,
        kafka_timestamp = src.kafkaTimestamp,
        shipment_receipt_id = src.shipmentReceiptId,
        transaction_id = src.transactionId,
        source_system = src.sourceSystem,
        employee_id = src.employee_id,
        employee_id_type = src.employee_idType,
        expected_item_sku_id = src.cartonDetails_expectedItemQuantity_item_id,
        expected_item_sku_id_type = src.cartonDetails_expectedItemQuantity_item_idType,
        expected_item_quantity = src.cartonDetails_expectedItemQuantity_quantity,
        expected_item_quantity_epcs = src.cartonDetails_expectedItemQuantity_epcs,
        is_in_rfid_program = src.cartonDetails_isInRfidProgram,
        scanned_item_sku_id_type = src.cartonDetails_scannedItemQuantities_item_idType,
        scanned_item_quantity = src.cartonDetails_scannedItemQuantities_quantity,
        scanned_item_quantity_epcs = src.cartonDetails_scannedItemQuantities_epcs,
        dw_batch_id=src.dw_batch_id,
	    dw_batch_date = src.dw_batch_date,
	    dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)

WHEN NOT MATCHED THEN INSERT (
        event_tmstp,
        event_tmstp_pacific,
        event_date_pacific,
        kafka_timestamp,
        store_number,
        carton_number,
        load_number,
        shipment_receipt_id,
        transaction_id,
        source_system,
        employee_id,
        employee_id_type,
        expected_item_sku_id,
        expected_item_sku_id_type,
        expected_item_quantity,
        expected_item_quantity_epcs,
        is_in_rfid_program,
        scanned_item_sku_id,
        scanned_item_sku_id_type,
        scanned_item_quantity,
        scanned_item_quantity_epcs,
        dw_batch_id,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp
)
VALUES (
        src.eventTime,
        src.eventTimePacific,
        src.eventDatePacific,
        src.kafkaTimestamp,
        src.storeNumber,
        src.cartonNumber,
        src.loadNumber,
        src.shipmentReceiptId,
        src.transactionId,
        src.sourceSystem,
        src.employee_id,
        src.employee_idType,
        src.cartonDetails_expectedItemQuantity_item_id,
        src.cartonDetails_expectedItemQuantity_item_idType,
        src.cartonDetails_expectedItemQuantity_quantity,
        src.cartonDetails_expectedItemQuantity_epcs,
        src.cartonDetails_isInRfidProgram,
        src.cartonDetails_scannedItemQuantities_item_id,
        src.cartonDetails_scannedItemQuantities_item_idType,
        src.cartonDetails_scannedItemQuantities_quantity,
        src.cartonDetails_scannedItemQuantities_epcs,
        src.dw_batch_id,
	    src.dw_batch_date,
        CURRENT_TIMESTAMP(0),
        CURRENT_TIMESTAMP(0)
    );
ET;
