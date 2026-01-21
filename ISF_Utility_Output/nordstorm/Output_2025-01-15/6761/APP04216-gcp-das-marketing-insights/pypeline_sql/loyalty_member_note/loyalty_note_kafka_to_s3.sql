-- Prepare final table from kafka source
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    loyaltyId,
    loyaltyNoteId,
    noteNumber.value AS noteNumber,
    noteNumber.authority AS noteNumber_authority,
    noteNumber.dataClassification AS noteNumber_dataClassification,
    noteState,
    noteType,
    expiryDate,
    modeOfNoteDelivery,
    noteDeliveryModeReason,
    issueDate,
    decode(headers.SystemTime,'UTF-8') AS SystemTime
FROM kafka_source;

-- Write result table
INSERT OVERWRITE loyalty_note_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE loyalty_note_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
