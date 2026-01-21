-- Read from kafka source Gift card funding type
CREATE TEMPORARY VIEW fundingClassAvro AS
SELECT
    cardClassId as class_id,
    transactionCode as activation_tran_code,
    fundingTypeCategory as fund_type,
    cast((cast(cast(lastUpdatedTime as double) as DECIMAL(14, 3)) * 1000) as long) as event_tmstp,
    decode(headers.SystemTime, 'UTF-8') sys_tmstp
FROM kafka_source;

-- write result table
INSERT OVERWRITE funding_data_extract
SELECT * FROM fundingClassAvro;

-- Audit table
INSERT OVERWRITE funding_final_count
SELECT COUNT(*) FROM fundingClassAvro;

