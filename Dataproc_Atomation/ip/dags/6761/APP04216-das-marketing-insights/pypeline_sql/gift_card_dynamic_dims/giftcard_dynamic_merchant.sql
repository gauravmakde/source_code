-- Read from kafka source Gift card funding type
CREATE TEMPORARY VIEW GC_merchant_final AS
SELECT
    merchantId as merchant_id,
    merchantDescription as merchant_desc,
    cast((cast(cast(lastUpdatedTime as double) as DECIMAL(14, 3)) * 1000) as long) as event_tmstp,
    decode(headers.SystemTime, 'UTF-8') sys_tmstp
FROM kafka_source;

-- write result table
INSERT OVERWRITE merchant_data_extract
SELECT * FROM GC_merchant_final;

-- Audit table
INSERT OVERWRITE merchant_final_count
SELECT COUNT(*) FROM GC_merchant_final;

