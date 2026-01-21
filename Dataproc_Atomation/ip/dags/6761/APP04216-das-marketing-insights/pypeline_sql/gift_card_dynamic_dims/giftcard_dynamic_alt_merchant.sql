-- Read from kafka source Gift card alt merchant type
CREATE TEMPORARY VIEW GC_altMerchant_final AS
SELECT
    alternateMerchantId as alt_merchant_id,
    merchantId as merchant_id,
    alternateMerchantDescription as alt_merchant_desc,
    cast((cast(cast(lastUpdatedTime as double) as DECIMAL(14, 3)) * 1000) as long) as event_tmstp,
    decode(headers.SystemTime, 'UTF-8') sys_tmstp
FROM kafka_source;

-- write result table
INSERT OVERWRITE alt_merchant_data_extract
SELECT * FROM GC_altMerchant_final;

-- Audit table
INSERT OVERWRITE alt_merchant_final_count
SELECT COUNT(*) FROM GC_altMerchant_final;

