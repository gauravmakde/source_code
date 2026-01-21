-- Read from kafka source Gift card funding type
CREATE TEMPORARY VIEW GC_promotion_final AS
SELECT
    promotionCode as promo_code,
    promotionDescription as promo_code_desc,
    cardClassId as class_id,
    expirationDate as expiration_date,
    expirationType as expiration_type,
    cast((cast(cast(lastUpdatedTime as double) as DECIMAL(14, 3)) * 1000) as long) as event_tmstp,
    decode(headers.SystemTime, 'UTF-8') sys_tmstp
FROM kafka_source;

-- write result table
INSERT OVERWRITE promotion_data_extract
SELECT * FROM GC_promotion_final;

-- Audit table
INSERT OVERWRITE promotion_final_count
SELECT COUNT(*) FROM GC_promotion_final;

