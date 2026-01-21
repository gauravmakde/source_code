-- Read kafka source data with exploded contactPreferences
CREATE TEMPORARY VIEW exploded_kafka_source AS
SELECT
    id id,
    auditTrail.customerDataSource auditTrail_customerDataSource,
    auditTrail.customerDataSubSource auditTrail_customerDataSubSource,
    auditTrail.sourceUpdateDate auditTrail_sourceUpdateDate,
    auditTrail.databaseUpdateDate auditTrail_databaseUpdateDate,
    customer.idType customerIdType,
    customer.id customerId,
    lastUpdateCountry lastUpdateCountry,
    explode_outer(contactPreferences) contactPreferences,
    lastUpdatedTime lastUpdatedTime,
    decode(headers.SourceEventType,'UTF-8') as SourceEventType,
    decode(headers.SystemTime,'UTF-8') as SystemTime
from kafka_source;

-- prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    id,
    auditTrail_customerDataSource,
    auditTrail_customerDataSubSource,
    auditTrail_sourceUpdateDate,
    auditTrail_databaseUpdateDate,
    customerIdType,
    customerId,
    lastUpdateCountry,
    contactPreferences.name as contactPreferences_name,
    contactPreferences.preferenceValue as contactPreferences_preferenceValue,
    contactPreferences.sourceUpdateDate as contactPreferences_sourceUpdateDate,
    contactPreferences.expressCaptured as contactPreferences_expressCaptured,
    lastUpdatedTime,
    SourceEventType,
    SystemTime
FROM exploded_kafka_source;


-- write result table
INSERT OVERWRITE cpp_postal_preference_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE cpp_postal_preference_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
