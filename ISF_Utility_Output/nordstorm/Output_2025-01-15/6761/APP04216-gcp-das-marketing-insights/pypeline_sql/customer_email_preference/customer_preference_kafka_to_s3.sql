-- Create temp view with exploded contactPreferences
CREATE TEMPORARY VIEW exploded_contactPreferences_view AS
SELECT
    preference.id AS enterprise_id,
    preference.regulationDomain AS country,
    preference.value.value AS tokenized_email,
    preference.type AS preference_type,
    preference.contactFrequencyType.frequencyType AS contactFrequencyType,
    explode_outer(preference.contactPreferences) AS contactPreferences,
    preference.receivedDetails.firstReceivedDate AS firstReceivedDate,
    preference.receivedDetails.customerDataSourceDetails.customerDataSource AS first_received_datasource,
    preference.receivedDetails.customerDataSourceDetails.customerDataSubSource AS first_received_datasubsource,
    decode(headers.SystemTime,'UTF-8') AS SystemTime
FROM kafka_source
WHERE preference.type='EMAIL';

-- Prepare final table from kafka source
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    enterprise_id,
    country,
    tokenized_email,
    preference_type,
    contactFrequencyType,
    contactPreferences.preferenceValue AS preferenceValue,
    firstReceivedDate,
    first_received_datasource,
    first_received_datasubsource,
    SystemTime
FROM exploded_contactPreferences_view;

-- Write result table
INSERT OVERWRITE customer_preference_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE customer_preference_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
