-- Read from kafka source EMAIL Cpp Preference type
CREATE TEMPORARY VIEW email_cpp_preferences AS
SELECT
    preference.id enterprise_id,
    preference.regulationDomain country,
    preference.type type,
    preference.contactFrequencyType.frequencyType contactFrequencyType,
    preference.contactFrequencyType.frequency contactFrequency,
    preference.contactPreferences contactPreferences,
    preference.receivedDetails.firstReceivedDate firstReceivedDate,
    preference.receivedDetails.customerDataSourceDetails.customerDataSource AS first_received_datasource ,
    preference.receivedDetails.customerDataSourceDetails.customerDataSubSource AS first_received_datasubsource,
    decode(headers.SystemTime,'UTF-8') AS SystemTime,
    preference.doNotUseReason doNotUseReason
FROM kafka_source
WHERE preference.type='EMAIL';

-- Temp view with exploded contact preferences
CREATE TEMPORARY VIEW exploded_contact_preferences AS
SELECT
    enterprise_id,
    type,
    country,
    contactFrequency,
    contactFrequencyType,
    explode_outer(contactPreferences) contactPreferences,
    firstReceivedDate,
    first_received_datasource,
    first_received_datasubsource,
    doNotUseReason,
    SystemTime
FROM email_cpp_preferences;

-- prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    enterprise_id,
    type,
    country,
    contactFrequency,
    contactFrequencyType,
    firstReceivedDate,
    first_received_datasource,
    first_received_datasubsource,
    contactPreferences.preferenceValue preferenceValue,
    contactPreferences.expressCaptured preference_consent,
    contactPreferences.sourceUpdateDate source_update_date,
    doNotUseReason,
    SystemTime
FROM exploded_contact_preferences;

-- write result table
INSERT OVERWRITE cpp_marttech_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE cpp_marttech_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
