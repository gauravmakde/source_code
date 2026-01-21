-- Select all needed fields into the final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    audienceCode as audience_code,
    metadata.lastTriggeringEventName as last_triggering_event_name,
    audienceDetails.name as audience_details_name,
    audienceDetails.description as audience_details_description,
    audienceDetails.definition as audience_details_definition,
    concat_ws(' , ', array_sort(audienceDetails.deterministicAttributes)) as audience_details_deterministic_attributes,
    concat_ws(' , ', array_sort(audienceDetails.probablisticAttributes)) as audience_details_probablistic_attributes,
    lastTriggeringEventTime as last_triggering_event_time,
    decode(headers.SystemTime,'UTF-8') AS SystemTime
FROM kafka_source;

-- write result table
INSERT OVERWRITE audience_definition_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE audience_definition_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
