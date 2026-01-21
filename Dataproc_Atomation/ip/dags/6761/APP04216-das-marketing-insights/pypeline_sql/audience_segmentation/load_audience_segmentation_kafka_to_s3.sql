-- Read kafka source data with exploded customerAudienceStates
CREATE TEMPORARY VIEW exploded_kafka_source AS
SELECT
    id customer_id,
    customer.idType customer_type,
    explode_outer(customerAudienceStates) customerAudienceState,
    lastTriggeringEventTime last_triggering_event_time,
    decode(headers.SystemTime,'UTF-8') as system_time
from kafka_source;

-- prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    customer_id,
    customer_type,
    customerAudienceState.audienceCode audience_code,
    customerAudienceState.audienceUpdatedTimestamp audience_update_timestamp,
    customerAudienceState.audienceChangeReason audience_change_reason,
    customerAudienceState.audienceCustomerMembershipStatus audience_customer_membership_status,
    last_triggering_event_time,
    system_time
FROM exploded_kafka_source;


-- write result table
INSERT OVERWRITE audience_segmentation_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE audience_segmentation_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
