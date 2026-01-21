-- Read kafka source data with row number over uniquesourceid ordered by eventTimestamp
CREATE TEMPORARY VIEW kafka_source_with_row_number AS
SELECT
    event.uniquesourceid AS uniquesource_id,
    customerAudit.updated.auditDate AS auditDate,
    customerIdentity.stdEmail.value AS stdEmail,
    customeridentity.stdalternateemails AS stdalternateemails,
    customerAudit.updated.auditDate AS sourceupdatedate,
    event.eventTimestamp AS eventTimestamp,
    event.type AS eventType,
    row_number() OVER (partition BY event.uniquesourceid ORDER BY event.eventTimestamp DESC) AS rn
FROM kafka_source;

-- Create temp view where row number equals 1
CREATE TEMPORARY VIEW temp_view_filtered_by_row_number AS
SELECT
    uniquesource_id,
    auditDate,
    stdEmail,
    stdalternateemails,
    sourceupdatedate,
    eventTimestamp,
    eventType
FROM kafka_source_with_row_number
WHERE rn=1;

-- Create temp view with exploded stdalternateemails
CREATE TEMPORARY VIEW exploded_stdalternateemails_view AS
SELECT
    uniquesource_id,
    auditDate,
    sourceupdatedate,
    eventTimestamp,
    eventType,
    explode_outer(stdalternateemails) AS alternateemails
FROM temp_view_filtered_by_row_number;

-- Create distinct temp view
CREATE TEMPORARY VIEW distinct_temp_view AS
SELECT
    DISTINCT uniquesource_id,
             auditDate,
             alternateemails.value.value AS alternateemails_value,
             sourceupdatedate,
             eventTimestamp,
             eventType
FROM exploded_stdalternateemails_view;

-- Prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    uniquesource_id,
    stdEmail,
    auditDate,
    sourceupdatedate,
    eventTimestamp,
    eventType
FROM temp_view_filtered_by_row_number
WHERE stdEmail IS NOT NULL
UNION
SELECT
    uniquesource_id,
    alternateemails_value,
    auditDate,
    sourceupdatedate,
    eventTimestamp,
    eventType
FROM distinct_temp_view
WHERE alternateemails_value IS NOT NULL;

-- Write result table
INSERT OVERWRITE customer_tokenized_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE customer_tokenized_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
