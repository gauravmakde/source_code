CREATE TEMPORARY VIEW contact_event_avro_src AS SELECT * FROM contact_event_avro_source;

CREATE TEMPORARY VIEW sent_event_final AS
SELECT
    value.enterpriseId as enterpriseId,
    value.sendId as sendId,
    value.clientId as clientId,
    value.eventSent.batchId as batch_id,
    value.eventSent.eventTime as eventTime
FROM contact_event_avro_src
WHERE value.eventSent is not null;

-- Write result table
INSERT OVERWRITE sent_event_data SELECT * FROM sent_event_final;

-- Audit table
INSERT OVERWRITE sent_event_audit SELECT COUNT(*) FROM sent_event_final;
