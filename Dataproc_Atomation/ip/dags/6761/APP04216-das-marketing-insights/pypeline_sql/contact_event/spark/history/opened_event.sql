CREATE TEMPORARY VIEW contact_event_avro_src AS SELECT * FROM contact_event_avro_source;

CREATE TEMPORARY VIEW opened_event_explode AS
SELECT
    explode(
        transform(
            value.eventOpened,
            x-> named_struct('subscriberKey', x.subscriberKey, 'sendId', x.sendId, 'clientId',  x.clientId, 'batchId', x.batchId, 'eventTime', x.eventTime)
        )
    ) as eventOpened
FROM contact_event_avro_src;

CREATE TEMPORARY VIEW opened_event_final AS
SELECT
    eventOpened.subscriberKey as enterpriseId,
    eventOpened.sendId as sendId,
    eventOpened.clientId as clientId,
    eventOpened.batchId as batch_id,
    eventOpened.eventTime as eventTime
FROM opened_event_explode;


-- Write result table
INSERT OVERWRITE opened_event_data SELECT * FROM opened_event_final;

-- Audit table
INSERT OVERWRITE opened_event_audit SELECT COUNT(*) FROM opened_event_final;
