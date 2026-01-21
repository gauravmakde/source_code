CREATE TEMPORARY VIEW contact_event_avro_src AS SELECT * FROM contact_event_avro_source;

CREATE TEMPORARY VIEW unsubscribed_event_explode AS
SELECT
    explode(
        transform(
            value.eventUnsubscribed,
            x-> named_struct('subscriberKey', x.subscriberKey, 'sendId', x.sendId, 'clientId',  x.clientId, 'batchId', x.batchId, 'reason', x.reason, 'eventTime', x.eventTime)
        )
    ) as eventUnsubscribed
FROM contact_event_avro_src;

CREATE TEMPORARY VIEW unsubscribed_event_final AS
SELECT
    eventUnsubscribed.subscriberKey as enterpriseId,
    eventUnsubscribed.sendId as sendId,
    eventUnsubscribed.clientId as clientId,
    eventUnsubscribed.batchId as batch_id,
    eventUnsubscribed.reason as reason,
    eventUnsubscribed.eventTime as eventTime
FROM unsubscribed_event_explode;


-- Write result table
INSERT OVERWRITE unsubscribed_event_data SELECT * FROM unsubscribed_event_final;

-- Audit table
INSERT OVERWRITE unsubscribed_event_audit SELECT COUNT(*) FROM unsubscribed_event_final;
