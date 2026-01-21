CREATE TEMPORARY VIEW contact_event_avro_src AS SELECT * FROM contact_event_avro_source;

CREATE TEMPORARY VIEW bounced_event_explode AS
SELECT
       explode(
            transform(
                value.eventBounced,
                x-> named_struct('subscriberKey', x.subscriberKey, 'sendId', x.sendId, 'clientId',  x.clientId, 'batchId', x.batchId, 'bounceCategory', x.bounceCategory, 'bounceReason', x.bounceReason, 'eventTime', x.eventTime)
            )
       ) as eventBounced
FROM contact_event_avro_src;

CREATE TEMPORARY VIEW bounced_event_final AS
SELECT
    eventBounced.subscriberKey as enterpriseId,
    eventBounced.sendId as sendId,
    eventBounced.clientId as clientId,
    eventBounced.batchId as batch_id,
    eventBounced.bounceCategory as bounceCategory,
    eventBounced.bounceReason as bounceReason,
    eventBounced.eventTime as eventTime
FROM bounced_event_explode;


-- Write result table
INSERT OVERWRITE bounced_event_data SELECT * FROM bounced_event_final;

-- Audit table
INSERT OVERWRITE bounced_event_audit SELECT COUNT(*) FROM bounced_event_final;
