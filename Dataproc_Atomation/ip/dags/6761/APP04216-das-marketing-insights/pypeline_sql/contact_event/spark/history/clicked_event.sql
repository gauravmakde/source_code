CREATE TEMPORARY VIEW contact_event_avro_src AS SELECT * FROM contact_event_avro_source;

CREATE TEMPORARY VIEW clicked_event_explode AS
SELECT
    explode(
        transform(
            value.eventClicked,
            x-> named_struct('subscriberKey', x.subscriberKey, 'sendId', x.sendId, 'clientId',  x.clientId, 'batchId', x.batchId, 'eventTime', x.eventTime, 'sendUrlId', x.sendUrlId, 'urlId', x.urlId, 'url', x.url)
        )
    ) as eventClicked
FROM contact_event_avro_src;

CREATE TEMPORARY VIEW clicked_event_final AS
SELECT
    eventClicked.subscriberKey as enterpriseId,
    eventClicked.sendId as sendId,
    eventClicked.clientId as clientId,
    eventClicked.batchId as batch_id,
    eventClicked.eventTime as eventTime,
    eventClicked.sendUrlId as sendUrlId,
    eventClicked.urlId as urlId,
    eventClicked.url as url
FROM clicked_event_explode;


-- Write result table
INSERT OVERWRITE clicked_event_data SELECT * FROM clicked_event_final;

-- Audit table
INSERT OVERWRITE clicked_event_audit SELECT COUNT(*) FROM clicked_event_final;
