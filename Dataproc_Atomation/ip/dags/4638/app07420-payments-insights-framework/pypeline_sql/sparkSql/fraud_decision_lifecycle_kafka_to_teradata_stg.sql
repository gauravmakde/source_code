-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=fraud_decision_lifecycle_kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

CREATE TEMPORARY VIEW fraud_decision_lifecycle_temp_object_model AS
SELECT *
FROM kafka_fraud_decision_lifecycle_analytical_avro;

INSERT INTO TABLE fraud_decision_lifecycle_ldg
SELECT
    ordernumber AS order_id,
    orderapproveddetail.frauddecisionauthority AS authority,
    'APPROVED' AS fraud_decision,
    orderapproveddetail.approvaldetails AS decision_detail,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    objectmetadata.createdtime AS metadata_created_time,
    objectmetadata.lastupdatedtime AS metadata_last_updated_time,
    objectmetadata.lasttriggeringeventname AS metadata_last_triggering_event_name,
    orderapproveddetail.eventid AS event_id,
    orderapproveddetail.eventtime AS event_time
FROM fraud_decision_lifecycle_temp_object_model
WHERE orderapproveddetail IS NOT NULL;

INSERT INTO TABLE fraud_decision_lifecycle_ldg
SELECT
    ordernumber AS order_id,
    orderdeclineddetail.frauddecisionauthority AS authority,
    'DECLINED' AS fraud_decision,
    orderdeclineddetail.declinedetails AS decision_detail,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    objectmetadata.createdtime AS metadata_created_time,
    objectmetadata.lastupdatedtime AS metadata_last_updated_time,
    objectmetadata.lasttriggeringeventname AS metadata_last_triggering_event_name,
    orderdeclineddetail.eventid AS event_id,
    orderdeclineddetail.eventtime AS event_time
FROM fraud_decision_lifecycle_temp_object_model
WHERE orderdeclineddetail IS NOT NULL;

CREATE TEMPORARY VIEW order_line_change_fraud_approved_exploded AS
SELECT
    ordernumber AS order_id,
    'APPROVED' AS fraud_decision,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    objectmetadata.createdtime AS metadata_created_time,
    objectmetadata.lastupdatedtime AS metadata_last_updated_time,
    objectmetadata.lasttriggeringeventname AS metadata_last_triggering_event_name,
    explode(orderlinechangeapproveddetails) AS orderlinechangeapproveddetails
FROM fraud_decision_lifecycle_temp_object_model
WHERE size(orderlinechangeapproveddetails) > 0;

INSERT INTO TABLE fraud_decision_lifecycle_ldg
SELECT
    order_id,
    orderlinechangeapproveddetails.orderlineid AS order_line_id,
    CASE
        WHEN orderlinechangeapproveddetails.frauddecisionauthority = 'DESERIALIZATION_DEFAULT_VALUE'
            THEN 'UNKNOWN'
        ELSE orderlinechangeapproveddetails.frauddecisionauthority
    END AS authority,
    fraud_decision,
    orderlinechangeapproveddetails.approvaldetails AS decision_detail,
    channel_country,
    channel_brand,
    selling_channel,
    orderlinechangeapproveddetails.store AS store_number,
    orderlinechangeapproveddetails.register AS register,
    orderlinechangeapproveddetails.feature AS feature,
    orderlinechangeapproveddetails.servicename AS service_name,
    orderlinechangeapproveddetails.serviceticketid AS service_ticket_id,
    metadata_created_time,
    metadata_last_updated_time,
    metadata_last_triggering_event_name,
    orderlinechangeapproveddetails.eventid AS event_id,
    orderlinechangeapproveddetails.eventtime AS event_time
FROM order_line_change_fraud_approved_exploded;

CREATE TEMPORARY VIEW order_line_change_fraud_declined_exploded AS
SELECT
    ordernumber AS order_id,
    'DECLINED' AS fraud_decision,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    objectmetadata.createdtime AS metadata_created_time,
    objectmetadata.lastupdatedtime AS metadata_last_updated_time,
    objectmetadata.lasttriggeringeventname AS metadata_last_triggering_event_name,
    explode(orderlinechangedeclineddetails) AS orderlinechangedeclineddetails
FROM fraud_decision_lifecycle_temp_object_model
WHERE size(orderlinechangedeclineddetails) > 0;

INSERT INTO TABLE fraud_decision_lifecycle_ldg
SELECT
    order_id,
    orderlinechangedeclineddetails.orderlineid AS order_line_id,
    CASE
        WHEN orderlinechangedeclineddetails.frauddecisionauthority = 'DESERIALIZATION_DEFAULT_VALUE'
            THEN 'UNKNOWN'
        ELSE orderlinechangedeclineddetails.frauddecisionauthority
    END AS authority,
    fraud_decision,
    orderlinechangedeclineddetails.declinedetails AS decision_detail,
    channel_country,
    channel_brand,
    selling_channel,
    orderlinechangedeclineddetails.store AS store_number,
    orderlinechangedeclineddetails.register AS register,
    orderlinechangedeclineddetails.feature AS feature,
    orderlinechangedeclineddetails.servicename AS service_name,
    orderlinechangedeclineddetails.serviceticketid AS service_ticket_id,
    metadata_created_time,
    metadata_last_updated_time,
    metadata_last_triggering_event_name,
    orderlinechangedeclineddetails.eventid AS event_id,
    orderlinechangedeclineddetails.eventtime AS event_time
FROM order_line_change_fraud_declined_exploded;
