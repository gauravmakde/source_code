-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app06788;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

-- Read Kafka into temp table

CREATE TEMPORARY VIEW temp_credit_account_analytical_object
AS
SELECT * FROM kafka_customer_credit_account_analytical_avro;

CREATE TEMPORARY VIEW temp_credit_account
AS
SELECT
    openedtime AS opened_time,
    closeddate AS closed_date,
    openedchannel.channelcountry AS opened_channel_country,
    openedchannel.channelbrand AS opened_channel_brand,
    openedchannel.sellingchannel AS selling_channel,
    creditaccountid AS credit_account_id,
    creditapplicationid AS credit_application_id,
    status AS status,
    objectmetadata.createdtime AS metadata_created_time,
    objectmetadata.lastupdatedtime AS metadata_last_updated_time,
    objectmetadata.lasttriggeringeventname AS metadata_last_triggering_event_name,
    applicationreceivedtime AS application_received_time,
    sourcecode AS source_code,
    element_at(headers, 'Id') AS hdr_id,
    element_at(headers, 'AppId') AS hdr_appid,
    element_at(headers, 'EventTime') AS hdr_eventtime,
    element_at(headers, 'SystemTime') AS hdr_systemtime
FROM temp_credit_account_analytical_object;

CREATE TEMPORARY VIEW temp_credit_account_bank_card AS
SELECT
    creditaccountid AS credit_account_id,
    element_at(headers, 'Id') AS hdr_id,
-- noqa: disable=all
    explode(bankcards)
-- noqa: enable=all
FROM temp_credit_account_analytical_object;


INSERT INTO TABLE credit_account_ldg
SELECT
    hdr_id,
    hdr_appid,
    hdr_eventtime,
    hdr_systemtime,
    opened_time,
    closed_date,
    opened_channel_country,
    opened_channel_brand,
    selling_channel,
    credit_account_id,
    credit_application_id,
    status,
    metadata_created_time,
    metadata_last_updated_time,
    metadata_last_triggering_event_name,
    application_received_time,
    source_code
FROM temp_credit_account;


INSERT INTO TABLE credit_account_bank_card_ldg
SELECT
    temp_credit_account_bank_card.hdr_id,
    temp_credit_account_bank_card.credit_account_id,
    temp_credit_account_bank_card.key AS card_id,
    temp_credit_account_bank_card.value.addeddate AS added_date,
    temp_credit_account_bank_card.value.closeddate AS closed_date,
    temp_credit_account_bank_card.value.role AS card_member_role,
    temp_credit_account_bank_card.value.creditaccountcard.cardexpirationdate.month AS card_expiration_month,
    temp_credit_account_bank_card.value.creditaccountcard.cardexpirationdate.year AS card_expiration_year,
    temp_credit_account_bank_card.value.creditaccountcard.paymentid AS card_payment_id,
    temp_credit_account_bank_card.value.creditaccountcard.status AS card_status,
    temp_credit_account_bank_card.value.creditaccountcard.nameoncard.value AS name_on_card_value,
    temp_credit_account_bank_card.value.creditaccountcard.nameoncard.authority AS name_on_card_authority,
    temp_credit_account_bank_card.value.creditaccountcard.nameoncard.strategy AS name_on_card_strategy,
    temp_credit_account_bank_card.value.creditaccountcard.nameoncard.dataclassification AS name_on_card_data_classification,
    temp_credit_account_bank_card.value.replacedcardid AS replaced_card_id,
    temp_credit_account_bank_card.value.activateddate AS activated_date,
    temp_credit_account_bank_card.value.tenderservice.tenderserviceid AS tender_service_id,
    temp_credit_account_bank_card.value.tenderservice.producttype AS tender_service_product_type,
    temp_credit_account_bank_card.value.city AS city,
    temp_credit_account_bank_card.value.state AS state,
    temp_credit_account_bank_card.value.coarsepostalcode AS coarse_postal_code,
    temp_credit_account_bank_card.value.bankcardaddedreason AS card_added_reason,
    temp_credit_account_bank_card.value.bankcardclosedreason AS card_closed_reason
FROM temp_credit_account_bank_card;
