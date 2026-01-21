-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
create temporary view temp_object_model as
select * from kafka_payment_settlement_intent_analytical_avro;

create temporary view temp_settlement_intent as
select
    eventtime as event_time,
    source.channelcountry as source_channel_country,
    source.channel as source_channel,
    source.platform as source_platform,
    source.feature as source_feature,
    source.servicename as source_service_name,
    source.store as source_store,
    source.register as source_register,
    purchaseidentifier.type as purchase_identifier_type,
    purchaseidentifier.id as purchase_identifier_id,
    transactionidentifier.type as transaction_identifier_type,
    transactionidentifier.id as transaction_identifier_id,
    totalrequestedamount.currencycode as total_requested_amount_currency_code,
    cast(
        cast(
            totalrequestedamount.units as INTEGER
        ) + (totalrequestedamount.nanos / 1000000000) as DECIMAL (12, 2)
    ) as total_requested_amount_amount,
    merchantIdentifier AS merchant_identifier,
    failurereason as failure_reason,
    serviceticketid as service_ticket_id,
    element_at(headers, 'Id') as hdr_id,
    element_at(headers, 'AppId') as hdr_appid,
    element_at(headers, 'EventTime') as hdr_eventtime,
    element_at(headers, 'SystemTime') as hdr_systemtime
from temp_object_model;

create temporary view temp_bankcard_settlement_results as
select
    transactionidentifier.type as transaction_identifier_type,
    transactionidentifier.id as transaction_identifier_id,
    explode(bankcardsettlementresults) as bankcard_settlement_results
from temp_object_model;

create temporary view temp_paypal_billing_agreement_settlement_results as
select
    transactionidentifier.type as transaction_identifier_type,
    transactionidentifier.id as transaction_identifier_id,
    explode(
        paypalbillingagreementsettlementresults
    ) as paypal_billing_agreement_settlement_results
from temp_object_model;

create temporary view temp_paypal_settlement_results as
select
    transactionidentifier.type as transaction_identifier_type,
    transactionidentifier.id as transaction_identifier_id,
    explode(paypalsettlementresults) as paypal_settlement_results
from temp_object_model;

create temporary view temp_giftcardnote_tender_results as
select
    transactionidentifier.type as transaction_identifier_type,
    transactionidentifier.id as transaction_identifier_id,
    explode(giftcardnotetenderresults) as giftcard_note_tender_results
from temp_object_model;

--Sink
insert into payment_settlement_intent_stg
select
    event_time,
    source_channel_country,
    source_channel,
    source_platform,
    source_feature,
    source_service_name,
    source_store,
    source_register,
    purchase_identifier_type,
    purchase_identifier_id,
    transaction_identifier_type,
    transaction_identifier_id,
    total_requested_amount_currency_code,
    total_requested_amount_amount,
    merchant_identifier,
    failure_reason,
    service_ticket_id
from temp_settlement_intent;

insert into bankcard_settlement_result_stg
select
    transaction_identifier_type,
    transaction_identifier_id,
    bankcard_settlement_results.bankcardresult.token.token.value as token_value,
    bankcard_settlement_results.bankcardresult.token.tokentype as token_type,
    bankcard_settlement_results.bankcardresult.cardtypeinfo.cardtype as card_type,
    bankcard_settlement_results.bankcardresult.cardtypeinfo.cardsubtype as card_subtype,
    cast(
        cast(bankcard_settlement_results.bankcardresult.total.units as INTEGER)
        + (
            bankcard_settlement_results.bankcardresult.total.nanos / 1000000000
        ) as DECIMAL (12, 2)
    ) as total_amount,
    bankcard_settlement_results.bankcardresult.transactiontime as transaction_time,
    bankcard_settlement_results.bankcardresult.transactionresult.resulttype as transaction_result_type,
    bankcard_settlement_results.bankcardresult.transactionresult.status as transaction_result_status,
    bankcard_settlement_results.bankcardresult.transactionresult.failurereason as transaction_result_failure_reason,
    bankcard_settlement_results.bankcardresult.tokenrequestorid as token_requestor_id,
    bankcard_settlement_results.vendorsettlementcode as vendor_settlement_code
from temp_bankcard_settlement_results;

insert into giftcard_note_tender_result_stg
select
    transaction_identifier_type,
    transaction_identifier_id,
    giftcard_note_tender_results.giftcardnotetype as gift_card_note_type,
    giftcard_note_tender_results.accountnumber.value as account_number,
    cast(
        cast(
            giftcard_note_tender_results.requestedamount.units as INTEGER
        ) + (giftcard_note_tender_results.requestedamount.nanos / 1000000000) as DECIMAL (12, 2)
    ) as requested_amount,
    cast(
        cast(
            giftcard_note_tender_results.appliedamount.units as INTEGER
        ) + (
            giftcard_note_tender_results.appliedamount.nanos / 1000000000
        ) as DECIMAL (12, 2)
    ) as applied_amount,
    giftcard_note_tender_results.transactiontime as transaction_time,
    giftcard_note_tender_results.tendertransactionresulttype as tender_transaction_result_type,
    giftcard_note_tender_results.transactionstatus as transaction_status
from temp_giftcardnote_tender_results;

insert into paypal_billing_agreement_settlement_result_stg
select
    transaction_identifier_type,
    transaction_identifier_id,
    paypal_billing_agreement_settlement_results.paypalbillingagreementtenderresult.payerid as payer_id,
    cast(
        cast(
            paypal_billing_agreement_settlement_results.paypalbillingagreementtenderresult.total.units as INTEGER
        ) + (
            paypal_billing_agreement_settlement_results.paypalbillingagreementtenderresult.total.nanos / 1000000000
        ) as DECIMAL (12, 2)
    ) as total,
    paypal_billing_agreement_settlement_results.paypalbillingagreementtenderresult.transactiontime as transaction_time,
    paypal_billing_agreement_settlement_results.paypalbillingagreementtenderresult.transactiontype as transaction_type,
    paypal_billing_agreement_settlement_results.paypalbillingagreementtenderresult.transactionstatus.status as transaction_status,
    paypal_billing_agreement_settlement_results.id as id,
    paypal_billing_agreement_settlement_results.parentreferenceid as parent_reference_id,
    cast(
        cast(
            paypal_billing_agreement_settlement_results.fee.units as INTEGER
        ) + (
            paypal_billing_agreement_settlement_results.fee.nanos / 1000000000
        ) as DECIMAL (12, 2)
    ) as fee,
    cast(
        cast(
            paypal_billing_agreement_settlement_results.netamount.units as INTEGER
        ) + (
            paypal_billing_agreement_settlement_results.netamount.nanos / 1000000000
        ) as DECIMAL (12, 2)
    ) as net_amount
from temp_paypal_billing_agreement_settlement_results;

insert into paypal_settlement_result_stg
select
    transaction_identifier_type,
    transaction_identifier_id,
    paypal_settlement_results.tenderresult.paypalorderid as paypal_order_id,
    paypal_settlement_results.tenderresult.payerid as payer_id,
    cast(
        cast(
            paypal_settlement_results.tenderresult.total.units as INTEGER
        ) + (
            paypal_settlement_results.tenderresult.total.nanos / 1000000000
        ) as DECIMAL (12, 2)
    ) as total,
    paypal_settlement_results.tenderresult.transactiontime as transaction_time,
    paypal_settlement_results.tenderresult.transactiontype as transaction_type,
    paypal_settlement_results.tenderresult.transactionstatus.status as transaction_status,
    paypal_settlement_results.tenderresult.transactionstatus.failuredescription as transaction_failure_description,
    paypal_settlement_results.id as id,
    paypal_settlement_results.parentreferenceid as parent_reference_id,
    cast(
        cast(
            paypal_settlement_results.fee.units as INTEGER
        ) + (paypal_settlement_results.fee.nanos / 1000000000) as DECIMAL (12, 2)
    ) as fee,
    cast(
        cast(
            paypal_settlement_results.netamount.units as INTEGER
        ) + (
            paypal_settlement_results.netamount.nanos / 1000000000
        ) as DECIMAL (12, 2)
    ) as net_amount
from temp_paypal_settlement_results;
