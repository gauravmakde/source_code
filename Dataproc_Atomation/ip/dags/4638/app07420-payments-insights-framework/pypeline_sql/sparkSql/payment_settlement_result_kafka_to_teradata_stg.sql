-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

create temporary view temp_object_model as
select *
from kafka_payment_settlement_result_analytical_avro;

create temporary view kafka_settlement_teradata_temporary as
select
    bankcardsettlementresult.settlementresultreceivedid as settlement_result_received_id,
    lastupdatedtime as last_updated_time,
    channel.channelcountry as channel_country,
    channel.channelbrand as channel_brand,
    channel.sellingchannel as selling_channel,
    acquirer as acquirer,
    settlementamount.currencycode as settlement_amount_currency_code,
    cast(
        cast(settlementamount.units as INTEGER) + (settlementamount.nanos / 1000000000) as DECIMAL (38, 9)
    ) as settlement_amount,
    storenumber as store_number,
    processingdate as processing_date,
    transactiontimestamp.timestamp as transaction_timestamp,
    transactiontimestamp.timezoneoffsetminutes as transaction_time_zone_offset_minutes,
    transactiontype as transaction_type,
    tendertype as tender_type,
    bankcardsettlementresult.acquirermerchantidentifier as acquirer_merchant_identifier,
    bankcardsettlementresult.bankcardauthorizationsource as bank_card_authorization_source,
    bankcardsettlementresult.authorizationcode as authorization_code,
    bankcardsettlementresult.vendorsettlementcode as vendor_settlement_code,
    bankcardsettlementresult.expirationdate.month as expiration_date_month,
    bankcardsettlementresult.expirationdate.year as expiration_date_year,
    bankcardsettlementresult.interchangecode as interchange_code,
    bankcardsettlementresult.interchangeamount.currencycode as interchange_amount_currency_code,
    cast(
        cast(
            bankcardsettlementresult.interchangeamount.units as INTEGER
        ) + (bankcardsettlementresult.interchangeamount.nanos / 1000000000) as DECIMAL (38, 9)
    ) as interchange_amount,
    bankcardsettlementresult.merchantcategorycode as merchant_category_code,
    bankcardsettlementresult.entrymode as entry_mode,
    bankcardsettlementresult.token.token.value as token_value,
    bankcardsettlementresult.token.token.authority as token_authority,
    bankcardsettlementresult.token.token.dataclassification as token_data_classification,
    bankcardsettlementresult.token.tokentype as token_type,
    bankcardsettlementresult.transactionmatchidentifier as transaction_match_identifier,
    bankcardsettlementresult.cardtype.cardtype as card_type,
    bankcardsettlementresult.cardtype.cardsubtype as card_subtype,
    bankcardsettlementresult.tokenrequestorid as token_requestor_id,
    bankcardsettlementresult.cardproducttype as card_product_type,
    bankcardsettlementresult.interchangeadjustmentamount.currencycode as interchange_adjustment_amount_currency_code,
    cast(
        cast(
            bankcardsettlementresult.interchangeadjustmentamount.units as INTEGER
        ) + (bankcardsettlementresult.interchangeadjustmentamount.nanos / 1000000000) as DECIMAL (38, 9)
    ) as interchange_adjustment_amount,
    bankcardsettlementresult.interchangeadjustmentreason as interchange_adjustment_reason,
    bankcardsettlementresult.emvtransactionindicator as emv_transaction_indicator,
    bankcardsettlementresult.networkreferencenumber as network_reference_number,
    bankcardsettlementresult.visatransactionnetworkdetails.visatransactionid as visa_transaction_id,
    bankcardsettlementresult.mastercardtransactionnetworkdetails.mastercardbanknetreferencenumber
    as mastercard_bank_net_reference_number,
    bankcardsettlementresult.terminalid as terminal_id,
    element_at(headers, 'Id') as hdr_id,
    element_at(headers, 'AppId') as hdr_appid,
    element_at(headers, 'EventTime') as hdr_eventtime,
    element_at(headers, 'SystemTime') as hdr_systemtime
from temp_object_model;

--Sink
insert into payment_settlement_result_stg
select
    hdr_id,
    hdr_appid,
    hdr_eventtime,
    hdr_systemtime,
    settlement_result_received_id,
    last_updated_time,
    channel_country,
    channel_brand,
    selling_channel,
    acquirer,
    settlement_amount_currency_code,
    settlement_amount,
    store_number,
    processing_date,
    transaction_timestamp,
    transaction_time_zone_offset_minutes,
    transaction_type,
    tender_type,
    acquirer_merchant_identifier,
    bank_card_authorization_source,
    authorization_code,
    vendor_settlement_code,
    expiration_date_month,
    expiration_date_year,
    interchange_code,
    interchange_amount_currency_code,
    interchange_amount,
    merchant_category_code,
    entry_mode,
    token_value,
    token_authority,
    token_data_classification,
    token_type,
    transaction_match_identifier,
    card_type,
    card_subtype,
    token_requestor_id,
    card_product_type,
    interchange_adjustment_amount_currency_code,
    interchange_adjustment_amount,
    interchange_adjustment_reason,
    emv_transaction_indicator,
    network_reference_number,
    visa_transaction_id,
    mastercard_bank_net_reference_number,
    terminal_id
from kafka_settlement_teradata_temporary;
