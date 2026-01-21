-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

create temporary view temp_object_model as
select *
from kafka_payment_chargeback_information_analytical_avro;

create temporary view kafka_chargeback_information_teradata_temporary as
select
    disputeamount.currencycode as dispute_amount_currency_code,
    cast(
        cast(disputeamount.units as INTEGER) + (disputeamount.nanos / 1000000000) as DECIMAL (38, 9)
    ) as dispute_amount,
    chargebackid as chargeback_id,
    vendorchargebackid as vendor_chargeback_id,
    acquirerstorenumber as acquirer_store_number,
    transactiontime.timestamp as transaction_timestamp,
    transactiontime.timezoneoffsetminutes as transaction_time_timezone_offset_minutes,
    chargebackcategory as chargeback_category,
    processstage as process_stage,
    reasoncode as reason_code,
    chargebackvendor as chargeback_vendor,
    chargebackstate as chargeback_state,
    vendorsettlementcode as vendor_settlement_code,
    duedate as due_date,
    vendoractivitytime.timestamp as vendor_activity_timestamp,
    cast(vendoractivitytime.timestamp as DATE) as vendor_activity_date,
    vendoractivitytime.timezoneoffsetminutes as vendor_activity_timezone_offset_minutes,
    vendorexportdate as vendor_export_date,
    paymentid as payment_id,
    bankcardtoken.token.value as token_value,
    bankcardtoken.token.authority as token_authority,
    bankcardtoken.token.dataclassification as token_data_classification,
    bankcardtoken.tokentype as token_type,
    cardtype.cardtype as card_type,
    cardtype.cardsubtype as card_subtype,
    settlementamount.currencycode as settlement_amount_currency_code,
    cast(
        cast(settlementamount.units as INTEGER) + (settlementamount.nanos / 1000000000) as DECIMAL (38, 9)
    ) as settlement_amount,
    financialimpact as financial_impact,
    chargebackactioncode as chargeback_action_code,
    worldpayvendordata.customerdiscretionaryfield2 as worldpay_customer_discretionary_field_2,
    cast(
        cast(worldpayvendordata.feeamount.units as INTEGER) + (worldpayvendordata.feeamount.nanos / 1000000000) as DECIMAL (38, 9)
    ) as worldpay_fee_amount,
    worldpayvendordata.registernumber as worldpay_register_number,
    worldpayvendordata.networkreferencenumber as worldpay_network_reference_number,
    worldpayvendordata.entrymode as worldpay_entry_mode,
    worldpayvendordata.bankcardauthorizationsource as worldpay_bank_card_authorization_source,
    element_at(headers, 'Id') as hdr_id,
    element_at(headers, 'AppId') as hdr_appid,
    element_at(headers, 'EventTime') as hdr_eventtime,
    element_at(headers, 'SystemTime') as hdr_systemtime
from temp_object_model;

--Sink
insert into payment_chargeback_information_ldg
select
    dispute_amount_currency_code,
    dispute_amount,
    chargeback_id,
    vendor_chargeback_id,
    acquirer_store_number,
    transaction_timestamp,
    transaction_time_timezone_offset_minutes,
    chargeback_category,
    process_stage,
    reason_code,
    chargeback_vendor,
    chargeback_state,
    vendor_settlement_code,
    due_date,
    vendor_activity_timestamp,
    vendor_activity_date,
    vendor_activity_timezone_offset_minutes,
    vendor_export_date,
    payment_id,
    token_value,
    token_authority,
    token_data_classification,
    token_type,
    card_type,
    card_subtype,
    settlement_amount_currency_code,
    settlement_amount,
    financial_impact,
    chargeback_action_code,
    worldpay_customer_discretionary_field_2,
    worldpay_fee_amount,
    worldpay_register_number,
    worldpay_network_reference_number,
    worldpay_entry_mode,
    worldpay_bank_card_authorization_source,
    hdr_id,
    hdr_appid,
    hdr_eventtime,
    hdr_systemtime
from kafka_chargeback_information_teradata_temporary;
