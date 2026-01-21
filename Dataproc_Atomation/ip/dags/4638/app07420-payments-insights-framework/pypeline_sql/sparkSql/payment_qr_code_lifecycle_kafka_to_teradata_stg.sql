-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
create temporary view temp_kafka_payment_qr_code_lifecycle_analytical_avro as
select * from kafka_payment_qr_code_lifecycle_analytical_avro;

--pulling the qrcode created status from the object model

--fabricated columns due to union is set to 'N/A' to differentiate between legit blanks/nulls
--coming from object model vs fields that do not exist in the object model
create temporary view qr_code_created as
select
    qrcodeid as qr_code_id,
    cast(objectmetadata.lastupdatedtime as timestamp) as last_updated_time,
    cast(createddetails.eventtime as timestamp) as created_timestamp,
    createddetails.channel.channelcountry as created_channel_country,
    createddetails.channel.channelbrand as created_channel_brand,
    createddetails.channel.sellingchannel as created_selling_channel,
    createddetails.deviceid.value as device_id,
    createddetails.useragent as user_agent,
    paymentsqrcodeinfo.customerwalletpaymentid as customer_wallet_payment_id,
    paymentsqrcodeinfo.customerid.idtype as customer_id_type,
    paymentsqrcodeinfo.customerid.id as customer_id,
    paymentsqrcodeinfo.lastfour.value as credit_card_last_four,
    'created' as lifecycle_status,
    'N/A' as status_event_id,
    createddetails.eventtime as status_event_time,
    createddetails.channel.channelcountry as status_channel_country,
    createddetails.channel.channelbrand as status_channel_brand,
    createddetails.channel.sellingchannel as status_selling_channel,
    'N/A' as status_store,
    'N/A' as status_register,
    'N/A' as status_qr_code_failure_reason
from temp_kafka_payment_qr_code_lifecycle_analytical_avro;

--pulling the qrcode redemption succeeded details
create temporary view qr_code_redemption_succeeded as
select
    qrcodeid as qr_code_id,
    cast(objectmetadata.lastupdatedtime as timestamp) as last_updated_time,
    cast(createddetails.eventtime as timestamp) as created_timestamp,
    createddetails.channel.channelcountry as created_channel_country,
    createddetails.channel.channelbrand as created_channel_brand,
    createddetails.channel.sellingchannel as created_selling_channel,
    createddetails.deviceid.value as device_id,
    createddetails.useragent as user_agent,
    paymentsqrcodeinfo.customerwalletpaymentid as customer_wallet_payment_id,
    paymentsqrcodeinfo.customerid.idtype as customer_id_type,
    paymentsqrcodeinfo.customerid.id as customer_id,
    paymentsqrcodeinfo.lastfour.value as credit_card_last_four,
    'redemption_succeeded' as lifecycle_status,
    redemptionsucceededdetails.eventid as status_event_id,
    cast(redemptionsucceededdetails.eventtime as timestamp) as status_event_time,
    redemptionsucceededdetails.channel.channelcountry as status_channel_country,
    redemptionsucceededdetails.channel.channelbrand as status_channel_brand,
    redemptionsucceededdetails.channel.sellingchannel as status_selling_channel,
    redemptionsucceededdetails.store as status_store,
    redemptionsucceededdetails.register as status_register,
    'N/A' as status_qr_code_failure_reason
from temp_kafka_payment_qr_code_lifecycle_analytical_avro
where redemptionsucceededdetails is not null;

--pulling the qr code redemption failed details
create temporary view qr_code_redemption_failed as
select
    qrcodeid as qr_code_id,
    cast(objectmetadata.lastupdatedtime as timestamp) as last_updated_time,
    cast(createddetails.eventtime as timestamp) as created_timestamp,
    createddetails.channel.channelcountry as created_channel_country,
    createddetails.channel.channelbrand as created_channel_brand,
    createddetails.channel.sellingchannel as created_selling_channel,
    createddetails.deviceid.value as device_id,
    createddetails.useragent as user_agent,
    paymentsqrcodeinfo.customerwalletpaymentid as customer_wallet_payment_id,
    paymentsqrcodeinfo.customerid.idtype as customer_id_type,
    paymentsqrcodeinfo.customerid.id as customer_id,
    paymentsqrcodeinfo.lastfour.value as credit_card_last_four,
    'redemption_failed' as lifecycle_status,
    explode(redemptionfaileddetails) as redemptionfaileddetails
from temp_kafka_payment_qr_code_lifecycle_analytical_avro;

create temporary view qr_code_redemption_failed_exploded as
select
    qr_code_id,
    last_updated_time,
    created_timestamp,
    created_channel_country,
    created_channel_brand,
    created_selling_channel,
    device_id,
    user_agent,
    customer_wallet_payment_id,
    customer_id_type,
    customer_id,
    credit_card_last_four,
    lifecycle_status,
    redemptionfaileddetails.eventid as status_event_id,
    cast(redemptionfaileddetails.eventtime as timestamp) as status_event_time,
    redemptionfaileddetails.channel.channelcountry as status_channel_country,
    redemptionfaileddetails.channel.channelbrand as status_channel_brand,
    redemptionfaileddetails.channel.sellingchannel as status_selling_channel,
    redemptionfaileddetails.store as status_store,
    redemptionfaileddetails.register as status_register,
    redemptionfaileddetails.qrcodefailurereason as status_qr_code_failure_reason
from qr_code_redemption_failed
where redemptionfaileddetails.eventid is not null;

--pulling the qr code retrieval succeeded details
create temporary view qr_code_retrieval_succeeded as
select
    qrcodeid as qr_code_id,
    cast(objectmetadata.lastupdatedtime as timestamp) as last_updated_time,
    cast(createddetails.eventtime as timestamp) as created_timestamp,
    createddetails.channel.channelcountry as created_channel_country,
    createddetails.channel.channelbrand as created_channel_brand,
    createddetails.channel.sellingchannel as created_selling_channel,
    createddetails.deviceid.value as device_id,
    createddetails.useragent as user_agent,
    paymentsqrcodeinfo.customerwalletpaymentid as customer_wallet_payment_id,
    paymentsqrcodeinfo.customerid.idtype as customer_id_type,
    paymentsqrcodeinfo.customerid.id as customer_id,
    paymentsqrcodeinfo.lastfour.value as credit_card_last_four,
    'retrieval_succeeded' as lifecycle_status,
    explode(retrievalsucceededdetails) as retrievalsucceededdetails
from temp_kafka_payment_qr_code_lifecycle_analytical_avro;

create temporary view qr_code_retrieval_succeeded_exploded as
select
    qr_code_id,
    last_updated_time,
    created_timestamp,
    created_channel_country,
    created_channel_brand,
    created_selling_channel,
    device_id,
    user_agent,
    customer_wallet_payment_id,
    customer_id_type,
    customer_id,
    credit_card_last_four,
    lifecycle_status,
    retrievalsucceededdetails.eventid as status_event_id,
    cast(retrievalsucceededdetails.eventtime as timestamp) as status_event_time,
    retrievalsucceededdetails.channel.channelcountry as status_channel_country,
    retrievalsucceededdetails.channel.channelbrand as status_channel_brand,
    retrievalsucceededdetails.channel.sellingchannel as status_selling_channel,
    retrievalsucceededdetails.store as status_store,
    retrievalsucceededdetails.register as status_register,
    'N/A' as status_qr_code_failure_reason
from qr_code_retrieval_succeeded
where retrievalsucceededdetails.eventid is not null;

--pulling the qr code retrieval failed details
create temporary view qr_code_retrieval_failed as
select
    qrcodeid as qr_code_id,
    cast(objectmetadata.lastupdatedtime as timestamp) as last_updated_time,
    cast(createddetails.eventtime as timestamp) as created_timestamp,
    createddetails.channel.channelcountry as created_channel_country,
    createddetails.channel.channelbrand as created_channel_brand,
    createddetails.channel.sellingchannel as created_selling_channel,
    createddetails.deviceid.value as device_id,
    createddetails.useragent as user_agent,
    paymentsqrcodeinfo.customerwalletpaymentid as customer_wallet_payment_id,
    paymentsqrcodeinfo.customerid.idtype as customer_id_type,
    paymentsqrcodeinfo.customerid.id as customer_id,
    paymentsqrcodeinfo.lastfour.value as credit_card_last_four,
    'retrieval_failed' as lifecycle_status,
    explode(retrievalfaileddetails) as retrievalfaileddetails
from temp_kafka_payment_qr_code_lifecycle_analytical_avro;

create temporary view qr_code_retrieval_failed_exploded as
select
    qr_code_id,
    last_updated_time,
    created_timestamp,
    created_channel_country,
    created_channel_brand,
    created_selling_channel,
    device_id,
    user_agent,
    customer_wallet_payment_id,
    customer_id_type,
    customer_id,
    credit_card_last_four,
    lifecycle_status,
    retrievalfaileddetails.eventid as status_event_id,
    cast(retrievalfaileddetails.eventtime as timestamp) as status_event_time,
    retrievalfaileddetails.channel.channelcountry as status_channel_country,
    retrievalfaileddetails.channel.channelbrand as status_channel_brand,
    retrievalfaileddetails.channel.sellingchannel as status_selling_channel,
    retrievalfaileddetails.store as status_store,
    retrievalfaileddetails.register as status_register,
    retrievalfaileddetails.qrcodefailurereason as status_qr_code_failure_reason
from qr_code_retrieval_failed
where retrievalfaileddetails.eventid is not null;

--union all datasets together
create temporary view qr_code_lifecycle_union as
select distinct *
from qr_code_created
union
select distinct *
from qr_code_redemption_succeeded
union
select distinct *
from qr_code_redemption_failed_exploded
union
select distinct *
from qr_code_retrieval_succeeded_exploded
union
select distinct *
from qr_code_retrieval_failed_exploded;

--insert into teradata stage table
insert into table payment_qr_code_lifecycle_stg
select distinct
    qr_code_id,
    last_updated_time,
    created_timestamp,
    created_channel_country,
    created_channel_brand,
    created_selling_channel,
    device_id,
    user_agent,
    customer_wallet_payment_id,
    customer_id_type,
    customer_id,
    credit_card_last_four,
    lifecycle_status,
    status_event_id,
    status_event_time,
    status_channel_country,
    status_channel_brand,
    status_selling_channel,
    status_store,
    status_register,
    status_qr_code_failure_reason
from qr_code_lifecycle_union;
