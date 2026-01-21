-- Read Kafka into temp table

CREATE TEMPORARY VIEW temp_object_model AS
SELECT *
FROM kafka_customer_credit_application_analytical_avro;

-- Transform in temp view

CREATE TEMPORARY VIEW temp_kafka_customer_credit_application_analytical_avro AS
SELECT
    temp_object_model.lastupdatedtime AS last_updated_time,
    temp_object_model.submittedtime AS submitted_time,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    temp_object_model.storenumber AS store_number,
    byemployee.idtype AS employee_id_type,
    byemployee.id AS employee_id,
    temp_object_model.creditapplicationid AS credit_application_id,
    customer.idtype AS customer_id_type,
    customer.id AS customer_id,
    temp_object_model.creditproductselection AS credit_product_selection,
    temp_object_model.acquisitionoffer AS acquisition_offer,
    temp_object_model.experience AS experience,
    temp_object_model.termsandconditionsbarcode AS terms_and_conditions_barcode,
    temp_object_model.ipaddress.value AS ip_address_value,
    temp_object_model.ipaddress.authority AS ip_address_authority,
    temp_object_model.ipaddress.dataclassification AS ip_address_data_classification,
    temp_object_model.shopperid AS shopper_id,
    temp_object_model.iovationdeviceid AS iovation_device_id,
    temp_object_model.contextid AS context_id,
    temp_object_model.email.value AS email_value,
    temp_object_model.email.authority AS email_authority,
    temp_object_model.email.dataclassification AS email_data_classification,
    temp_object_model.annualincome.currencycode AS annual_income_currency_code,
    temp_object_model.annualincome.salary.value AS annual_income_salary_value,
    temp_object_model.annualincome.salary.authority AS annual_income_salary_authority,
    temp_object_model.annualincome.salary.dataclassification AS annual_income_salary_data_classification,
    element_at(temp_object_model.headers, 'Id') AS hdr_id,
    element_at(temp_object_model.headers, 'AppId') AS hdr_appid,
    element_at(temp_object_model.headers, 'SystemTime') AS hdr_systemtime
FROM temp_object_model;

-- Sink to Teradata
INSERT INTO TABLE credit_application_ldg
SELECT
    hdr_id,
    hdr_appid,
    hdr_systemtime,
    last_updated_time,
    '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as last_updated_time_tz,
    submitted_time,
    '-0'||extract(hours from to_utc_timestamp(current_timestamp,'UTC' )-from_utc_timestamp(current_timestamp,'America/Los_Angeles' ))||":00" as submitted_time_tz,
    channel_country,
    channel_brand,
    selling_channel,
    store_number,
    employee_id_type,
    employee_id,
    credit_application_id,
    customer_id_type,
    customer_id,
    credit_product_selection,
    acquisition_offer,
    experience,
    terms_and_conditions_barcode,
    ip_address_value,
    ip_address_authority,
    ip_address_data_classification,
    shopper_id,
    iovation_device_id,
    context_id,
    email_value,
    email_authority,
    email_data_classification,
    annual_income_currency_code,
    annual_income_salary_value,
    annual_income_salary_authority,
    annual_income_salary_data_classification
FROM temp_kafka_customer_credit_application_analytical_avro;
