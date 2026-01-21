BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
-- mandatory Query Band part
-- SET QUERY_BAND = 'App_ID=app06788; DAG_ID=credit_application_teradata; Task_Name=teradata_stg_to_teradata_fct_job;' -- noqa
-- FOR SESSION VOLATILE;
/*Create temporary tables for selecting distinct records by
'credit_application_id'.*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS credit_application_ldg_temp
AS
SELECT DISTINCT *
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_credit_stg.credit_application_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY credit_application_id ORDER BY last_updated_time DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- Insert or update if exist
BEGIN
SET _ERROR_CODE  =  0;
MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_credit_fct.credit_application_fact AS caf
USING credit_application_ldg_temp AS src
ON LOWER(src.credit_application_id) = LOWER(caf.credit_application_id)
WHEN MATCHED THEN UPDATE SET
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
    last_updated_time = src.last_updated_time,
    last_updated_time_tz = src.last_updated_time_tz,
    submitted_time = src.submitted_time,
    submitted_time_tz = src.submitted_time_tz,
    channel_country = src.channel_country,
    channel_brand = src.channel_brand,
    selling_channel = src.selling_channel,
    store_number = src.store_number,
    employee_id_type = src.employee_id_type,
    employee_id = src.employee_id,
    customer_id_type = src.customer_id_type,
    customer_id = src.customer_id,
    credit_product_selection = src.credit_product_selection,
    acquisition_offer = src.acquisition_offer,
    experience = src.experience,
    terms_and_conditions_barcode = src.terms_and_conditions_barcode,
    ip_address_value = src.ip_address_value,
    ip_address_authority = src.ip_address_authority,
    ip_address_data_classification = src.ip_address_data_classification,
    shopper_id = src.shopper_id,
    iovation_device_id = src.iovation_device_id,
    context_id = src.context_id,
    email_value = src.email_value,
    email_authority = src.email_authority,
    email_data_classification = src.email_data_classification,
    annual_income_currency_code = src.annual_income_currency_code,
    annual_income_salary_value = src.annual_income_salary_value,
    annual_income_salary_authority = src.annual_income_salary_authority,
    annual_income_salary_data_classification = src.annual_income_salary_data_classification
WHEN NOT MATCHED THEN INSERT VALUES(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME()) AS DATETIME), 
src.last_updated_time, src.last_updated_time_tz, src.submitted_time, src.submitted_time_tz,src.channel_country, 
src.channel_brand, src.selling_channel, src.store_number, src.employee_id_type, src.employee_id, src.credit_application_id, 
src.customer_id_type, src.customer_id, src.credit_product_selection, src.acquisition_offer, src.experience, 
src.terms_and_conditions_barcode, src.ip_address_value, src.ip_address_authority, src.ip_address_data_classification, 
src.shopper_id, src.iovation_device_id, src.context_id, src.email_value, src.email_authority, src.email_data_classification, 
src.annual_income_currency_code, src.annual_income_salary_value, src.annual_income_salary_authority, src.annual_income_salary_data_classification);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- Remove records from temporary tables
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_credit_stg.credit_application_ldg;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- noqa
END;
