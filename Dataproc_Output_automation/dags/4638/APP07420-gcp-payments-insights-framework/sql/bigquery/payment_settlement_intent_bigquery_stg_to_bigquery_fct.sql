
/*SET QUERY_BAND = 'App_ID=app07420; DAG_ID=payment_settlement_intent_teradata; Task_Name=teradata_stg_to_teradata_fct_job;' -- noqa
FOR SESSION VOLATILE;*/


-- Create temporary tables for selecting distinct records by 'transaction_identifier_id'


CREATE TEMPORARY TABLE IF NOT EXISTS settlement_intent_stg_temp AS (
    SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_settlement_intent_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY transaction_identifier_id ORDER BY event_time DESC)) = 1
);


CREATE TEMPORARY TABLE IF NOT EXISTS bank_card_settlement_results_stg_temp AS (
    SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.bankcard_settlement_results_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY transaction_identifier_id ORDER BY transaction_time DESC)) = 1
);


CREATE TEMPORARY TABLE IF NOT EXISTS giftcard_note_tender_results_stg_temp AS (
    SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.giftcard_note_tender_results_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY transaction_identifier_id ORDER BY transaction_time DESC)) = 1
);

CREATE TEMPORARY TABLE IF NOT EXISTS
paypal_billing_agreement_settlement_results_stg_temp AS (
    SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.paypal_billing_agreement_settlement_results_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY transaction_identifier_id ORDER BY transaction_time DESC)) = 1
);

CREATE TEMPORARY TABLE IF NOT EXISTS paypal_settlement_result_stg_temp AS (
    SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.paypal_settlement_result_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY transaction_identifier_id ORDER BY transaction_time DESC)) = 1
) ;

-- PAYMENT_SETTLEMENT_INTENT_FACT
-- Purge and reprocess data to account for updates

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_settlement_intent_fact
WHERE
    TRANSACTION_IDENTIFIER_ID IN (SELECT TRANSACTION_IDENTIFIER_ID FROM settlement_intent_stg_temp);

-- Insert new event data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_settlement_intent_fact (
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP,
    DW_BATCH_DATE,
    EVENT_TIME,
    EVENT_DATE,
    SOURCE_CHANNEL_COUNTRY,
    SOURCE_CHANNEL,
    SOURCE_PLATFORM,
    SOURCE_FEATURE,
    SOURCE_SERVICE_NAME,
    SOURCE_STORE,
    SOURCE_REGISTER,
    PURCHASE_IDENTIFIER_TYPE,
    PURCHASE_IDENTIFIER_ID,
    TRANSACTION_IDENTIFIER_TYPE,
    TRANSACTION_IDENTIFIER_ID,
    TOTAL_REQUESTED_AMOUNT_CURRENCY_CODE,
    TOTAL_REQUESTED_AMOUNT_AMOUNT,
    MERCHANT_IDENTIFIER,
    FAILURE_REASON,
    SERVICE_TICKET_ID
)
SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(event_time AS DATETIME) AS event_time,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(event_time AS DATETIME)) AS DATETIME) AS DATE) AS event_date,
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
FROM settlement_intent_stg_temp;

-- BANKCARD_SETTLEMENT_RESULTS_FACT
-- Purge and reprocess data to account for updates
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.bankcard_settlement_results_fact
WHERE
    TRANSACTION_IDENTIFIER_ID IN (SELECT TRANSACTION_IDENTIFIER_ID FROM bank_card_settlement_results_stg_temp);

-- Insert new event data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.bankcard_settlement_results_fact (
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP,
    DW_BATCH_DATE,
    TRANSACTION_TIME,
    TRANSACTION_DATE,
    TRANSACTION_IDENTIFIER_TYPE,
    TRANSACTION_IDENTIFIER_ID,
    TOKEN_VALUE,
    TOKEN_TYPE,
    CARD_TYPE,
    CARD_SUBTYPE,
    TOTAL_AMOUNT,
    TRANSACTION_RESULT_TYPE,
    TRANSACTION_RESULT_STATUS,
    TRANSACTION_RESULT_FAILURE_REASON,
    TOKEN_REQUESTOR_ID,
    VENDOR_SETTLEMENT_CODE
)
SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(transaction_time AS DATETIME) AS transaction_time,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(transaction_time AS DATETIME)) AS DATETIME) AS DATE) AS transaction_date,
 transaction_identifier_type,
 transaction_identifier_id,
 token_value,
 token_type,
 card_type,
 card_subtype,
 total_amount,
 transaction_result_type,
 transaction_result_status,
 transaction_result_failure_reason,
 token_requestor_id,
 vendor_settlement_code
FROM bank_card_settlement_results_stg_temp;


-- GIFTCARD_NOTE_TENDER_RESULTS_FACT
-- Purge and reprocess data to account for updates
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.giftcard_note_tender_results_fact
WHERE
    TRANSACTION_IDENTIFIER_ID IN (SELECT TRANSACTION_IDENTIFIER_ID FROM giftcard_note_tender_results_stg_temp);

-- Insert new event data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.giftcard_note_tender_results_fact (
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP,
    DW_BATCH_DATE,
    TRANSACTION_TIME,
    TRANSACTION_DATE,
    TRANSACTION_IDENTIFIER_TYPE,
    TRANSACTION_IDENTIFIER_ID,
    GIFT_CARD_NOTE_TYPE,
    ACCOUNT_NUMBER,
    REQUESTED_AMOUNT,
    APPLIED_AMOUNT,
    TENDER_TRANSACTION_RESULT_TYPE,
    TRANSACTION_STATUS
)
SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(transaction_time AS DATETIME) AS transaction_time,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(transaction_time AS DATETIME)) AS DATETIME) AS DATE) AS
 transaction_date,
 transaction_identifier_type,
 transaction_identifier_id,
 gift_card_note_type,
 account_number,
 requested_amount,
 applied_amount,
 tender_transaction_result_type,
 transaction_status
FROM giftcard_note_tender_results_stg_temp;

-- PAYPAL_BILLING_AGREEMENT_SETTLEMENT_RESULTS_FACT
-- Purge and reprocess data to account for updates
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.paypal_billing_agreement_settlement_results_fact
WHERE
    TRANSACTION_IDENTIFIER_ID IN (
        SELECT TRANSACTION_IDENTIFIER_ID FROM paypal_billing_agreement_settlement_results_stg_temp
    );

-- Insert new event data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.paypal_billing_agreement_settlement_results_fact (
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP,
    DW_BATCH_DATE,
    TRANSACTION_TIME,
    TRANSACTION_DATE,
    TRANSACTION_IDENTIFIER_TYPE,
    TRANSACTION_IDENTIFIER_ID,
    PAYER_ID,
    TOTAL,
    TRANSACTION_TYPE,
    TRANSACTION_STATUS,
    ID,
    PARENT_REFERENCE_ID,
    FEE,
    NET_AMOUNT
)
SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(transaction_time AS DATETIME) AS transaction_time,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(transaction_time AS DATETIME)) AS DATETIME) AS DATE) AS
 transaction_date,
 transaction_identifier_type,
 transaction_identifier_id,
 payer_id,
 total,
 transaction_type,
 transaction_status,
 id,
 parent_reference_id,
 fee,
 net_amount
FROM paypal_billing_agreement_settlement_results_stg_temp;

-- PAYPAL_SETTLEMENT_RESULT_FACT
-- Purge and reprocess data to account for updates
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.paypal_settlement_result_fact
WHERE
    TRANSACTION_IDENTIFIER_ID IN (SELECT TRANSACTION_IDENTIFIER_ID FROM paypal_settlement_result_stg_temp);

-- Insert new event data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.paypal_settlement_result_fact (
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP,
    DW_BATCH_DATE,
    TRANSACTION_TIME,
    TRANSACTION_DATE,
    TRANSACTION_IDENTIFIER_TYPE,
    TRANSACTION_IDENTIFIER_ID,
    PAYPAL_ORDER_ID,
    PAYER_ID,
    TOTAL,
    TRANSACTION_TYPE,
    TRANSACTION_STATUS,
    TRANSACTION_FAILURE_DESCRIPTION,
    ID,
    PARENT_REFERENCE_ID,
    FEE,
    NET_AMOUNT
)
SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(transaction_time AS DATETIME) AS transaction_time,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(transaction_time AS DATETIME)) AS DATETIME) AS DATE) AS
 transaction_date,
 transaction_identifier_type,
 transaction_identifier_id,
 paypal_order_id,
 payer_id,
 total,
 transaction_type,
 transaction_status,
 transaction_failure_description,
 id,
 parent_reference_id,
 fee,
 net_amount
FROM paypal_settlement_result_stg_temp;

-- Remove records from temporary tables
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.bankcard_settlement_results_ldg;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.giftcard_note_tender_results_ldg;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_settlement_intent_ldg;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.paypal_billing_agreement_settlement_results_ldg;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.paypal_settlement_result_ldg;


