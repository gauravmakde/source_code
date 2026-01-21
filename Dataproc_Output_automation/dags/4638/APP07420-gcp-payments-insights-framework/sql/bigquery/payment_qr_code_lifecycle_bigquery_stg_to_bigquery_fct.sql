BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
-- mandatory Query Band part
--SET QUERY_BAND = 'App_ID=app07420; DAG_ID=payment_qr_code_lifecycle_teradata; Task_Name=teradata_stg_to_teradata_fct_job;' -- noqa
--FOR SESSION VOLATILE;
-- Create temporary tables for selecting distinct records by 'transaction_identifier_id'
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS qr_code_lifecycle_stg_temp
AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_qr_code_lifecycle_ldg
QUALIFY (RANK() OVER (PARTITION BY qr_code_id ORDER BY last_updated_time DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- Purge and reprocess data to account for updates
BEGIN
SET _ERROR_CODE  =  0; 
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_qr_code_lifecycle_fact
WHERE qr_code_id IN (SELECT DISTINCT qr_code_id
        FROM qr_code_lifecycle_stg_temp);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- Insert new event data

BEGIN
SET _ERROR_CODE = 0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_qr_code_lifecycle_fact (
        qr_code_id,
        last_updated_time,
        created_timestamp,
        created_date,
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
        status_qr_code_failure_reason,
        qr_code_invoked,
        qr_code_expired_without_use,
        qr_code_used,
        redemption_time_since_creation_sec,
        dw_batch_date,
        dw_sys_load_tmstp,
        dw_sys_updt_tmstp
    ) (
        SELECT
            qr_code_id,
            CAST(
                FORMAT_TIMESTAMP(
                    '%F %H:%M:%E6S',
                    CAST(last_updated_time AS DATETIME)
                ) AS DATETIME
            ) AS last_updated_time,
            CAST(
                FORMAT_TIMESTAMP(
                    '%F %H:%M:%E6S',
                    CAST(created_timestamp AS DATETIME)
                ) AS DATETIME
            ) AS created_timestamp,
            CAST(
                CAST(
                    FORMAT_TIMESTAMP(
                        '%F %H:%M:%E6S',
                        CAST(created_timestamp AS DATETIME)
                    ) AS DATETIME
                ) AS DATE
            ) AS created_date,
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
            CAST(
                FORMAT_TIMESTAMP(
                    '%F %H:%M:%E6S',
                    CAST(status_event_time AS DATETIME)
                ) AS DATETIME
            ) AS status_event_time,
            status_channel_country,
            status_channel_brand,
            status_selling_channel,
            status_store,
            status_register,
            status_qr_code_failure_reason,
            'Y' AS qr_code_invoked,
            '' AS qr_code_expired_without_use,
            'N' AS qr_code_used,
            CAST(
                TIMESTAMP_DIFF(
                    CAST(
                        FORMAT_TIMESTAMP(
                            '%F %H:%M:%E6S',
                            CAST(status_event_time AS DATETIME)
                        ) AS DATETIME
                    ),
                    CAST(
                        FORMAT_TIMESTAMP(
                            '%F %H:%M:%E6S',
                            CAST(created_timestamp AS DATETIME)
                        ) AS DATETIME
                    ),
                    SECOND
                ) AS STRING
            ) AS redemption_time_since_creation_sec,
            CURRENT_DATE('PST8PDT') AS dw_batch_date,
            CAST(
                FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME
            ) AS dw_sys_load_tmstp,
            CAST(
                FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME
            ) AS dw_sys_updt_tmstp
        FROM qr_code_lifecycle_stg_temp
    );

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;
SET _ERROR_MESSAGE = @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_qr_code_lifecycle_fact AS tgt  
SET
    qr_code_used = 'Y' 
    FROM (SELECT DISTINCT qr_code_id, last_updated_time
        FROM qr_code_lifecycle_stg_temp
        WHERE LOWER(lifecycle_status) = LOWER('redemption_succeeded')) AS SRC
WHERE LOWER(SRC.qr_code_id) = LOWER(tgt.qr_code_id) 
AND CAST(SRC.last_updated_time AS DATETIME) = tgt.last_updated_time;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_qr_code_lifecycle_fact 
SET 
    qr_code_expired_without_use = CASE WHEN LOWER(payment_qr_code_lifecycle_fact.qr_code_used) = LOWER('Y') THEN 'N' ELSE 'Y' END
WHERE qr_code_id IN (SELECT DISTINCT qr_code_id
        FROM qr_code_lifecycle_stg_temp);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

-- Remove records from temporary tables
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_qr_code_lifecycle_ldg;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

/*SET QUERY_BAND = NONE FOR SESSION;*/
-- noqa
END;
