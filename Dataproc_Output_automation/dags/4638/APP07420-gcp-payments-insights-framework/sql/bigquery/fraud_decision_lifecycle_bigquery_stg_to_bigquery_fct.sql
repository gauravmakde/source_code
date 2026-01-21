


CREATE TEMPORARY TABLE IF NOT EXISTS fraud_decision_lifecycle_ldg_temp
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_pymnt_fraud_stg.fraud_decision_lifecycle_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY event_id, order_id ORDER BY event_time DESC)) = 1;

------------------------------------------------------------------------------
-- COMMIT TRANSACTION;
------------------------------------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_pymnt_fraud_fct.fraud_decision_lifecycle_fact
WHERE (event_id, order_id) IN (SELECT (event_id, order_id)
        FROM fraud_decision_lifecycle_ldg_temp);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_pymnt_fraud_fct.fraud_decision_lifecycle_fact (
ORDER_ID,
ORDER_LINE_ID,
AUTHORITY,
FRAUD_DECISION,
DECISION_DETAIL,
CHANNEL_COUNTRY,
CHANNEL_BRAND,
SELLING_CHANNEL,
STORE_NUMBER,
REGISTER,
FEATURE,
SERVICE_NAME,
SERVICE_TICKET_ID,
METADATA_CREATED_TIME,
METADATA_LAST_UPDATED_TIME,
METADATA_LAST_TRIGGERING_EVENT_NAME,
EVENT_ID,
EVENT_TIME,
EVENT_DATE,
DW_BATCH_DATE,
DW_SYS_LOAD_TMSTP,
DW_SYS_UPDT_TMSTP
)
SELECT
ORDER_ID,
ORDER_LINE_ID,
AUTHORITY,
FRAUD_DECISION,
DECISION_DETAIL,
CHANNEL_COUNTRY,
CHANNEL_BRAND,
SELLING_CHANNEL,
STORE_NUMBER,
REGISTER,
FEATURE,
SERVICE_NAME,
SERVICE_TICKET_ID,
CAST(METADATA_CREATED_TIME AS DATETIME),
CAST(METADATA_LAST_UPDATED_TIME  AS DATETIME) AS METADATA_LAST_UPDATED_TIME,
METADATA_LAST_TRIGGERING_EVENT_NAME,
EVENT_ID,
cast(EVENT_TIME AS DATETIME) AS EVENT_TIME,
cast(cast(EVENT_TIME AS TIMESTAMP) AS DATE) AS EVENT_DATE,
current_date('PST8PDT') AS DW_BATCH_DATE,
(current_datetime('PST8PDT')) AS DW_SYS_LOAD_TMSTP,
(current_datetime('PST8PDT')) AS DW_SYS_UPDT_TMSTP
FROM fraud_decision_lifecycle_ldg_temp;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_pymnt_fraud_stg.fraud_decision_lifecycle_ldg;

-- COMMIT TRANSACTION;