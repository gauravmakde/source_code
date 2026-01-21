-- mandatory Query Band part
SET QUERY_BAND = 'App_ID=app07420; DAG_ID=payment_settlement_intent_v2_data_fix_teradata;Task_Name=teradata_stg_to_teradata_fct_job;' -- noqa
FOR SESSION VOLATILE;

END TRANSACTION;

-- Update records in HDR fact table if they have a matching record in staging table
UPDATE T1
FROM {db_env}_NAP_BASE_VWS.PAYMENT_SETTLEMENT_INTENT_HDR_V2_FACT T1, {db_env}_NAP_STG.PAYMENT_SETTLEMENT_INTENT_DATA_FIX_LDG T2
SET FAILURE_REASON = NULL
WHERE T1.TRANSACTION_ID = T2.TRANSACTION_IDENTIFIER_ID;

-- Remove records from staging table
DELETE FROM {db_env}_NAP_STG.PAYMENT_SETTLEMENT_INTENT_DATA_FIX_LDG;

SET QUERY_BAND = NONE FOR SESSION; -- noqa

END TRANSACTION;
