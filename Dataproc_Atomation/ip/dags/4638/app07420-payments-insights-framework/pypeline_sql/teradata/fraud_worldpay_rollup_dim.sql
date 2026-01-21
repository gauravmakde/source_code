-- mandatory Query Band part
SET QUERY_BAND = 'App_ID=app07420; DAG_ID=fraud_worldpay_bin; Task_Name=td_stg_to_td_dim_job;' -- noqa
FOR SESSION VOLATILE;

END TRANSACTION;

MERGE INTO {db_env}_NAP_PYMNT_FRAUD_BASE_VWS.WORLDPAY_BIN_ROLLUP_DIM AS T
    USING {db_env}_NAP_PYMNT_FRAUD_BASE_VWS.WORLDPAY_BIN_ROLLUP_LDG AS S
    ON T.BIN_SIX = S.BIN_SIX
    WHEN MATCHED THEN UPDATE SET
        BASE_CARD_NETWORKS = S.BASE_CARD_NETWORKS,
        CARD_NETWORKS = S.CARD_NETWORKS,
        DEBIT_FLAGS = S.DEBIT_FLAGS,
        CHECKCARD_FLAGS = S.CHECKCARD_FLAGS,
        CREDIT_CARD_FLAGS = S.CREDIT_CARD_FLAGS,
        COMMERCIAL_BUSINESS_FLAGS = S.COMMERCIAL_BUSINESS_FLAGS,
        FLEET_CARD_FLAGS = S.FLEET_CARD_FLAGS,
        PREPAID_CARD_FLAGS = S.PREPAID_CARD_FLAGS,
        HSA_FLAGS = S.HSA_FLAGS,
        PINLESS_BILL_PAY_FLAGS = S.PINLESS_BILL_PAY_FLAGS,
        EBT_FLAGS = S.EBT_FLAGS,
        WIC_FLAGS = S.WIC_FLAGS,
        INTERNATIONAL_FLAGS = S.INTERNATIONAL_FLAGS,
        NETWORK_REGULATION_VALUES = S.NETWORK_REGULATION_VALUES,
        PINLESS_POS_FLAGS = S.PINLESS_POS_FLAGS,
        TOKEN_FLAGS = S.TOKEN_FLAGS,
        RELOAD_FLAGS = S.RELOAD_FLAGS,
        CARDHOLDER_FUNDS_TRANSFER_FLAGS = S.CARDHOLDER_FUNDS_TRANSFER_FLAGS,
        DW_BATCH_DATE = CURRENT_DATE,
        DW_SYS_LOAD_TMSTP = CURRENT_TIMESTAMP(0),
        DW_SYS_UPDT_TMSTP = CURRENT_TIMESTAMP(0)
    WHEN NOT MATCHED THEN INSERT (
                      BIN_SIX,
                      BASE_CARD_NETWORKS,
                      CARD_NETWORKS,
                      DEBIT_FLAGS,
                      CHECKCARD_FLAGS,
                      CREDIT_CARD_FLAGS,
                      COMMERCIAL_BUSINESS_FLAGS,
                      FLEET_CARD_FLAGS,
                      PREPAID_CARD_FLAGS,
                      HSA_FLAGS,
                      PINLESS_BILL_PAY_FLAGS,
                      EBT_FLAGS,
                      WIC_FLAGS,
                      INTERNATIONAL_FLAGS,
                      NETWORK_REGULATION_VALUES,
                      PINLESS_POS_FLAGS,
                      TOKEN_FLAGS,
                      RELOAD_FLAGS,
                      CARDHOLDER_FUNDS_TRANSFER_FLAGS,
                      DW_BATCH_DATE,
                      DW_SYS_LOAD_TMSTP,
                      DW_SYS_UPDT_TMSTP
    )
        VALUES
            (
                          S.BIN_SIX,
                          S.BASE_CARD_NETWORKS,
                          S.CARD_NETWORKS,
                          S.DEBIT_FLAGS,
                          S.CHECKCARD_FLAGS,
                          S.CREDIT_CARD_FLAGS,
                          S.COMMERCIAL_BUSINESS_FLAGS,
                          S.FLEET_CARD_FLAGS,
                          S.PREPAID_CARD_FLAGS,
                          S.HSA_FLAGS,
                          S.PINLESS_BILL_PAY_FLAGS,
                          S.EBT_FLAGS,
                          S.WIC_FLAGS,
                          S.INTERNATIONAL_FLAGS,
                          S.NETWORK_REGULATION_VALUES,
                          S.PINLESS_POS_FLAGS,
                          S.TOKEN_FLAGS,
                          S.RELOAD_FLAGS,
                          S.CARDHOLDER_FUNDS_TRANSFER_FLAGS,
                          CURRENT_DATE,
                          CURRENT_TIMESTAMP(0),
                          CURRENT_TIMESTAMP(0)
                          );

UPDATE {db_env}_NAP_PYMNT_FRAUD_BASE_VWS.WORLDPAY_BIN_DIM
SET RECENTLY_UPDATED = null
WHERE 1 = 1;

DELETE FROM {db_env}_NAP_PYMNT_FRAUD_BASE_VWS.WORLDPAY_BIN_ROLLUP_LDG;

SET QUERY_BAND = NONE FOR SESSION; -- noqa

END TRANSACTION;
