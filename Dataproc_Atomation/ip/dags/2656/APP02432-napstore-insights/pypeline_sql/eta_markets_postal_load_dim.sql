SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=eta_markets_load_2656_napstore_insights;
Task_Name=eta_markets_data_load_load_3_postal_dim_table;'
FOR SESSION VOLATILE;

ET;

UPDATE {db_env}_NAP_DIM.LOCAL_MARKET_POSTAL_DIM
FROM {db_env}_NAP_STG.LOCAL_MARKET_POSTAL_LDG AS ldg
SET eff_end_date = ldg.updateTime
WHERE coarse_postal_code = ldg.coarsePostalCode AND eff_end_date IS UNTIL_CHANGED;

INSERT INTO {db_env}_NAP_DIM.LOCAL_MARKET_POSTAL_DIM (
    local_market,
    coarse_postal_code,
    eff_begin_date,
    eff_end_date,
    dw_batch_date,
    dw_sys_load_tmstp
)
SELECT  localMarket,
        coarsePostalCode,
        updateTime,
        UNTIL_CHANGED,
        CURRENT_DATE,
        CURRENT_TIMESTAMP(0)
FROM {db_env}_NAP_STG.LOCAL_MARKET_POSTAL_LDG;
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
