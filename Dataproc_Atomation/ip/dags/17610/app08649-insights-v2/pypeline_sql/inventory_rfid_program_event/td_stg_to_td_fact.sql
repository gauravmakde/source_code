--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : inventory_rfid_program_event.td_stg_to_td_fact.sql
-- Author                  : Eugene Rudakov
-- Description             : ETL to write data from staging table <DBENV>_NAP_STG.INVENTORY_RFID_PROGRAM_EVENT_LDG into fact table <DBENV>_NAP_FCT.INVENTORY_RFID_PROGRAM_EVENT_FACT.
-- Data Source             : Staging table <DBENV>_NAP_STG.INVENTORY_RFID_PROGRAM_EVENT_LDG
-- ETL Run Frequency       : Daily
--*************************************************************************************************************************************

CREATE VOLATILE MULTISET TABLE INVENTORY_RFID_PROGRAM_EVENT_LDG AS(
  WITH ETL_BATCH_INFO AS (
    SELECT MAX(batch_id) AS dw_batch_id
         , MAX(batch_date) AS dw_batch_date
    FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
    WHERE INTERFACE_CODE = 'INVENTORY_RFID_PROGRAM_EVENT_FACT'
    AND dw_sys_end_tmstp IS NULL ),
  INVENTORY_RFID_PROGRAM_EVENT_LDG AS (
    SELECT
        rmsSkuId AS rms_sku_id
      , CASE
          WHEN eventType = 'SkuAddedToRfidProgram' THEN 'ADDED'
          WHEN eventType = 'SkuRemovedFromRfidProgram' THEN 'REMOVED'
        END AS event_type_code
      , eventTime AS event_tmstp
      , CAST(eventTime AS TIMESTAMP(6)) AT 'America Pacific' AS event_tmstp_pacific
      , dw_batch_id
      , dw_batch_date
      , current_timestamp(0) AS dw_sys_load_tmstp
      , current_timestamp(0) AS dw_sys_updt_tmstp
    FROM {db_env}_NAP_BASE_VWS.INVENTORY_RFID_PROGRAM_EVENT_LDG stg
    LEFT JOIN ETL_BATCH_INFO ON 1=1 )
  SELECT
      rms_sku_id
    , event_type_code
    , event_tmstp
    , event_tmstp_pacific
    , dw_batch_id
    , dw_batch_date
    , dw_sys_load_tmstp
    , dw_sys_updt_tmstp
  FROM INVENTORY_RFID_PROGRAM_EVENT_LDG stg
  WHERE NOT EXISTS (SELECT NULL
                      FROM {db_env}_NAP_BASE_VWS.INVENTORY_RFID_PROGRAM_EVENT_FACT AS fct
                     WHERE fct.rms_sku_id = stg.rms_sku_id
                       AND fct.event_type_code = stg.event_type_code
                       AND fct.event_tmstp_pacific >= stg.event_tmstp_pacific)
) WITH DATA
ON COMMIT PRESERVE ROWS;
ET;

INSERT INTO {db_env}_NAP_FCT.INVENTORY_RFID_PROGRAM_EVENT_FACT
  SELECT
      rms_sku_id
    , event_type_code
    , event_tmstp
    , event_tmstp_pacific
    , dw_batch_id
    , dw_batch_date
    , dw_sys_load_tmstp
    , dw_sys_updt_tmstp
  FROM INVENTORY_RFID_PROGRAM_EVENT_LDG;
