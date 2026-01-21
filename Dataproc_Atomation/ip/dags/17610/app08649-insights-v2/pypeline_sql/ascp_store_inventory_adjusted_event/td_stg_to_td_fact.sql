--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : ascp_store_inventory_adjusted_event/td_stg_to_td_fact.sql
-- Author                  : Eugene Rudakov
-- Description             : ETL to write data from staging table <DBENV>_NAP_STG.STORE_INVENTORY_ADJUSTED_EVENT_LDG into fact table <DBENV>_NAP_FCT.STORE_INVENTORY_ADJUSTED_EVENT_FACT.
-- Data Source             : Staging table <DBENV>_NAP_STG.STORE_INVENTORY_ADJUSTED_EVENT_LDG
-- ETL Run Frequency       : Daily
--*************************************************************************************************************************************

CREATE VOLATILE MULTISET TABLE #STORE_INVENTORY_ADJUSTED_EVENT_LDG AS (
  WITH ETL_BATCH_INFO AS (
    SELECT MAX(batch_id) AS dw_batch_id
         , MAX(batch_date) AS dw_batch_date
    FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
    WHERE INTERFACE_CODE = 'STORE_INVENTORY_ADJUSTED_EVENT_FACT'
    AND dw_sys_end_tmstp IS NULL)
  SELECT locationId AS location_id
       , id AS event_id
       , inventoryAdjustmentId AS adjustment_id
       , adjustmentDetails_sku_Id AS sku_num
       , adjustmentDetails_sku_IdType AS sku_type
       , adjustmentDetails_quantity AS adjustment_qty
       , adjustmentDetails_fromDisposition AS from_disposition_code
       , adjustmentDetails_toDisposition AS to_disposition_code
       , adjustmentDetails_reasonCode AS reason_code
       , eventTime AS event_tmstp
       , CAST(eventTime AS TIMESTAMP(6) WITH TIME ZONE) AT 'America Pacific' AS event_tmstp_pacific
       , CAST(CAST(eventTime AS TIMESTAMP(6) WITH TIME ZONE) AS DATE FORMAT 'YYYY-MM-DD') AS event_date
       , CAST(CAST(eventTime AS TIMESTAMP(6) WITH TIME ZONE) AT 'America Pacific' AS DATE FORMAT 'YYYY-MM-DD') AS event_date_pacific
       , dw_batch_id
       , dw_batch_date
       , current_timestamp(0) AS dw_sys_load_tmstp
       , current_timestamp(0) AS dw_sys_updt_tmstp
  FROM {db_env}_NAP_BASE_VWS.STORE_INVENTORY_ADJUSTED_EVENT_LDG stg
  LEFT JOIN ETL_BATCH_INFO ON 1=1
) WITH DATA
ON COMMIT PRESERVE ROWS;
ET;

INSERT INTO {db_env}_NAP_FCT.STORE_INVENTORY_ADJUSTED_EVENT_FACT
  SELECT
      location_id
    , event_id
    , adjustment_id
    , sku_num
    , sku_type
    , adjustment_qty
    , from_disposition_code
    , to_disposition_code
    , reason_code
    , event_tmstp
    , event_tmstp_pacific
    , event_date
    , event_date_pacific
    , dw_batch_id
    , dw_batch_date
    , dw_sys_load_tmstp
    , dw_sys_updt_tmstp
  FROM #STORE_INVENTORY_ADJUSTED_EVENT_LDG;
