--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : inventory_sim_store_inventory_adjusted/td_stg_to_td_fact.sql
-- Author                  : Enes Topuz
-- Description             : ETL to write data from staging table <DBENV>_NAP_STG.INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_LDG into fact table <DBENV>_NAP_FCT.INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_FACT.
-- Data Source             : Staging table <DBENV>_NAP_STG.INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_LDG
-- ETL Run Frequency       : Daily
-- JIRA Issue Link         : https://jira.nordstrom.com/browse/FA-12738
--*************************************************************************************************************************************

CREATE VOLATILE MULTISET TABLE  #INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_EVENT_DETAIL_LDG
AS
    (    SELECT
              locationId
            , id
            , inventoryAdjustmentId
            , adjustmentDetails_sku_Id
            , adjustmentDetails_sku_IdType
            , adjustmentDetails_quantity
            , adjustmentDetails_fromDisposition
            , adjustmentDetails_toDisposition
            , adjustmentDetails_reasonCode
            , eventTime
           FROM {db_env}_NAP_BASE_VWS.INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_LDG
        QUALIFY ROW_NUMBER() OVER (PARTITION BY inventoryAdjustmentId, adjustmentDetails_sku_Id ORDER BY CAST(eventTime AS TIMESTAMP(6)) DESC) = 1
    )
WITH DATA
PRIMARY INDEX(inventoryAdjustmentId, adjustmentDetails_sku_Id)
ON COMMIT PRESERVE ROWS;
ET;

-- Collect stats for tables
COLLECT STATISTICS INDEX (inventoryAdjustmentId, adjustmentDetails_sku_Id) ON #INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_EVENT_DETAIL_LDG;
ET;

--Create temporary table with all of transformation and renaming all columns to target table names
CREATE VOLATILE MULTISET TABLE  #INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_EVENT_DETAIL_LDG_FINAL
AS 
(WITH  ETL_BATCH_INFO AS
    (SELECT BATCH_ID, BATCH_DATE 
       FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO 
      WHERE INTERFACE_CODE = 'INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_FACT'
        AND DW_SYS_END_TMSTP IS NULL)
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
        FROM (
            SELECT
                locationId AS location_id
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
                , batch_id AS dw_batch_id
                , batch_date AS dw_batch_date 
                , current_timestamp(0) AS dw_sys_load_tmstp
                , current_timestamp(0) AS dw_sys_updt_tmstp
            FROM #INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_EVENT_DETAIL_LDG
                LEFT JOIN ETL_BATCH_INFO ON 1=1
        ) tmp
    )
WITH DATA
PRIMARY INDEX(adjustment_id, sku_num)
ON COMMIT PRESERVE ROWS;
ET;

--Collect stats for final staging temporary table before merge to target fact table
COLLECT STATISTICS INDEX (adjustment_id,sku_num) ON #INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_EVENT_DETAIL_LDG_FINAL;
ET;

LOCK TABLE {db_env}_NAP_FCT.INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_FACT fct
USING
    (
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
        FROM #INVENTORY_SIM_STORE_INVENTORY_ADJUSTED_EVENT_DETAIL_LDG_FINAL
    ) stg
        ON   fct.adjustment_id  =  stg.adjustment_id
        AND  fct.sku_num        =  stg.sku_num
        AND  fct.event_date     =  stg.event_date
WHEN MATCHED
THEN
UPDATE SET
        location_id             =  stg.location_id
      , event_id                =  stg.event_id
      , sku_type                =  stg.sku_type
      , adjustment_qty          =  stg.adjustment_qty
      , from_disposition_code   =  stg.from_disposition_code
      , to_disposition_code     =  stg.to_disposition_code
      , reason_code             =  stg.reason_code
      , event_tmstp             =  stg.event_tmstp
      , event_tmstp_pacific     =  stg.event_tmstp_pacific
      , event_date_pacific      =  stg.event_date_pacific
      , dw_batch_id             =  stg.dw_batch_id
      , dw_batch_date           =  stg.dw_batch_date
      , dw_sys_load_tmstp       =  stg.dw_sys_load_tmstp
      , dw_sys_updt_tmstp       =  stg.dw_sys_updt_tmstp
WHEN NOT MATCHED
THEN
INSERT (
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
      , dw_sys_updt_tmstp)
VALUES (
        stg.location_id
      , stg.event_id
      , stg.adjustment_id
      , stg.sku_num
      , stg.sku_type
      , stg.adjustment_qty
      , stg.from_disposition_code
      , stg.to_disposition_code
      , stg.reason_code
      , stg.event_tmstp
      , stg.event_tmstp_pacific
      , stg.event_date
      , stg.event_date_pacific
      , stg.dw_batch_id
      , stg.dw_batch_date
      , stg.dw_sys_load_tmstp
      , stg.dw_sys_updt_tmstp
);
