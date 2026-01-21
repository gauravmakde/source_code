--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : inventory_rfid_exit_reader_event/td_stg_to_td_fact.sql
-- Author                  : Eugene Rudakov
-- Description             : ETL to write data from staging table <DBENV>_NAP_STG.INVENTORY_RFID_EXIT_READER_EVENT_LDG into fact table <DBENV>_NAP_FCT.INVENTORY_RFID_EXIT_READER_EVENT_FACT.
-- Data Source             : Staging table <DBENV>_NAP_STG.INVENTORY_RFID_EXIT_READER_EVENT_LDG
-- ETL Run Frequency       : Daily
--*************************************************************************************************************************************

CREATE VOLATILE MULTISET TABLE #INVENTORY_RFID_EXIT_READER_EVENT_LDG AS (
  WITH ETL_BATCH_INFO AS (
    SELECT MAX(batch_id) AS dw_batch_id
         , MAX(batch_date) AS dw_batch_date
    FROM {db_env}_NAP_BASE_VWS.ETL_BATCH_INFO
    WHERE INTERFACE_CODE = 'INVENTORY_RFID_EXIT_READER_EVENT_FACT'
    AND dw_sys_end_tmstp IS NULL ),
  INVENTORY_RFID_EXIT_READER_EVENT_LDG AS (
    SELECT
        location_id
      , floor
      , entrance_name
      , scan_direction
      , epc_encoding
      , epc_value
      , epc_upc
      , entrance_scan_lane
      , COALESCE(entrance_scan_zone_name, 'N/A') AS entrance_scan_zone_name
      , CAST(COALESCE(CONCAT(entrance_scan_tmstp, '+00:00'), '4444-04-04 00:00:00+00:00') AS TIMESTAMP WITH TIME ZONE FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z') AS entrance_scan_tmstp
      , CAST(event_tmstp AS TIMESTAMP WITH TIME ZONE FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z') AS event_tmstp
      , CAST(event_tmstp AS TIMESTAMP(6) WITH TIME ZONE) AT 'America Pacific' AS event_tmstp_pacific
      , CAST(CAST(event_tmstp AS TIMESTAMP(6) WITH TIME ZONE) AS DATE FORMAT 'YYYY-MM-DD') AS event_date
      , CAST(CAST(event_tmstp AS TIMESTAMP(6) WITH TIME ZONE) AT 'America Pacific' AS DATE FORMAT 'YYYY-MM-DD') AS event_date_pacific
      , dw_batch_id
      , dw_batch_date
      , current_timestamp(0) AS dw_sys_load_tmstp
      , current_timestamp(0) AS dw_sys_updt_tmstp
    FROM {db_env}_NAP_BASE_VWS.INVENTORY_RFID_EXIT_READER_EVENT_LDG stg
    LEFT JOIN ETL_BATCH_INFO ON 1=1 )
  SELECT
      location_id as location_num
    , floor as floor_num
    , entrance_name
    , scan_direction
    , epc_encoding as epc_encoding_scheme
    , epc_value
    , epc_upc
    , entrance_scan_lane
    , entrance_scan_zone_name
    , entrance_scan_tmstp
    , event_tmstp
    , event_tmstp_pacific
    , event_date
    , event_date_pacific
    , dw_batch_id
    , dw_batch_date
    , dw_sys_load_tmstp
    , dw_sys_updt_tmstp
  FROM INVENTORY_RFID_EXIT_READER_EVENT_LDG stg
) WITH DATA
PRIMARY INDEX(epc_upc, event_tmstp, entrance_scan_zone_name, entrance_scan_tmstp)
ON COMMIT PRESERVE ROWS;
ET;

COLLECT STATISTICS INDEX (epc_upc, event_tmstp, entrance_scan_zone_name, entrance_scan_tmstp) ON #INVENTORY_RFID_EXIT_READER_EVENT_LDG;
ET;

LOCK TABLE {db_env}_NAP_FCT.INVENTORY_RFID_EXIT_READER_EVENT_FACT FOR ACCESS
MERGE INTO {db_env}_NAP_FCT.INVENTORY_RFID_EXIT_READER_EVENT_FACT fct
USING (
    SELECT
          location_num
         , floor_num
         , entrance_name
         , scan_direction
         , epc_encoding_scheme
         , epc_value
         , epc_upc
         , entrance_scan_lane
         , entrance_scan_zone_name
         , entrance_scan_tmstp
         , event_tmstp
         , event_tmstp_pacific
         , event_date
         , event_date_pacific
         , dw_batch_id
         , dw_batch_date
         , dw_sys_load_tmstp
         , dw_sys_updt_tmstp
    FROM #INVENTORY_RFID_EXIT_READER_EVENT_LDG
) stg
    ON   fct.epc_upc                 = stg.epc_upc
    AND  fct.event_tmstp             = stg.event_tmstp
    AND  fct.entrance_scan_zone_name = stg.entrance_scan_zone_name
    AND  fct.entrance_scan_tmstp     = stg.entrance_scan_tmstp
    AND  fct.event_date              = stg.event_date
WHEN MATCHED
THEN
UPDATE SET
       location_num            = stg.location_num
     , floor_num               = stg.floor_num
     , entrance_name           = stg.entrance_name
     , scan_direction          = stg.scan_direction
     , epc_encoding_scheme     = stg.epc_encoding_scheme
     , epc_value               = stg.epc_value
     , entrance_scan_lane      = stg.entrance_scan_lane
     , dw_batch_id             = stg.dw_batch_id
     , dw_batch_date           = stg.dw_batch_date
     , dw_sys_load_tmstp       = stg.dw_sys_load_tmstp
     , dw_sys_updt_tmstp       = stg.dw_sys_updt_tmstp
WHEN NOT MATCHED
THEN
INSERT (
       location_num
     , floor_num
     , entrance_name
     , scan_direction
     , epc_encoding_scheme
     , epc_value
     , epc_upc
     , entrance_scan_lane
     , entrance_scan_zone_name
     , entrance_scan_tmstp
     , event_tmstp
     , event_tmstp_pacific
     , event_date
     , event_date_pacific
     , dw_batch_id
     , dw_batch_date
     , dw_sys_load_tmstp
     , dw_sys_updt_tmstp)
VALUES (
       stg.location_num
     , stg.floor_num
     , stg.entrance_name
     , stg.scan_direction
     , stg.epc_encoding_scheme
     , stg.epc_value
     , stg.epc_upc
     , stg.entrance_scan_lane
     , stg.entrance_scan_zone_name
     , stg.entrance_scan_tmstp
     , stg.event_tmstp
     , stg.event_tmstp_pacific
     , stg.event_date
     , stg.event_date_pacific
     , stg.dw_batch_id
     , stg.dw_batch_date
     , stg.dw_sys_load_tmstp
     , stg.dw_sys_updt_tmstp
);
