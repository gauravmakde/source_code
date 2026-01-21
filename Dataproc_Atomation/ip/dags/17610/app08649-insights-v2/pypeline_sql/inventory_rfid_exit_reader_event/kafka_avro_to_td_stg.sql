--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : inventory_rfid_exit_reader_event/kafka_avro_to_td_stg.sql
-- Author                  : Eugene Rudakov
-- Description             : ETL to write data from kafka topic "inventory-epc-scanned-at-entrance-avro" to <DBENV>_NAP_STG.INVENTORY_RFID_EXIT_READER_EVENT_LDG
-- Data Source             : Kafka topic "inventory-epc-scanned-at-entrance-avro"
-- ETL Run Frequency       : Daily
--*************************************************************************************************************************************

SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=inventory_rfid_exit_reader_event_17610_DAS_SC_OUTBOUND_sc_outbound_insights_framework;
Task_Name=main_job_2_load_avro_td_stg;
LoginUser={td_login_user};
Job_Name=sco_inventory_rfid_exit_reader_event;
Data_Plane=Inventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

--Reading Data from Source Kafka Topic Name=inventory-epc-scanned-at-entrance-avro
create temporary view kafka_inventory_rfid_exit_reader_tab as
select  cast(eventTime as string) || '+00:00' as eventTime
      , locationId
      , floor
      , entranceName
      , scanDirection
      , epcDetail.encoding as epcDetail_encoding
      , epcDetail.value as epcDetail_value
      , epcDetail.upc as epcDetail_upc
      , lane
      , explode_outer(zones) as zones
   from kafka_inventory_rfid_exit_reader src
;

insert overwrite table inventory_rfid_exit_reader_event_ldg
      (
        location_id
      , floor
      , entrance_name
      , scan_direction
      , epc_encoding
      , epc_value
      , epc_upc
      , entrance_scan_lane
      , entrance_scan_zone_name
      , entrance_scan_tmstp
      , event_tmstp
      , dw_sys_load_tmstp
      )
select  locationId as location_id
      , floor
      , entranceName as entrance_name
      , scanDirection as scan_direction
      , epcDetail_encoding as epc_encoding
      , epcDetail_value as epc_value
      , epcDetail_upc as epc_upc
      , lane as entrance_scan_lane
      , zones.zoneName as entrance_scan_zone_name
      , zones.scanTime as entrance_scan_tmstp
      , eventTime as event_tmstp
      , current_timestamp() as dw_sys_load_tmstp
   from kafka_inventory_rfid_exit_reader_tab;
