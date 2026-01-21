--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : inventory_rfid_program_event.kafka_avro_to_td_stg.sql
-- Author                  : Eugene Rudakov
-- Description             : ETL to write data from kafka topic "inventory-rfid-program-avro" to <DBENV>_NAP_STG.INVENTORY_RFID_PROGRAM_EVENT_LDG
-- Data Source             : Returns Lifecycle Object model kafka topic "inventory-item-return-lifecycle-analytical-avro"
-- ETL Run Frequency       : Daily
--*************************************************************************************************************************************

SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=inventory_rfid_program_event_13569_DAS_SC_OUTBOUND_sc_outbound_insights_framework;
Task_Name=main_job_2_load_avro_td_stg;
LoginUser={td_login_user};
Job_Name=sco_inventory_rfid_program_event;
Data_Plane=Inventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

--Reading Data from Source Kafka Topic Name=inventory-rfid-program-avro
-- sku_added
create temporary view inventory_rfid_program_sku_added_prod as
select
    rmsSkuId
  , element_at(from_json(to_json(headers), 'map<string, string>'), 'Type') as eventType
  , cast(eventTime as string) || '+00:00' as eventTime
from kafka_inventory_rfid_program_sku_added
where element_at(from_json(to_json(headers), 'map<string, string>'), 'X-Environment') = 'prod';

create temporary view inventory_rfid_program_sku_added_tab as
select rmsSkuId
     , eventType
     , eventTime
from (
    select  rmsSkuId
          , eventType
          , eventTime
          , row_number() over (partition by rmsSkuId, eventType, eventTime order by eventTime) as rn
       from inventory_rfid_program_sku_added_prod src
) t
where rn = 1;

-- sku_removed
create temporary view inventory_rfid_program_sku_removed_prod as
select
    rmsSkuId
  , element_at(from_json(to_json(headers), 'map<string, string>'), 'Type') as eventType
  , cast(eventTime as string) || '+00:00' as eventTime
from kafka_inventory_rfid_program_sku_removed
where element_at(from_json(to_json(headers), 'map<string, string>'), 'X-Environment') = 'prod';

create temporary view inventory_rfid_program_sku_removed_tab as
select rmsSkuId
     , eventType
     , eventTime
from (
    select  rmsSkuId
          , eventType
          , eventTime
          , row_number() over (partition by rmsSkuId, eventType, eventTime order by eventTime) as rn
       from inventory_rfid_program_sku_removed_prod src
) t
where rn = 1;

insert overwrite table inventory_rfid_program_event_ldg
      (
        rmsSkuId
      , eventType
      , eventTime
      , dw_sys_load_tmstp
      )
select  rmsSkuId
      , eventType
      , eventTime
      , current_timestamp() as dw_sys_load_tmstp
   from inventory_rfid_program_sku_added_tab
union all
select  rmsSkuId
      , eventType
      , eventTime
      , current_timestamp() as dw_sys_load_tmstp
   from inventory_rfid_program_sku_removed_tab;
