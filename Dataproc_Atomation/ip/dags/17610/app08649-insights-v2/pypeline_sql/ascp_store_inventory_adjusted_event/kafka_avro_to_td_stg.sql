--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : ascp_store_inventory_adjusted_event/kafka_avro_to_td_stg.sql
-- Author                  : Eugene Rudakov
-- Description             : ETL to write data from kafka topic "inventory-return-store-inventory-adjusted-avro" to <DBENV>_NAP_STG.STORE_INVENTORY_ADJUSTED_EVENT_LDG
-- Data Source             : Returns Lifecycle Object model kafka topic "inventory-item-return-lifecycle-analytical-avro"
-- ETL Run Frequency       : Daily
--*************************************************************************************************************************************

SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=ascp_store_inventory_adjusted_event_17610_DAS_SC_OUTBOUND_APP08649_insights_v2;
Task_Name=main_job_2_load_avro_td_stg;
LoginUser={td_login_user};
Job_Name=ascp_store_inventory_adjusted_event;
Data_Plane=Inventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

--Reading Data from Source Kafka Topic Name=inventory-return-store-inventory-adjusted-avro
create temporary view store_inventory_adjusted as
select locationId
     , id
     , inventoryAdjustmentId
     , cast(eventTime as string) || '+00:00' as eventTime
     , explode(adjustmentDetails) as adjustmentDetails
from kafka_inventory_return_store_inventory_adjusted;


insert overwrite table store_inventory_adjusted_event_ldg (
       locationId
     , id
     , inventoryAdjustmentId
     , eventTime
     , adjustmentDetails_sku_Id
     , adjustmentDetails_sku_IdType
     , adjustmentDetails_quantity
     , adjustmentDetails_fromDisposition
     , adjustmentDetails_toDisposition
     , adjustmentDetails_reasonCode
     , dw_sys_load_tmstp
)
select locationId
     , id
     , inventoryAdjustmentId
     , eventTime
     , adjustmentDetails.product.id as adjustmentDetails_sku_Id
     , adjustmentDetails.product.idType as adjustmentDetails_sku_IdType
     , adjustmentDetails.quantity as adjustmentDetails_quantity
     , adjustmentDetails.fromDisposition as adjustmentDetails_fromDisposition
     , adjustmentDetails.toDisposition as adjustmentDetails_toDisposition
     , adjustmentDetails.reasonCode as adjustmentDetails_reasonCode
     , current_timestamp() as dw_sys_load_tmstp
   from store_inventory_adjusted;
