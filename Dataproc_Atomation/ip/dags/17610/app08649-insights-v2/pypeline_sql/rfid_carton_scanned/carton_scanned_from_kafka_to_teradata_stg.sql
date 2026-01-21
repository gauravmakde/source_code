--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : carton_scanned_from_kafka_to_teradata_stg.sql
-- Description             : Reading Data from Source Kafka Topic and writing to CARTON_SCANNED_STG table
-- Data Source             : Order Object model kafka topic "inventory-store-carton-rfid-scanned-avro"
--*************************************************************************************************************************************
SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=inventory_rfid_carton_scanned_event_13569_DAS_SC_OUTBOUND_sc_outbound_insights_framework;
Task_Name=main_job_2_load_avro_td_stg;
LoginUser={td_login_user};
Job_Name=inventory_rfid_carton_scanned_event;
Data_Plane=Inventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

create
temporary view carton_scanned_input AS
select *
from kafka_carton_scanned_input;

-- Writing Kafka to Semantic Layer:

create temporary view carton_item_details_vw as
select eventTime,
        timestamp_millis(cast(element_at(headers, 'SystemTime') as BIGINT)) as kafkaTimestamp,
        storeNumber,
        cartonNumber,
        loadNumber,
        shipmentReceiptId,
        transactionId,
        sourceSystem,
        employee.id as employee_id,
        employee.idType as employee_idType,
        explode(cartonDetails) as cartonDetails
from carton_scanned_input
where cast(element_at(headers, 'X-Environment') as string) <> 'stage'
    and element_at(headers, 'Nord-Test') is null
    and element_at(headers, 'Nord-Load') is null;

create temporary view carton_scanned_item_expected_quantities_vw as
select eventTime,
        kafkaTimestamp,
        storeNumber,
        cartonNumber,
        loadNumber,
        shipmentReceiptId,
        transactionId,
        sourceSystem,
        employee_id,
        employee_idType,
        cartonDetails.expectedItemQuantity.item.id as cartonDetails_expectedItemQuantity_item_id,
        cartonDetails.expectedItemQuantity.item.idType as cartonDetails_expectedItemQuantity_item_idType,
        cartonDetails.expectedItemQuantity.quantity as cartonDetails_expectedItemQuantity_quantity,
        concat_ws(',',cartonDetails.expectedItemQuantity.epcs) as cartonDetails_expectedItemQuantity_epcs,
        cartonDetails.isInRfidProgram as cartonDetails_isInRfidProgram,
        explode(cartonDetails.scannedItemQuantities) as scannedItemQuantities
from carton_item_details_vw;

create temporary view carton_scanned_item_expected_and_scanned_quantities_vw as
select eventTime,
        kafkaTimestamp,
        storeNumber,
        cartonNumber,
        loadNumber,
        shipmentReceiptId,
        transactionId,
        sourceSystem,
        employee_id,
        employee_idType,
        cartonDetails_expectedItemQuantity_item_id,
        cartonDetails_expectedItemQuantity_item_idType,
        cartonDetails_expectedItemQuantity_quantity,
        cartonDetails_expectedItemQuantity_epcs,
        cartonDetails_isInRfidProgram,
        scannedItemQuantities.item.id as cartonDetails_scannedItemQuantities_item_id,
        scannedItemQuantities.item.idType as cartonDetails_scannedItemQuantities_item_idType,
        scannedItemQuantities.quantity as cartonDetails_scannedItemQuantities_quantity,
        concat_ws(',',scannedItemQuantities.epcs) as cartonDetails_scannedItemQuantities_epcs
from carton_scanned_item_expected_quantities_vw;

insert
overwrite table carton_scanned_stg_table
select  eventTime,
        kafkaTimestamp,
        storeNumber,
        cartonNumber,
        loadNumber,
        shipmentReceiptId,
        transactionId,
        sourceSystem,
        employee_id,
        employee_idType,
        cartonDetails_expectedItemQuantity_item_id,
        cartonDetails_expectedItemQuantity_item_idType,
        cartonDetails_expectedItemQuantity_quantity,
        cartonDetails_expectedItemQuantity_epcs,
        cartonDetails_isInRfidProgram,
        cartonDetails_scannedItemQuantities_item_id,
        cartonDetails_scannedItemQuantities_item_idType,
        cartonDetails_scannedItemQuantities_quantity,
        cartonDetails_scannedItemQuantities_epcs
from carton_scanned_item_expected_and_scanned_quantities_vw;

