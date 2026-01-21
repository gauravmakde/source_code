SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_poreceipt_kafka_to_teradata_10976_tech_nap_merch;
Task_Name=poreceipt_kafka_to_td_tpt_stage_1_job_0;'
FOR SESSION VOLATILE;

-- Reading Data from Source Kafka Topic Name = inventory-stockledger-avro using SQL API CODE
create temporary view  temp_poreceipt_ledger_object_model AS select * from kafka_poreceipt_ledger_object_model_avro ;
-- Extracting Required Column from Kafka (Cache in previous SQL)
create temporary view kafka_poreceipt_ledger_object_model_avro_extract_array as select split(id,'_')[0] as poreceipt_order_number, element_at(headers,'LastUpdatedTime') as last_updated_time_in_millis,
explode(purchaseOrderReceiptLedgerEntries) as poreceiptLedgerEntry from temp_poreceipt_ledger_object_model;
-- Formulating the view with data columns 
create temporary view kafka_poreceipt_ledger_object_model_avro_extract_columns as 
select 
poreceipt_order_number as PORECEIPT_ORDER_NUMBER,
last_updated_time_in_millis as LAST_UPDATED_TIME_IN_MILLIS,
poreceiptLedgerEntry.eventId as EVENT_ID,
poreceiptLedgerEntry.eventTime as EVENT_TIME,
poreceiptLedgerEntry.shipmentId as SHIPMENT_NUM,
poreceiptLedgerEntry.adjustmentType as ADJUSTMENT_TYPE,
poreceiptLedgerEntry.ledgerAttributes.locationId as STORE_NUM,
poreceiptLedgerEntry.ledgerAttributes.productHierarchy.departmentNumber as DEPARTMENT_NUM,
poreceiptLedgerEntry.ledgerAttributes.productHierarchy.classNumber as CLASS_NUM,
poreceiptLedgerEntry.ledgerAttributes.productHierarchy.subClassNumber as SUBCLASS_NUM,
poreceiptLedgerEntry.ledgerAttributes.product.idType as SKU_NUM_TYPE,
poreceiptLedgerEntry.ledgerAttributes.product.id as SKU_NUM,
poreceiptLedgerEntry.ledgerAttributes.transactionDate as TRAN_DATE,
poreceiptLedgerEntry.ledgerAttributes.transactionCode as TRAN_CODE,
poreceiptLedgerEntry.ledgerAttributes.quantity as QUANTITY,
poreceiptLedgerEntry.ledgerAttributes.totalCost.currencyCode as TOTAL_COST_CURR_CODE,
poreceiptLedgerEntry.ledgerAttributes.totalCost.units as TOTAL_COST_UNITS,
poreceiptLedgerEntry.ledgerAttributes.totalCost.nanos as TOTAL_COST_NANOS,
poreceiptLedgerEntry.ledgerAttributes.totalRetail.currencyCode as TOTAL_RETAIL_CURR_CODE,
poreceiptLedgerEntry.ledgerAttributes.totalRetail.units as TOTAL_RETAIL_UNITS,
poreceiptLedgerEntry.ledgerAttributes.totalRetail.nanos as TOTAL_RETAIL_NANOS 
from kafka_poreceipt_ledger_object_model_avro_extract_array;
---Writing Kafka Data to Teradata using SQL API CODE
insert into table poreceipt 
SELECT 
PORECEIPT_ORDER_NUMBER,
LAST_UPDATED_TIME_IN_MILLIS,
EVENT_ID,
CAST(EVENT_TIME AS STRING),
SHIPMENT_NUM,
ADJUSTMENT_TYPE,
STORE_NUM,
DEPARTMENT_NUM,
CLASS_NUM,
SUBCLASS_NUM,
SKU_NUM_TYPE,
SKU_NUM,
CAST(TRAN_CODE AS STRING),
CAST(TRAN_DATE AS STRING),
CAST(QUANTITY AS STRING),
TOTAL_COST_CURR_CODE,
CAST(TOTAL_COST_UNITS AS STRING),
CAST(TOTAL_COST_NANOS AS STRING),
TOTAL_RETAIL_CURR_CODE,
CAST(TOTAL_RETAIL_UNITS AS STRING),
CAST(TOTAL_RETAIL_NANOS AS STRING)
FROM (SELECT mplf.*, row_number() over (PARTITION BY PORECEIPT_ORDER_NUMBER,SKU_NUM,STORE_NUM,TRAN_DATE,EVENT_ID ORDER BY cast(LAST_UPDATED_TIME_IN_MILLIS as bigint) DESC) as 
ranking FROM kafka_poreceipt_ledger_object_model_avro_extract_columns mplf) tmp where ranking == 1;

