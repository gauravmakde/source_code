SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_transferledger_kafka_to_teradata_10976_tech_nap_merch;
Task_Name=mfpc_transferledger_kafka_to_teradata_10976_tech_nap_merch_job_0003;'
FOR SESSION VOLATILE;

--Source
--Reading Data from  Source Kafka Topic Name=inventory-transfer-ledger-analytical-avro using SQL API CODE
create temporary view  temp_transfer_ledger_object_model AS select * from kafka_transferledger_object_model_avro ;
--Transform Logic
--Extracting Required Column from Kafka (Cache in previous SQL)
create temporary view kafka_transferledger_object_model_avro_extract_array as select id as TRANSFER_LEDGER_ID, 
element_at(headers,'LastUpdatedTime') as LAST_UPDATED_TIME_IN_MILLIS,
transferOperationType as TRANSFER_OPERATION_TYPE, explode(transferLedgerEntries) as transferLedgerEntry 
from temp_transfer_ledger_object_model;

create temporary view kafka_transferledger_object_model_avro_extract_columns as
select TRANSFER_LEDGER_ID as TRANSFER_LEDGER_ID, 
LAST_UPDATED_TIME_IN_MILLIS as LAST_UPDATED_TIME_IN_MILLIS,
TRANSFER_OPERATION_TYPE as TRANSFER_OPERATION_TYPE,
transferLedgerEntry.eventId as EVENT_ID,
transferLedgerEntry.eventTime as EVENT_TIME,
transferLedgerEntry.shipmentId as SHIPMENT_NUM,
transferLedgerEntry.eventType as EVENT_TYPE,
transferLedgerEntry.ledgerAttributes.locationId as LOCATION_NUM,
transferLedgerEntry.ledgerAttributes.productHierarchy.departmentNumber as DEPARTMENT_NUM,
transferLedgerEntry.ledgerAttributes.productHierarchy.classNumber as CLASS_NUM,
transferLedgerEntry.ledgerAttributes.productHierarchy.subClassNumber as SUBCLASS_NUM,
transferLedgerEntry.ledgerAttributes.product.idType as SKU_TYPE,
transferLedgerEntry.ledgerAttributes.product.id as SKU_NUM,
transferLedgerEntry.ledgerAttributes.transactionDate as TRANSACTION_DATE,
transferLedgerEntry.ledgerAttributes.transactionCode as TRANSACTION_CODE,
transferLedgerEntry.ledgerAttributes.quantity as QUANTITY,
transferLedgerEntry.ledgerAttributes.totalCost.currencyCode as TOTAL_COST_CURRENCY_CODE,
transferLedgerEntry.ledgerAttributes.totalCost.units as TOTAL_COST_UNITS,
transferLedgerEntry.ledgerAttributes.totalCost.nanos as TOTAL_COST_NANOS,
transferLedgerEntry.ledgerAttributes.totalRetail.currencyCode as TOTAL_RETAIL_CURRENCY_CODE,
transferLedgerEntry.ledgerAttributes.totalRetail.units as TOTAL_RETAIL_UNITS,
transferLedgerEntry.ledgerAttributes.totalRetail.nanos as TOTAL_RETAIL_NANOS
from kafka_transferledger_object_model_avro_extract_array;
--Sink

---Writing Kafka Data to S3 in csv format for TPT load
--- insert overwrite table transferledger
insert into table transferledger partition (year, month, day )
SELECT
TRANSFER_LEDGER_ID, 
LAST_UPDATED_TIME_IN_MILLIS,
TRANSFER_OPERATION_TYPE,
EVENT_ID,
cast(EVENT_TIME as string) as EVENT_TIME,
SHIPMENT_NUM,
EVENT_TYPE,
LOCATION_NUM,
DEPARTMENT_NUM,
CLASS_NUM,
SUBCLASS_NUM,
SKU_TYPE,
SKU_NUM,
cast(TRANSACTION_DATE as string) as TRANSACTION_DATE,
cast(TRANSACTION_CODE as string) as TRANSACTION_CODE,
cast(QUANTITY as string) as QUANTITY,
TOTAL_COST_CURRENCY_CODE,
cast(TOTAL_COST_UNITS as string) as TOTAL_COST_UNITS,
cast(TOTAL_COST_NANOS as string) as TOTAL_COST_NANOS,
TOTAL_RETAIL_CURRENCY_CODE,
cast(TOTAL_RETAIL_UNITS as string) as TOTAL_RETAIL_UNITS,
cast(TOTAL_RETAIL_NANOS as string) as TOTAL_RETAIL_NANOS,
cast(DW_SYS_LOAD_TMSTP as string) as  DW_SYS_LOAD_TMSTP,
year( current_date()) as year,month( current_date()) as month,day( current_date()) as day
FROM (SELECT *, current_timestamp as DW_SYS_LOAD_TMSTP, row_number() over (PARTITION BY TRANSFER_LEDGER_ID, TRANSFER_OPERATION_TYPE, SKU_NUM, SKU_TYPE, LOCATION_NUM, TRANSACTION_DATE, EVENT_ID ORDER BY cast(LAST_UPDATED_TIME_IN_MILLIS as bigint) DESC) as
ranking FROM kafka_transferledger_object_model_avro_extract_columns) tmp where ranking == 1;
