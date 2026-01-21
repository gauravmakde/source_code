-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=mfpc_mos_kafka_to_teradata;
-- Task_Name=job_mos_first_exec_2;'
-- FOR SESSION VOLATILE;

--Source
--Reading Data from  Source Kafka Topic Name=inventory-expense-transfer-ledger-analytical-avro
--using SQL API CODE
create temporary view temp_mos_object_model AS select * from kafka_mos_object_model_avro;
--Transform Logic
--Extracting Required Column from Kafka (Cache in previous SQL)

create temporary view kafka_mos_object_model_avro_extract_array as
select generalLedgerReferenceNumber as GENERAL_LEDGER_REFERENCE_NUMBER,
reasonCode as REASON_CODE,
element_at(headers,'LastUpdatedTime') as LAST_UPDATED_TIME_IN_MILLIS,
explode(merchExpenseTransferLedgerEntries)
as merchExpenseTransferLedgerEntry from temp_mos_object_model;

create temporary view kafka_mos_object_model_avro_extract_columns as
select
GENERAL_LEDGER_REFERENCE_NUMBER as GENERAL_LEDGER_REFERENCE_NUMBER,
REASON_CODE as REASON_CODE,
LAST_UPDATED_TIME_IN_MILLIS as LAST_UPDATED_TIME_IN_MILLIS,
merchExpenseTransferLedgerEntry.eventId as EVENT_ID,
merchExpenseTransferLedgerEntry.eventTime as EVENT_TIME,
merchExpenseTransferLedgerEntry.ledgerAttributes.locationId as LOCATION_NUM,
merchExpenseTransferLedgerEntry.ledgerAttributes.productHierarchy.departmentNumber as DEPARTMENT_NUM,
merchExpenseTransferLedgerEntry.ledgerAttributes.productHierarchy.classNumber as CLASS_NUM,
merchExpenseTransferLedgerEntry.ledgerAttributes.productHierarchy.subClassNumber as SUBCLASS_NUM,
merchExpenseTransferLedgerEntry.ledgerAttributes.product.idType as SKU_TYPE,
merchExpenseTransferLedgerEntry.ledgerAttributes.product.id as SKU_NUM,
merchExpenseTransferLedgerEntry.ledgerAttributes.transactionDate as TRANSACTION_DATE,
merchExpenseTransferLedgerEntry.ledgerAttributes.transactionCode as TRANSACTION_CODE,
merchExpenseTransferLedgerEntry.ledgerAttributes.quantity as QUANTITY,
merchExpenseTransferLedgerEntry.ledgerAttributes.totalCost.currencyCode as TOTAL_COST_CURRENCY_CODE,
merchExpenseTransferLedgerEntry.ledgerAttributes.totalCost.units as TOTAL_COST_UNITS,
merchExpenseTransferLedgerEntry.ledgerAttributes.totalCost.nanos as TOTAL_COST_NANOS,
merchExpenseTransferLedgerEntry.ledgerAttributes.totalRetail.currencyCode as TOTAL_RETAIL_CURRENCY_CODE,
merchExpenseTransferLedgerEntry.ledgerAttributes.totalRetail.units as TOTAL_RETAIL_UNITS,
merchExpenseTransferLedgerEntry.ledgerAttributes.totalRetail.nanos as TOTAL_RETAIL_NANOS
from kafka_mos_object_model_avro_extract_array;
--Sink

---Writing Kafka Data to S3 in csv format for TPT load
insert into table mos
SELECT
GENERAL_LEDGER_REFERENCE_NUMBER as general_ledger_reference_number,
REASON_CODE as reason_code,
LAST_UPDATED_TIME_IN_MILLIS as last_updated_time_in_millis,
cast(EVENT_TIME as string) as event_time,
EVENT_ID as event_id,
LOCATION_NUM as location_num,
DEPARTMENT_NUM as department_num,
CLASS_NUM as class_num,
SUBCLASS_NUM as subclass_num,
SKU_TYPE as sku_type,
SKU_NUM as sku_num,
cast(TRANSACTION_DATE as string) as transaction_date,
cast(TRANSACTION_CODE as string) as transaction_code,
cast(QUANTITY as string) as quantity,
TOTAL_COST_CURRENCY_CODE as total_cost_currency_code,
cast(TOTAL_COST_UNITS as string) as total_cost_units,
cast(TOTAL_COST_NANOS as string) as total_cost_nanos,
TOTAL_RETAIL_CURRENCY_CODE as total_retail_currency_code,
cast(TOTAL_RETAIL_UNITS as STRING) as total_retail_units,
cast(TOTAL_RETAIL_NANOS as STRING) as total_retail_nanos,
cast(DW_SYS_LOAD_TMSTP as STRING) as dw_sys_load_tmstp
FROM (SELECT *, current_timestamp as DW_SYS_LOAD_TMSTP,
 rank() over (PARTITION BY GENERAL_LEDGER_REFERENCE_NUMBER, REASON_CODE, TRANSACTION_DATE, SKU_NUM, LOCATION_NUM
 ORDER BY LAST_UPDATED_TIME_IN_MILLIS DESC) as
ranking FROM kafka_mos_object_model_avro_extract_columns) tmp where ranking == 1;
