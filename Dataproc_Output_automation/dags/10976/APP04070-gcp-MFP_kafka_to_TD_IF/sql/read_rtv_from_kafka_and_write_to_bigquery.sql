-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=mfpc_return_to_vendor_kafka_to_teradata;
-- Task_Name=job_rtv_first_exec_004;'
-- FOR SESSION VOLATILE;

--Source
--Reading Data from  Source Kafka Topic Name=inventory-return-to-vendor-ledger-analytical-avro
--using SQL API CODE
create temporary view temp_rtv_object_model AS select * from kafka_rtv_object_model_avro;
--Transform Logic
--Extracting Required Column from Kafka (Cache in previous SQL)
--create temporary view kafka_rtv_object_model_avro_extract_columns as select productSku.id as sku_id from temp_rtv_object_model;

create temporary view kafka_rtv_object_model_avro_extract_array as select id as RTV_ID,
element_at(headers,'LastUpdatedTime') as LAST_UPDATED_TIME_IN_MILLIS,
explode(rtvLedgerEntries)
as rtvLedgerEntry from temp_rtv_object_model;

create temporary view kafka_rtv_object_model_avro_extract_columns as
select
RTV_ID as RTV_ID,
LAST_UPDATED_TIME_IN_MILLIS as LAST_UPDATED_TIME_IN_MILLIS,
rtvLedgerEntry.eventId as EVENT_ID,
rtvLedgerEntry.eventTime as EVENT_TIME,
rtvLedgerEntry.ledgerAttributes.locationId as LOCATION_NUM,
rtvLedgerEntry.ledgerAttributes.productHierarchy.departmentNumber as DEPARTMENT_NUM,
rtvLedgerEntry.ledgerAttributes.productHierarchy.classNumber as CLASS_NUM,
rtvLedgerEntry.ledgerAttributes.productHierarchy.subClassNumber as SUBCLASS_NUM,
rtvLedgerEntry.ledgerAttributes.product.idType as SKU_TYPE,
rtvLedgerEntry.ledgerAttributes.product.id as SKU_NUM,
rtvLedgerEntry.ledgerAttributes.transactionDate as TRANSACTION_DATE,
rtvLedgerEntry.ledgerAttributes.transactionCode as TRANSACTION_CODE,
rtvLedgerEntry.ledgerAttributes.quantity as QUANTITY,
rtvLedgerEntry.ledgerAttributes.totalCost.currencyCode as TOTAL_COST_CURRENCY_CODE,
rtvLedgerEntry.ledgerAttributes.totalCost.units as TOTAL_COST_UNITS,
rtvLedgerEntry.ledgerAttributes.totalCost.nanos as TOTAL_COST_NANOS,
rtvLedgerEntry.ledgerAttributes.totalRetail.currencyCode as TOTAL_RETAIL_CURRENCY_CODE,
rtvLedgerEntry.ledgerAttributes.totalRetail.units as TOTAL_RETAIL_UNITS,
rtvLedgerEntry.ledgerAttributes.totalRetail.nanos as TOTAL_RETAIL_NANOS
from kafka_rtv_object_model_avro_extract_array;
--Sink

---Writing Kafka Data to S3 in csv format for TPT load
insert into table returntovendor
SELECT
RTV_ID,
LAST_UPDATED_TIME_IN_MILLIS,
CAST(EVENT_TIME AS STRING) EVENT_TIME,
EVENT_ID,
LOCATION_NUM,
DEPARTMENT_NUM,
CLASS_NUM,
SUBCLASS_NUM,
SKU_TYPE,
SKU_NUM,
CAST(TRANSACTION_DATE AS STRING) TRANSACTION_DATE,
CAST(TRANSACTION_CODE AS STRING) TRANSACTION_CODE,
cast(QUANTITY as string) QUANTITY,
TOTAL_COST_CURRENCY_CODE,
CAST(TOTAL_COST_UNITS AS STRING) TOTAL_COST_UNITS,
CAST(TOTAL_COST_NANOS AS STRING) TOTAL_COST_NANOS,
TOTAL_RETAIL_CURRENCY_CODE,
cast(TOTAL_RETAIL_UNITS as string) TOTAL_RETAIL_UNITS,
cast(TOTAL_RETAIL_NANOS as string) TOTAL_RETAIL_NANOS,
CAST(DW_SYS_LOAD_TMSTP AS STRING) DW_SYS_LOAD_TMSTP
FROM (SELECT *, current_timestamp as DW_SYS_LOAD_TMSTP,
 rank() over (PARTITION BY RTV_ID ORDER BY LAST_UPDATED_TIME_IN_MILLIS DESC) as
ranking FROM kafka_rtv_object_model_avro_extract_columns) tmp where ranking = 1;
