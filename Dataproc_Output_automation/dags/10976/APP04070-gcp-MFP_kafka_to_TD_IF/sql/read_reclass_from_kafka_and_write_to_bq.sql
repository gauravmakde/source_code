-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=project_mfp_reclass_kafka_to_td_tpt;
-- Task_Name=read_reclass_from_kafka_and_write_to_teradata_job_1;'
-- FOR SESSION VOLATILE;

create temporary view kafka_reclass_object_model_avro_extract_array as select productSku.id as sku_num, productSku.idType as sku_num_type, transactionDate as tranDate, explode(reclassLedgerEntries) as reclassLedgerEntry, element_at(headers,'LastUpdatedTime') as last_updated_time_in_millis from kafka_reclass_tpt_table;

create temporary view kafka_reclass_object_model_avro_extract_columns as
select sku_num,
sku_num_type,
tranDate as tran_date,
last_updated_time_in_millis,
reclassLedgerEntry.eventId as event_num,
reclassLedgerEntry.eventTime as event_time,
reclassLedgerEntry.packIndicator as pack_indicator,
reclassLedgerEntry.ledgerAttributes.locationId as store_num,
reclassLedgerEntry.ledgerAttributes.productHierarchy.departmentNumber as dept_num,
reclassLedgerEntry.ledgerAttributes.productHierarchy.classNumber as class_num,
reclassLedgerEntry.ledgerAttributes.productHierarchy.subClassNumber as subclass_num,
reclassLedgerEntry.ledgerAttributes.transactionDate as calendar_tran_date,
reclassLedgerEntry.ledgerAttributes.transactionCode as tran_code,
reclassLedgerEntry.ledgerAttributes.quantity as quantity,
reclassLedgerEntry.ledgerAttributes.totalCost.currencyCode as total_cost_currency_code,
reclassLedgerEntry.ledgerAttributes.totalCost.units as total_cost_units,
reclassLedgerEntry.ledgerAttributes.totalCost.nanos as total_cost_nanos
from kafka_reclass_object_model_avro_extract_array;


---Writing Kafka Data to Teradata using SQL API CODE
insert into table reclass
select 
sku_num,
sku_num_type,
cast(tran_date as string),
last_updated_time_in_millis,
event_num,
cast(event_time as string),
cast(pack_indicator as string),
store_num,
dept_num,
class_num,
subclass_num,
cast(calendar_tran_date as string),
cast(tran_code as string),
cast(quantity as string),
total_cost_currency_code,
cast(total_cost_units as string),
cast(total_cost_nanos as string)
FROM (SELECT *, rank() over (PARTITION BY sku_num,tran_date ORDER BY last_updated_time_in_millis DESC) as
ranking FROM kafka_reclass_object_model_avro_extract_columns) tmp where ranking == 1;
