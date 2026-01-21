SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_spe_ledger_kafka_to_teradata_v1;
Task_Name=job_spe_ledger_first_exec_04;'
FOR SESSION VOLATILE;

--Source
--Reading Data from  Source Kafka Topic Name=inventory-supplier-group-profitability-expectations-analytical-avro using SQL API CODE
create temporary view temp_spe_ledger_object_model AS select * from kafka_spe_ledger_object_model_avro;
--Transform Logic
--Extracting Required Column from Kafka (Cache in previous SQL)
--create temporary view kafka_spe_ledger_object_model_avro_extract_columns as select productSku.id as sku_id from temp_poitermsdisc_object_model limit 1000;


create temporary view kafka_spe_ledger_object_model_avro_extract_array as select tsl.supplierGroupId as supplier_group_id,tsl.year as year_num,tsl.sellingCountry as selling_country,tsl.plannedProfitabilityExpectationPercent as planned_profitability_expectation_percent,
tsl.initialMarkUpExpectationPercent  as initial_markup_expectation_percent,
tsl.regularPriceSalesExpectationPercent as regular_price_sales_expectation_percent,
tsl.lastUpdatedTime as event_time,
element_at(headers,'LastUpdatedTime') as last_updated_time_in_millis                          
from temp_spe_ledger_object_model tsl;

create temporary view kafka_spe_ledger_object_model_avro_extract_columns as
select event_time,
supplier_group_id,
year_num, 
selling_country,
planned_profitability_expectation_percent,
initial_markup_expectation_percent,
regular_price_sales_expectation_percent,
last_updated_time_in_millis
from kafka_spe_ledger_object_model_avro_extract_array;
--Sink 

---Writing Kafka Data to S3 in csv format for TPT load
--- insert overwrite table speledger
insert into table speledger partition (year, month, day )
SELECT
supplier_group_id,
cast(year_num as string) as year_num,
selling_country,
cast(planned_profitability_expectation_percent as string) as planned_profitability_expectation_percent ,
cast(event_time as string) as event_time,
last_updated_time_in_millis,
cast(dw_sys_load_tmstp as string) as dw_sys_load_tmstp,
cast(initial_markup_expectation_percent as string) as initial_markup_expectation_percent ,
cast(regular_price_sales_expectation_percent as string) as regular_price_sales_expectation_percent,
year( current_date()) as year,month( current_date()) as month,day( current_date()) as day
FROM (SELECT *, current_timestamp as DW_SYS_LOAD_TMSTP, rank() over (PARTITION BY supplier_group_id,year_num,selling_country ORDER BY last_updated_time_in_millis DESC) as 
ranking FROM kafka_spe_ledger_object_model_avro_extract_columns) tmp where ranking == 1;
