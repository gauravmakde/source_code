SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_costvarianceledger_kafka_to_teradata_10976_tech_nap_merch;
Task_Name=cost_variance_ledger_03_read_from_kafka_write_to_s3_03;'
FOR SESSION VOLATILE;

--Source
--Reading Data from Source Kafka Topic Name=inventory-stockledger-analytical-avro using SQL API CODE
create temporary view temp_costvarianceledger_object_model as select * from kafka_costvarianceledger_table;

-- Flatten the cost variance ledger object array 
create temporary view costvarianceledger_object_model_exploded as
select
      structure.locationId as location_id
     ,element_at(headers,'LastUpdatedTime') as last_updated_time_in_millis
     ,structure.productSku.id as sku_id
     ,structure.transactionDate as transaction_date
     ,explode(entries) as costvarianceledgerentry
from temp_costvarianceledger_object_model;

--Sink
--Writing Kafka Data to S3 in csv format for TPT load
insert into table costvarianceledger partition (year, month, day )
select
      sbqy.LOCATION_ID
     ,sbqy.LAST_UPDATED_TIME_IN_MILLIS
     ,sbqy.SKU_ID
     ,cast(sbqy.TRAN_DATE as string)
     ,sbqy.EVENT_ID
     ,cast(sbqy.EVENT_TIME as string)
     ,sbqy.CORRELATION_ID
     ,sbqy.EVENT_TYPE
     ,sbqy.REFERENCE_ID
     ,cast(sbqy.TRAN_CODE as string)
     ,cast(sbqy.QUANTITY as string)
     ,sbqy.DEPT_NUM
     ,sbqy.CLASS_NUM
     ,sbqy.SUBCLASS_NUM
     ,sbqy.SKU_TYPE
     ,sbqy.TOTAL_COST_CURRCYCD
     ,cast(sbqy.TOTAL_COST_UNITS as string)
     ,cast(sbqy.TOTAL_COST_NANOS as string)
     ,sbqy.TOTAL_RETAIL_CURRCYCD
     ,cast(sbqy.TOTAL_RETAIL_UNITS as string)
     ,cast(sbqy.TOTAL_RETAIL_NANOS as string)
     ,cast(sbqy.DW_SYS_LOAD_TMSTP as string)
     ,year( current_date()) as year
     ,month( current_date()) as month
     ,day( current_date()) as day

from (
       select
             location_id as LOCATION_ID
            ,last_updated_time_in_millis as LAST_UPDATED_TIME_IN_MILLIS
            ,sku_id as SKU_ID
            ,transaction_date as TRAN_DATE
            ,costvarianceledgerentry.id as EVENT_ID
            ,costvarianceledgerentry.lastUpdateTime as EVENT_TIME
            ,costvarianceledgerentry.correlationId as CORRELATION_ID
            ,costvarianceledgerentry.ledgerReference.referenceType as EVENT_TYPE
            ,costvarianceledgerentry.ledgerReference.referenceId as REFERENCE_ID
            ,costvarianceledgerentry.ledgerAttributes.transactionCode as TRAN_CODE
            ,costvarianceledgerentry.ledgerAttributes.quantity as QUANTITY
            ,costvarianceledgerentry.ledgerAttributes.productHierarchy.departmentNumber as DEPT_NUM
            ,costvarianceledgerentry.ledgerAttributes.productHierarchy.classNumber as CLASS_NUM
            ,costvarianceledgerentry.ledgerAttributes.productHierarchy.subClassNumber as SUBCLASS_NUM
            ,costvarianceledgerentry.ledgerAttributes.product.idType as SKU_TYPE
            ,costvarianceledgerentry.ledgerAttributes.totalCost.currencyCode as TOTAL_COST_CURRCYCD
            ,costvarianceledgerentry.ledgerAttributes.totalCost.units as TOTAL_COST_UNITS
            ,costvarianceledgerentry.ledgerAttributes.totalCost.nanos as TOTAL_COST_NANOS
            ,costvarianceledgerentry.ledgerAttributes.totalRetail.currencyCode as TOTAL_RETAIL_CURRCYCD
            ,costvarianceledgerentry.ledgerAttributes.totalRetail.units as TOTAL_RETAIL_UNITS
            ,costvarianceledgerentry.ledgerAttributes.totalRetail.nanos as TOTAL_RETAIL_NANOS
            ,current_timestamp as DW_SYS_LOAD_TMSTP
            ,row_number() over (partition by location_id, transaction_date, sku_id, costvarianceledgerentry.id order by last_updated_time_in_millis desc) as RN
       from costvarianceledger_object_model_exploded
     ) as sbqy
where sbqy.RN == 1;