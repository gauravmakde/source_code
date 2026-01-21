SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_rtvcostadjustmentledger_kafka_to_teradata;
Task_Name=read_from_kafka_write_to_s3_03;'
FOR SESSION VOLATILE;

--Source
--Reading Data from Source Kafka Topic Name=inventory-return-to-vendor-cost-adjustment-ledger-analytical-avro using SQL API CODE
create temporary view temp_rtvcostadjustmentledger_object_model as select * from kafka_rtvcostadjustmentledger_object_model_avro;

-- Flatten the cost variance ledger object array 
create temporary view kafka_rtvcostadjustmentledger_object_model_avro_extract_array as 
select 
      key.returnToVendorNumber as rtv_num,
      element_at(headers,'LastUpdatedTime') as last_updated_time_in_millis,
      explode(returnToVendorCostAdjustmentLedgerEntries) as returnToVendorCostAdjustmentLedgerEntry  
from temp_rtvcostadjustmentledger_object_model;

--Sink
--Writing Kafka Data to S3 in csv format for TPT load
insert into table rtvcostadjustmentledger 
select  RTV_NUM,
 EVENT_ID,
 CAST(EVENT_TIME AS STRING),
 LAST_UPDATED_TIME_IN_MILLIS,
 CORRELATION_ID,
 REFERENCE_ID,
 LOCATION_ID,
 SKU_ID,
 SKU_TYPE,
 DEPT_NUM,
 CLASS_NUM,
 SUBCLASS_NUM,
 CAST(TRAN_DATE AS STRING),
 CAST(TRAN_CODE AS STRING),
 CAST(QUANTITY AS STRING),
 TOTAL_COST_CURRCYCD,
 CAST(TOTAL_COST_UNITS AS STRING),
 CAST(TOTAL_COST_NANOS AS STRING),
 TOTAL_RETAIL_CURRCYCD,
 CAST(TOTAL_RETAIL_UNITS AS STRING),
 CAST(TOTAL_RETAIL_NANOS AS STRING),
 CAST(DW_SYS_LOAD_TMSTP AS STRING)
from (
      select 
            rtv_num,
            returnToVendorCostAdjustmentLedgerEntry.id as event_id,
            returnToVendorCostAdjustmentLedgerEntry.eventTime as event_time,
            last_updated_time_in_millis,
            returnToVendorCostAdjustmentLedgerEntry.correlationId as correlation_id,
            returnToVendorCostAdjustmentLedgerEntry.generalLedgerReferenceNumber as reference_id,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.locationId as location_id,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.product.id as sku_id,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.product.idType as sku_type,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.productHierarchy.departmentNumber as dept_num,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.productHierarchy.classNumber as class_num,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.productHierarchy.subClassNumber as subclass_num,      
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.transactionDate as tran_date,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.transactionCode as tran_code,     
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.quantity as quantity,    
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.totalCost.currencyCode as total_cost_currcycd,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.totalCost.units as total_cost_units,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.totalCost.nanos as total_cost_nanos,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.totalRetail.currencyCode as total_retail_currcycd,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.totalRetail.units as total_retail_units,
            returnToVendorCostAdjustmentLedgerEntry.costAdjustmentLedgerAttributes.totalRetail.nanos as total_retail_nanos,
            current_timestamp as dw_sys_load_tmstp,
            row_number() over (partition by rtv_num, returnToVendorCostAdjustmentLedgerEntry.id order by last_updated_time_in_millis desc) as rn
      from kafka_rtvcostadjustmentledger_object_model_avro_extract_array
      ) as t
where t.rn == 1;
