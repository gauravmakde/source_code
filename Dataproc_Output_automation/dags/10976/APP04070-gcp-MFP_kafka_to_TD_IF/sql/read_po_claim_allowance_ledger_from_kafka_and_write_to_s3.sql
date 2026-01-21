SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_poclaimallowanceledger_kafka_to_teradata;
Task_Name=read_from_kafka_write_to_s3_04;'
FOR SESSION VOLATILE;

--Source
--Reading Data from Source Kafka Topic Name=inventory-purchase-order-claim-allowance-ledger-analytical-avro using SQL API CODE
create temporary view temp_poclaimallowanceledger_object_model as select * from kafka_poclaimallowanceledger_object_model_avro;

-- Flatten the cost variance ledger object array 
create temporary view kafka_poclaimallowanceledger_object_model_avro_extract_array as 
select 
      invoiceNumber as invoice_num,
      purchaseOrderNumber as po_num,
      element_at(headers,'LastUpdatedTime') as last_updated_time_in_millis,
      explode(purchaseOrderClaimAllowanceLedgerEntries) as purchaseOrderClaimAllowanceLedgerEntry  
from temp_poclaimallowanceledger_object_model;

--Sink
--Writing Kafka Data to S3 in csv format for TPT load
--insert overwrite table poclaimallowanceledger
insert into table poclaimallowanceledger
select po_num,
      invoice_num, 
      event_id,
      event_time,
      last_updated_time_in_millis,
      reference_id,
      location_id,
      sku_id,
      sku_type,
      dept_num,
      class_num,
      subclass_num,      
      tran_date,
      tran_code,     
      quantity,    
      total_cost_currcycd,
      total_cost_units,
      total_cost_nanos,
      total_retail_currcycd,
      total_retail_units,
      total_retail_nanos,
      dw_sys_load_tmstp
from (
      select 
      po_num,
      invoice_num, 
      purchaseOrderClaimAllowanceLedgerEntry.id as event_id,
      CAST(purchaseOrderClaimAllowanceLedgerEntry.eventTime AS STRING) as event_time,
      last_updated_time_in_millis,
      purchaseOrderClaimAllowanceLedgerEntry.generalLedgerReferenceNumber as reference_id,
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.locationId as location_id,
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.product.id as sku_id,
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.product.idType as sku_type,
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.productHierarchy.departmentNumber as dept_num,
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.productHierarchy.classNumber as class_num,
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.productHierarchy.subClassNumber as subclass_num,      
      CAST(purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.transactionDate AS STRING) as tran_date,
      CAST(purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.transactionCode AS STRING) as tran_code,     
      CAST(purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.quantity AS STRING) as quantity,    
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.totalCost.currencyCode as total_cost_currcycd,
      CAST(purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.totalCost.units AS STRING) as total_cost_units,
      CAST(purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.totalCost.nanos AS STRING) as total_cost_nanos,
      purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.totalRetail.currencyCode as total_retail_currcycd,
      CAST(purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.totalRetail.units AS STRING) as total_retail_units,
      CAST(purchaseOrderClaimAllowanceLedgerEntry.claimAllowanceLedgerAttributes.totalRetail.nanos AS STRING) as total_retail_nanos,
      CAST(current_timestamp AS STRING) as dw_sys_load_tmstp,
      row_number() over (partition by po_num, invoice_num, purchaseOrderClaimAllowanceLedgerEntry.id order by last_updated_time_in_millis desc) as rn
      from kafka_poclaimallowanceledger_object_model_avro_extract_array ) as t
where rn == 1;
