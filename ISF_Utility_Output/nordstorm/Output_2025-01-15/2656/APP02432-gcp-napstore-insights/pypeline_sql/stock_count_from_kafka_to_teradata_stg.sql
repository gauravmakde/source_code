--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : stock_count_from_kafka_to_teradata_stg.sql
-- Description             : Reading Data from Source Kafka Topic and writing to STOCK_COUNT_STG table
-- Data Source             : Order Object model kafka topic "stock-count-analytical-avro"
--*************************************************************************************************************************************

create
temporary view stock_count_input AS
select *
from kafka_stock_count_input;

-- Writing Kafka Data to S3 Path
-- insert into table stock_count_s3_orc_output partition(year, month, day)
-- select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
-- from stock_count_input;

-- Writing Kafka to Semantic Layer:

-- STOCK_COUNT_STG
insert
overwrite table stock_count_stg_table
select stockCountId as stock_count_id,
       createdAt as created_at,
       lastUpdatedAt as last_updated_at,
       timestamp_millis(cast(element_at(headers, 'CreationTime') as BIGINT)) as kafka_timestamp,
       createdTransactionId as created_transaction_id,
       stockCountName as stock_count_name,
       stockCountType as stock_count_type,
       locationId as location_id,
       sourceSystem as source_system,
       externalStockCountId as external_stock_count_id,
       externalSystem as external_system,
       stockCountScheduledTime as stock_count_scheduled_time,
       string(createdByUser) as created_by_user,
       submitManifestedAt as submit_manifested_at,
       submitManifestedTransactionId as submit_manifested_transaction_id,
       stockCountStartedTime as stock_count_started_time,
       stockCountSubmittedTime as stock_count_submitted_time,
       concat_ws(',', submittedTransactionIds) as submitted_transaction_ids,
       canceledAt as canceled_at,
       canceledTransactionId as canceled_transaction_id,
       string(canceledByUser) as canceled_by_user,
       expiredAt as expired_at,
       expiredTransactionId as expired_transaction_id
from stock_count_input;


-- STOCK_COUNT_CRITERIA_STG
create temporary view stock_count_criteria_input as
select stockCountId as stockCountId, timestamp_millis(cast(element_at(headers, 'CreationTime') as BIGINT)) as kafkaTimestamp, explode(stockCountCriteria) as stockCountCriteria
from stock_count_input;

create temporary view stock_count_criteria_ids as
select stockCountId as stockCountId, kafkaTimestamp as kafkaTimestamp,
        stockCountCriteria.criterionType as criterionType,
        explode(stockCountCriteria.criterionIds) as stockCountCriteriaIds
from stock_count_criteria_input;

create temporary view stock_count_criteria_class_input as
select stockCountId as stockCountId, kafkaTimestamp as kafkaTimestamp,
        stockCountCriteria.criterionType as criterionType,
        explode(stockCountCriteria.classCriteria) as stockCountCriteriaClassCriteria
from stock_count_criteria_input;

create temporary view stock_count_criteria_class_final as
select stockCountId as stockCountId, kafkaTimestamp as kafkaTimestamp,
        criterionType as criterionType,
        stockCountCriteriaClassCriteria.departmentNumber as departmentNumber,
        explode(stockCountCriteriaClassCriteria.classNumbers) as classNumbers
from stock_count_criteria_class_input;

create temporary view stock_count_criteria_subclass_input as
select stockCountId as stockCountId, kafkaTimestamp as kafkaTimestamp,
        stockCountCriteria.criterionType as criterionType,
        explode(stockCountCriteria.subClassCriteria) as stockCountCriteriaSubclassCriteria
from stock_count_criteria_input;

create temporary view stock_count_criteria_subclass_final as
select stockCountId as stockCountId, kafkaTimestamp as kafkaTimestamp,
        criterionType as criterionType,
        stockCountCriteriaSubclassCriteria.departmentNumber as departmentNumber,
        stockCountCriteriaSubclassCriteria.classNumber as classNumber,
        explode(stockCountCriteriaSubclassCriteria.subClassNumbers) as subClassNumbers
from stock_count_criteria_subclass_input;

create temporary view stock_count_criteria_items_input as
select stockCountId as stockCountId, kafkaTimestamp as kafkaTimestamp,
        stockCountCriteria.criterionType as criterionType,
        explode(stockCountCriteria.items) as stockCountCriteriaItems
from stock_count_criteria_input;

-- create temporary view stock_count_criteria_items_final as
-- select  kafkaTimestamp as kafkaTimestamp,
--        criterionType as criterionType,
--        stockCountCriteriaItems.id as item_id,
--        stockCountCriteriaItems.idType as item_id_type,
-- from stock_count_criteria_items_input;

insert
overwrite table stock_count_criteria_stg_table
select stockCountId as stock_count_id,
       kafkaTimestamp as kafka_timestamp,
       stockCountCriteria.criterionType as criterion_type,
       explode(stockCountCriteria.criterionIds) as criterion_id,
       null as department_number,
       null as class_number,
       null as subclass_number,
       null as item_id_type,
       null as item_id
from stock_count_criteria_input
union
select stockCountId as stock_count_id,
       kafkaTimestamp as kafka_timestamp,
       criterionType as criterion_type,
       null as criterion_id,
       stockCountCriteriaClassCriteria.departmentNumber as department_number,
       explode(stockCountCriteriaClassCriteria.classNumbers) as class_number,
       null as subclass_number,
       null as item_id_type,
       null as item_id
from stock_count_criteria_class_input
union
select stockCountId as stock_count_id,
       kafkaTimestamp as kafka_timestamp,
       criterionType as criterion_type,
       null as criterion_id,
       stockCountCriteriaSubclassCriteria.departmentNumber as department_number,
       stockCountCriteriaSubclassCriteria.classNumber as class_number,
       explode(stockCountCriteriaSubclassCriteria.subClassNumbers) as subclass_number,
       null as item_id_type,
       null as item_id
from stock_count_criteria_subclass_input
union
select stockCountId as stock_count_id,
       kafkaTimestamp as kafka_timestamp,
       criterionType as criterion_type,
       null as criterion_id,
       null as department_number,
       null as class_number,
       null as subclass_number,
       stockCountCriteriaItems.idType as item_id_type,
       stockCountCriteriaItems.id as item_id
from stock_count_criteria_items_input;


-- STOCK_COUNT_SUBMITTED_DETAILS_STG
create temporary view stock_count_submitted_details_input as
select stockCountId as stockCountId, timestamp_millis(cast(element_at(headers, 'CreationTime') as BIGINT)) as kafkaTimestamp, explode(stockCountSubmittedDetails) as stockCountSubmittedDetails
from stock_count_input;

insert
overwrite table stock_count_submitted_details_stg_table
select stockCountId as stock_count_id,
       kafkaTimestamp as kafka_timestamp,
       stockCountSubmittedDetails.submittedAt as submitted_at,
       stockCountSubmittedDetails.transactionId as submitted_transaction_id,
       stockCountSubmittedDetails.sourceSystem as source_system
from stock_count_submitted_details_input;

-- STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_STG
create temporary view stock_count_submitted_details_product_result_input as
select stockCountId as stockCountId,
        kafkaTimestamp as kafkaTimestamp,
        stockCountSubmittedDetails.transactionId as submittedDetailsTransactionId,
        explode(stockCountSubmittedDetails.stockCountProductResults) as stockCountProductResults
from stock_count_submitted_details_input;

insert
overwrite table stock_count_submitted_product_result_stg_table
select CONCAT(submittedDetailsTransactionId, '-', stockCountProductResults.expectedItemQuantity.item.id) as id,
       kafkaTimestamp as kafka_timestamp,
       stockCountId as stock_count_id,
       submittedDetailsTransactionId as submitted_details_transaction_id,
       stockCountProductResults.expectedItemQuantity.item.id as expected_item_id,
       stockCountProductResults.expectedItemQuantity.item.idType as expected_item_id_type,
       stockCountProductResults.expectedItemQuantity.quantity as expected_item_quantity,
       concat_ws(',',stockCountProductResults.expectedItemQuantity.epcs) as expected_item_epcs,
       stockCountProductResults.isInRFIDProgram as is_in_rfid_program,
       stockCountProductResults.userId.id as user_id,
       stockCountProductResults.userId.idType as user_id_type
from stock_count_submitted_details_product_result_input;

-- STOCK_COUNT_SUBMITTED_PRODUCT_RESULT_COUNTED_ITEM_QUANTITY_STG
create temporary view stock_count_submitted_details_product_result_counted_item_quantity_input as
select stockCountId as stockCountId, kafkaTimestamp as kafkaTimestamp,
        CONCAT(submittedDetailsTransactionId, '-', stockCountProductResults.expectedItemQuantity.item.id) as stockCountSubmittedProductResultId,
        explode(stockCountProductResults.countedItemQuantities) as countedItemQuantities
from stock_count_submitted_details_product_result_input;

insert
overwrite table stock_count_submitted_product_result_counted_item_quantity_stg_table
select CONCAT(stockCountSubmittedProductResultId, '-', countedItemQuantities.item.id) as id,
       stockCountId as stock_count_id,
       kafkaTimestamp as kafka_timestamp,
       stockCountSubmittedProductResultId as stock_count_product_result_id,
       countedItemQuantities.item.id as counted_item_id,
       countedItemQuantities.item.idType as counted_item_id_type,
       countedItemQuantities.quantity as counted_item_quantity,
       concat_ws(',', countedItemQuantities.epcs) as counted_item_epcs
from stock_count_submitted_details_product_result_counted_item_quantity_input;

