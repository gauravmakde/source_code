SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=assortment_supplier_group_cluster_plan_kafka_to_teradata_10976_tech_nap_merch;
Task_Name=read_from_kafka_write_to_s3;'
FOR SESSION VOLATILE;

--Source
--Reading Data from Source Kafka Topic Name=inventory-supplier-group-cluster-level-plan-analytical-avro using SQL API CODE
create temporary view temp_assortmentsupplier_object_model as select * from kafka_assortment_supplier_group_cluster_plan_table;

-- Flatten supplierGroupPlanDetails s array
create temporary view assortmentsupplier_object_model_exploded as
select
      lastEventTime as EVENT_TIME,
      planPublishedDate as PLAN_PUBLISHED_DATE,
      id as SUPP_GROUP_ID,
      country as COUNTRY,
      cluster as CLUSTER_NAME,
      category as CATEGORY,
      concat(CAST(monthDimension.year as string), ' ', LEFT(monthDimension.month, 3)) as MONTH_LABEL,
      element_at(headers,'LastUpdatedTime') as LAST_UPDATED_TIME_IN_MILLIS,
      explode(supplierGroupPlanDetails) as supplierGroupPlanDetail
 from
    temp_assortmentsupplier_object_model
;

-- AO deduplication, if the same AO is published more than one time within one batch processing
create temporary view assortmentsupplier_object_model_RN as
select
      EVENT_TIME as EVENT_TIME,
      PLAN_PUBLISHED_DATE as PLAN_PUBLISHED_DATE,
      SUPP_GROUP_ID as SUPP_GROUP_ID,
      COUNTRY as COUNTRY,
      CLUSTER_NAME as CLUSTER_NAME,
      CATEGORY as CATEGORY,
      MONTH_LABEL as MONTH_LABEL,
      supplierGroupPlanDetail.alternateInventoryModel,
      supplierGroupPlanDetail,
      LAST_UPDATED_TIME_IN_MILLIS as LAST_UPDATED_TIME_IN_MILLIS,
      row_number() over (PARTITION BY PLAN_PUBLISHED_DATE, SUPP_GROUP_ID, COUNTRY, CLUSTER_NAME, CATEGORY, MONTH_LABEL,
                                      supplierGroupPlanDetail.alternateInventoryModel
                         ORDER BY LAST_UPDATED_TIME_IN_MILLIS DESC) as RN
 from
    assortmentsupplier_object_model_exploded
;

-- another view to extract all required elements for terradata table
create temporary view assortmentsupplier_object_model_exploded_columns as
select
      EVENT_TIME as EVENT_TIME,
      PLAN_PUBLISHED_DATE as PLAN_PUBLISHED_DATE,
      SUPP_GROUP_ID as SUPP_GROUP_ID,
      COUNTRY as COUNTRY,
      CLUSTER_NAME as CLUSTER_NAME,
      CATEGORY as CATEGORY,
      MONTH_LABEL as MONTH_LABEL,
      LAST_UPDATED_TIME_IN_MILLIS as LAST_UPDATED_TIME_IN_MILLIS,
      supplierGroupPlanDetail.alternateInventoryModel as ALTERNATE_INVENTORY_MODEL,
      supplierGroupPlanDetail.replenishmentReceipt.units as REPLENISHMENT_RECEIPT_UNITS,
      supplierGroupPlanDetail.replenishmentReceipt.retail.currencyCode as REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.replenishmentReceipt.retail.units as REPLENISHMENT_RECEIPT_RETAIL_UNITS,
      supplierGroupPlanDetail.replenishmentReceipt.retail.nanos as REPLENISHMENT_RECEIPT_RETAIL_NANOS,
      supplierGroupPlanDetail.replenishmentReceipt.cost.currencyCode as REPLENISHMENT_RECEIPT_COST_CURRCYCD,
      supplierGroupPlanDetail.replenishmentReceipt.cost.units as REPLENISHMENT_RECEIPT_COST_UNITS,
      supplierGroupPlanDetail.replenishmentReceipt.cost.nanos as REPLENISHMENT_RECEIPT_COST_NANOS,
      supplierGroupPlanDetail.replenishmentReceiptLessReserve.units as REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS,
      supplierGroupPlanDetail.replenishmentReceiptLessReserve.retail.currencyCode as REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.replenishmentReceiptLessReserve.retail.units as REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_UNITS,
      supplierGroupPlanDetail.replenishmentReceiptLessReserve.retail.nanos as REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_NANOS,
      supplierGroupPlanDetail.replenishmentReceiptLessReserve.cost.currencyCode as REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD,
      supplierGroupPlanDetail.replenishmentReceiptLessReserve.cost.units as REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_UNITS,
      supplierGroupPlanDetail.replenishmentReceiptLessReserve.cost.nanos as REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_NANOS,
      supplierGroupPlanDetail.nonReplenishmentReceipt.units as NON_REPLENISHMENT_RECEIPT_UNITS,
      supplierGroupPlanDetail.nonReplenishmentReceipt.retail.currencyCode as NON_REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.nonReplenishmentReceipt.retail.units as NON_REPLENISHMENT_RECEIPT_RETAIL_UNITS,
      supplierGroupPlanDetail.nonReplenishmentReceipt.retail.nanos as NON_REPLENISHMENT_RECEIPT_RETAIL_NANOS,
      supplierGroupPlanDetail.nonReplenishmentReceipt.cost.currencyCode as NON_REPLENISHMENT_RECEIPT_COST_CURRCYCD,
      supplierGroupPlanDetail.nonReplenishmentReceipt.cost.units as NON_REPLENISHMENT_RECEIPT_COST_UNITS,
      supplierGroupPlanDetail.nonReplenishmentReceipt.cost.nanos as NON_REPLENISHMENT_RECEIPT_COST_NANOS,
      supplierGroupPlanDetail.nonReplenishmentReceiptLessReserve.units as NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS,
      supplierGroupPlanDetail.nonReplenishmentReceiptLessReserve.retail.currencyCode as NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.nonReplenishmentReceiptLessReserve.retail.units as NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_UNITS,
      supplierGroupPlanDetail.nonReplenishmentReceiptLessReserve.retail.nanos as NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_NANOS,
      supplierGroupPlanDetail.nonReplenishmentReceiptLessReserve.cost.currencyCode as NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD,
      supplierGroupPlanDetail.nonReplenishmentReceiptLessReserve.cost.units as NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_UNITS,
      supplierGroupPlanDetail.nonReplenishmentReceiptLessReserve.cost.nanos as NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_NANOS,
      supplierGroupPlanDetail.dropshipReceipt.units as DROP_SHIP_RECEIPT_UNITS,
      supplierGroupPlanDetail.dropshipReceipt.retail.currencyCode as DROP_SHIP_RECEIPT_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.dropshipReceipt.retail.units as DROP_SHIP_RECEIPT_RETAIL_UNITS,
      supplierGroupPlanDetail.dropshipReceipt.retail.nanos as DROP_SHIP_RECEIPT_RETAIL_NANOS,
      supplierGroupPlanDetail.dropshipReceipt.cost.currencyCode as DROP_SHIP_RECEIPT_COST_CURRCYCD,
      supplierGroupPlanDetail.dropshipReceipt.cost.units as DROP_SHIP_RECEIPT_COST_UNITS,
      supplierGroupPlanDetail.dropshipReceipt.cost.nanos as DROP_SHIP_RECEIPT_COST_NANOS,
      supplierGroupPlanDetail.averageInventory.units as AVERAGE_INVENTORY_UNITS,
      supplierGroupPlanDetail.averageInventory.retail.currencyCode as AVERAGE_INVENTORY_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.averageInventory.retail.units as AVERAGE_INVENTORY_RETAIL_UNITS,
      supplierGroupPlanDetail.averageInventory.retail.nanos as AVERAGE_INVENTORY_RETAIL_NANOS,
      supplierGroupPlanDetail.averageInventory.cost.currencyCode as AVERAGE_INVENTORY_COST_CURRCYCD,
      supplierGroupPlanDetail.averageInventory.cost.units as AVERAGE_INVENTORY_COST_UNITS,
      supplierGroupPlanDetail.averageInventory.cost.nanos as AVERAGE_INVENTORY_COST_NANOS,
      supplierGroupPlanDetail.beginningOfPeriodInventory.units as BEGINNING_OF_PERIOD_INVENTORY_UNITS,
      supplierGroupPlanDetail.beginningOfPeriodInventory.retail.currencyCode as BEGINNING_OF_PERIOD_INVENTORY_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.beginningOfPeriodInventory.retail.units as BEGINNING_OF_PERIOD_INVENTORY_RETAIL_UNITS,
      supplierGroupPlanDetail.beginningOfPeriodInventory.retail.nanos as BEGINNING_OF_PERIOD_INVENTORY_RETAIL_NANOS,
      supplierGroupPlanDetail.beginningOfPeriodInventory.cost.currencyCode as BEGINNING_OF_PERIOD_INVENTORY_COST_CURRCYCD,
      supplierGroupPlanDetail.beginningOfPeriodInventory.cost.units as BEGINNING_OF_PERIOD_INVENTORY_COST_UNITS,
      supplierGroupPlanDetail.beginningOfPeriodInventory.cost.nanos as BEGINNING_OF_PERIOD_INVENTORY_COST_NANOS,
      supplierGroupPlanDetail.beginningOfPeriodInventoryTarget.units as BEGINNING_OF_PERIOD_INVENTORY_TARGET_UNITS,
      supplierGroupPlanDetail.beginningOfPeriodInventoryTarget.retail.currencyCode as BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.beginningOfPeriodInventoryTarget.retail.units as BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_UNITS,
      supplierGroupPlanDetail.beginningOfPeriodInventoryTarget.retail.nanos as BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_NANOS,
      supplierGroupPlanDetail.beginningOfPeriodInventoryTarget.cost.currencyCode as BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_CURRCYCD,
      supplierGroupPlanDetail.beginningOfPeriodInventoryTarget.cost.units as BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_UNITS,
      supplierGroupPlanDetail.beginningOfPeriodInventoryTarget.cost.nanos as BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_NANOS,
      supplierGroupPlanDetail.returnToVendor.units as RETURN_TO_VENDOR_UNITS,
      supplierGroupPlanDetail.returnToVendor.retail.currencyCode as RETURN_TO_VENDOR_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.returnToVendor.retail.units as RETURN_TO_VENDOR_RETAIL_UNITS,
      supplierGroupPlanDetail.returnToVendor.retail.nanos as RETURN_TO_VENDOR_RETAIL_NANOS,
      supplierGroupPlanDetail.returnToVendor.cost.currencyCode as RETURN_TO_VENDOR_COST_CURRCYCD,
      supplierGroupPlanDetail.returnToVendor.cost.units as RETURN_TO_VENDOR_COST_UNITS,
      supplierGroupPlanDetail.returnToVendor.cost.nanos as RETURN_TO_VENDOR_COST_NANOS,
      supplierGroupPlanDetail.rackTransfer.units as RACK_TRANSFER_UNITS,
      supplierGroupPlanDetail.rackTransfer.retail.currencyCode as RACK_TRANSFER_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.rackTransfer.retail.units as RACK_TRANSFER_RETAIL_UNITS,
      supplierGroupPlanDetail.rackTransfer.retail.nanos as RACK_TRANSFER_RETAIL_NANOS,
      supplierGroupPlanDetail.rackTransfer.cost.currencyCode as RACK_TRANSFER_COST_CURRCYCD,
      supplierGroupPlanDetail.rackTransfer.cost.units as RACK_TRANSFER_COST_UNITS,
      supplierGroupPlanDetail.rackTransfer.cost.nanos as RACK_TRANSFER_COST_NANOS,
      supplierGroupPlanDetail.activeInventoryIn.units as ACTIVE_INVENTORY_IN_UNITS,
      supplierGroupPlanDetail.activeInventoryIn.retail.currencyCode as ACTIVE_INVENTORY_IN_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.activeInventoryIn.retail.units as ACTIVE_INVENTORY_IN_RETAIL_UNITS,
      supplierGroupPlanDetail.activeInventoryIn.retail.nanos as ACTIVE_INVENTORY_IN_RETAIL_NANOS,
      supplierGroupPlanDetail.activeInventoryIn.cost.currencyCode as ACTIVE_INVENTORY_IN_COST_CURRCYCD,
      supplierGroupPlanDetail.activeInventoryIn.cost.units as ACTIVE_INVENTORY_IN_COST_UNITS,
      supplierGroupPlanDetail.activeInventoryIn.cost.nanos as ACTIVE_INVENTORY_IN_COST_NANOS,
      supplierGroupPlanDetail.plannableInventory.units as PLANNABLE_INVENTORY_UNITS,
      supplierGroupPlanDetail.plannableInventory.retail.currencyCode as PLANNABLE_INVENTORY_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.plannableInventory.retail.units as PLANNABLE_INVENTORY_RETAIL_UNITS,
      supplierGroupPlanDetail.plannableInventory.retail.nanos as PLANNABLE_INVENTORY_RETAIL_NANOS,
      supplierGroupPlanDetail.plannableInventory.cost.currencyCode as PLANNABLE_INVENTORY_COST_CURRCYCD,
      supplierGroupPlanDetail.plannableInventory.cost.units as PLANNABLE_INVENTORY_COST_UNITS,
      supplierGroupPlanDetail.plannableInventory.cost.nanos as PLANNABLE_INVENTORY_COST_NANOS,
      supplierGroupPlanDetail.plannableInventoryReceiptLessReserve.units as PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_UNITS,
      supplierGroupPlanDetail.plannableInventoryReceiptLessReserve.retail.currencyCode as PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.plannableInventoryReceiptLessReserve.retail.units as PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_UNITS,
      supplierGroupPlanDetail.plannableInventoryReceiptLessReserve.retail.nanos as PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_NANOS,
      supplierGroupPlanDetail.plannableInventoryReceiptLessReserve.cost.currencyCode as PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_CURRCYCD,
      supplierGroupPlanDetail.plannableInventoryReceiptLessReserve.cost.units as PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_UNITS,
      supplierGroupPlanDetail.plannableInventoryReceiptLessReserve.cost.nanos as PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_NANOS,
      supplierGroupPlanDetail.shrink.units as SHRINK_UNITS,
      supplierGroupPlanDetail.shrink.retail.currencyCode as SHRINK_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.shrink.retail.units as SHRINK_RETAIL_UNITS,
      supplierGroupPlanDetail.shrink.retail.nanos as SHRINK_RETAIL_NANOS,
      supplierGroupPlanDetail.shrink.cost.currencyCode as SHRINK_COST_CURRCYCD,
      supplierGroupPlanDetail.shrink.cost.units as SHRINK_COST_UNITS,
      supplierGroupPlanDetail.shrink.cost.nanos as SHRINK_COST_NANOS,
      supplierGroupPlanDetail.netSales.units as NET_SALES_UNITS,
      supplierGroupPlanDetail.netSales.retail.currencyCode as NET_SALES_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.netSales.retail.units as NET_SALES_RETAIL_UNITS,
      supplierGroupPlanDetail.netSales.retail.nanos as NET_SALES_RETAIL_NANOS,
      supplierGroupPlanDetail.netSales.cost.currencyCode as NET_SALES_COST_CURRCYCD,
      supplierGroupPlanDetail.netSales.cost.units as NET_SALES_COST_UNITS,
      supplierGroupPlanDetail.netSales.cost.nanos as NET_SALES_COST_NANOS,
      supplierGroupPlanDetail.demand.units as DEMAND_UNITS,
      supplierGroupPlanDetail.demand.dollars.currencyCode as DEMAND_DOLLAR_CURRCYCD,
      supplierGroupPlanDetail.demand.dollars.units as DEMAND_DOLLAR_UNITS,
      supplierGroupPlanDetail.demand.dollars.nanos as DEMAND_DOLLAR_NANOS,
      supplierGroupPlanDetail.grossSales.units as GROSS_SALES_UNITS,
      supplierGroupPlanDetail.grossSales.dollars.currencyCode as GROSS_SALES_DOLLAR_CURRCYCD,
      supplierGroupPlanDetail.grossSales.dollars.units as GROSS_SALES_DOLLAR_UNITS,
      supplierGroupPlanDetail.grossSales.dollars.nanos as GROSS_SALES_DOLLAR_NANOS,
      supplierGroupPlanDetail.returns.units as RETURNS_UNITS,
      supplierGroupPlanDetail.returns.dollars.currencyCode as RETURNS_DOLLAR_CURRCYCD,
      supplierGroupPlanDetail.returns.dollars.units as RETURNS_DOLLAR_UNITS,
      supplierGroupPlanDetail.returns.dollars.nanos as RETURNS_DOLLAR_NANOS,
      supplierGroupPlanDetail.productMarginRetail.currencyCode as PRODUCT_MARGIN_RETAIL_CURRCYCD,
      supplierGroupPlanDetail.productMarginRetail.units as PRODUCT_MARGIN_RETAIL_UNITS,
      supplierGroupPlanDetail.productMarginRetail.nanos as PRODUCT_MARGIN_RETAIL_NANOS,
      supplierGroupPlanDetail.demandNextTwoMonthRunRate as DEMAND_NEXT_TWO_MONTH_RUN_RATE,
      supplierGroupPlanDetail.salesNextTwoMonthRunRate as SALES_NEXT_TWO_MONTH_RUN_RATE
from assortmentsupplier_object_model_RN
where RN == 1
;

-- writting to s3

insert into table assortmentsuppliergroupclusterplan
SELECT
CAST(EVENT_TIME AS STRING)
 ,CAST(PLAN_PUBLISHED_DATE AS STRING)
 ,SUPP_GROUP_ID 
 ,COUNTRY 
 ,CLUSTER_NAME 
 ,CATEGORY 
 ,MONTH_LABEL 
 ,LAST_UPDATED_TIME_IN_MILLIS 
 ,ALTERNATE_INVENTORY_MODEL 
 ,CAST(REPLENISHMENT_RECEIPT_UNITS AS STRING)
 ,REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD 
 ,CAST(REPLENISHMENT_RECEIPT_RETAIL_UNITS AS STRING)
 ,CAST(REPLENISHMENT_RECEIPT_RETAIL_NANOS AS STRING)
 ,REPLENISHMENT_RECEIPT_COST_CURRCYCD 
 ,CAST(REPLENISHMENT_RECEIPT_COST_UNITS AS STRING)
 ,CAST(REPLENISHMENT_RECEIPT_COST_NANOS AS STRING)
 ,CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS AS STRING)
 ,REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD 
 ,CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_UNITS AS STRING)
 ,CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_NANOS AS STRING)
 ,REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD 
 ,CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_UNITS AS STRING)
 ,CAST(REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_NANOS AS STRING)
 ,CAST(NON_REPLENISHMENT_RECEIPT_UNITS AS STRING)
 ,NON_REPLENISHMENT_RECEIPT_RETAIL_CURRCYCD 
 ,CAST(NON_REPLENISHMENT_RECEIPT_RETAIL_UNITS AS STRING)
 ,CAST(NON_REPLENISHMENT_RECEIPT_RETAIL_NANOS AS STRING)
 ,NON_REPLENISHMENT_RECEIPT_COST_CURRCYCD 
 ,CAST(NON_REPLENISHMENT_RECEIPT_COST_UNITS AS STRING)
 ,CAST(NON_REPLENISHMENT_RECEIPT_COST_NANOS AS STRING)
 ,CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_UNITS AS STRING)
 ,NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD 
 ,CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_UNITS AS STRING)
 ,CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_RETAIL_NANOS AS STRING)
 ,NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_CURRCYCD 
 ,CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_UNITS AS STRING)
 ,CAST(NON_REPLENISHMENT_RECEIPT_LESS_RESERVE_COST_NANOS AS STRING)
 ,CAST(DROP_SHIP_RECEIPT_UNITS AS STRING)
 ,DROP_SHIP_RECEIPT_RETAIL_CURRCYCD 
 ,CAST(DROP_SHIP_RECEIPT_RETAIL_UNITS AS STRING)
 ,CAST(DROP_SHIP_RECEIPT_RETAIL_NANOS AS STRING)
 ,DROP_SHIP_RECEIPT_COST_CURRCYCD 
 ,CAST(DROP_SHIP_RECEIPT_COST_UNITS AS STRING)
 ,CAST(DROP_SHIP_RECEIPT_COST_NANOS AS STRING)
 ,CAST(AVERAGE_INVENTORY_UNITS AS STRING)
 ,AVERAGE_INVENTORY_RETAIL_CURRCYCD 
 ,CAST(AVERAGE_INVENTORY_RETAIL_UNITS AS STRING)
 ,CAST(AVERAGE_INVENTORY_RETAIL_NANOS AS STRING)
 ,AVERAGE_INVENTORY_COST_CURRCYCD 
 ,CAST(AVERAGE_INVENTORY_COST_UNITS AS STRING)
 ,CAST(AVERAGE_INVENTORY_COST_NANOS AS STRING)
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_UNITS AS STRING)
 ,BEGINNING_OF_PERIOD_INVENTORY_RETAIL_CURRCYCD 
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_RETAIL_UNITS AS STRING)
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_RETAIL_NANOS AS STRING)
 ,BEGINNING_OF_PERIOD_INVENTORY_COST_CURRCYCD 
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_COST_UNITS AS STRING)
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_COST_NANOS AS STRING)
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_UNITS AS STRING)
 ,BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_CURRCYCD 
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_UNITS AS STRING)
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_RETAIL_NANOS AS STRING)
 ,BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_CURRCYCD 
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_UNITS AS STRING)
 ,CAST(BEGINNING_OF_PERIOD_INVENTORY_TARGET_COST_NANOS AS STRING)
 ,CAST(RETURN_TO_VENDOR_UNITS AS STRING)
 ,RETURN_TO_VENDOR_RETAIL_CURRCYCD 
 ,CAST(RETURN_TO_VENDOR_RETAIL_UNITS AS STRING)
 ,CAST(RETURN_TO_VENDOR_RETAIL_NANOS AS STRING)
 ,RETURN_TO_VENDOR_COST_CURRCYCD 
 ,CAST(RETURN_TO_VENDOR_COST_UNITS AS STRING)
 ,CAST(RETURN_TO_VENDOR_COST_NANOS AS STRING)
 ,CAST(RACK_TRANSFER_UNITS AS STRING)
 ,RACK_TRANSFER_RETAIL_CURRCYCD 
 ,CAST(RACK_TRANSFER_RETAIL_UNITS AS STRING)
 ,CAST(RACK_TRANSFER_RETAIL_NANOS AS STRING)
 ,RACK_TRANSFER_COST_CURRCYCD 
 ,CAST(RACK_TRANSFER_COST_UNITS AS STRING)
 ,CAST(RACK_TRANSFER_COST_NANOS AS STRING)
 ,CAST(ACTIVE_INVENTORY_IN_UNITS AS STRING)
 ,ACTIVE_INVENTORY_IN_RETAIL_CURRCYCD 
 ,CAST(ACTIVE_INVENTORY_IN_RETAIL_UNITS AS STRING)
 ,CAST(ACTIVE_INVENTORY_IN_RETAIL_NANOS AS STRING)
 ,ACTIVE_INVENTORY_IN_COST_CURRCYCD 
 ,CAST(ACTIVE_INVENTORY_IN_COST_UNITS AS STRING)
 ,CAST(ACTIVE_INVENTORY_IN_COST_NANOS AS STRING)
 ,CAST(PLANNABLE_INVENTORY_UNITS AS STRING)
 ,PLANNABLE_INVENTORY_RETAIL_CURRCYCD 
 ,CAST(PLANNABLE_INVENTORY_RETAIL_UNITS AS STRING)
 ,CAST(PLANNABLE_INVENTORY_RETAIL_NANOS AS STRING)
 ,PLANNABLE_INVENTORY_COST_CURRCYCD 
 ,CAST(PLANNABLE_INVENTORY_COST_UNITS AS STRING)
 ,CAST(PLANNABLE_INVENTORY_COST_NANOS AS STRING)
 ,CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_UNITS AS STRING)
 ,PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD 
 ,CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_UNITS AS STRING)
 ,CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_RETAIL_NANOS AS STRING)
 ,PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_CURRCYCD 
 ,CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_UNITS AS STRING)
 ,CAST(PLANNABLE_INVENTORY_RECEIPT_LESS_RESERVE_COST_NANOS AS STRING)
 ,CAST(SHRINK_UNITS AS STRING)
 ,SHRINK_RETAIL_CURRCYCD 
 ,CAST(SHRINK_RETAIL_UNITS AS STRING)
 ,CAST(SHRINK_RETAIL_NANOS AS STRING)
 ,SHRINK_COST_CURRCYCD 
 ,CAST(SHRINK_COST_UNITS AS STRING)
 ,CAST(SHRINK_COST_NANOS AS STRING)
 ,CAST(NET_SALES_UNITS AS STRING)
 ,NET_SALES_RETAIL_CURRCYCD 
 ,CAST(NET_SALES_RETAIL_UNITS AS STRING)
 ,CAST(NET_SALES_RETAIL_NANOS AS STRING)
 ,NET_SALES_COST_CURRCYCD 
 ,CAST(NET_SALES_COST_UNITS AS STRING)
 ,CAST(NET_SALES_COST_NANOS AS STRING)
 ,CAST(DEMAND_UNITS AS STRING)
 ,DEMAND_DOLLAR_CURRCYCD 
 ,CAST(DEMAND_DOLLAR_UNITS AS STRING)
 ,CAST(DEMAND_DOLLAR_NANOS AS STRING)
 ,CAST(GROSS_SALES_UNITS AS STRING)
 ,GROSS_SALES_DOLLAR_CURRCYCD 
 ,CAST(GROSS_SALES_DOLLAR_UNITS AS STRING)
 ,CAST(GROSS_SALES_DOLLAR_NANOS AS STRING)
 ,CAST(RETURNS_UNITS AS STRING)
 ,RETURNS_DOLLAR_CURRCYCD 
 ,CAST(RETURNS_DOLLAR_UNITS AS STRING)
 ,CAST(RETURNS_DOLLAR_NANOS AS STRING)
 ,PRODUCT_MARGIN_RETAIL_CURRCYCD 
 ,CAST(PRODUCT_MARGIN_RETAIL_UNITS AS STRING)
 ,CAST(PRODUCT_MARGIN_RETAIL_NANOS AS STRING)
 ,CAST(DEMAND_NEXT_TWO_MONTH_RUN_RATE AS STRING)
 ,CAST(SALES_NEXT_TWO_MONTH_RUN_RATE AS STRING)
FROM assortmentsupplier_object_model_exploded_columns
;
