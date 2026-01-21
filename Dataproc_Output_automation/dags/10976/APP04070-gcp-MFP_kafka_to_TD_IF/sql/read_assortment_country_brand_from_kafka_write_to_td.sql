SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_assortment_country_brand_kafka_to_teradata;
Task_Name=read_from_kafka_write_to_s3_v2;'
FOR SESSION VOLATILE;

-- Source
-- Reading Data from AO Kafka Topic Name = inventory-assortment-category-country-brand-level-plan-analytical-avro
create temporary view temp_assortment_category_country_brand_object_model as select * from kafka_assortment_country_brand_object_model_avro;

-- Flatten categoryCountryBrandInventoryPlanDetails array
create temporary view assortment_category_country_brand_exploded as
select
    element_at(headers,'LastUpdatedTime') as LAST_UPDATED_TIME_IN_MILLIS,
    lastUpdatedTime as EVENT_TIME,
    planPublishedDate as PLAN_PUBLISHED_DATE,
    sellingCountry as SELLING_COUNTRY,
    sellingBrand as SELLING_BRAND,
    category as CATEGORY,
    priceBand as PRICE_BAND,
    departmentNumber as DEPT_NUM,
    concat(CAST(monthDimension.year as string), ' ', LEFT(monthDimension.month, 3)) as MONTH_LABEL,
    explode(categoryCountryBrandInventoryPlanDetails) as categoryCountryBrandDetails
from
    temp_assortment_category_country_brand_object_model
;

-- AO deduplication, if the same AO is published more than one time within one batch processing
create temporary view assortment_category_country_brand_RN as
select
    LAST_UPDATED_TIME_IN_MILLIS as LAST_UPDATED_TIME_IN_MILLIS,
    PLAN_PUBLISHED_DATE as PLAN_PUBLISHED_DATE,
    EVENT_TIME as EVENT_TIME,
    SELLING_COUNTRY as SELLING_COUNTRY,
    SELLING_BRAND as SELLING_BRAND,
    CATEGORY as CATEGORY,
    PRICE_BAND as PRICE_BAND,
    DEPT_NUM as DEPT_NUM,
    MONTH_LABEL as MONTH_LABEL,
    categoryCountryBrandDetails.alternateInventoryModel,
    categoryCountryBrandDetails,
    row_number() over (partition by PLAN_PUBLISHED_DATE, SELLING_COUNTRY, SELLING_BRAND, CATEGORY, PRICE_BAND, DEPT_NUM, MONTH_LABEL,
                                    categoryCountryBrandDetails.alternateInventoryModel
                           order by LAST_UPDATED_TIME_IN_MILLIS desc) as RN
from
    assortment_category_country_brand_exploded
;

-- another view to extract all required elements for terradata table
create temporary view assortment_category_country_brand_exploded_columns as
select
    LAST_UPDATED_TIME_IN_MILLIS,
    PLAN_PUBLISHED_DATE,
    EVENT_TIME,
    SELLING_COUNTRY,
    SELLING_BRAND,
    CATEGORY,
    PRICE_BAND,
    DEPT_NUM,
    MONTH_LABEL,
    categoryCountryBrandDetails.alternateinventorymodel as ALTERNATE_INVENTORY_MODEL,
    -- averageInventory
    categoryCountryBrandDetails.averageinventory.units as AVERAGE_INVENTORY_UNITS,
    categoryCountryBrandDetails.averageinventory.retail.currencycode as AVERAGE_INVENTORY_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.averageinventory.retail.units as AVERAGE_INVENTORY_RETAIL_UNITS,
    categoryCountryBrandDetails.averageinventory.retail.nanos as AVERAGE_INVENTORY_RETAIL_NANOS,
    categoryCountryBrandDetails.averageinventory.cost.currencycode as AVERAGE_INVENTORY_COST_CURRCYCD,
    categoryCountryBrandDetails.averageinventory.cost.units as AVERAGE_INVENTORY_COST_UNITS,
    categoryCountryBrandDetails.averageinventory.cost.nanos as AVERAGE_INVENTORY_COST_NANOS,
    -- beginningOfPeriodInventory
    categoryCountryBrandDetails.beginningofperiodinventory.units as BEGINNING_OF_PERIOD_INVENTORY_UNITS,
    categoryCountryBrandDetails.beginningofperiodinventory.retail.currencycode as BEGINNING_OF_PERIOD_INVENTORY_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.beginningofperiodinventory.retail.units as BEGINNING_OF_PERIOD_INVENTORY_RETAIL_UNITS,
    categoryCountryBrandDetails.beginningofperiodinventory.retail.nanos as BEGINNING_OF_PERIOD_INVENTORY_RETAIL_NANOS,
    categoryCountryBrandDetails.beginningofperiodinventory.cost.currencycode as BEGINNING_OF_PERIOD_INVENTORY_COST_CURRCYCD,
    categoryCountryBrandDetails.beginningofperiodinventory.cost.units as BEGINNING_OF_PERIOD_INVENTORY_COST_UNITS,
    categoryCountryBrandDetails.beginningofperiodinventory.cost.nanos as BEGINNING_OF_PERIOD_INVENTORY_COST_NANOS,
    -- returnToVendor
    categoryCountryBrandDetails.returntovendor.units as RETURN_TO_VENDOR_UNITS,
    categoryCountryBrandDetails.returntovendor.retail.currencycode as RETURN_TO_VENDOR_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.returntovendor.retail.units as RETURN_TO_VENDOR_RETAIL_UNITS,
    categoryCountryBrandDetails.returntovendor.retail.nanos as RETURN_TO_VENDOR_RETAIL_NANOS,
    categoryCountryBrandDetails.returntovendor.cost.currencycode as RETURN_TO_VENDOR_COST_CURRCYCD,
    categoryCountryBrandDetails.returntovendor.cost.units as RETURN_TO_VENDOR_COST_UNITS,
    categoryCountryBrandDetails.returntovendor.cost.nanos as RETURN_TO_VENDOR_COST_NANOS,
    -- rackTransfer
    categoryCountryBrandDetails.racktransfer.units as RACK_TRANSFER_UNITS,
    categoryCountryBrandDetails.racktransfer.retail.currencycode as RACK_TRANSFER_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.racktransfer.retail.units as RACK_TRANSFER_RETAIL_UNITS,
    categoryCountryBrandDetails.racktransfer.retail.nanos as RACK_TRANSFER_RETAIL_NANOS,
    categoryCountryBrandDetails.racktransfer.cost.currencycode as RACK_TRANSFER_COST_CURRCYCD,
    categoryCountryBrandDetails.racktransfer.cost.units as RACK_TRANSFER_COST_UNITS,
    categoryCountryBrandDetails.racktransfer.cost.nanos as RACK_TRANSFER_COST_NANOS,
    -- activeInventoryIn
    categoryCountryBrandDetails.activeinventoryin.units as ACTIVE_INVENTORY_IN_UNITS,
    categoryCountryBrandDetails.activeinventoryin.retail.currencycode as ACTIVE_INVENTORY_IN_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.activeinventoryin.retail.units as ACTIVE_INVENTORY_IN_RETAIL_UNITS,
    categoryCountryBrandDetails.activeinventoryin.retail.nanos as ACTIVE_INVENTORY_IN_RETAIL_NANOS,
    categoryCountryBrandDetails.activeinventoryin.cost.currencycode as ACTIVE_INVENTORY_IN_COST_CURRCYCD,
    categoryCountryBrandDetails.activeinventoryin.cost.units as ACTIVE_INVENTORY_IN_COST_UNITS,
    categoryCountryBrandDetails.activeinventoryin.cost.nanos as ACTIVE_INVENTORY_IN_COST_NANOS,
    -- activeInventoryOut
    categoryCountryBrandDetails.activeinventoryout.units as ACTIVE_INVENTORY_OUT_UNITS,
    categoryCountryBrandDetails.activeinventoryout.retail.currencycode as ACTIVE_INVENTORY_OUT_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.activeinventoryout.retail.units as ACTIVE_INVENTORY_OUT_RETAIL_UNITS,
    categoryCountryBrandDetails.activeinventoryout.retail.nanos as ACTIVE_INVENTORY_OUT_RETAIL_NANOS,
    categoryCountryBrandDetails.activeinventoryout.cost.currencycode as ACTIVE_INVENTORY_OUT_COST_CURRCYCD,
    categoryCountryBrandDetails.activeinventoryout.cost.units as ACTIVE_INVENTORY_OUT_COST_UNITS,
    categoryCountryBrandDetails.activeinventoryout.cost.nanos as ACTIVE_INVENTORY_OUT_COST_NANOS,
    -- receipts
    categoryCountryBrandDetails.receipts.units as RECEIPTS_UNITS,
    categoryCountryBrandDetails.receipts.retail.currencycode as RECEIPTS_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.receipts.retail.units as RECEIPTS_RETAIL_UNITS,
    categoryCountryBrandDetails.receipts.retail.nanos as RECEIPTS_RETAIL_NANOS,
    categoryCountryBrandDetails.receipts.cost.currencycode as RECEIPTS_COST_CURRCYCD,
    categoryCountryBrandDetails.receipts.cost.units as RECEIPTS_COST_UNITS,
    categoryCountryBrandDetails.receipts.cost.nanos as RECEIPTS_COST_NANOS,
    -- receiptLessReserve
    categoryCountryBrandDetails.receiptlessreserve.units as RECEIPT_LESS_RESERVE_UNITS,
    categoryCountryBrandDetails.receiptlessreserve.retail.currencycode as RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.receiptlessreserve.retail.units as RECEIPT_LESS_RESERVE_RETAIL_UNITS,
    categoryCountryBrandDetails.receiptlessreserve.retail.nanos as RECEIPT_LESS_RESERVE_RETAIL_NANOS,
    categoryCountryBrandDetails.receiptlessreserve.cost.currencycode as RECEIPT_LESS_RESERVE_COST_CURRCYCD,
    categoryCountryBrandDetails.receiptlessreserve.cost.units as RECEIPT_LESS_RESERVE_COST_UNITS,
    categoryCountryBrandDetails.receiptlessreserve.cost.nanos as RECEIPT_LESS_RESERVE_COST_NANOS,
    -- packAndHoldTransferIn
    categoryCountryBrandDetails.packandholdtransferin.units as PAH_TRANSFER_IN_UNITS,
    categoryCountryBrandDetails.packandholdtransferin.retail.currencycode as PAH_TRANSFER_IN_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.packandholdtransferin.retail.units as PAH_TRANSFER_IN_RETAIL_UNITS,
    categoryCountryBrandDetails.packandholdtransferin.retail.nanos as PAH_TRANSFER_IN_RETAIL_NANOS,
    categoryCountryBrandDetails.packandholdtransferin.cost.currencycode as PAH_TRANSFER_IN_COST_CURRCYCD,
    categoryCountryBrandDetails.packandholdtransferin.cost.units as PAH_TRANSFER_IN_COST_UNITS,
    categoryCountryBrandDetails.packandholdtransferin.cost.nanos as PAH_TRANSFER_IN_COST_NANOS,
    -- shrink
    categoryCountryBrandDetails.shrink.units as SHRINK_UNITS,
    categoryCountryBrandDetails.shrink.retail.currencycode as SHRINK_RETAIL_CURRCYCD,
    categoryCountryBrandDetails.shrink.retail.units as SHRINK_RETAIL_UNITS,
    categoryCountryBrandDetails.shrink.retail.nanos as SHRINK_RETAIL_NANOS,
    categoryCountryBrandDetails.shrink.cost.currencycode as SHRINK_COST_CURRCYCD,
    categoryCountryBrandDetails.shrink.cost.units as SHRINK_COST_UNITS,
    categoryCountryBrandDetails.shrink.cost.nanos as SHRINK_COST_NANOS
from assortment_category_country_brand_RN
where RN == 1
;

-- Sink
-- Writing Data to S3 in csv format for TPT load
---insert overwrite table assortment_country_brand
insert into table assortment_country_brand 
select
    LAST_UPDATED_TIME_IN_MILLIS,
    CAST(PLAN_PUBLISHED_DATE AS STRING),
    CAST(EVENT_TIME AS STRING),
    SELLING_COUNTRY,
    SELLING_BRAND,
    CATEGORY,
    PRICE_BAND,
    DEPT_NUM,
    MONTH_LABEL,
    ALTERNATE_INVENTORY_MODEL,
    -- averageInventory
    CAST(AVERAGE_INVENTORY_UNITS AS STRING),
    AVERAGE_INVENTORY_RETAIL_CURRCYCD,
    CAST(AVERAGE_INVENTORY_RETAIL_UNITS AS STRING),
    CAST(AVERAGE_INVENTORY_RETAIL_NANOS AS STRING),
    AVERAGE_INVENTORY_COST_CURRCYCD,
    CAST(AVERAGE_INVENTORY_COST_UNITS AS STRING),
    CAST(AVERAGE_INVENTORY_COST_NANOS AS STRING),
    -- beginningOfPeriodInventory
    CAST(BEGINNING_OF_PERIOD_INVENTORY_UNITS AS STRING),
    BEGINNING_OF_PERIOD_INVENTORY_RETAIL_CURRCYCD,
    CAST(BEGINNING_OF_PERIOD_INVENTORY_RETAIL_UNITS AS STRING),
    CAST(BEGINNING_OF_PERIOD_INVENTORY_RETAIL_NANOS AS STRING),
    BEGINNING_OF_PERIOD_INVENTORY_COST_CURRCYCD,
    CAST(BEGINNING_OF_PERIOD_INVENTORY_COST_UNITS AS STRING),
    CAST(BEGINNING_OF_PERIOD_INVENTORY_COST_NANOS AS STRING),
    -- returnToVendor
    CAST(RETURN_TO_VENDOR_UNITS AS STRING),
    RETURN_TO_VENDOR_RETAIL_CURRCYCD,
    CAST(RETURN_TO_VENDOR_RETAIL_UNITS AS STRING),
    CAST(RETURN_TO_VENDOR_RETAIL_NANOS AS STRING),
    RETURN_TO_VENDOR_COST_CURRCYCD,
    CAST(RETURN_TO_VENDOR_COST_UNITS AS STRING),
    CAST(RETURN_TO_VENDOR_COST_NANOS AS STRING),
    -- rackTransfer
    CAST(RACK_TRANSFER_UNITS AS STRING),
    RACK_TRANSFER_RETAIL_CURRCYCD,
    CAST(RACK_TRANSFER_RETAIL_UNITS AS STRING),
    CAST(RACK_TRANSFER_RETAIL_NANOS AS STRING),
    RACK_TRANSFER_COST_CURRCYCD,
    CAST(RACK_TRANSFER_COST_UNITS AS STRING),
    CAST(RACK_TRANSFER_COST_NANOS AS STRING),
    -- activeInventoryIn
    CAST(ACTIVE_INVENTORY_IN_UNITS AS STRING),
    ACTIVE_INVENTORY_IN_RETAIL_CURRCYCD,
    CAST(ACTIVE_INVENTORY_IN_RETAIL_UNITS AS STRING),
    CAST(ACTIVE_INVENTORY_IN_RETAIL_NANOS AS STRING),
    ACTIVE_INVENTORY_IN_COST_CURRCYCD,
    CAST(ACTIVE_INVENTORY_IN_COST_UNITS AS STRING),
    CAST(ACTIVE_INVENTORY_IN_COST_NANOS AS STRING),
    -- activeInventoryOut
    CAST(ACTIVE_INVENTORY_OUT_UNITS AS STRING),
    ACTIVE_INVENTORY_OUT_RETAIL_CURRCYCD,
    CAST(ACTIVE_INVENTORY_OUT_RETAIL_UNITS AS STRING),
    CAST(ACTIVE_INVENTORY_OUT_RETAIL_NANOS AS STRING),
    ACTIVE_INVENTORY_OUT_COST_CURRCYCD,
    CAST(ACTIVE_INVENTORY_OUT_COST_UNITS AS STRING),
    CAST(ACTIVE_INVENTORY_OUT_COST_NANOS AS STRING),
    -- receipts
    CAST(RECEIPTS_UNITS AS STRING),
    RECEIPTS_RETAIL_CURRCYCD,
    CAST(RECEIPTS_RETAIL_UNITS AS STRING),
    CAST(RECEIPTS_RETAIL_NANOS AS STRING),
    RECEIPTS_COST_CURRCYCD,
    CAST(RECEIPTS_COST_UNITS AS STRING),
    CAST(RECEIPTS_COST_NANOS AS STRING),
    -- receiptLessReserve
    CAST(RECEIPT_LESS_RESERVE_UNITS AS STRING),
    RECEIPT_LESS_RESERVE_RETAIL_CURRCYCD,
    CAST(RECEIPT_LESS_RESERVE_RETAIL_UNITS AS STRING),
    CAST(RECEIPT_LESS_RESERVE_RETAIL_NANOS AS STRING),
    RECEIPT_LESS_RESERVE_COST_CURRCYCD,
    CAST(RECEIPT_LESS_RESERVE_COST_UNITS AS STRING),
    CAST(RECEIPT_LESS_RESERVE_COST_NANOS AS STRING),
    -- packAndHoldTransferIn
    CAST(PAH_TRANSFER_IN_UNITS AS STRING),
    PAH_TRANSFER_IN_RETAIL_CURRCYCD,
    CAST(PAH_TRANSFER_IN_RETAIL_UNITS AS STRING),
    CAST(PAH_TRANSFER_IN_RETAIL_NANOS AS STRING),
    PAH_TRANSFER_IN_COST_CURRCYCD,
    CAST(PAH_TRANSFER_IN_COST_UNITS AS STRING),
    CAST(PAH_TRANSFER_IN_COST_NANOS AS STRING),
    -- shrink
    CAST(SHRINK_UNITS AS STRING),
    SHRINK_RETAIL_CURRCYCD,
    CAST(SHRINK_RETAIL_UNITS AS STRING),
    CAST(SHRINK_RETAIL_NANOS AS STRING),
    SHRINK_COST_CURRCYCD,
    CAST(SHRINK_COST_UNITS AS STRING),
    CAST(SHRINK_COST_NANOS AS STRING)
from assortment_category_country_brand_exploded_columns
;
