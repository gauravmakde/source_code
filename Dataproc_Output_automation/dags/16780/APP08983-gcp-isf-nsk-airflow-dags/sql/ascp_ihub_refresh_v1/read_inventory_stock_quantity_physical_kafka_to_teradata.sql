--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_inventory_stock_quantity_physical_kafka_to_teradata.sql
-- Author            : Bharat Mahajan
-- Description       : Reading Data from Source Kafka Topic Name=ascp-inventory-stock-quantity-eis-object-model-avro using SQL API CODE
-- Source topic      : ascp-inventory-stock-quantity-eis-object-model-avro
-- Object model      : InventoryStockQuantity
-- ETL Run Frequency : Every day
-- Version :         : 0.1
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-10-02  Mahajan Bharat   FA-9933: New ISF DAG for EIS Physical Inventory Flow with new added bucket fields
-- 2023-11-10  Andrew Ivchuk    FA-10494: migration from 10908 to 15850
-- 2024-03-01  Chaichenko Oleksandr       FA-11683: Migrate IHUB_REFRESH to Airflow NSK
--*************************************************************************************************************************************

create temporary view inventory_stock_quantity_eis_rn AS
select
	valueUpdatedTime,
    rmsSkuId,
    locationId,
    immediatelySellableQty,
    stockOnHandQty,
    unavailableQty,
    custOrderReturn,
    preInspectCustomerReturn,
    unusableQty,
    rtvReqstResrv,
    metReqstResrv,
    tcPreview,
    tcClubhouse,
    tcMini1,
    tcMini2,
    tcMini3,
    problem,
    damagedReturn,
    damagedCosmeticReturn,
    ertmHolds,
    ndUnavailableQty,
    pbHoldsQty,
    comCOHolds,
    wmHolds,
    storeReserve,
    tcHolds,
    returnsHolds,
    fpHolds,
    transfersReserveQty,
    returnToVendorQty,
    inTransitQty,
    storeTransferReservedQty,
    storeTransferExpectedQty,
    backOrderReserveQty,
    omsBackOrderReserveQty,
    onReplenishment,
    lastReceivedDate,
    locationType,
    epmId,
    upc,
    returnInspectionQty,
    receivingQty,
    damagedQty,
    holdQty,
    offsiteEvents,
    alterationsAndRepairs,
    studioServices,
    designerApproval,
    singleShoe,
    pickable_hold,
    pickable_repairable_soh,
    pickable_repairable,
    pickable_used_soh,
    pickable_used,
    headers.SystemTime,
    row_number() OVER (partition by rmsSkuId, locationId, locationType order by headers.SystemTime desc) rn
from inventory_stock_quantity_physical_avro;

create temporary view inventory_stock_quantity_eis_cnt AS
select
    count(*) as total_cnt
from inventory_stock_quantity_physical_avro;

create temporary view inventory_stock_quantity_eis as
select
	valueUpdatedTime,
    rmsSkuId,
    locationId,
    immediatelySellableQty,
    stockOnHandQty,
    unavailableQty,
    custOrderReturn,
    preInspectCustomerReturn,
    unusableQty,
    rtvReqstResrv,
    metReqstResrv,
    tcPreview,
    tcClubhouse,
    tcMini1,
    tcMini2,
    tcMini3,
    problem,
    damagedReturn,
    damagedCosmeticReturn,
    ertmHolds,
    ndUnavailableQty,
    pbHoldsQty,
    comCOHolds,
    wmHolds,
    storeReserve,
    tcHolds,
    returnsHolds,
    fpHolds,
    transfersReserveQty,
    returnToVendorQty,
    inTransitQty,
    storeTransferReservedQty,
    storeTransferExpectedQty,
    backOrderReserveQty,
    omsBackOrderReserveQty,
    onReplenishment,
    lastReceivedDate,
    locationType,
    epmId,
    upc,
    returnInspectionQty,
    receivingQty,
    damagedQty,
    holdQty,
    offsiteEvents,
    alterationsAndRepairs,
    studioServices,
    designerApproval,
    singleShoe,
    pickable_hold,
    pickable_repairable_soh,
    pickable_repairable,
    pickable_used_soh,
    pickable_used
from inventory_stock_quantity_eis_rn
where rn = 1;

---Writing Error Data to S3 in csv format
insert overwrite table inventory_stock_quantity_physical_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	valueUpdatedTime,
    rmsSkuId,
    locationId,
    immediatelySellableQty,
    stockOnHandQty,
    unavailableQty,
    custOrderReturn,
    preInspectCustomerReturn,
    unusableQty,
    rtvReqstResrv,
    metReqstResrv,
    tcPreview,
    tcClubhouse,
    tcMini1,
    tcMini2,
    tcMini3,
    problem,
    damagedReturn,
    damagedCosmeticReturn,
    ertmHolds,
    ndUnavailableQty,
    pbHoldsQty,
    comCOHolds,
    wmHolds,
    storeReserve,
    tcHolds,
    returnsHolds,
    fpHolds,
    transfersReserveQty,
    returnToVendorQty,
    inTransitQty,
    storeTransferReservedQty,
    storeTransferExpectedQty,
    backOrderReserveQty,
    omsBackOrderReserveQty,
    onReplenishment,
    lastReceivedDate,
    locationType,
    epmId,
    upc,
    returnInspectionQty,
    receivingQty,
    damagedQty,
    holdQty,
    offsiteEvents,
    alterationsAndRepairs,
    studioServices,
    designerApproval,
    singleShoe,
    pickable_hold,
    pickable_repairable_soh,
    pickable_repairable,
    pickable_used_soh,
    pickable_used,
    substr(current_date,1,4) as year,
    substr(current_date,6,2) as month,
    substr(current_date,9,2) as day,
    substr(current_timestamp,12,2) as hour
from inventory_stock_quantity_eis
where
	valueUpdatedTime is null
    or rmsSkuId is null
    or locationId is null
    or rmsSkuId = ''
    or locationType = ''
    or locationId = ''
    or rmsSkuId = '""'
    or locationId = '""'
    or locationType = '""'
    or rmsSkuId = 'null'
    or locationId = 'null'
    or locationType = 'null';


---Writing Kafka Data to Teradata staging tables
insert overwrite table inventory_stock_quantity_physical_ldg_table_csv
select
	valueUpdatedTime,
    rmsSkuId,
    locationId,
    immediatelySellableQty,
    stockOnHandQty,
    unavailableQty,
    custOrderReturn,
    preInspectCustomerReturn,
    unusableQty,
    rtvReqstResrv,
    metReqstResrv,
    tcPreview,
    tcClubhouse,
    tcMini1,
    tcMini2,
    tcMini3,
    problem,
    damagedReturn,
    damagedCosmeticReturn,
    ertmHolds,
    ndUnavailableQty,
    pbHoldsQty,
    comCOHolds,
    wmHolds,
    storeReserve,
    tcHolds,
    returnsHolds,
    fpHolds,
    transfersReserveQty,
    returnToVendorQty,
    inTransitQty,
    storeTransferReservedQty,
    storeTransferExpectedQty,
    backOrderReserveQty,
    omsBackOrderReserveQty,
    onReplenishment,
    lastReceivedDate,
    locationType,
    epmId,
    upc,
    returnInspectionQty,
    receivingQty,
    damagedQty,
    holdQty,
    offsiteEvents,
    alterationsAndRepairs,
    studioServices,
    designerApproval,
    singleShoe,
    pickable_hold,
    pickable_repairable_soh,
    pickable_repairable,
    pickable_used_soh,
    pickable_used
from inventory_stock_quantity_eis
where
	valueUpdatedTime is not null
    and rmsSkuId <> ''
    and locationId <> ''
    and rmsSkuId <> '""'
    and locationId <> '""'
    and rmsSkuId <> 'null'
    and locationId <> 'null'
    and ((locationType <> 'null'
    and locationType <> ''
    and locationType <> '""')
    or locationType is null);

insert into table inventory_stock_quantity_physical_spark_log
select
    isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	cast(metric_value as STRING) as metric_value,
	cast(metric_tmstp as STRING) as metric_tmstp
from (
       select
          '{dag_name}' as isf_dag_nm,
	      'Loading to csv' as step_nm,
	      'INVENTORY_STOCK_QUANTITY_PHYSICAL_LDG' as tbl_nm,
	      'PROCESSED_ROWS_CNT' as metric_nm,
	      count(*) as metric_value,
		  current_timestamp() as metric_tmstp
       from inventory_stock_quantity_eis
	   where
	      valueUpdatedTime is not null
          and rmsSkuId <> ''
          and locationId <> ''
          and rmsSkuId <> '""'
          and locationId <> '""'
          and rmsSkuId <> 'null'
          and locationId <> 'null'
          and((locationType <> 'null'
          and locationType <> ''
          and locationType <> '""')
          or locationType is null)
	   union all
	   select
	      '{dag_name}' as isf_dag_nm,
	      'Reading from kafka' as step_nm,
	      cast (null as string) as tbl_nm,
	      'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
	      total_cnt as metric_value,
	      current_timestamp() as metric_tmstp
	   from inventory_stock_quantity_eis_cnt
	 ) a
;
