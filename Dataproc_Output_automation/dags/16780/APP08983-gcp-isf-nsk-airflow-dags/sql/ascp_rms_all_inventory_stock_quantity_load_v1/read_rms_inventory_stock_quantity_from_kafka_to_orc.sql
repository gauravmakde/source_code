--Source
--Reading Data from  Source Kafka Topic Name=ascp-rms-all-locations-inventory-event-avro using SQL API CODE
create temporary view rms_all_locations_inventory_rn AS select
to_timestamp(from_unixtime(unix_timestamp(headers.ValueUpdatedTime,"yyyy-MM-dd'T'HH:mm:ssXXX"), "yyyy-MM-dd HH:mm:ss.SSS"), "yyyy-MM-dd HH:mm:ss.SSS") as valueUpdatedTime, rmsSkuId, locationId, immediatelySellableQty, stockOnHandQty, unavailableQty, custOrderReturn,
preInspectCustomerReturn, unusableQty, rtvReqstResrv, metReqstResrv, tcPreview, tcClubhouse, tcMini1, tcMini2,
tcMini3, problem, damagedReturn, damagedCosmeticReturn, ertmHolds, ndUnavailableQty, pbHoldsQty, comCOHolds, wmHolds,
storeReserve, tcHolds, returnsHolds, fpHolds, transfersReserveQty, returnToVendorQty, inTransitQty, storeTransferReservedQty,
storeTransferExpectedQty, backOrderReserveQty, omsBackOrderReserveQty, onReplenishment, lastReceivedDate, locationType,
epmId, upc, returnInspectionQty, receivingQty, damagedQty, holdQty, rmsStockOnHandUpdateDateTime, rmsLastUpdateDateTime, csn, opSeqNo, `position` as positionNo,
row_number() over (partition by rmsSkuId, locationId order by cast(csn as long) desc, cast(opSeqNo as int) desc, cast(`position` as long) desc) as rn
from rms_all_locations_inventory_base;

create temporary view rms_inventory_event AS select
valueUpdatedTime, rmsSkuId, locationId, immediatelySellableQty, stockOnHandQty, unavailableQty, custOrderReturn,
preInspectCustomerReturn, unusableQty, rtvReqstResrv, metReqstResrv, tcPreview, tcClubhouse, tcMini1, tcMini2,
tcMini3, problem, damagedReturn, damagedCosmeticReturn, ertmHolds, ndUnavailableQty, pbHoldsQty, comCOHolds, wmHolds,
storeReserve, tcHolds, returnsHolds, fpHolds, transfersReserveQty, returnToVendorQty, inTransitQty, storeTransferReservedQty,
storeTransferExpectedQty, backOrderReserveQty, omsBackOrderReserveQty, onReplenishment, lastReceivedDate, locationType,
epmId, upc, returnInspectionQty, receivingQty, damagedQty, holdQty, rmsStockOnHandUpdateDateTime, rmsLastUpdateDateTime, csn, opSeqNo, positionNo
from rms_all_locations_inventory_rn
where rn = 1;

create temporary view rms_inventory_event_final as
select
date_format("valueUpdatedTime", "yyyy-MM-dd HH:mm:ss") as value_updated_time,
    rmsSkuId as rms_sku_id,
    locationId as location_id,
    immediatelySellableQty as immediately_sellable_qty,
    stockOnHandQty as stock_on_hand_qty,
    unavailableQty as unavailable_qty,
    custOrderReturn as cust_order_return,
    preInspectCustomerReturn as pre_inspect_customer_return,
    unusableQty as unusable_qty,
    rtvReqstResrv as rtv_reqst_resrv,
    metReqstResrv as met_reqst_resrv,
    tcPreview as tc_preview,
    tcClubhouse as tc_clubhouse,
    tcMini1 as tc_mini_1,
    tcMini2 as tc_mini_2,
    tcMini3 as tc_mini_3,
    problem as problem,
    damagedReturn as damaged_return,
    damagedCosmeticReturn as damaged_cosmetic_return,
    ertmHolds as ertm_holds,
    ndUnavailableQty as nd_unavailable_qty,
    pbHoldsQty as pb_holds_qty,
    comCOHolds as com_co_holds,
    wmHolds as wm_holds,
    storeReserve as store_reserve,
    tcHolds as tc_holds,
    returnsHolds as returns_holds,
    fpHolds as fp_holds,
    transfersReserveQty as transfers_reserve_qty,
    returnToVendorQty as return_to_vendor_qty,
    inTransitQty as in_transit_qty,
    storeTransferReservedQty as store_transfer_reserved_qty,
    storeTransferExpectedQty as store_transfer_expected_qty,
    backOrderReserveQty as back_order_reserve_qty,
    omsBackOrderReserveQty as oms_back_order_reserve_qty,
    onReplenishment as on_replenishment,
    lastReceivedDate as last_received_date,
    locationType as location_type,
    epmId as epm_id,
    upc as upc,
    returnInspectionQty as return_inspection_qty,
    receivingQty as receiving_qty,
    damagedQty as damage_qty,
    holdQty as hold_qty,
    rmsStockOnHandUpdateDateTime as rms_stockonhand_update_datetime,
    rmsLastUpdateDateTime as rms_last_update_datetime,
    csn as csn,
    opSeqNo as opseqno,
    positionNo as positionNo
from rms_inventory_event;

--Sink
---Writing Kafka Data to S3 in ORC format
create temp view rms_inventory_stock_quantity_batch_control_vw as select * from rms_inventory_stock_quantity_batch_control;

insert into rms_all_inventory_stock_quantity_orc_v1
SELECT a.*, (select batch_id from rms_inventory_stock_quantity_batch_control) as batch_id
from rms_inventory_event a;


---Writing Error Data to S3 in csv format
insert overwrite table rms_inventory_stock_quantity
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
    value_updated_time,
    rms_sku_id,
    location_id,
    immediately_sellable_qty,
    stock_on_hand_qty,
    unavailable_qty,
    cust_order_return,
    pre_inspect_customer_return,
    unusable_qty,
    rtv_reqst_resrv,
    met_reqst_resrv,
    tc_preview,
    tc_clubhouse,
    tc_mini_1,
    tc_mini_2,
    tc_mini_3,
    problem,
    damaged_return,
    damaged_cosmetic_return,
    ertm_holds,
    nd_unavailable_qty,
    pb_holds_qty,
    com_co_holds,
    wm_holds,
    store_reserve,
    tc_holds,
    returns_holds,
    fp_holds,
    transfers_reserve_qty,
    return_to_vendor_qty,
    in_transit_qty,
    store_transfer_reserved_qty,
    store_transfer_expected_qty,
    back_order_reserve_qty,
    oms_back_order_reserve_qty,
    on_replenishment,
    last_received_date,
    location_type,
    epm_id,
    upc,
    return_inspection_qty,
    receiving_qty,
    damage_qty,
    hold_qty,
    csn,
    opseqno,
    positionNo,
    cast(year(current_date) as string) as year,
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month,
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM rms_inventory_event_final
where rms_sku_id is null or rms_sku_id = '' or rms_sku_id = '""'
 or value_updated_time is null or value_updated_time = '' or value_updated_time = '""'
 or location_id is null or location_id = '' or location_id = '""';
