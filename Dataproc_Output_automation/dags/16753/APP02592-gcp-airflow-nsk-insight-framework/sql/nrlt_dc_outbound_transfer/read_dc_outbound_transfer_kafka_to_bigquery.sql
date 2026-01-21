--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File              : read_dc_outbound_transfer_kafka_to_teradata.sql
-- Author            : Dmytro Utte
-- Description       : Reading Data from  Source Kafka Topic Name=inventory-distribution-center-outbound-carton-analytical-avro using SQL API CODE
-- Source topic      : inventory-distribution-center-outbound-carton-analytical-avro
-- Object model      : DistributionCenterOutboundCarton
-- ETL Run Frequency : Every hour
-- Version :         : 0.2
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-08-21  Utte Dmytro    FA-9944:  Migrate NRLT_inventory_distribution_center_outbound_carton DAG to IsF
-- 2024-03-06  Andrii Skyba   FA-11137: IsF NSK migration
-- 2024-03-19  Tetiana Ivchyk FA-11580: Bulk sort issue: to change the ongoing flow
-- 2024-03-22  Tetiana Ivchyk FA-12201: Fix variance between s3 and td
-- 2024-06-06  Roman Tymchik  FA-13067: IsF nrlt_dc_outbound_transfer: remove nlsFlag & SystemTime
--*************************************************************************************************************************************

create temporary view dc_outbound_transfer_cnt as
select
	count(*) as total_cnt
from kafka_dc_outbound_transfer_avro;

--WAREHOUSE_EVENTS--
create temporary view dc_outbound_transfer_warehouse_events as
select
	cartonLpn,
	packTime,
	packType,
	warehouseEvents,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	objectMetadata.lastUpdatedTime
from kafka_dc_outbound_transfer_avro;

create temporary view explode_warehouseEvents_explode1 as
select
	cartonLpn,
	packTime,
	packType,
	explode_outer(warehouseEvents) events,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	lastUpdatedTime
from dc_outbound_transfer_warehouse_events;

create temporary view explode_warehouseEvents_explode2 as
select
	cartonLpn,
	packTime,
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	events.warehouseEventContext.originatingEventId event_id,
	events.warehouseEventContext.locationId location_id,
	events.warehouseEventContext.userId.id user_id,
	events.warehouseEventType event_type,
	events.eventTime event_time,
	row_number() over (partition by cartonLpn, events.warehouseEventContext.locationId, events.warehouseEventContext.originatingEventId  order by lastUpdatedTime desc) as rn
from explode_warehouseEvents_explode1;

create temporary view explode_warehouseEvents_final as
select
	cartonLpn,
	packTime,
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	event_id,
	location_id,
	user_id,
	event_type,
	event_time
from explode_warehouseEvents_explode2
where rn = 1;


--PACKED_ITEMS--
create temporary view dc_outbound_transfer_packed_items_rn AS
select
	cartonLpn,
	packTime,
	packType,
	packedItems,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	objectMetadata.lastUpdatedTime,
	row_number() over (partition by cartonLpn, packType, packTime order by objectMetadata.lastUpdatedTime desc) as rn
from kafka_dc_outbound_transfer_avro;

create temporary view dc_outbound_transfer_packed_items_stg as
select
	cartonLpn,
	packTime,
	packType,
	packedItems,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber
from dc_outbound_transfer_packed_items_rn
where rn = 1;

create temporary view explode_packedItems_explode1 as
select
	cartonLpn,
	packTime,
	packType,
	explode_outer(packedItems) items,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber
from dc_outbound_transfer_packed_items_stg;

create temporary view explode_packedItems_final as
select
	cartonLpn,
	packTime,
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	items.itemQuantity.productSku.id sku,
	items.itemQuantity.productSku.idType sku_type,
	sum(items.itemQuantity.quantity) as quantity,
	items.itemQuantity.multiplier multiplier,
	items.originalCartonLpn originalCartonLpn,
	items.warehouseOrderId.id warehouseOrderId
from explode_packedItems_explode1
where packTime is not null
	and cartonLpn <> '' and items.itemQuantity.productSku.id <> '' 
	and cartonLpn <> '""' and items.itemQuantity.productSku.id <> '""'
group by 
	cartonLpn,
	packTime,
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	items.itemQuantity.productSku.id,
	items.itemQuantity.productSku.idType,
	items.itemQuantity.multiplier,
	items.originalCartonLpn,
	items.warehouseOrderId.id;


---Writing Error Data to S3 in csv format
insert overwrite table dc_outbound_transfer_carton_events_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	cartonLpn,
	packTime,
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	event_id,
	location_id,
	user_id,
	event_type,
	event_time,
	substr(current_date, 1, 4) as year,
	substr(current_date, 6, 2) as month,
	substr(current_date, 9, 2) as day,
	substr(current_timestamp, 12, 2) as hour
from explode_warehouseEvents_final
where cartonLpn is null or packTime is null or event_id is null
	or cartonLpn = '' or event_id = '' 
	or cartonLpn = '""' or event_id = '""';


insert overwrite table dc_outbound_transfer_packed_items_err
partition(year, month, day, hour)
select /*+ COALESCE(1) */
	cartonLpn,
	packTime,
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	items.itemQuantity.productSku.id sku,
	items.itemQuantity.productSku.idType sku_type,
	items.itemQuantity.quantity,
	items.itemQuantity.multiplier,
	items.originalCartonLpn,
	items.warehouseOrderId.id warehouseOrderId,
	substr(current_date, 1, 4) as year,
	substr(current_date, 6, 2) as month,
	substr(current_date, 9, 2) as day,
	substr(current_timestamp, 12, 2) as hour
from explode_packedItems_explode1
where cartonLpn is null or items.itemQuantity.productSku.id is null
	or cartonLpn = '' or items.itemQuantity.productSku.id = '' 
	or cartonLpn = '""' or items.itemQuantity.productSku.id = '""';


---Writing Kafka Data to Teradata staging tables
insert overwrite table dc_outbound_transfer_carton_events_ldg_table
select
	cartonLpn,
	cast(packTime as string),
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	event_id,
	location_id,
	user_id,
	event_type,
	cast (event_time as string)
from explode_warehouseEvents_final
where packTime is not null
	and cartonLpn <> '' and event_id <> '' 
	and cartonLpn <> '""' and event_id <> '""'
;

insert overwrite table dc_outbound_transfer_packed_items_ldg_table
select
	cartonLpn,
	cast(packTime as string),
	packType,
	destinationLocationId,
	masterPackNumber,
	laneNumber,
	routeNumber,
	dockDoorLpn,
	palletLpn,
	trailerLpn,
	loadNumber,
	sku,
	sku_type,
	cast(quantity as string),
	cast(multiplier as string),
	originalCartonLpn,
	warehouseOrderId
from explode_packedItems_final;


---Writing Spark job count metrics 
insert into table processed_data_spark_log_table
select
	isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	CAST(metric_value AS STRING),
	CAST(metric_tmstp AS STRING)
from (
	select 
		'nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
		'Loading to ldg' as step_nm,
		'INVENTORY_DISTRIBUTION_CENTER_OUTBOUND_CARTON_EVENTS_LDG' as tbl_nm,
		'PROCESSED_ROWS_CNT' as metric_nm,
		count(*) as metric_value,
		current_timestamp() as metric_tmstp
	from explode_warehouseEvents_final
	where packTime is not null
		and cartonLpn <> '' and event_id <> '' 
		and cartonLpn <> '""' and event_id <> '""'
	union all
	select
		'nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
		'Loading to ldg' as step_nm,
		'WM_OUTBOUND_CARTON_PACKED_ITEMS_LDG' as tbl_nm,
		'PROCESSED_ROWS_CNT' as metric_nm,
		count(*) as metric_value,
		current_timestamp() as metric_tmstp
		from explode_packedItems_final
	union all
	select
		'nrlt_dc_outbound_transfer_lifecycle_16753_TECH_SC_NAP_insights' as isf_dag_nm,
		'Reading from kafka' as step_nm,
		cast (null as string) as tbl_nm,
		'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
		total_cnt as metric_value,
		current_timestamp() as metric_tmstp
	from dc_outbound_transfer_cnt) a 
;
