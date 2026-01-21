-- read from kafka and de-dupe:
create temporary view top_frame as
select
  *,
  row_number() over (partition by exceptionId order by element_at(headers, 'SystemTime') desc) as row_num
from kafka_po_discrepancy_lifecycle;

create temporary view deduped_top_frame as
select
  *
from top_frame
where row_num = 1;

-- load new partition in po_discrepancy_lifecycle table:
insert overwrite table scoi_po_discrepancy_lifecycle
select
  exceptionId,
  purchaseOrderShipmentDiscrepancyAcceptedDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedBulkStoreMismatchDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedClosedPurchaseOrderDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedItemNotOrderedDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedMisShipDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedMissingPurchaseOrderDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedOverageDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedShipWindowViolationDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedStoreNotOnPurchaseOrderDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedSupplierDepartmentMismatchDetails,
  purchaseOrderShipmentDiscrepancyIdentifiedVirtualStoreMismatchDetails,
  purchaseOrderShipmentDiscrepancyRefusedDetails,
  'SQL.PARAM.AIRFLOW_RUN_ID' as batch_id
from deduped_top_frame;





insert into table processed_data_spark_log
select
    isf_dag_nm,
	step_nm,
	tbl_nm,
	metric_nm,
	metric_value,
	metric_tmstp
from (
    select
        '{dag_name}' as isf_dag_nm,
        'Spark reading from Kafka' as step_nm,
        cast (null as string) as tbl_nm,
        'SPARK_PROCESSED_MESSAGE_CNT' as metric_nm,
        count(*) as metric_value,
        current_timestamp() as metric_tmstp
    from kafka_po_discrepancy_lifecycle
    union all
    select
        '{dag_name}' as isf_dag_nm,
        'Spark loading to ORC' as step_nm,
        '{hive_table_name}' as tbl_nm,
        'LOADED_TO_ORC_CNT' as metric_nm,
        count(*) as metric_value,
        current_timestamp() as metric_tmstp
    from deduped_top_frame
    ) a
;
