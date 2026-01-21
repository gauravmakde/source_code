-- read from kafka and de-dupe:
create temporary view top_frame as
select
  *,
  row_number() over (partition by chargebackId order by element_at(headers, 'SystemTime') desc) as row_num
from kafka_chargeback_lifecycle_object_model_avro;

create temporary view deduped_top_frame as
select
  *
from top_frame
where row_num = 1;

-- load new partition in chargeback_lifecycle table:
insert overwrite table scoi_chargeback_lifecycle
select
  chargebackId,
  lastUpdatedTime,
  vendorChargebackExemptedDetails,
  vendorChargebackIssuedDetails,
  vendorChargebackReversedDetails,
  'SQL.PARAM.AIRFLOW_RUN_ID' as batch_id
from deduped_top_frame;

