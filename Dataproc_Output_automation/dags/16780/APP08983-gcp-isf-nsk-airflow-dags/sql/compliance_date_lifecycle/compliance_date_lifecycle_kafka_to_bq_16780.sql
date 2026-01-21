create temporary view top_frame as
select
  *,
  row_number() over (partition by vendorId order by element_at(headers, 'SystemTime') desc) as row_num
from kafka_compliance_date_lifecycle_object_model_avro;

create temporary view deduped_top_frame as
select *
from top_frame
where row_num = 1;

insert overwrite table scoi_compliance_date_lifecycle
partition(batch_id)
select
  vendorid,
  lastupdatedtime,
  vendorcompliancedateaddeddetails,
  vendorcompliancedateextendeddetails,
  'SQL.PARAM.AIRFLOW_RUN_ID' as batch_id
from deduped_top_frame;



