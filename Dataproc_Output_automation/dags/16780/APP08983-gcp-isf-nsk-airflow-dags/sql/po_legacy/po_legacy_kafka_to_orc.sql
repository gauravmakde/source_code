create temporary view top_frame as
select
    *,
    row_number() over (partition by purchaseorder.externalid order by element_at(headers, 'SystemTime') desc) as row_num
from kafka_po_legacy_object_model_avro;

create temporary view deduped_top_frame as
select
    *
from top_frame
where row_num = 1;

insert overwrite purchase_order_legacy
partition(batch_id)
select
    errors,
    metadata,
    purchaseorder,
    warnings,
    (select batch_id from po_legacy_batch_control) as batch_id
from deduped_top_frame;
