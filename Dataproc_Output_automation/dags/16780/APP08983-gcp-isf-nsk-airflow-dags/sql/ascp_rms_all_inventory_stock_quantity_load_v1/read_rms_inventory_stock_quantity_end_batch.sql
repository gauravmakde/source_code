DELETE FROM {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_hist WHERE batch_id IN (SELECT DISTINCT batch_id FROM ascp.rms_inventory_stock_quantity_batch_hist);
INSERT INTO ascp.rms_inventory_stock_quantity_batch_hist (start_time,end_time,batch_id)
select
  start_time,
  current_datetime() as end_time,
  batch_id
from {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_control;