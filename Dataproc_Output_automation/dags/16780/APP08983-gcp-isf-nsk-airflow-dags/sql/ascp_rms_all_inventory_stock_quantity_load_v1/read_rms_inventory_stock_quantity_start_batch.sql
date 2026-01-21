TRUNCATE TABLE {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_control;
INSERT INTO  {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_control
select
  cast(current_timestamp() as datetime) as start_time,
  coalesce(max(batch_id),0)+1 as batch_id
from {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_hist;

DELETE FROM {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_hist WHERE batch_id IN (SELECT DISTINCT batch_id FROM {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_hist);
INSERT INTO {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_hist (start_time,end_time,batch_id)
select
  start_time,
  null as end_time,
  batch_id
from {{params.gcp_project_id}}.ascp.rms_inventory_stock_quantity_batch_control;