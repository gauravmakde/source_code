MERGE INTO `{{params.gcp_project_id}}`.scoi.po_legacy_batch_hist USING (
  SELECT DISTINCT
      po_legacy_batch_control.batch_id AS batch_id
    FROM
      `{{params.gcp_project_id}}`.scoi.po_legacy_batch_control
) AS po_legacy_batch_control
ON po_legacy_batch_hist.batch_id IS NOT DISTINCT FROM po_legacy_batch_control.batch_id
   WHEN MATCHED THEN DELETE 
;
INSERT INTO `{{params.gcp_project_id}}`.scoi.po_legacy_batch_hist 
  SELECT
      po_legacy_batch_control.start_time,
      current_datetime() AS _c1,
      po_legacy_batch_control.batch_id
    FROM
      `{{params.gcp_project_id}}`.scoi.po_legacy_batch_control
;
