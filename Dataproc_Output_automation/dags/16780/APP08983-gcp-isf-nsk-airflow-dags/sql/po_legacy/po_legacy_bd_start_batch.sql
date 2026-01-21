TRUNCATE TABLE `{{params.gcp_project_id}}`.scoi.po_legacy_batch_control;
INSERT INTO `{{params.gcp_project_id}}`.scoi.po_legacy_batch_control 
  SELECT
      current_datetime() AS _c0,
      coalesce(max(po_legacy_batch_hist.batch_id),0) + 1 AS _c1
    FROM
      `{{params.gcp_project_id}}`.scoi.po_legacy_batch_hist
;
MERGE INTO `{{params.gcp_project_id}}`.scoi.po_legacy_batch_hist USING (
  SELECT DISTINCT
      po_legacy_batch_control.batch_id AS batch_id
    FROM
      `{{params.gcp_project_id}}`.scoi.po_legacy_batch_control
) AS po_legacy_batch_control
ON po_legacy_batch_hist.batch_id IS NOT DISTINCT FROM po_legacy_batch_control.batch_id
   WHEN MATCHED THEN DELETE 
;
INSERT INTO `{{params.gcp_project_id}}`.scoi.po_legacy_batch_hist (start_time,end_time, batch_id)
  SELECT
      po_legacy_batch_control.start_time,
      NULL AS _c1,
      po_legacy_batch_control.batch_id
    FROM
      `{{params.gcp_project_id}}`.scoi.po_legacy_batch_control
;
