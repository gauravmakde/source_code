CREATE TEMPORARY TABLE all_po
  AS
    SELECT
        *,
        row_number() OVER (PARTITION BY purchaseorder.externalid ORDER BY purchase_order_legacy.batch_id DESC) AS row_num
      FROM
        `{{params.gcp_project_id}}`.scoi.purchase_order_legacy
;
TRUNCATE TABLE `{{params.gcp_project_id}}`.scoi.purchase_order_legacy_full;
INSERT INTO `{{params.gcp_project_id}}`.scoi.purchase_order_legacy_full 
  SELECT
      all_po.errors,
      all_po.metadata,
      all_po.purchaseorder,
      all_po.warnings
    FROM
      all_po
    WHERE all_po.row_num = 1
;
