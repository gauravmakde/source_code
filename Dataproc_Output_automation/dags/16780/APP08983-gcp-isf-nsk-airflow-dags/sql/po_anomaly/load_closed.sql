  --Legacy --New
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.dq_po_anomaly_fact (legacy_po_id,
    new_po_id,
    legacy_po_num,
    new_po_num,
    legacy_po_status,
    new_po_status,
    legacy_po_tmstp,
    new_po_tmstp,
    new_po_approval_count,
    comparison_comment,
    event_type,
    action_date) (
  SELECT
    missed_po_from_legacy.id AS legacy_po_id,
    all_rows_new_po.epo_purchase_order_id AS new_po_id,
    missed_po_from_legacy.purchase_order_num AS legacy_po_num,
    all_rows_new_po.purchase_order_number AS new_po_num,
    new_po_latest_status.epo_most_recent_status AS legacy_po_status,
    all_rows_new_po.status AS new_po_status,
    CAST(missed_po_from_legacy.dw_sys_updt_tmstp AS DATETIME) AS legacy_po_tmstp,
    CAST(all_rows_new_po.latest_event_tmstp_pacific AS DATETIME) AS new_po_tmstp,
    new_po_latest_status.closed_event_count AS new_po_approval_count,
    CASE
      WHEN new_po_latest_status.epo_purchase_order_id IS NULL THEN 'Is not received from source'
      WHEN LOWER(new_po_latest_status.epo_most_recent_status) = LOWER('CLOSED')
    AND new_po_latest_status.closed_event_count = 0 THEN 'No Closed events received'
      WHEN LOWER(all_rows_new_po.status) <> LOWER('CLOSED') THEN 'Actual status <> CLOSED'
      ELSE NULL
  END
    AS comparison_comment,
    'CLOSED',
    DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY) AS action_date
  FROM (
    SELECT
      id,
      purchase_order_num,
      dw_sys_updt_tmstp
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_fact
    WHERE
      purchase_order_num IN (
      SELECT
        purchase_order_num
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_fact AS pot
      WHERE
        LOWER(UPPER(status)) = LOWER('CLOSED')
        AND (LOWER(dropship_ind) IN (LOWER('f'))
          OR dropship_ind IS NULL)
        AND CAST(original_approval_date AS DATE) = DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY)
      EXCEPT DISTINCT
      SELECT
        purchase_order_number
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS ht
      WHERE
        LOWER(UPPER(status)) = LOWER('CLOSED')
        AND CAST(first_approval_event_tmstp_pacific AS DATE) = DATE_SUB(current_date('PST8PDT'), INTERVAL 1 DAY))) AS missed_po_from_legacy
  LEFT JOIN (
    SELECT
      purchase_order_number,
      epo_purchase_order_id,
      status,
      latest_event_tmstp_pacific
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact) AS all_rows_new_po
  ON
    LOWER(missed_po_from_legacy.id) = LOWER(all_rows_new_po .epo_purchase_order_id)
  LEFT JOIN (
    SELECT
      epo_purchase_order_id,
      LAST_VALUE(status) OVER (PARTITION BY epo_purchase_order_id ORDER BY revision_id, event_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS epo_most_recent_status,
      SUM(CASE
          WHEN LOWER(event_name) = LOWER('PurchaseOrderClosed') THEN 1
          ELSE 0
      END
        ) OVER (PARTITION BY epo_purchase_order_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS closed_event_count
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.purchase_order_header_event_fact
    QUALIFY
      (ROW_NUMBER() OVER (PARTITION BY epo_purchase_order_id ORDER BY revision_id DESC, event_time DESC)) = 1) AS new_po_latest_status
  ON
    LOWER(missed_po_from_legacy.id) = LOWER(new_po_latest_status.epo_purchase_order_id));