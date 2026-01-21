SELECT CAST(TRUNC(1 / CASE
     WHEN COUNT(*) = 0
     THEN 1
     ELSE 0
     END) AS INT64) AS errordetection
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.dq_po_anomaly_fact
WHERE LOWER(event_type) IN (LOWER('CLOSED'), LOWER('WORKSHEET'), LOWER('APPROVED'))
 AND action_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY);