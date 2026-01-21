CREATE TEMPORARY TABLE IF NOT EXISTS xref_1
AS
SELECT purchase_order_number AS order_no,
 cross_reference_external_id AS rms_xref_po,
 written_date,
 CAST(latest_close_event_tmstp_pacific AS DATE) AS closed_date,
 DATE_DIFF(CURRENT_DATE('PST8PDT'), written_date, DAY) AS days_since_written
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact
WHERE otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY);


CREATE TEMPORARY TABLE IF NOT EXISTS xref_2
AS
SELECT po.purchase_order_number AS order_no,
 COALESCE(po.cross_reference_external_id, xr.order_no) AS rms_xref_po,
 po.written_date,
 CAST(po.latest_close_event_tmstp_pacific AS DATE) AS closed_date,
 DATE_DIFF(CURRENT_DATE('PST8PDT'), po.written_date, DAY) AS days_since_written
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS po
 INNER JOIN (SELECT DISTINCT rms_xref_po,
   order_no,
   written_date
  FROM xref_1
  WHERE rms_xref_po NOT IN (SELECT DISTINCT order_no
     FROM xref_1
     WHERE rms_xref_po IS NOT NULL)
   OR rms_xref_po NOT IN (SELECT DISTINCT order_no
     FROM xref_1)
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_xref_po ORDER BY written_date DESC)) = 1) AS xr ON LOWER(po.purchase_order_number
   ) = LOWER(xr.rms_xref_po)
GROUP BY order_no,
 rms_xref_po,
 po.written_date,
 closed_date,
 days_since_written;


CREATE TEMPORARY TABLE IF NOT EXISTS voh
AS
SELECT COALESCE(xref_1.order_no, xref_2.order_no) AS order_no,
 COALESCE(xref_1.rms_xref_po, xref_2.rms_xref_po) AS rms_xref_po,
 COALESCE(xref_1.written_date, xref_2.written_date) AS written_date,
 COALESCE(xref_1.closed_date, xref_2.closed_date) AS closed_date,
 COALESCE(xref_1.days_since_written, xref_2.days_since_written) AS days_since_written
FROM xref_1
 FULL JOIN xref_2 ON LOWER(xref_1.order_no) = LOWER(xref_2.order_no);


CREATE TEMPORARY TABLE IF NOT EXISTS internal_po_staging
AS
SELECT DISTINCT order_no,
 xref_order_no AS parent_po,
 reversal_xref,
  CASE
  WHEN LOWER(order_no) = LOWER(xref_order_no)
  THEN 'Y'
  ELSE 'N'
  END AS parent_po_ind,
  CASE
  WHEN xref_order_no IS NOT NULL AND LOWER(CASE
      WHEN LOWER(order_no) = LOWER(xref_order_no)
      THEN 'Y'
      ELSE 'N'
      END) = LOWER('N')
  THEN 'Y'
  ELSE 'N'
  END AS xref_po_ind
FROM (SELECT v1.order_no,
    v2.order_no AS xref_order_no,
    v1.days_since_written,
    v2.days_since_written AS xref_days_since_written,
     CASE
     WHEN v2.closed_date < v1.written_date
     THEN 1
     ELSE 0
     END AS reversal_xref,
     ROW_NUMBER() OVER (PARTITION BY v1.order_no ORDER BY v2.days_since_written DESC) AS rnk
   FROM voh AS v1
    INNER JOIN voh AS v2 ON TRUE
   WHERE LOWER(v1.rms_xref_po) = LOWER(v2.rms_xref_po)
   UNION ALL
   SELECT v10.order_no,
    v20.order_no AS xref_order_no,
    v10.days_since_written,
    v20.days_since_written AS xref_days_since_written,
     CASE
     WHEN v20.closed_date < v10.written_date
     THEN 1
     ELSE 0
     END AS reversal_xref,
     ROW_NUMBER() OVER (PARTITION BY v10.order_no ORDER BY v20.days_since_written DESC) AS rnk2
   FROM voh AS v10
    INNER JOIN voh AS v20 ON TRUE
   WHERE LOWER(v10.order_no) = LOWER(v20.rms_xref_po)
   UNION ALL
   SELECT v11.order_no,
    v21.order_no AS xref_order_no,
    v11.days_since_written,
    v21.days_since_written AS xref_days_since_written,
     CASE
     WHEN v21.closed_date < v11.written_date
     THEN 1
     ELSE 0
     END AS reversal_xref,
     ROW_NUMBER() OVER (PARTITION BY v11.order_no ORDER BY v21.days_since_written DESC) AS rnk3
   FROM voh AS v11
    INNER JOIN voh AS v21 ON TRUE
   WHERE LOWER(v11.rms_xref_po) = LOWER(v21.order_no)) AS a
WHERE rnk = 1;


CREATE TEMPORARY TABLE IF NOT EXISTS cancels_base AS
WITH base_data AS (
  SELECT 
    hdr.purchase_order_number,
    po_ev.event_date_pacific,
    dt.month_idnt,
    dt.month_label,
    po_fct.rms_sku_num,
    po_ev.ship_location_id,
    COALESCE(po_ev.quantity_canceled, 0) AS cancel_event,
    CASE 
      WHEN po_ev.quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id ORDER BY po_ev.revision_id, po_ev.event_time) 
    END AS total_rn,
    COALESCE(po_ev.buyer_quantity_canceled, 0) AS buyer_cancel_event,
    CASE 
      WHEN po_ev.buyer_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id ORDER BY po_ev.revision_id, po_ev.event_time) 
    END AS buyer_rn,
    COALESCE(po_ev.vendor_quantity_canceled, 0) AS vendor_cancel_event,
    CASE 
      WHEN po_ev.vendor_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id ORDER BY po_ev.revision_id, po_ev.event_time) 
    END AS vendor_rn,
    COALESCE(po_ev.auto_quantity_canceled, 0) AS auto_cancel_event,
    CASE 
      WHEN po_ev.auto_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id ORDER BY po_ev.revision_id, po_ev.event_time) 
    END AS auto_rn,
    COALESCE(po_ev.other_quantity_canceled, 0) AS other_cancel_event,
    CASE 
      WHEN po_ev.other_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id ORDER BY po_ev.revision_id, po_ev.event_time) 
    END AS other_rn
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_event_fact po_ev
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact hdr
    ON po_ev.epo_purchase_order_id = hdr.epo_purchase_order_id
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_fact po_fct
    ON hdr.purchase_order_number = po_fct.purchase_order_number
   AND po_ev.sku_num = po_fct.sku_num
   AND po_ev.ship_location_id = po_fct.ship_location_id
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim dt 
    ON dt.day_date = po_ev.event_date_pacific
  WHERE 
    (hdr.status IN ('APPROVED', 'WORKSHEET') OR (hdr.status = 'CLOSED' AND hdr.otb_eow_date >= CURRENT_DATE('PST8PDT') - INTERVAL 395 DAY))
    AND po_ev.event_date_pacific >= DATE('2024-01-10')
    AND (
      po_ev.buyer_quantity_canceled > 0 
      OR po_ev.vendor_quantity_canceled > 0
      OR po_ev.auto_quantity_canceled > 0
      OR po_ev.other_quantity_canceled > 0
    )
),
grouped_data AS (
  SELECT 
    b.purchase_order_number,
    b.rms_sku_num,
    b.ship_location_id,
    b.month_idnt,
    b.month_label,
    MAX(CASE WHEN total_rn = 1 THEN cancel_event ELSE 0 END) AS total_cancel_event,
    MAX(CASE WHEN buyer_rn = 1 THEN buyer_cancel_event ELSE 0 END) AS buyer_cancel_event,
    MAX(CASE WHEN vendor_rn = 1 THEN vendor_cancel_event ELSE 0 END) AS vendor_cancel_event,
    MAX(CASE WHEN auto_rn = 1 THEN auto_cancel_event ELSE 0 END) AS auto_cancel_event,
    MAX(CASE WHEN other_rn = 1 THEN other_cancel_event ELSE 0 END) AS other_cancel_event
  FROM base_data b
  GROUP BY 
    b.purchase_order_number, b.rms_sku_num, b.ship_location_id, b.month_idnt, b.month_label
)
SELECT * FROM grouped_data
WHERE cast(total_cancel_event as bool) OR cast(buyer_cancel_event as bool) OR cast(vendor_cancel_event as bool) OR cast(auto_cancel_event as bool) OR cast(other_cancel_event as bool);



CREATE TEMPORARY TABLE IF NOT EXISTS cancels_base_sku
AS
SELECT purchase_order_number,
 rms_sku_num,
 channel_num,
 month_idnt,
 month_label,
 SUM(total_cancel_u) AS total_cancel_u,
 SUM(buyer_cancel_u) AS buyer_cancel_u,
 SUM(vendor_cancel_u) AS vendor_cancel_u,
 SUM(auto_cancel_u) AS auto_cancel_u,
 SUM(other_cancel_u) AS other_cancel_u,
 0 AS inbound_rcpt_u,
 0 AS outbound_rcpt_u,
 0 AS outbound_rcpt_c,
 0 AS on_order_u
FROM (SELECT cn.purchase_order_number,
   cn.rms_sku_num,
   d.channel_num,
   cn.month_idnt,
   cn.month_label,
    CASE
    WHEN cn.total_cancel_event > 0
    THEN CAST(TRUNC(cn.total_cancel_event - IFNULL(LAG(cn.total_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt), 0)) AS INTEGER)
    ELSE 0
    END AS total_cancel_u,
    CASE
    WHEN cn.buyer_cancel_event > 0
    THEN CAST(TRUNC(cn.buyer_cancel_event - IFNULL(LAG(cn.buyer_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt), 0)) AS INTEGER)
    ELSE 0
    END AS buyer_cancel_u,
    CASE
    WHEN cn.vendor_cancel_event > 0
    THEN CAST(TRUNC(cn.vendor_cancel_event - IFNULL(LAG(cn.vendor_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt), 0)) AS INTEGER)
    ELSE 0
    END AS vendor_cancel_u,
    CASE
    WHEN cn.auto_cancel_event > 0
    THEN CAST(TRUNC(cn.auto_cancel_event - IFNULL(LAG(cn.auto_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt), 0)) AS INTEGER)
    ELSE 0
    END AS auto_cancel_u,
    CASE
    WHEN cn.other_cancel_event > 0
    THEN CAST(TRUNC(cn.other_cancel_event - IFNULL(LAG(cn.other_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt), 0)) AS INTEGER)
    ELSE 0
    END AS other_cancel_u
  FROM cancels_base AS cn
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS d ON CAST(cn.ship_location_id AS FLOAT64) = d.store_num) AS base
GROUP BY purchase_order_number,
 rms_sku_num,
 channel_num,
 month_idnt,
 month_label;


CREATE TEMPORARY TABLE IF NOT EXISTS cancels_sku
AS
SELECT purchase_order_number,
 rms_sku_num,
 month_idnt,
 month_label,
 SUM(total_cancel_u) AS total_cancel_u,
 SUM(buyer_cancel_u) AS buyer_cancel_u,
 SUM(vendor_cancel_u) AS vendor_cancel_u,
 SUM(auto_cancel_u) AS auto_cancel_u,
 SUM(other_cancel_u) AS other_cancel_u,
 0 AS inbound_rcpt_u,
 0 AS outbound_rcpt_u,
 0 AS outbound_rcpt_c,
 0 AS on_order_u
FROM cancels_base_sku
GROUP BY purchase_order_number,
 rms_sku_num,
 month_idnt,
 month_label;


CREATE TEMPORARY TABLE IF NOT EXISTS dc_sku
AS
SELECT e.purchase_order_number,
 e.rms_sku_num,
 c.month_idnt,
 c.month_label,
 0 AS total_cancel_u,
 0 AS buyer_cancel_u,
 0 AS vendor_cancel_u,
 0 AS auto_cancel_u,
 0 AS other_cancel_u,
 SUM(e.shipment_qty) AS inbound_rcpt_u,
 0 AS outbound_rcpt_u,
 0 AS outbound_rcpt_c,
 0 AS on_order_u
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.wm_inbound_carton_fact_vw AS e
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS hdr ON LOWER(e.purchase_order_number) = LOWER(hdr.purchase_order_number
   )
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c ON CAST(e.received_tmstp AS DATE) = c.day_date
WHERE (LOWER(hdr.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')) OR LOWER(hdr.status) = LOWER('CLOSED') AND hdr.otb_eow_date
      >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 395 DAY))
 AND e.shipment_qty <> 0
 AND CAST(e.received_tmstp AS DATE) >= DATE '2024-01-10'
 AND LOWER(e.location_type_code) = LOWER('S')
 AND hdr.otb_eow_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 180 DAY)
 AND LOWER(e.location_id) NOT IN (LOWER('889'), LOWER('859'), LOWER('896'), LOWER('869'), LOWER('891'), LOWER('868'))
GROUP BY e.rms_sku_num,
 e.purchase_order_number,
 c.month_idnt,
 c.month_label;


CREATE TEMPORARY TABLE IF NOT EXISTS out_oo_sku
AS
SELECT ob.purchase_order_num AS purchase_order_number,
 ob.sku_num AS rms_sku_num,
 wk.month_idnt,
 wk.month_label,
 0 AS total_cancel_u,
 0 AS buyer_cancel_u,
 0 AS vendor_cancel_u,
 0 AS auto_cancel_u,
 0 AS other_cancel_u,
 0 AS inbound_rcpt_u,
 SUM(ob.rcpt_units) AS outbound_rcpt_u,
 SUM(ob.rcpt_cost) AS outbound_rcpt_c,
 SUM(ob.quantity_open) AS on_order_u
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.outbound_po_sku_lkp AS ob
 INNER JOIN (SELECT week_idnt,
   month_idnt,
   month_label
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date >= DATE '2024-01-10'
  GROUP BY week_idnt,
   month_idnt,
   month_label) AS wk ON ob.week_idnt = wk.week_idnt
GROUP BY purchase_order_number,
 rms_sku_num,
 wk.month_idnt,
 wk.month_label;


CREATE TEMPORARY TABLE IF NOT EXISTS pos_sku_base AS WITH metrics AS (SELECT purchase_order_number,
   rms_sku_num,
   month_idnt,
   month_label,
   SUM(total_cancel_u) AS total_cancel_u,
   SUM(buyer_cancel_u) AS buyer_cancel_u,
   SUM(vendor_cancel_u) AS vendor_cancel_u,
   SUM(auto_cancel_u) AS auto_cancel_u,
   SUM(other_cancel_u) AS other_cancel_u,
   SUM(COALESCE(inbound_rcpt_u, 0)) AS inbound_rcpt_u,
   SUM(COALESCE(outbound_rcpt_u, 0)) AS outbound_rcpt_u,
   SUM(COALESCE(outbound_rcpt_c, 0)) AS outbound_rcpt_c,
   SUM(COALESCE(on_order_u, 0)) AS on_order_u
  FROM (SELECT *
     FROM cancels_sku
     UNION ALL
     SELECT *
     FROM out_oo_sku
     UNION ALL
     SELECT *
     FROM dc_sku) AS skus
  GROUP BY purchase_order_number,
   rms_sku_num,
   month_idnt,
   month_label) (SELECT m.purchase_order_number,
   hdr.status,
   otb.month_idnt AS otb_month_idnt,
   otb.month_label AS otb_month_label,
   m.month_idnt,
   m.month_label AS action_month_label,
   m.rms_sku_num,
   h.div_idnt,
   h.div_desc,
   h.grp_idnt,
   h.grp_desc,
   h.dept_idnt,
   h.dept_desc,
     TRIM(FORMAT('%11d', h.div_idnt)) || ': ' || h.div_desc AS division,
     TRIM(FORMAT('%11d', h.grp_idnt)) || ': ' || h.grp_desc AS subdivision,
     TRIM(FORMAT('%11d', h.dept_idnt)) || ': ' || h.dept_desc AS department,
   h.quantrix_category,
   h.vendor_name,
   h.npg_ind,
    CASE
    WHEN LOWER(hdr.order_type) IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER'))
    THEN 'RP'
    ELSE 'NRP'
    END AS rp_ind,
   h.customer_choice,
   hdr.po_type,
   hdr.purchase_type,
   hdr.comments,
   hdr.internal_po_ind,
   COALESCE(po.parent_po_ind, 'N') AS parent_po_ind,
   COALESCE(po.xref_po_ind, 'N') AS xref_po_ind,
   COALESCE(po.reversal_xref, 0) AS reversal_xref,
   po.parent_po,
   po_fct.unit_cost,
   po_fct.total_expenses_per_unit,
   po_fct.total_duty_per_unit,
   COALESCE(po_fct.original_quantity_ordered, 0) AS original_ordered_u,
      COALESCE(po_fct.original_quantity_ordered, 0) * po_fct.unit_cost + COALESCE(po_fct.original_quantity_ordered, 0) *
      po_fct.total_expenses_per_unit + COALESCE(po_fct.original_quantity_ordered, 0) * po_fct.total_duty_per_unit AS
   original_ordered_c,
   COALESCE(po_fct.approved_quantity_ordered, 0) AS approved_ordered_u,
      COALESCE(po_fct.approved_quantity_ordered, 0) * po_fct.unit_cost + COALESCE(po_fct.approved_quantity_ordered, 0) *
      po_fct.total_expenses_per_unit + COALESCE(po_fct.approved_quantity_ordered, 0) * po_fct.total_duty_per_unit AS
   approved_ordered_c,
   COALESCE(po_fct.quantity_ordered, 0) AS ordered_u,
      COALESCE(po_fct.quantity_ordered, 0) * po_fct.unit_cost + COALESCE(po_fct.quantity_ordered, 0) * po_fct.total_expenses_per_unit
       + COALESCE(po_fct.quantity_ordered, 0) * po_fct.total_duty_per_unit AS ordered_c,
   COALESCE(m.vendor_cancel_u + m.auto_cancel_u, 0) AS supplier_cancel_u,
      COALESCE(m.vendor_cancel_u + m.auto_cancel_u, 0) * po_fct.unit_cost + COALESCE(m.vendor_cancel_u + m.auto_cancel_u
        , 0) * po_fct.total_expenses_per_unit + COALESCE(m.vendor_cancel_u + m.auto_cancel_u, 0) * po_fct.total_duty_per_unit
      AS supplier_cancel_c,
   m.buyer_cancel_u,
      m.buyer_cancel_u * po_fct.unit_cost + m.buyer_cancel_u * po_fct.total_expenses_per_unit + m.buyer_cancel_u *
     po_fct.total_duty_per_unit AS buyer_cancel_c,
   m.other_cancel_u,
      m.other_cancel_u * po_fct.unit_cost + m.other_cancel_u * po_fct.total_expenses_per_unit + m.other_cancel_u *
     po_fct.total_duty_per_unit AS other_cancel_c,
   m.inbound_rcpt_u,
      m.inbound_rcpt_u * po_fct.unit_cost + m.inbound_rcpt_u * po_fct.total_expenses_per_unit + m.inbound_rcpt_u *
     po_fct.total_duty_per_unit AS inbound_rcpt_c,
   m.outbound_rcpt_u,
   m.outbound_rcpt_c,
   m.on_order_u,
      m.on_order_u * po_fct.unit_cost + m.on_order_u * po_fct.total_expenses_per_unit + m.on_order_u * po_fct.total_duty_per_unit
      AS on_order_c
  FROM metrics AS m
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS hdr ON LOWER(m.purchase_order_number) = LOWER(hdr.purchase_order_number
     )
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS otb ON hdr.otb_eow_date = otb.day_date
   LEFT JOIN (SELECT purchase_order_number,
     rms_sku_num,
     SUM(COALESCE(approved_quantity_ordered, 0)) AS approved_quantity_ordered,
     SUM(COALESCE(original_quantity_ordered, 0)) AS original_quantity_ordered,
     SUM(COALESCE(quantity_ordered, 0)) AS quantity_ordered,
     AVG(unit_cost) AS unit_cost,
     AVG(total_expenses_per_unit) AS total_expenses_per_unit,
     AVG(total_duty_per_unit) AS total_duty_per_unit
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_shiplocation_fact AS po_fct
    GROUP BY purchase_order_number,
     rms_sku_num) AS po_fct ON LOWER(m.purchase_order_number) = LOWER(po_fct.purchase_order_number) AND LOWER(m.rms_sku_num
      ) = LOWER(po_fct.rms_sku_num)
   LEFT JOIN internal_po_staging AS po ON LOWER(po.order_no) = LOWER(m.purchase_order_number)
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h ON LOWER(m.rms_sku_num) = LOWER(h.sku_idnt) AND LOWER(h
      .channel_country) = LOWER('US'));


CREATE TEMPORARY TABLE IF NOT EXISTS false_cancels
AS
SELECT p.parent_po,
 p.month_idnt,
 p.rms_sku_num,
 SUM(x.inbound_rcpt_u) AS xref_inbound_rcpt_u,
 SUM(x.outbound_rcpt_u) AS xref_outbound_rcpt_u,
 SUM(x.on_order_u) AS xref_on_order_u,
 MAX(p.on_order_u) AS parent_on_order_u,
 SUM(CASE
   WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND x.reversal_xref = 0 AND p.supplier_cancel_u
       > 0 AND p.supplier_cancel_u = x.outbound_rcpt_u
   THEN p.supplier_cancel_u
   ELSE 0
   END) AS invalid,
 SUM(CASE
   WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND x.reversal_xref = 0 AND p.supplier_cancel_u
        > 0 AND x.outbound_rcpt_u > 0 AND p.supplier_cancel_u > x.outbound_rcpt_u
   THEN x.outbound_rcpt_u
   WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND x.reversal_xref = 0 AND p.supplier_cancel_u
        > 0 AND x.outbound_rcpt_u > 0 AND p.supplier_cancel_u < x.outbound_rcpt_u
   THEN CAST(p.supplier_cancel_u AS BIGNUMERIC)
   ELSE 0
   END) AS partial_invalid,
 SUM(CASE
   WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('CLOSED') AND x.reversal_xref = 1 AND p.supplier_cancel_u
        > 0 AND x.outbound_rcpt_u > 0 AND p.supplier_cancel_u <= x.outbound_rcpt_u
   THEN CAST(p.supplier_cancel_u AS BIGNUMERIC)
   ELSE 0
   END) AS reversal,
 SUM(CASE
   WHEN LOWER(p.status) = LOWER('APPROVED') AND LOWER(x.status) = LOWER('APPROVED') AND p.supplier_cancel_u > 0 AND x.inbound_rcpt_u
       > 0 AND p.supplier_cancel_u < x.inbound_rcpt_u
   THEN p.supplier_cancel_u
   WHEN LOWER(p.status) = LOWER('APPROVED') AND LOWER(x.status) = LOWER('APPROVED') AND p.supplier_cancel_u > 0 AND x.inbound_rcpt_u
       > 0 AND p.supplier_cancel_u >= x.inbound_rcpt_u
   THEN x.inbound_rcpt_u
   ELSE 0
   END) AS potential,
 SUM(CASE
   WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('APPROVED') AND p.supplier_cancel_u > 0 AND x.on_order_u
       > 0 AND p.supplier_cancel_u < x.on_order_u
   THEN CAST(p.supplier_cancel_u AS BIGNUMERIC)
   WHEN LOWER(p.status) = LOWER('CLOSED') AND LOWER(x.status) = LOWER('APPROVED') AND p.supplier_cancel_u > 0 AND x.on_order_u
       > 0 AND p.supplier_cancel_u >= x.on_order_u
   THEN x.on_order_u
   ELSE 0
   END) AS potential_oo
FROM (SELECT *
  FROM pos_sku_base
  WHERE LOWER(parent_po_ind) = LOWER('Y')
   AND supplier_cancel_u > 0) AS p
 INNER JOIN (SELECT parent_po,
   purchase_order_number,
   rms_sku_num,
   status,
   MAX(reversal_xref) AS reversal_xref,
   SUM(inbound_rcpt_u) AS inbound_rcpt_u,
   SUM(outbound_rcpt_u) AS outbound_rcpt_u,
   SUM(on_order_u) AS on_order_u
  FROM pos_sku_base
  WHERE LOWER(xref_po_ind) = LOWER('Y')
   OR LOWER(internal_po_ind) = LOWER('T')
  GROUP BY parent_po,
   purchase_order_number,
   rms_sku_num,
   status) AS x ON LOWER(x.parent_po) = LOWER(p.purchase_order_number) AND LOWER(x.rms_sku_num) = LOWER(p.rms_sku_num)
GROUP BY p.parent_po,
 p.month_idnt,
 p.rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS po_dist
AS
SELECT t4.purchase_order_number,
 t4.rms_sku_num,
 t4.banner
FROM (SELECT dist.purchase_order_num AS purchase_order_number,
   dist.rms_sku_num,
   st.channel_brand AS banner,
   COUNT(*) AS A372081672
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS dist
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st ON dist.distribute_location_id = st.store_num
   INNER JOIN (SELECT DISTINCT purchase_order_number
    FROM pos_sku_base) AS c ON LOWER(c.purchase_order_number) = LOWER(dist.purchase_order_num)
  GROUP BY purchase_order_number,
   dist.rms_sku_num,
   banner
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY purchase_order_number, dist.rms_sku_num ORDER BY A372081672 DESC)) = 1) AS t4;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.po_sku_cancels;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.po_sku_cancels
(SELECT d.banner,
  b.purchase_order_number,
  b.status,
  b.otb_month_idnt,
  b.otb_month_label,
  b.month_idnt,
  b.action_month_label,
  b.rms_sku_num,
  b.div_idnt,
  b.div_desc,
  b.grp_idnt,
  b.grp_desc,
  b.dept_idnt,
  b.dept_desc,
  b.division,
  b.subdivision,
  b.department,
  b.quantrix_category,
  b.vendor_name,
  b.npg_ind,
  b.rp_ind,
  b.customer_choice,
  b.po_type,
  b.purchase_type,
  b.comments,
  b.internal_po_ind,
  b.parent_po_ind,
  b.xref_po_ind,
  b.reversal_xref,
  b.parent_po,
  b.unit_cost,
  b.total_expenses_per_unit,
  b.total_duty_per_unit,
  b.original_ordered_u,
  ROUND(CAST(b.original_ordered_c AS NUMERIC), 2) AS original_ordered_c,
  b.approved_ordered_u,
  ROUND(CAST(b.approved_ordered_c AS NUMERIC), 2) AS approved_ordered_c,
  b.ordered_u,
  ROUND(CAST(b.ordered_c AS NUMERIC), 2) AS ordered_c,
  b.supplier_cancel_u,
  ROUND(CAST(b.supplier_cancel_c AS NUMERIC), 2) AS supplier_cancel_c,
  b.buyer_cancel_u,
  ROUND(CAST(b.buyer_cancel_c AS NUMERIC), 2) AS buyer_cancel_c,
  b.other_cancel_u,
  ROUND(CAST(b.other_cancel_c AS NUMERIC), 2) AS other_cancel_c,
  b.inbound_rcpt_u,
  ROUND(CAST(b.inbound_rcpt_c AS NUMERIC), 2) AS inbound_rcpt_c,
  CAST(TRUNC(b.outbound_rcpt_u) AS INTEGER) AS outbound_rcpt_u,
  ROUND(CAST(b.outbound_rcpt_c AS NUMERIC), 2) AS outbound_rcpt_c,
  CAST(TRUNC(b.on_order_u) AS INTEGER) AS on_order_u,
  ROUND(CAST(b.on_order_c AS NUMERIC), 2) AS on_order_c,
  COALESCE(c.xref_inbound_rcpt_u, 0) AS xref_inbound_rcpt_u,
  CAST(TRUNC(COALESCE(c.xref_outbound_rcpt_u, 0)) AS INTEGER) AS xref_outbound_rcpt_u,
  CAST(TRUNC(COALESCE(c.xref_on_order_u, 0)) AS INTEGER) AS xref_on_order_u,
  COALESCE(c.invalid, 0) AS invalid_cancel_u,
  ROUND(CAST(COALESCE(c.invalid, 0) * b.unit_cost + COALESCE(c.invalid, 0) * b.total_expenses_per_unit + COALESCE(c.invalid, 0) * b.total_duty_per_unit AS NUMERIC)
   , 2) AS invalid_cancel_c,
  CAST(TRUNC(COALESCE(c.partial_invalid, 0)) AS INTEGER) AS partial_invalid_cancel_u,
  ROUND(CAST(COALESCE(c.partial_invalid, 0) * b.unit_cost + COALESCE(c.partial_invalid, 0) * b.total_expenses_per_unit + COALESCE(c.partial_invalid, 0) * b.total_duty_per_unit AS NUMERIC)
   , 2) AS partial_invalid_cancel_c,
  CAST(TRUNC(COALESCE(c.reversal, 0)) AS INTEGER) AS reversal_cancel_u,
  ROUND(CAST(COALESCE(c.reversal, 0) * b.unit_cost + COALESCE(c.reversal, 0) * b.total_expenses_per_unit + COALESCE(c.reversal, 0) * b.total_duty_per_unit AS NUMERIC)
   , 2) AS reversal_cancel_c,
  COALESCE(c.potential, 0) AS potential_cancel_u,
  ROUND(CAST(COALESCE(c.potential, 0) * b.unit_cost + COALESCE(c.potential, 0) * b.total_expenses_per_unit + COALESCE(c.potential, 0) * b.total_duty_per_unit AS NUMERIC)
   , 2) AS potential_cancel_c,
  CAST(TRUNC(COALESCE(c.potential_oo, 0)) AS INTEGER) AS potential_oo_cancel_u,
  ROUND(CAST(COALESCE(c.potential_oo, 0) * b.unit_cost + COALESCE(c.potential_oo, 0) * b.total_expenses_per_unit + COALESCE(c.potential_oo, 0) * b.total_duty_per_unit AS NUMERIC)
   , 2) AS potential_oo_cancel_c,
  CAST(TRUNC(CASE
    WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
    THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
    ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
    END) AS INTEGER) AS false_cancel_u,
  ROUND(CAST(CASE
        WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
        THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
        ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
        END * b.unit_cost + CASE
        WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) +
            COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
        THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
        ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
        END * b.total_expenses_per_unit + CASE
       WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) +
           COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
       THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
       ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
       END * b.total_duty_per_unit AS NUMERIC), 2) AS false_cancel_c,
  CAST(TRUNC(CASE
    WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
    THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
    ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
    END) AS INTEGER) AS potential_false_cancel_u,
  ROUND(CAST(CASE
        WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
        THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
        ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
        END * b.unit_cost + CASE
        WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0) +
           COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
        THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
        ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
        END * b.total_expenses_per_unit + CASE
       WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0) +
          COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
       THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
       ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
       END * b.total_duty_per_unit AS NUMERIC), 2) AS potential_false_cancel_c,
  CAST(TRUNC(b.supplier_cancel_u - CASE
     WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
     THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
     ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
     END) AS INTEGER) AS true_cancel_u,
  ROUND(CAST(b.supplier_cancel_c - (CASE
          WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
          THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
          ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
          END * b.unit_cost + CASE
          WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) +
              COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
          THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
          ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
          END * b.total_expenses_per_unit + CASE
         WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) +
             COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
         THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
         ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
         END * b.total_duty_per_unit) AS NUMERIC), 2) AS true_cancel_c,
  CAST(TRUNC(b.supplier_cancel_u - CASE
      WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
      THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
      ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
      END - CASE
     WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0) +
        COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
     THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
     ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
     END) AS INTEGER) AS potential_true_cancel_u,
  ROUND(CAST(b.supplier_cancel_c - (CASE
           WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
           THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
           ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
           END * b.unit_cost + CASE
           WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) +
               COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
           THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
           ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
           END * b.total_expenses_per_unit + CASE
          WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('CLOSED') OR COALESCE(c.invalid, 0) +
              COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0) > b.supplier_cancel_u
          THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
          ELSE COALESCE(c.invalid, 0) + COALESCE(c.partial_invalid, 0) + COALESCE(c.reversal, 0)
          END * b.total_duty_per_unit) - (CASE
          WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0
              ) + COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
          THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
          ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
          END * b.unit_cost + CASE
          WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0
              ) + COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
          THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
          ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
          END * b.total_expenses_per_unit + CASE
         WHEN LOWER(b.internal_po_ind) = LOWER('T') AND LOWER(b.status) = LOWER('APPROVED') OR COALESCE(c.potential, 0)
            + COALESCE(c.potential_oo, 0) > b.supplier_cancel_u
         THEN CAST(b.supplier_cancel_u AS BIGNUMERIC)
         ELSE COALESCE(c.potential, 0) + COALESCE(c.potential_oo, 0)
         END * b.total_duty_per_unit) AS NUMERIC), 2) AS potential_true_cancel_c,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME(('PST8PDT'))) AS DATETIME) AS update_timestamp
 FROM pos_sku_base AS b
  INNER JOIN po_dist AS d ON LOWER(b.purchase_order_number) = LOWER(d.purchase_order_number) AND LOWER(b.rms_sku_num) =
    LOWER(d.rms_sku_num)
  LEFT JOIN false_cancels AS c ON LOWER(b.purchase_order_number) = LOWER(c.parent_po) AND LOWER(b.rms_sku_num) = LOWER(c
      .rms_sku_num) AND b.month_idnt = c.month_idnt);