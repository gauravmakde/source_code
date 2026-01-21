/*
Purpose:        Identify Canceled POs and Link Xref POs 
Variable(s):    {{environment_schema}} T2DL_DAS_OPEN_TO_BUY (prod) or T3DL_ACE_ASSORTMENT
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):      Sara Riker
Date Created:   1/31/24

cost = SUM((unit_cost * qty) + (total_expenses_per_unit * qty) + (total_duty_per_unit * qty))

Tables Used:
    - prd_nap_usr_vws
        - product_sku_dim_vw
        - day_cal_454_dim
        - purchase_order_shiplocation_event_fact
        - purchase_order_header_fact
        - purchase_order_shiplocation_fact
        - purchase_order_item_distributelocation_fact
        - store_dim
        - purchase_order_fact
    - t2dl_das_open_to_buy
        - xref_po_daily
*/

-- Identify all POs and thier Xref
-- DROP TABLE xref_1;
CREATE MULTISET VOLATILE TABLE xref_1 AS (
    SELECT
        purchase_order_number as order_no
       ,cross_reference_external_id as rms_xref_po
       ,written_date
       ,CAST(latest_close_event_tmstp_pacific AS DATE) AS closed_date
       ,CURRENT_DATE - written_date AS days_since_written
    FROM prd_nap_usr_vws.purchase_order_header_fact
    WHERE otb_eow_date >= CURRENT_DATE - 395
--       AND status IN ('APPROVED', 'WORKSHEET', 'CLOSED')
) WITH DATA PRIMARY 
INDEX(order_no) 
ON COMMIT PRESERVE ROWS;
 
COLLECT STATS
    PRIMARY INDEX (order_no)
    ,COLUMN (rms_xref_po)
    ,COLUMN (written_date)
        ON xref_1;

-- Pull any missing PO that is an XRef identified in xref_1
-- DROP TABLE xref_2;
CREATE MULTISET VOLATILE TABLE xref_2 AS (
SELECT 
    purchase_order_number as order_no
   ,COALESCE(cross_reference_external_id, order_no) as rms_xref_po
   ,po.written_date
   ,CAST(latest_close_event_tmstp_pacific AS DATE) AS closed_date
   ,CURRENT_DATE - po.written_date AS days_since_written
FROM prd_nap_usr_vws.purchase_order_header_fact po
JOIN (
        SELECT DISTINCT 
             rms_xref_po 
            ,order_no
            ,written_date
        FROM xref_1 
        WHERE (rms_xref_po NOT IN (SELECT DISTINCT order_no FROM xref_1 WHERE rms_xref_po IS NOT NULL)
           OR rms_xref_po NOT IN (SELECT DISTINCT order_no FROM xref_1))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rms_xref_po ORDER BY written_date DESC) = 1		
    ) xr
  ON po.purchase_order_number = xr.rms_xref_po
GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX(order_no) ON COMMIT PRESERVE ROWS; 

COLLECT STATS 
	PRIMARY INDEX (order_no)
	,COLUMN (rms_xref_po)
	,COLUMN (written_date)
		ON xref_2;            
		
-- Combine xref tables
-- DROP TABLE voh;
CREATE MULTISET VOLATILE table voh AS (
SELECT 
     COALESCE(xref_1.order_no, xref_2.order_no) AS order_no
    ,COALESCE(xref_1.rms_xref_po, xref_2.rms_xref_po) AS rms_xref_po
    ,COALESCE(xref_1.written_date, xref_2.written_date) AS written_date
    ,COALESCE(xref_1.closed_date, xref_2.closed_date) AS closed_date
    ,COALESCE(xref_1.days_since_written, xref_2.days_since_written) AS days_since_written
FROM xref_1
FULL OUTER JOIN xref_2 
  ON xref_1.order_no = xref_2.order_no
) WITH DATA
PRIMARY INDEX (order_no, rms_xref_po)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
	PRIMARY INDEX (order_no, rms_xref_po)
    ,COLUMN (order_no)
	,COLUMN (rms_xref_po)
	,COLUMN (written_date)
ON voh;

-- Map All related POs and identify Parent PO by earliest Written Date
-- DROP TABLE internal_po_staging;
CREATE MULTISET VOLATILE TABLE internal_po_staging AS (
SELECT 
     a.order_no
    ,a.xref_order_no AS parent_po
    ,reversal_xref
    ,CASE WHEN a.order_no = a.xref_order_no THEN 'Y' ELSE 'N' END AS parent_po_ind
    ,CASE WHEN a.xref_order_no IS NOT NULL AND parent_po_ind = 'N' THEN 'Y' ELSE 'N' END AS xref_po_ind
FROM (
    SELECT DISTINCT 
         order_no
        ,xref_order_no
        ,days_since_written
        ,xref_days_since_written
        ,reversal_xref
        ,row_number() OVER (PARTITION BY order_no ORDER BY xref_days_since_written DESC) AS rnk
    FROM (       
            SELECT 
                 v1.order_no
                ,v2.order_no AS xref_order_no
                ,v1.days_since_written
                ,v2.days_since_written AS xref_days_since_written
                ,CASE WHEN v2.closed_date IS NOT NULL AND v2.closed_date < v1.written_date THEN 1 ELSE 0 END AS reversal_xref
            FROM voh v1, voh v2
            WHERE v1.rms_xref_po = v2.rms_xref_po

            UNION ALL

            SELECT 
                 v1.order_no
                ,v2.order_no AS xref_order_no
                ,v1.days_since_written
                ,v2.days_since_written AS xref_days_since_written
                ,CASE WHEN v2.closed_date IS NOT NULL AND v2.closed_date < v1.written_date THEN 1 ELSE 0 END AS reversal_xref
            FROM voh v1, voh v2
            WHERE v1.order_no = v2.rms_xref_po 

            UNION ALL

            SELECT 
                 v1.order_no
                ,v2.order_no AS xref_order_no
                ,v1.days_since_written
                ,v2.days_since_written AS xref_days_since_written
                ,CASE WHEN v2.closed_date IS NOT NULL AND v2.closed_date < v1.written_date THEN 1 ELSE 0 END AS reversal_xref
            FROM voh v1, voh v2
            WHERE v1.rms_xref_po = v2.order_no 
        ) a
    ) a
WHERE a.rnk = 1
) WITH DATA 
PRIMARY INDEX (order_no) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
	PRIMARY INDEX (order_no)
ON internal_po_staging;

-- Find all Canceled POs 
-- DROP TABLE cancels_base;
CREATE MULTISET VOLATILE TABLE cancels_base AS (
SELECT
     b.purchase_order_number
    ,b.rms_sku_num
    ,b.ship_location_id
    ,b.month_idnt
    ,b.month_label
    ,MAX(CASE WHEN total_rn = 1 THEN cancel_event ELSE 0 END) AS total_cancel_event
    ,MAX(CASE WHEN buyer_rn = 1 THEN buyer_cancel_event ELSE 0 END) AS buyer_cancel_event
    ,MAX(CASE WHEN vendor_rn = 1 THEN vendor_cancel_event ELSE 0 END) AS vendor_cancel_event
    ,MAX(CASE WHEN auto_rn = 1 THEN auto_cancel_event ELSE 0 END) AS auto_cancel_event
    ,MAX(CASE WHEN other_rn = 1 THEN other_cancel_event ELSE 0 END) AS other_cancel_event
FROM (
    SELECT 
         hdr.purchase_order_number
        ,po_ev.event_date_pacific
        ,dt.month_idnt
        ,dt.month_label
        ,po_fct.rms_sku_num
        ,po_ev.ship_location_id
        -- -- Get the first record of the buyer cancel 
        ,COALESCE(po_ev.quantity_canceled, 0) AS cancel_event
        ,CASE WHEN po_ev.quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id, cancel_event ORDER BY po_ev.revision_id, po_ev.event_time) END AS total_rn
        ,COALESCE(po_ev.buyer_quantity_canceled, 0) AS buyer_cancel_event
        ,CASE WHEN po_ev.buyer_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id, buyer_cancel_event ORDER BY po_ev.revision_id, po_ev.event_time) END AS buyer_rn
        ,COALESCE(po_ev.vendor_quantity_canceled, 0) AS vendor_cancel_event
        ,CASE WHEN po_ev.vendor_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id, vendor_cancel_event ORDER BY po_ev.revision_id, po_ev.event_time) END AS vendor_rn
        ,COALESCE(po_ev.auto_quantity_canceled, 0) AS auto_cancel_event
        ,CASE WHEN po_ev.auto_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id, auto_cancel_event ORDER BY po_ev.revision_id, po_ev.event_time) END AS auto_rn
        ,COALESCE(po_ev.other_quantity_canceled, 0) AS other_cancel_event
        ,CASE WHEN po_ev.other_quantity_canceled > 0 THEN ROW_NUMBER() OVER (PARTITION BY hdr.purchase_order_number, po_fct.rms_sku_num, po_ev.ship_location_id, other_cancel_event ORDER BY po_ev.revision_id, po_ev.event_time) END AS other_rn
    FROM prd_nap_usr_vws.purchase_order_shiplocation_event_fact po_ev
    JOIN prd_nap_usr_vws.purchase_order_header_fact hdr
      ON po_ev.epo_purchase_order_id = hdr.epo_purchase_order_id
    JOIN prd_nap_usr_vws.purchase_order_shiplocation_fact po_fct
      ON hdr.purchase_order_number = po_fct.purchase_order_number
     AND po_ev.sku_num = po_fct.sku_num
     AND po_ev.ship_location_id = po_fct.ship_location_id
    JOIN prd_nap_usr_vws.day_cal_454_dim dt 
      ON dt.day_date = po_ev.event_date_pacific
    WHERE (hdr.status IN ('APPROVED', 'WORKSHEET') 
       OR (hdr.status = 'CLOSED' AND hdr.otb_eow_date >= current_date - 395))
      AND (po_ev.event_date_pacific >= '2024-01-10'               -- date data is validated/signed off on [confirming]
      AND (po_ev.buyer_quantity_canceled > 0 
       OR po_ev.vendor_quantity_canceled > 0
       OR po_ev.auto_quantity_canceled > 0
       OR po_ev.other_quantity_canceled > 0))
    ) b 
GROUP BY 1,2,3,4,5
WHERE total_rn = 1 OR buyer_rn = 1 OR vendor_rn = 1 OR auto_rn = 1 OR other_rn = 1
) WITH DATA 
PRIMARY INDEX (purchase_order_number, ship_location_id, rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, ship_location_id, rms_sku_num)
    ON cancels_base
;

-- DROP TABLE cancels_base_sku;
CREATE MULTISET VOLATILE TABLE cancels_base_sku AS (
SELECT 
     purchase_order_number
    ,rms_sku_num
    ,channel_num
    ,month_idnt
    ,month_label
    ,SUM(total_cancel_u) AS total_cancel_u
    ,SUM(buyer_cancel_u) AS buyer_cancel_u
    ,SUM(vendor_cancel_u) AS vendor_cancel_u
    ,SUM(auto_cancel_u) AS auto_cancel_u
    ,SUM(other_cancel_u) AS other_cancel_u
    ,CAST(0 AS INTEGER) AS inbound_rcpt_u
    ,CAST(0 AS INTEGER) AS outbound_rcpt_u
    ,CAST(0 AS DECIMAL(38,4)) AS outbound_rcpt_c
    ,CAST(0 AS INTEGER) AS on_order_u
FROM (
        SELECT 
             cn.purchase_order_number
            ,cn.month_idnt
            ,cn.month_label
            ,cn.rms_sku_num
            ,cn.ship_location_id
            ,d.channel_num
            -- cancels
                -- total
            ,CASE WHEN cn.total_cancel_event > 0 THEN cn.total_cancel_event - ZEROIFNULL(LAG(cn.total_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt)) 
                      ELSE 0 END AS total_cancel_u
                -- buyer
            ,CASE WHEN cn.buyer_cancel_event > 0 THEN cn.buyer_cancel_event - ZEROIFNULL(LAG(cn.buyer_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt)) 
                      ELSE 0 END AS buyer_cancel_u
                -- vendor
            ,CASE WHEN cn.vendor_cancel_event > 0 THEN cn.vendor_cancel_event - ZEROIFNULL(LAG(cn.vendor_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt)) 
                      ELSE 0 END AS vendor_cancel_u
                -- auto
            ,CASE WHEN cn.auto_cancel_event > 0 THEN cn.auto_cancel_event - ZEROIFNULL(LAG(cn.auto_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt)) 
                      ELSE 0 END AS auto_cancel_u
                -- other
            ,CASE WHEN cn.other_cancel_event > 0 THEN cn.other_cancel_event - ZEROIFNULL(LAG(cn.other_cancel_event) OVER (PARTITION BY cn.purchase_order_number, cn.rms_sku_num, cn.ship_location_id ORDER BY cn.month_idnt)) 
                      ELSE 0 END AS other_cancel_u
        FROM cancels_base cn
        JOIN prd_nap_usr_vws.store_dim d
          ON cn.ship_location_id = d.store_num
    ) base
GROUP BY 1,2,3,4,5
) WITH DATA
PRIMARY INDEX (purchase_order_number, rms_sku_num, channel_num) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num, channel_num)
    ON cancels_base_sku
;

-- cancels
-- DROP TABLE cancels_sku;
CREATE MULTISET VOLATILE TABLE cancels_sku AS (
SELECT 
     purchase_order_number
    ,rms_sku_num
    ,month_idnt
    ,month_label
    ,SUM(total_cancel_u) AS total_cancel_u
    ,SUM(buyer_cancel_u) AS buyer_cancel_u
    ,SUM(vendor_cancel_u) AS vendor_cancel_u
    ,SUM(auto_cancel_u) AS auto_cancel_u
    ,SUM(other_cancel_u) AS other_cancel_u
    ,CAST(0 AS INTEGER) AS inbound_rcpt_u
    ,CAST(0 AS INTEGER) AS outbound_rcpt_u
    ,CAST(0 AS DECIMAL(38,4)) AS outbound_rcpt_c
    ,CAST(0 AS INTEGER) AS on_order_u
FROM cancels_base_sku 
GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (purchase_order_number, rms_sku_num) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num)
ON cancels_sku;
 
-- DROP TABLE dc_sku;
CREATE MULTISET VOLATILE TABLE dc_sku AS (
SELECT 
     e.purchase_order_number AS purchase_order_number
    ,e.rms_sku_num
    ,c.month_idnt
    ,c.month_label
    ,CAST(0 AS INTEGER) AS total_cancel_u
    ,CAST(0 AS INTEGER) AS buyer_cancel_u
    ,CAST(0 AS INTEGER) AS vendor_cancel_u
    ,CAST(0 AS INTEGER) AS auto_cancel_u
    ,CAST(0 AS INTEGER) AS other_cancel_u
    ,SUM(e.shipment_qty) AS inbound_rcpt_u
    ,CAST(0 AS INTEGER) AS outbound_rcpt_u
    ,CAST(0 AS DECIMAL(38,4)) AS outbound_rcpt_c
    ,CAST(0 AS INTEGER) AS on_order_u
FROM prd_nap_usr_vws.wm_inbound_carton_fact_vw e
JOIN prd_nap_usr_vws.purchase_order_header_fact hdr
  ON e.purchase_order_number = hdr.purchase_order_number
JOIN prd_nap_usr_vws.day_cal_454_dim c
  ON CAST(e.received_tmstp AS DATE) = c.day_date
WHERE location_id NOT IN ('889','859','896','869','891','868')
  AND (hdr.status IN ('APPROVED', 'WORKSHEET') 
   OR (hdr.status = 'CLOSED' 
  AND otb_eow_date >= current_date - 395))
  AND e.shipment_qty <> 0 
  AND CAST(e.received_tmstp AS DATE) >= '2024-01-10'
  AND e.location_type_code = 'S'
  AND hdr.otb_eow_date >= CURRENT_DATE - 180                                -- removes cleanup Inbound Rcpts
GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (purchase_order_number, rms_sku_num) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num)
    ON dc_sku
;

-- outbound Receipts & OO 
-- DROP TABLE out_oo_sku;
CREATE MULTISET VOLATILE TABLE out_oo_sku AS (
SELECT
     ob.purchase_order_num AS purchase_order_number
    ,ob.sku_num AS rms_sku_num
    ,wk.month_idnt
    ,wk.month_label
    ,CAST(0 AS INTEGER) AS total_cancel_u
    ,CAST(0 AS INTEGER) AS buyer_cancel_u
    ,CAST(0 AS INTEGER) AS vendor_cancel_u
    ,CAST(0 AS INTEGER) AS auto_cancel_u
    ,CAST(0 AS INTEGER) AS other_cancel_u
    ,CAST(0 AS INTEGER) AS inbound_rcpt_u
    ,SUM(rcpt_units) AS outbound_rcpt_u
    ,SUM(rcpt_cost) AS outbound_rcpt_c
    ,SUM(quantity_open) AS on_order_u
FROM t2dl_das_open_to_buy.outbound_po_sku_lkp ob
JOIN (
        SELECT
             week_idnt
            ,month_idnt
            ,month_label
        FROM prd_nap_usr_vws.day_cal_454_dim
        WHERE day_date >= DATE '2024-01-10'
        GROUP BY 1,2,3
    ) wk
  ON ob.week_idnt = wk.week_idnt
GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (purchase_order_number, rms_sku_num) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num)
    ON out_oo_sku
;


-- Combine
-- DROP TABLE pos_sku_base;
CREATE MULTISET VOLATILE TABLE pos_sku_base AS (
WITH metrics AS (
    SELECT 
         purchase_order_number
        ,rms_sku_num
        ,month_idnt
        ,month_label
        ,SUM(COALESCE(total_cancel_u, 0)) AS total_cancel_u
        ,SUM(COALESCE(buyer_cancel_u, 0)) AS buyer_cancel_u
        ,SUM(COALESCE(vendor_cancel_u, 0)) AS vendor_cancel_u
        ,SUM(COALESCE(auto_cancel_u, 0)) AS auto_cancel_u
        ,SUM(COALESCE(other_cancel_u, 0)) AS other_cancel_u
        ,SUM(COALESCE(inbound_rcpt_u, 0)) AS inbound_rcpt_u
        ,SUM(COALESCE(outbound_rcpt_u, 0)) AS outbound_rcpt_u
        ,SUM(COALESCE(outbound_rcpt_c, 0)) AS outbound_rcpt_c
        ,SUM(COALESCE(on_order_u, 0)) AS on_order_u
    FROM (
            SELECT * 
            FROM cancels_sku

            UNION ALL 

            SELECT * 
            FROM out_oo_sku

            UNION ALL 

            SELECT * 
            FROM dc_sku
        ) skus
    GROUP BY 1,2,3,4
)
SELECT 
     m.purchase_order_number
    ,hdr.status
    ,otb.month_idnt AS otb_month_idnt
    ,otb.month_label AS otb_month_label
    ,m.month_idnt
    ,m.month_label AS action_month_label
    ,m.rms_sku_num
    ,h.div_idnt
    ,h.div_desc
    ,h.grp_idnt
    ,h.grp_desc
    ,h.dept_idnt
    ,h.dept_desc
    ,TRIM(h.div_idnt) || ': ' || h.div_desc AS Division
    ,TRIM(h.grp_idnt) || ': ' || h.grp_desc AS Subdivision
    ,TRIM(h.dept_idnt) || ': ' || h.dept_desc AS Department
    ,h.quantrix_category
    ,h.vendor_name
    ,h.npg_ind
    ,CASE WHEN hdr.order_type IN ('AUTOMATIC_REORDER', 'BUYER_REORDER') THEN 'RP' ELSE 'NRP' END AS rp_ind
    ,h.customer_choice
    ,hdr.po_type
    ,hdr.purchase_type
    ,hdr.comments
    ,hdr.internal_po_ind
    ,COALESCE(po.parent_po_ind, 'N') AS parent_po_ind
    ,COALESCE(po.xref_po_ind, 'N') AS xref_po_ind
    ,COALESCE(po.reversal_xref, 0) AS reversal_xref
    ,po.parent_po
    ,po_fct.unit_cost
    ,po_fct.total_expenses_per_unit
    ,po_fct.total_duty_per_unit
    ,COALESCE(po_fct.original_quantity_ordered, 0) AS original_ordered_u
    ,(original_ordered_u * po_fct.unit_cost) + (original_ordered_u * po_fct.total_expenses_per_unit) + (original_ordered_u * po_fct.total_duty_per_unit) AS original_ordered_c
    ,COALESCE(po_fct.approved_quantity_ordered, 0) AS approved_ordered_u
    ,(approved_ordered_u * po_fct.unit_cost) + (approved_ordered_u * po_fct.total_expenses_per_unit) + (approved_ordered_u * po_fct.total_duty_per_unit) AS approved_ordered_c
    ,COALESCE(po_fct.quantity_ordered, 0) AS ordered_u
    ,(ordered_u * po_fct.unit_cost) + (ordered_u * po_fct.total_expenses_per_unit) + (ordered_u * po_fct.total_duty_per_unit) AS ordered_c
    ,COALESCE(m.vendor_cancel_u + m.auto_cancel_u, 0) AS supplier_cancel_u
    ,(supplier_cancel_u * po_fct.unit_cost) + (supplier_cancel_u * po_fct.total_expenses_per_unit) + (supplier_cancel_u * po_fct.total_duty_per_unit) AS supplier_cancel_c
    ,COALESCE(m.buyer_cancel_u, 0) AS buyer_cancel_u
    ,(COALESCE(m.buyer_cancel_u, 0) * po_fct.unit_cost) + (COALESCE(m.buyer_cancel_u, 0) * po_fct.total_expenses_per_unit) + (COALESCE(m.buyer_cancel_u, 0) * po_fct.total_duty_per_unit) AS buyer_cancel_c
    ,COALESCE(m.other_cancel_u, 0) AS other_cancel_u
    ,(COALESCE(m.other_cancel_u, 0) * po_fct.unit_cost) + (COALESCE(m.other_cancel_u, 0) * po_fct.total_expenses_per_unit) + (COALESCE(m.other_cancel_u, 0) * po_fct.total_duty_per_unit) AS other_cancel_c
    ,COALESCE(m.inbound_rcpt_u, 0) AS inbound_rcpt_u
    ,(COALESCE(m.inbound_rcpt_u, 0) * po_fct.unit_cost) + (COALESCE(m.inbound_rcpt_u, 0) * po_fct.total_expenses_per_unit) + (COALESCE(m.inbound_rcpt_u, 0) * po_fct.total_duty_per_unit) AS inbound_rcpt_c
    ,COALESCE(m.outbound_rcpt_u, 0) AS outbound_rcpt_u
    ,COALESCE(m.outbound_rcpt_c, 0) AS outbound_rcpt_c
    ,COALESCE(m.on_order_u, 0) AS on_order_u
    ,(COALESCE(m.on_order_u, 0) * po_fct.unit_cost) + (COALESCE(m.on_order_u, 0) * po_fct.total_expenses_per_unit) + (COALESCE(m.on_order_u, 0) * po_fct.total_duty_per_unit) AS on_order_c
FROM metrics m
JOIN prd_nap_usr_vws.purchase_order_header_fact hdr
  ON m.purchase_order_number = hdr.purchase_order_number
JOIN prd_nap_usr_vws.day_cal_454_dim otb
  ON hdr.otb_eow_date = otb.day_date
LEFT JOIN (
        SELECT
             purchase_order_number
            ,rms_sku_num
            ,SUM(COALESCE(po_fct.approved_quantity_ordered, 0)) AS approved_quantity_ordered
            ,SUM(COALESCE(po_fct.original_quantity_ordered, 0)) AS original_quantity_ordered
            ,SUM(COALESCE(po_fct.quantity_ordered, 0)) AS quantity_ordered
            ,AVG(po_fct.unit_cost) AS unit_cost
            ,AVG(po_fct.total_expenses_per_unit) AS total_expenses_per_unit
            ,AVG(po_fct.total_duty_per_unit) AS total_duty_per_unit
        FROM prd_nap_usr_vws.purchase_order_shiplocation_fact po_fct
        GROUP BY 1,2
    ) po_fct
  ON m.purchase_order_number = po_fct.purchase_order_number
 AND m.rms_sku_num = po_fct.rms_sku_num
LEFT JOIN internal_po_staging po
  ON po.order_no = m.purchase_order_number
JOIN t2dl_das_assortment_dim.assortment_hierarchy h
  ON m.rms_sku_num = h.sku_idnt
 AND h.channel_country = 'US'
) WITH DATA 
PRIMARY INDEX(purchase_order_number, rms_sku_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num)
    ON pos_sku_base
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num)
    ON pos_sku_base
;

-- Identify False Cancels based off 4 scenarios (https://confluence.nordstrom.com/display/SABA/Monthly+Receipt+Recon+-+Enhancement+-+Supplier+Cancel%3A+Remove+Double+Booking)
-- DROP TABLE false_cancels;
CREATE MULTISET VOLATILE TABLE false_cancels AS (
SELECT
     parent_po
    ,month_idnt
    ,rms_sku_num
    ,SUM(xref_inbound_rcpt_u) AS xref_inbound_rcpt_u
    ,SUM(xref_outbound_rcpt_u) AS xref_outbound_rcpt_u
    ,SUM(xref_on_order_u) AS xref_on_order_u
    ,MAX(parent_on_order_u) AS parent_on_order_u
    ,SUM(invalid) AS invalid
    ,SUM(partial_invalid) AS partial_invalid
    ,SUM(reversal) AS reversal
    ,SUM(potential) AS potential
    ,SUM(potential_oo) AS potential_oo 
FROM (
        SELECT 
             p.parent_po
            ,p.month_idnt
            ,x.purchase_order_number AS xref_po
            ,p.status AS parent_status
            ,x.status AS xref_status
            ,p.rms_sku_num
            ,COALESCE(p.supplier_cancel_u, 0) AS parent_supp_cancel_u
            ,COALESCE(x.inbound_rcpt_u, 0) AS xref_inbound_rcpt_u
            ,COALESCE(p.inbound_rcpt_u, 0) AS parent_inbound_rcpt_u
            ,COALESCE(x.outbound_rcpt_u, 0) AS xref_outbound_rcpt_u
            ,COALESCE(p.outbound_rcpt_u, 0) AS parent_outbound_rcpt_u
            ,COALESCE(x.on_order_u, 0) AS xref_on_order_u
            ,COALESCE(p.on_order_u, 0) AS parent_on_order_u
            ,CASE -- invalid
                  -- Parent Cancels = Xref Outbound Receipts
                  WHEN p.status = 'CLOSED'
                   AND x.status = 'CLOSED'
                   AND x.reversal_xref = 0 
                   AND parent_supp_cancel_u > 0
                   AND parent_supp_cancel_u = xref_outbound_rcpt_u 
                  THEN parent_supp_cancel_u
                  ELSE 0 END AS invalid
                  -- Parent Cancels > Xref Outbound Receipts
            , CASE WHEN p.status = 'CLOSED'
                   AND x.status = 'CLOSED'
                   AND x.reversal_xref = 0 
                   AND parent_supp_cancel_u > 0
                   AND xref_outbound_rcpt_u > 0
                   AND parent_supp_cancel_u > xref_outbound_rcpt_u 
                  THEN xref_outbound_rcpt_u
                    -- Parent Cancels < Xref Outbound Receipts
                  WHEN p.status = 'CLOSED'
                   AND x.status = 'CLOSED'
                   AND x.reversal_xref = 0 
                   AND parent_supp_cancel_u > 0
                   AND xref_outbound_rcpt_u > 0
                   AND parent_supp_cancel_u < xref_outbound_rcpt_u 
                  THEN parent_supp_cancel_u
                  ELSE 0 END AS partial_invalid
            ,CASE -- Reversal (XRef opened after OG Closed)
                  WHEN p.status = 'CLOSED'
                   AND x.status = 'CLOSED'
                   AND x.reversal_xref = 1
                   AND x.reversal_xref = 0 
                   AND parent_supp_cancel_u > 0
                   AND xref_outbound_rcpt_u > 0
                   AND parent_supp_cancel_u > xref_outbound_rcpt_u 
                  THEN xref_outbound_rcpt_u 
                  WHEN p.status = 'CLOSED'
                   AND x.status = 'CLOSED'
                   AND x.reversal_xref = 1
                   AND parent_supp_cancel_u > 0
                   AND xref_outbound_rcpt_u > 0
                   AND parent_supp_cancel_u <= xref_outbound_rcpt_u 
                   THEN parent_supp_cancel_u 
                  ELSE 0 END AS reversal
            ,CASE -- Preventative
                  -- Parent Cancels <> Xref Inbound Receipts
                  WHEN p.status = 'APPROVED'
                   AND x.status = 'APPROVED'
                   AND parent_supp_cancel_u > 0
                   AND xref_inbound_rcpt_u > 0
                   AND parent_supp_cancel_u < xref_inbound_rcpt_u 
                  THEN parent_supp_cancel_u 
                  WHEN p.status = 'APPROVED'
                   AND x.status = 'APPROVED'
                   AND parent_supp_cancel_u > 0
                   AND xref_inbound_rcpt_u > 0
                   AND parent_supp_cancel_u >= xref_inbound_rcpt_u 
                  THEN xref_inbound_rcpt_u
                  ELSE 0 END AS potential
            ,CASE -- Preventative OO
                  -- Parent Cancels <> On Order
                  WHEN p.status = 'CLOSED'
                   AND x.status = 'APPROVED'
                   AND parent_supp_cancel_u > 0
                   AND xref_on_order_u > 0
                   AND parent_supp_cancel_u < xref_on_order_u 
                  THEN parent_supp_cancel_u 
                  WHEN p.status = 'CLOSED'
                   AND x.status = 'APPROVED'
                   AND parent_supp_cancel_u > 0
                   AND xref_on_order_u > 0
                   AND parent_supp_cancel_u >= xref_on_order_u 
                  THEN xref_on_order_u
                  ELSE 0 END AS potential_oo
            
        FROM (
                SELECT * 
                FROM pos_sku_base
                WHERE parent_po_ind = 'Y'
                  AND supplier_cancel_u > 0
             ) p
        JOIN (
                SELECT 
                     parent_po
                    ,purchase_order_number
                    ,rms_sku_num
                    ,status
                    ,MAX(reversal_xref) AS reversal_xref
                    ,SUM(inbound_rcpt_u) AS inbound_rcpt_u
                    ,SUM(outbound_rcpt_u) AS outbound_rcpt_u
                    ,SUM(on_order_u) AS on_order_u
                FROM pos_sku_base
                WHERE xref_po_ind = 'Y' 
                   OR internal_po_ind = 'T'
                GROUP BY 1,2,3,4
             ) x
          ON x.parent_po = p.purchase_order_number
         AND x.rms_sku_num = p.rms_sku_num
    ) a
GROUP BY 1,2,3
) WITH DATA 
PRIMARY INDEX(parent_po, rms_sku_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
    PRIMARY INDEX (parent_po, rms_sku_num)
    ON false_cancels
;

-- Get Banner
CREATE MULTISET VOLATILE TABLE po_dist AS (
SELECT 
     dist.purchase_order_num AS purchase_order_number
    ,dist.rms_sku_num
    ,st.channel_brand AS banner
FROM prd_nap_usr_vws.purchase_order_item_distributelocation_fact dist
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON dist.distribute_location_id = st.store_num
JOIN (SELECT DISTINCT purchase_order_number FROM pos_sku_base) c
  ON c.purchase_order_number = dist.purchase_order_num
GROUP BY 1,2,3
QUALIFY ROW_NUMBER() OVER (PARTITION BY dist.purchase_order_num, dist.rms_sku_num ORDER BY count(*) DESC) = 1		-- take channel with most records in case of dups
) WITH DATA
PRIMARY INDEX(purchase_order_number, rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num)
    ON po_dist
;

-- Combine all with base table 
DELETE FROM {environment_schema}.po_sku_cancels ALL;

INSERT INTO {environment_schema}.po_sku_cancels
SELECT 
     d.banner
    ,b.*
    ,COALESCE(c.xref_inbound_rcpt_u, 0) AS xref_inbound_rcpt_u
    ,COALESCE(c.xref_outbound_rcpt_u, 0) AS xref_outbound_rcpt_u
    ,COALESCE(c.xref_on_order_u, 0) AS xref_on_order_u
    ,COALESCE(c.invalid, 0) AS invalid_cancel_u
    ,(invalid_cancel_u * unit_cost) + (invalid_cancel_u * total_expenses_per_unit) + (invalid_cancel_u * total_duty_per_unit) AS invalid_cancel_c
    ,COALESCE(c.partial_invalid, 0) AS partial_invalid_cancel_u
    ,(partial_invalid_cancel_u * unit_cost) + (partial_invalid_cancel_u * total_expenses_per_unit) + (partial_invalid_cancel_u * total_duty_per_unit) AS partial_invalid_cancel_c
    ,COALESCE(c.reversal, 0) AS reversal_cancel_u
    ,(reversal_cancel_u * unit_cost) + (reversal_cancel_u * total_expenses_per_unit) + (reversal_cancel_u * total_duty_per_unit) AS reversal_cancel_c
    ,COALESCE(c.potential, 0) AS potential_cancel_u
    ,(potential_cancel_u * unit_cost) + (potential_cancel_u * total_expenses_per_unit) + (potential_cancel_u * total_duty_per_unit) AS potential_cancel_c
    ,COALESCE(c.potential_oo, 0) AS potential_oo_cancel_u
    ,(potential_oo_cancel_u * unit_cost) + (potential_oo_cancel_u * total_expenses_per_unit) + (potential_oo_cancel_u * total_duty_per_unit) AS potential_oo_cancel_c
    ,CASE WHEN internal_po_ind = 'T' AND status = 'CLOSED' THEN supplier_cancel_u
          WHEN invalid_cancel_u + partial_invalid_cancel_u + reversal_cancel_u > supplier_cancel_u THEN supplier_cancel_u
          ELSE invalid_cancel_u + partial_invalid_cancel_u + reversal_cancel_u 
          END AS false_cancel_u
    ,(false_cancel_u * unit_cost) + (false_cancel_u * total_expenses_per_unit) + (false_cancel_u * total_duty_per_unit) AS false_cancel_c
    ,CASE WHEN internal_po_ind = 'T' AND status = 'APPROVED' THEN supplier_cancel_u
          WHEN potential_cancel_u + potential_oo_cancel_u > supplier_cancel_u THEN supplier_cancel_u
          ELSE potential_cancel_u + potential_oo_cancel_u 
          END AS potential_false_cancel_u
    ,(potential_false_cancel_u * unit_cost) + (potential_false_cancel_u * total_expenses_per_unit) + (potential_false_cancel_u * total_duty_per_unit) AS potential_false_cancel_c
    ,supplier_cancel_u - false_cancel_u AS true_cancel_u
    ,supplier_cancel_c - false_cancel_c AS true_cancel_c
    ,true_cancel_u - potential_false_cancel_u AS potential_true_cancel_u
    ,true_cancel_c - potential_false_cancel_c AS potential_true_cancel_c
    ,current_timestamp AS update_timestamp
FROM pos_sku_base b
JOIN po_dist d
  ON b.purchase_order_number = d.purchase_order_number
 AND b.rms_sku_num = d.rms_sku_num
LEFT JOIN false_cancels c
  ON b.purchase_order_number = c.parent_po
 AND b.rms_sku_num = c.rms_sku_num
 AND b.month_idnt = c.month_idnt
;