CREATE TEMPORARY TABLE IF NOT EXISTS date_filter
CLUSTER BY month_rank
AS
SELECT dt.month_start_day_date,
 dt.month_idnt,
 dt.month_label,
 rnk.month_rank
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dt
 INNER JOIN (SELECT month_idnt,
   DENSE_RANK() OVER (ORDER BY month_idnt DESC) AS month_rank
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 70 DAY) AND (CURRENT_DATE)
  QUALIFY (DENSE_RANK() OVER (ORDER BY month_idnt DESC)) <= 2) AS rnk ON dt.month_idnt = rnk.month_idnt
GROUP BY dt.month_start_day_date,
 dt.month_idnt,
 dt.month_label,
 rnk.month_rank;


--COLLECT STATS  	PRIMARY INDEX (month_rank) 	,COLUMN (month_start_day_date) 	,COLUMN (month_label) 		ON date_filter


CREATE TEMPORARY TABLE IF NOT EXISTS hier
CLUSTER BY rms_sku_num, channel_country
AS
SELECT sku.rms_sku_num,
 sku.channel_country,
   TRIM(FORMAT('%11d', dept.division_num)) || ', ' || dept.division_short_name AS division,
   TRIM(FORMAT('%11d', dept.subdivision_num)) || ', ' || dept.subdivision_short_name AS subdivision,
   TRIM(FORMAT('%11d', dept.dept_num)) || ', ' || dept.dept_short_name AS department,
   TRIM(FORMAT('%11d', sku.class_num)) || ', ' || sku.class_desc AS class,
   TRIM(FORMAT('%11d', sku.sbclass_num)) || ', ' || sku.sbclass_desc AS subclass,
 sku.supp_part_num AS vpn,
 sku.style_desc,
   sku.supp_part_num || ', ' || sku.style_desc AS vpn_label,
 sku.color_num AS nrf_color_code,
 sku.color_desc,
 sku.supp_color AS supplier_color,
 sku.smart_sample_ind,
 sku.gwp_ind,
 sku.prmy_supp_num,
  CASE
  WHEN LOWER(sku.prmy_supp_num) IN (LOWER('5126449'), LOWER('5126999'), LOWER('5100390'), LOWER('5120919'), LOWER('5152177'
     ), LOWER('5161101'), LOWER('5161102'), LOWER('5176832'), LOWER('150644198'), LOWER('5104976'), LOWER('5181094'),
    LOWER('5181095'), LOWER('5181089'), LOWER('5181089'), LOWER('5181091'), LOWER('5181090'), LOWER('5181093'), LOWER('5181100'
     ), LOWER('5183652'), LOWER('5166161'), LOWER('5181099'), LOWER('5183653'), LOWER('5091625'), LOWER('5178535'),
    LOWER('5171696'), LOWER('5183720'), LOWER('5183719'), LOWER('5124721'), LOWER('5178414'), LOWER('5096063'), LOWER('5178075'
     ), LOWER('5181600'), LOWER('829373414'), LOWER('5105467'), LOWER('5074184'), LOWER('5128244'), LOWER('782328831'),
    LOWER('5088180'), LOWER('5104979'), LOWER('5154667'), LOWER('5154668'), LOWER('217647238'), LOWER('5111027'), LOWER('5172126'
     ), LOWER('5171642'), LOWER('5099697'), LOWER('5104036'), LOWER('5109967'), LOWER('281265053'), LOWER('5155947'),
    LOWER('46737022'), LOWER('43121461'), LOWER('463750533'), LOWER('5104978'), LOWER('5082460'), LOWER('459799368'),
    LOWER('5104980'), LOWER('5160819'), LOWER('5167910'), LOWER('5113519'), LOWER('5104600'), LOWER('5090481'), LOWER('5090361'
     ), LOWER('5090341'), LOWER('5090272'))
  THEN 'Y'
  ELSE 'N'
  END AS prepaid_supplier_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dept ON sku.dept_num = dept.dept_num;


--COLLECT STATS  	PRIMARY INDEX (rms_sku_num, channel_country) 	,COLUMN (rms_sku_num, channel_country) 		ON hier

CREATE TEMPORARY TABLE IF NOT EXISTS dist_org
CLUSTER BY store_num
AS
SELECT DISTINCT store_num,
 banner_country,
 TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel
FROM `{{params.gcp_project_id}}`.t2dl_das_phase_zero.ban_cntry_store_dim_vw;


--COLLECT STATS  	PRIMARY INDEX (store_num) 	,COLUMN (store_num) 		ON dist_org


CREATE TEMPORARY TABLE IF NOT EXISTS hist_po
CLUSTER BY purchase_order_num
AS
SELECT DISTINCT purchase_order_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact
WHERE close_date < DATE '2022-04-23'
 AND (LOWER(dropship_ind) = LOWER('f') OR dropship_ind IS NULL)
 AND LOWER(sendonly_ticket_partner_edi_ind) = LOWER('f');


--COLLECT STATS  	PRIMARY INDEX (purchase_order_num) 	,COLUMN (purchase_order_num) 		ON hist_po



CREATE TEMPORARY TABLE IF NOT EXISTS main 
 CLUSTER BY month_start_day_date, purchase_order_number
AS
SELECT bom_otb_eow_date,
 bom_status,
 bom_unit_cost,
 bom_total_duty_per_unit,
 bom_total_expenses_per_unit,
 edi_ind,
 end_ship_date,
 eom_otb_eow_date,
 eom_status,
 eom_unit_cost,
 eom_total_duty_per_unit,
 eom_total_expenses_per_unit,
 exclude_from_backorder_avail_ind,
 first_approval_event_tmstp_pacific,
 import_country,
 latest_edi_date,
 month_end_day_date,
 month_start_day_date,
 npg_ind,
 internal_po_ind,
 order_from_vendor_id,
 order_type,
 original_end_ship_date,
 original_quantity_ordered,
 ownership_price_amt,
 po_type,
 po_vasn_signal,
 purchase_order_number,
 epo_purchase_order_id,
 purchase_type,
 rms_sku_num,
 ship_location_id,
 start_ship_date,
 written_date,
 COALESCE(bom_quantity_canceled, 0) AS bom_quantity_canceled,
 COALESCE(bom_quantity_ordered, 0) AS bom_quantity_ordered,
 COALESCE(bom_receipt_qty, 0) AS bom_receipt_qty,
 COALESCE(bom_vasn_qty, 0) AS bom_vasn_qty,
 COALESCE(eom_quantity_canceled, 0) AS eom_quantity_canceled,
 COALESCE(eom_quantity_ordered, 0) AS eom_quantity_ordered,
 COALESCE(eom_receipt_qty, 0) AS eom_receipt_qty,
 COALESCE(eom_vasn_qty, 0) AS eom_vasn_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.receipt_recon_fact_vw
WHERE month_start_day_date >= (SELECT MIN(month_start_day_date)
   FROM date_filter);





--COLLECT STATS  	PRIMARY INDEX (month_start_day_date, purchase_order_number) 	,COLUMN (month_start_day_date) 	,COLUMN (purchase_order_number) 		ON main


CREATE TEMPORARY TABLE IF NOT EXISTS staging1 
 CLUSTER BY purchase_order_number, rms_sku_num, import_country
AS
SELECT COALESCE(lmo.purchase_order_number, cmo.purchase_order_number) AS purchase_order_number,
 COALESCE(lmo.rms_sku_num, cmo.rms_sku_num) AS rms_sku_num,
 COALESCE(lmo.ship_location_id, cmo.ship_location_id) AS ship_location_id,
 COALESCE(lmo.purchase_type, cmo.purchase_type) AS purchase_type,
 COALESCE(lmo.edi_ind, cmo.edi_ind) AS edi_ind,
 COALESCE(lmo.first_approval_event_tmstp_pacific, cmo.first_approval_event_tmstp_pacific) AS
 first_approval_event_tmstp_pacific,
 COALESCE(lmo.original_end_ship_date, cmo.original_end_ship_date) AS original_end_ship_date,
 COALESCE(lmo.ownership_price_amt, cmo.ownership_price_amt) AS ownership_price_amt,
 COALESCE(lmo.written_date, cmo.written_date) AS written_date,
 COALESCE(cmo.latest_edi_date, lmo.latest_edi_date) AS latest_edi_date,
 COALESCE(cmo.npg_ind, lmo.npg_ind) AS npg_ind,
 COALESCE(cmo.internal_po_ind, lmo.internal_po_ind) AS internal_po_ind,
 COALESCE(cmo.exclude_from_backorder_avail_ind, lmo.exclude_from_backorder_avail_ind) AS
 exclude_from_backorder_avail_ind,
 COALESCE(cmo.po_vasn_signal, lmo.po_vasn_signal) AS po_vasn_signal,
 COALESCE(cmo.po_type, lmo.po_type) AS po_type,
 COALESCE(cmo.order_type, lmo.order_type) AS order_type,
 COALESCE(cmo.order_from_vendor_id, lmo.order_from_vendor_id) AS order_from_vendor_id,
 COALESCE(cmo.import_country, lmo.import_country) AS import_country,
 lmo.month_start_day_date AS recon_month_start_day_date,
 lmo.month_end_day_date AS recon_month_end_day_date,
 lmo.bom_otb_eow_date,
 lmo.bom_status,
 lmo.bom_unit_cost,
 lmo.bom_total_duty_per_unit,
 lmo.bom_total_expenses_per_unit,
 cmo.eom_otb_eow_date,
 cmo.eom_status,
 cmo.eom_unit_cost,
 cmo.eom_total_duty_per_unit,
 cmo.eom_total_expenses_per_unit,
 lmo.start_ship_date AS eom_start_ship_date,
 lmo.end_ship_date AS eom_end_ship_date,
 cmo.eom_otb_eow_date AS cur_otb_eow_date,
 cmo.eom_status AS cur_status,
 cmo.eom_unit_cost AS cur_unit_cost,
 cmo.eom_total_duty_per_unit AS cur_total_duty_per_unit,
 cmo.eom_total_expenses_per_unit AS cur_total_expenses_per_unit,
 cmo.start_ship_date AS cur_start_ship_date,
 cmo.end_ship_date AS cur_end_ship_date,
 SUM(lmo.original_quantity_ordered) AS original_quantity_ordered,
 SUM(lmo.bom_quantity_canceled) AS bom_quantity_canceled,
 SUM(lmo.bom_quantity_ordered) AS bom_quantity_ordered,
 SUM(lmo.bom_receipt_qty) AS bom_receipt_qty,
 SUM(lmo.bom_vasn_qty) AS bom_vasn_qty,
 SUM(lmo.eom_quantity_canceled) AS eom_quantity_canceled,
 SUM(lmo.eom_quantity_ordered) AS eom_quantity_ordered,
 SUM(lmo.eom_receipt_qty) AS eom_receipt_qty,
 SUM(lmo.eom_vasn_qty) AS eom_vasn_qty,
 SUM(cmo.eom_quantity_canceled) AS cur_quantity_canceled,
 SUM(cmo.eom_quantity_ordered) AS cur_quantity_ordered,
 SUM(cmo.eom_receipt_qty) AS cur_receipt_qty,
 SUM(cmo.eom_vasn_qty) AS cur_vasn_qty
FROM (SELECT *
  FROM main
  WHERE month_start_day_date = (SELECT DISTINCT month_start_day_date
     FROM date_filter
     WHERE month_rank = 2)
   AND purchase_order_number NOT IN (SELECT purchase_order_num
     FROM hist_po)) AS lmo
 FULL JOIN (SELECT *
  FROM main
  WHERE month_start_day_date = (SELECT DISTINCT month_start_day_date
     FROM date_filter
     WHERE month_rank = 1)
   AND purchase_order_number NOT IN (SELECT purchase_order_num
     FROM hist_po)) AS cmo ON LOWER(lmo.purchase_order_number) = LOWER(cmo.purchase_order_number) AND LOWER(lmo.rms_sku_num
     ) = LOWER(cmo.rms_sku_num) AND LOWER(lmo.ship_location_id) = LOWER(cmo.ship_location_id)
GROUP BY purchase_order_number,
 rms_sku_num,
 ship_location_id,
 purchase_type,
 edi_ind,
 first_approval_event_tmstp_pacific,
 original_end_ship_date,
 ownership_price_amt,
 written_date,
 latest_edi_date,
 npg_ind,
 internal_po_ind,
 exclude_from_backorder_avail_ind,
 po_vasn_signal,
 po_type,
 order_type,
 order_from_vendor_id,
 import_country,
 recon_month_start_day_date,
 recon_month_end_day_date,
 lmo.bom_otb_eow_date,
 lmo.bom_status,
 lmo.bom_unit_cost,
 lmo.bom_total_duty_per_unit,
 lmo.bom_total_expenses_per_unit,
 lmo.eom_otb_eow_date,
 lmo.eom_status,
 lmo.eom_unit_cost,
 lmo.eom_total_duty_per_unit,
 lmo.eom_total_expenses_per_unit,
 eom_start_ship_date,
 eom_end_ship_date,
 cur_otb_eow_date,
 cur_status,
 cur_unit_cost,
 cur_total_duty_per_unit,
 cur_total_expenses_per_unit,
 cur_start_ship_date,
 cur_end_ship_date;


--COLLECT STATS 	PRIMARY INDEX (purchase_order_number, rms_sku_num, import_country) 	,COLUMN (purchase_order_number, ship_location_id) 	,COLUMN (ship_location_id) 	,COLUMN (purchase_order_number) 	,COLUMN (rms_sku_num, import_country) 	,COLUMN (order_from_vendor_id) 	,COLUMN (recon_month_start_day_date) 	,COLUMN (cur_otb_eow_date) 		ON staging1

CREATE TEMPORARY TABLE IF NOT EXISTS voh
CLUSTER BY order_no
AS
SELECT purchase_order_num AS order_no,
 crossreference_external_id AS rms_xref_po,
 written_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact
WHERE written_date >= DATE_SUB(CURRENT_DATE, INTERVAL 365 DAY);


--COLLECT STATS  	PRIMARY INDEX (order_no) 	,COLUMN (rms_xref_po) 	,COLUMN (written_date) 		ON voh


CREATE TEMPORARY TABLE IF NOT EXISTS internal_orders_stage
CLUSTER BY order_no
AS
SELECT GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(COALESCE(CASE
           WHEN DATE_DIFF(voh.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                     WHEN voh.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
                     END, CASE
                     WHEN voh2.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
                     END), CASE
                    WHEN voh3.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
                    END), CASE
                   WHEN voh4.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
                   END), CASE
                  WHEN voh5.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
                  END), CASE
                 WHEN voh6.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
                 END), CASE
                WHEN voh7.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
                END), CASE
               WHEN voh8.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh9.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
              END)
           THEN voh.order_no
           ELSE NULL
           END, FORMAT('%4d', 0)), COALESCE(CASE
           WHEN DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                     WHEN voh.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
                     END, CASE
                     WHEN voh2.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
                     END), CASE
                    WHEN voh3.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
                    END), CASE
                   WHEN voh4.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
                   END), CASE
                  WHEN voh5.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
                  END), CASE
                 WHEN voh6.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
                 END), CASE
                WHEN voh7.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
                END), CASE
               WHEN voh8.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh9.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
              END)
           THEN voh2.order_no
           ELSE NULL
           END, FORMAT('%4d', 0))), COALESCE(CASE
          WHEN DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                    WHEN voh.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
                    END, CASE
                    WHEN voh2.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
                    END), CASE
                   WHEN voh3.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
                   END), CASE
                  WHEN voh4.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
                  END), CASE
                 WHEN voh5.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
                 END), CASE
                WHEN voh6.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
                END), CASE
               WHEN voh7.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh8.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
              END), CASE
             WHEN voh9.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
             END)
          THEN voh3.order_no
          ELSE NULL
          END, FORMAT('%4d', 0))), COALESCE(CASE
         WHEN DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                   WHEN voh.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
                   END, CASE
                   WHEN voh2.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
                   END), CASE
                  WHEN voh3.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
                  END), CASE
                 WHEN voh4.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
                 END), CASE
                WHEN voh5.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
                END), CASE
               WHEN voh6.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh7.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
              END), CASE
             WHEN voh8.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
             END), CASE
            WHEN voh9.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
            END)
         THEN voh4.order_no
         ELSE NULL
         END, FORMAT('%4d', 0))), COALESCE(CASE
        WHEN DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                  WHEN voh.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
                  END, CASE
                  WHEN voh2.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
                  END), CASE
                 WHEN voh3.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
                 END), CASE
                WHEN voh4.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
                END), CASE
               WHEN voh5.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh6.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
              END), CASE
             WHEN voh7.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
             END), CASE
            WHEN voh8.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
            END), CASE
           WHEN voh9.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
           END)
        THEN voh5.order_no
        ELSE NULL
        END, FORMAT('%4d', 0))), COALESCE(CASE
       WHEN DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                 WHEN voh.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
                 END, CASE
                 WHEN voh2.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
                 END), CASE
                WHEN voh3.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
                END), CASE
               WHEN voh4.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh5.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
              END), CASE
             WHEN voh6.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
             END), CASE
            WHEN voh7.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
            END), CASE
           WHEN voh8.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
           END), CASE
          WHEN voh9.written_date IS NULL
          THEN 0
          ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
          END)
       THEN voh6.order_no
       ELSE NULL
       END, FORMAT('%4d', 0))), COALESCE(CASE
      WHEN DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                WHEN voh.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
                END, CASE
                WHEN voh2.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
                END), CASE
               WHEN voh3.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh4.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
              END), CASE
             WHEN voh5.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
             END), CASE
            WHEN voh6.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
            END), CASE
           WHEN voh7.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
           END), CASE
          WHEN voh8.written_date IS NULL
          THEN 0
          ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
          END), CASE
         WHEN voh9.written_date IS NULL
         THEN 0
         ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
         END)
      THEN voh7.order_no
      ELSE NULL
      END, FORMAT('%4d', 0))), COALESCE(CASE
     WHEN DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
               WHEN voh.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
               END, CASE
               WHEN voh2.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
               END), CASE
              WHEN voh3.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
              END), CASE
             WHEN voh4.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
             END), CASE
            WHEN voh5.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
            END), CASE
           WHEN voh6.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
           END), CASE
          WHEN voh7.written_date IS NULL
          THEN 0
          ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
          END), CASE
         WHEN voh8.written_date IS NULL
         THEN 0
         ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
         END), CASE
        WHEN voh9.written_date IS NULL
        THEN 0
        ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
        END)
     THEN voh8.order_no
     ELSE NULL
     END, FORMAT('%4d', 0))), COALESCE(CASE
    WHEN DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
              WHEN voh.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh.written_date, CURRENT_DATE, DAY)
              END, CASE
              WHEN voh2.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE, DAY)
              END), CASE
             WHEN voh3.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE, DAY)
             END), CASE
            WHEN voh4.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE, DAY)
            END), CASE
           WHEN voh5.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE, DAY)
           END), CASE
          WHEN voh6.written_date IS NULL
          THEN 0
          ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE, DAY)
          END), CASE
         WHEN voh7.written_date IS NULL
         THEN 0
         ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE, DAY)
         END), CASE
        WHEN voh8.written_date IS NULL
        THEN 0
        ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE, DAY)
        END), CASE
       WHEN voh9.written_date IS NULL
       THEN 0
       ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE, DAY)
       END)
    THEN voh9.order_no
    ELSE NULL
    END, FORMAT('%4d', 0))) AS parent_po,
 voh.order_no
FROM voh
 INNER JOIN (SELECT purchase_order_num AS order_no
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_shiplocation_fact
  WHERE ship_location_id NOT IN (808, 828)
  GROUP BY order_no) AS ol ON LOWER(voh.order_no) = LOWER(ol.order_no)
 LEFT JOIN voh AS voh2 ON LOWER(voh.rms_xref_po) = LOWER(voh2.order_no)
 LEFT JOIN voh AS voh3 ON LOWER(CASE
    WHEN LOWER(voh2.rms_xref_po) = LOWER(voh.order_no)
    THEN NULL
    ELSE voh2.rms_xref_po
    END) = LOWER(voh3.order_no)
 LEFT JOIN voh AS voh4 ON LOWER(CASE
    WHEN LOWER(voh3.rms_xref_po) = LOWER(voh2.order_no)
    THEN NULL
    WHEN LOWER(voh3.rms_xref_po) = LOWER(voh.order_no)
    THEN NULL
    ELSE voh3.rms_xref_po
    END) = LOWER(voh4.order_no)
 LEFT JOIN voh AS voh5 ON LOWER(CASE
    WHEN LOWER(voh4.rms_xref_po) = LOWER(voh3.order_no)
    THEN NULL
    WHEN LOWER(voh4.rms_xref_po) = LOWER(voh2.order_no)
    THEN NULL
    WHEN LOWER(voh4.rms_xref_po) = LOWER(voh.order_no)
    THEN NULL
    ELSE voh4.rms_xref_po
    END) = LOWER(voh5.order_no)
 LEFT JOIN voh AS voh6 ON LOWER(CASE
    WHEN LOWER(voh5.rms_xref_po) = LOWER(voh4.order_no)
    THEN NULL
    WHEN LOWER(voh5.rms_xref_po) = LOWER(voh3.order_no)
    THEN NULL
    WHEN LOWER(voh5.rms_xref_po) = LOWER(voh2.order_no)
    THEN NULL
    WHEN LOWER(voh5.rms_xref_po) = LOWER(voh.order_no)
    THEN NULL
    ELSE voh5.rms_xref_po
    END) = LOWER(voh6.order_no)
 LEFT JOIN voh AS voh7 ON LOWER(CASE
    WHEN LOWER(voh6.rms_xref_po) = LOWER(voh5.order_no)
    THEN NULL
    WHEN LOWER(voh6.rms_xref_po) = LOWER(voh4.order_no)
    THEN NULL
    WHEN LOWER(voh6.rms_xref_po) = LOWER(voh3.order_no)
    THEN NULL
    WHEN LOWER(voh6.rms_xref_po) = LOWER(voh2.order_no)
    THEN NULL
    WHEN LOWER(voh6.rms_xref_po) = LOWER(voh.order_no)
    THEN NULL
    ELSE voh6.rms_xref_po
    END) = LOWER(voh7.order_no)
 LEFT JOIN voh AS voh8 ON LOWER(CASE
    WHEN LOWER(voh7.rms_xref_po) = LOWER(voh6.order_no)
    THEN NULL
    WHEN LOWER(voh7.rms_xref_po) = LOWER(voh5.order_no)
    THEN NULL
    WHEN LOWER(voh7.rms_xref_po) = LOWER(voh4.order_no)
    THEN NULL
    WHEN LOWER(voh7.rms_xref_po) = LOWER(voh3.order_no)
    THEN NULL
    WHEN LOWER(voh7.rms_xref_po) = LOWER(voh2.order_no)
    THEN NULL
    WHEN LOWER(voh7.rms_xref_po) = LOWER(voh.order_no)
    THEN NULL
    ELSE voh7.rms_xref_po
    END) = LOWER(voh8.order_no)
 LEFT JOIN voh AS voh9 ON LOWER(CASE
    WHEN LOWER(voh8.rms_xref_po) = LOWER(voh7.order_no)
    THEN NULL
    WHEN LOWER(voh8.rms_xref_po) = LOWER(voh6.order_no)
    THEN NULL
    WHEN LOWER(voh8.rms_xref_po) = LOWER(voh5.order_no)
    THEN NULL
    WHEN LOWER(voh8.rms_xref_po) = LOWER(voh4.order_no)
    THEN NULL
    WHEN LOWER(voh8.rms_xref_po) = LOWER(voh3.order_no)
    THEN NULL
    WHEN LOWER(voh8.rms_xref_po) = LOWER(voh2.order_no)
    THEN NULL
    WHEN LOWER(voh8.rms_xref_po) = LOWER(voh.order_no)
    THEN NULL
    ELSE voh8.rms_xref_po
    END) = LOWER(voh9.order_no);


--COLLECT STATS  	PRIMARY INDEX (order_no) 	,COLUMN (parent_po) 		ON internal_orders_stage


CREATE TEMPORARY TABLE IF NOT EXISTS parent_orders_base
CLUSTER BY parent_po
AS
SELECT DISTINCT parent_po
FROM internal_orders_stage
WHERE LOWER(parent_po) <> LOWER(order_no);


--COLLECT STATS  	PRIMARY INDEX (parent_po) 		ON parent_orders_base




CREATE TEMPORARY TABLE IF NOT EXISTS internal_orders_base
CLUSTER BY order_no
AS
SELECT *
FROM internal_orders_stage
WHERE LOWER(parent_po) <> LOWER(order_no);


--COLLECT STATS  	PRIMARY INDEX (order_no) 	,COLUMN (parent_po) 		ON internal_orders_base


CREATE TEMPORARY TABLE IF NOT EXISTS staging2
CLUSTER BY rms_sku_num, purchase_order_number, cur_otb_eow_date
AS
SELECT dt.month_label,
 otb.month_label AS otb_month_label,
 fct.purchase_order_number,
 fct.rms_sku_num,
 fct.purchase_type,
 CAST(fct.first_approval_event_tmstp_pacific AS DATE) AS approval_date,
 fct.original_end_ship_date,
 fct.ownership_price_amt,
 fct.written_date,
 fct.latest_edi_date,
 fct.po_vasn_signal,
 fct.po_type,
 fct.order_type,
 fct.import_country,
 fct.recon_month_start_day_date,
 fct.recon_month_end_day_date,
 fct.bom_otb_eow_date,
 fct.bom_status,
   fct.bom_unit_cost + fct.bom_total_duty_per_unit + fct.bom_total_expenses_per_unit AS bom_unit_cost,
 fct.eom_otb_eow_date,
 fct.eom_status,
   fct.eom_unit_cost + fct.eom_total_duty_per_unit + fct.eom_total_expenses_per_unit AS eom_unit_cost,
 fct.eom_start_ship_date,
 fct.eom_end_ship_date,
 fct.cur_otb_eow_date,
 fct.cur_status,
   fct.cur_unit_cost + fct.cur_total_duty_per_unit + fct.cur_total_expenses_per_unit AS cur_unit_cost,
 fct.cur_start_ship_date,
 fct.cur_end_ship_date,
  CASE
  WHEN LOWER(fct.edi_ind) = LOWER('T')
  THEN 'Y'
  ELSE 'N'
  END AS edi_ind,
  CASE
  WHEN LOWER(fct.npg_ind) = LOWER('T')
  THEN 'Y'
  ELSE 'N'
  END AS npg_ind,
  CASE
  WHEN LOWER(fct.internal_po_ind) = LOWER('T')
  THEN 'Y'
  ELSE 'N'
  END AS internal_po_ind,
  CASE
  WHEN LOWER(fct.exclude_from_backorder_avail_ind) = LOWER('T')
  THEN 'Y'
  ELSE 'N'
  END AS exclude_from_backorder_avail_ind,
  CASE
  WHEN inx.order_no IS NOT NULL
  THEN 'Y'
  ELSE 'N'
  END AS xref_po_ind,
  CASE
  WHEN par.parent_po IS NOT NULL
  THEN 'Y'
  ELSE 'N'
  END AS parent_po_ind,
 COALESCE(par.parent_po, inx.parent_po) AS parent_po,
 COALESCE(dist_org.banner_country, dist_org_temp.banner_country) AS banner_country,
 COALESCE(dist_org.channel, dist_org_temp.channel) AS channel,
 hier.division,
 hier.subdivision,
 hier.department,
 hier.class,
 hier.subclass,
 hier.vpn,
 hier.style_desc,
 hier.vpn_label,
 hier.nrf_color_code,
 hier.color_desc,
 hier.supplier_color,
 hier.prmy_supp_num,
 supp.vendor_name AS supplier,
 manu.vendor_name AS manufacturer,
 COALESCE(hier.smart_sample_ind, 'N') AS smart_sample_ind,
 COALESCE(hier.gwp_ind, 'N') AS gwp_ind,
 COALESCE(hier.prepaid_supplier_ind, 'N') AS prepaid_supplier_ind,
 SUM(fct.original_quantity_ordered) AS original_quantity_ordered,
 SUM(fct.bom_quantity_ordered) AS bom_quantity_ordered,
 SUM(fct.bom_quantity_canceled) AS bom_quantity_canceled,
 SUM(fct.bom_receipt_qty) AS bom_receipt_qty,
 SUM(fct.bom_vasn_qty) AS bom_vasn_qty,
 SUM(fct.eom_quantity_ordered) AS eom_quantity_ordered,
 SUM(fct.eom_quantity_canceled) AS eom_quantity_canceled,
 SUM(fct.eom_receipt_qty) AS eom_receipt_qty,
 SUM(fct.eom_vasn_qty) AS eom_vasn_qty,
 SUM(fct.cur_quantity_ordered) AS cur_quantity_ordered,
 SUM(fct.cur_quantity_canceled) AS cur_quantity_canceled,
 SUM(fct.cur_receipt_qty) AS cur_receipt_qty,
 SUM(fct.cur_vasn_qty) AS cur_vasn_qty,
 SUM(fct.eom_quantity_ordered - fct.bom_quantity_ordered) AS m_quantity_ordered,
 SUM(fct.eom_quantity_canceled - fct.bom_quantity_canceled) AS m_quantity_canceled,
 SUM(fct.eom_receipt_qty - fct.bom_receipt_qty) AS m_receipt_qty,
 SUM(fct.eom_vasn_qty - fct.bom_vasn_qty) AS m_vasn_qty,
 SUM(fct.cur_quantity_ordered - fct.bom_quantity_ordered) AS c_quantity_ordered,
 SUM(fct.cur_quantity_canceled - fct.bom_quantity_canceled) AS c_quantity_canceled,
 SUM(fct.cur_receipt_qty - fct.bom_receipt_qty) AS c_receipt_qty,
 SUM(fct.cur_vasn_qty - fct.bom_vasn_qty) AS c_vasn_qty
FROM staging1 AS fct
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_phase_zero.po_distribute_store_vw AS dist ON CAST(fct.ship_location_id AS FLOAT64) = dist.ship_location_id
    AND LOWER(fct.purchase_order_number) = LOWER(dist.purchase_order_num)
 LEFT JOIN dist_org ON dist.min_dist_loc = dist_org.store_num
 LEFT JOIN dist_org AS dist_org_temp ON CAST(fct.ship_location_id AS FLOAT64) = dist_org_temp.store_num
 LEFT JOIN internal_orders_base AS inx ON LOWER(fct.purchase_order_number) = LOWER(inx.order_no)
 LEFT JOIN parent_orders_base AS par ON LOWER(fct.purchase_order_number) = LOWER(par.parent_po)
 LEFT JOIN hier ON LOWER(fct.rms_sku_num) = LOWER(hier.rms_sku_num) AND LOWER(fct.import_country) = LOWER(hier.channel_country
    )
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(COALESCE(hier.prmy_supp_num, fct.order_from_vendor_id)) = LOWER(supp
   .vendor_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS manu ON LOWER(fct.order_from_vendor_id) = LOWER(manu.vendor_num)
 LEFT JOIN (SELECT DISTINCT month_start_day_date,
   month_label
  FROM date_filter
  WHERE month_rank = 2) AS dt ON fct.recon_month_start_day_date = dt.month_start_day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS otb ON fct.cur_otb_eow_date = otb.day_date
GROUP BY dt.month_label,
 otb_month_label,
 fct.purchase_order_number,
 fct.rms_sku_num,
 fct.purchase_type,
 approval_date,
 fct.original_end_ship_date,
 fct.ownership_price_amt,
 fct.written_date,
 fct.latest_edi_date,
 fct.po_vasn_signal,
 fct.po_type,
 fct.order_type,
 fct.import_country,
 fct.recon_month_start_day_date,
 fct.recon_month_end_day_date,
 fct.bom_otb_eow_date,
 fct.bom_status,
 bom_unit_cost,
 fct.eom_otb_eow_date,
 fct.eom_status,
 eom_unit_cost,
 fct.eom_start_ship_date,
 fct.eom_end_ship_date,
 fct.cur_otb_eow_date,
 fct.cur_status,
 cur_unit_cost,
 fct.cur_start_ship_date,
 fct.cur_end_ship_date,
 edi_ind,
 npg_ind,
 internal_po_ind,
 exclude_from_backorder_avail_ind,
 xref_po_ind,
 parent_po_ind,
 parent_po,
 banner_country,
 channel,
 hier.division,
 hier.subdivision,
 hier.department,
 hier.class,
 hier.subclass,
 hier.vpn,
 hier.style_desc,
 hier.vpn_label,
 hier.nrf_color_code,
 hier.color_desc,
 hier.supplier_color,
 hier.prmy_supp_num,
 supplier,
 manufacturer,
 smart_sample_ind,
 gwp_ind,
 prepaid_supplier_ind;


--COLLECT STATS  	PRIMARY INDEX (rms_sku_num, purchase_order_number, cur_otb_eow_date) --	,COLUMN (bom_status) --	,COLUMN (bom_otb_eow_date) --	,COLUMN (recon_month_start_day_date) --	,COLUMN (recon_month_end_day_date) --	,COLUMN (cur_status) --	,COLUMN (cur_otb_eow_date) --	,COLUMN (eom_status) --	,COLUMN (eom_otb_eow_date) --	,COLUMN (internal_po_ind) --	,COLUMN (approval_date) 		ON staging2


CREATE TEMPORARY TABLE IF NOT EXISTS staging3
CLUSTER BY purchase_order_number, rms_sku_num, channel
AS
SELECT month_label,
 otb_month_label,
 purchase_order_number,
 rms_sku_num,
 purchase_type,
 approval_date,
 original_end_ship_date,
 ownership_price_amt,
 written_date,
 latest_edi_date,
 po_vasn_signal,
 po_type,
 order_type,
 import_country,
 recon_month_start_day_date,
 recon_month_end_day_date,
 bom_otb_eow_date,
 bom_status,
 bom_unit_cost,
 eom_otb_eow_date,
 eom_status,
 eom_unit_cost,
 eom_start_ship_date,
 eom_end_ship_date,
 cur_otb_eow_date,
 cur_status,
 cur_unit_cost,
 cur_start_ship_date,
 cur_end_ship_date,
 edi_ind,
 npg_ind,
 internal_po_ind,
 exclude_from_backorder_avail_ind,
 xref_po_ind,
 parent_po_ind,
 parent_po,
 banner_country,
 channel,
 division,
 subdivision,
 department,
 class,
 subclass,
 vpn,
 style_desc,
 vpn_label,
 nrf_color_code,
 color_desc,
 supplier_color,
 prmy_supp_num,
 supplier,
 manufacturer,
 smart_sample_ind,
 gwp_ind,
 prepaid_supplier_ind,
 original_quantity_ordered,
 bom_quantity_ordered,
 bom_quantity_canceled,
 bom_receipt_qty,
 bom_vasn_qty,
 eom_quantity_ordered,
 eom_quantity_canceled,
 eom_receipt_qty,
 eom_vasn_qty,
 cur_quantity_ordered,
 cur_quantity_canceled,
 cur_receipt_qty,
 cur_vasn_qty,
 m_quantity_ordered,
 m_quantity_canceled,
 m_receipt_qty,
 m_vasn_qty,
 c_quantity_ordered,
 c_quantity_canceled,
 c_receipt_qty,
 c_vasn_qty,
  COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0) AS eom_net_receipts_qty,
  COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0) AS cur_net_receipts_qty,
  CASE
  WHEN COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0) > 0 AND CURRENT_DATE > cur_otb_eow_date
  THEN COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0)
  ELSE 0
  END AS cur_open_qty,
  CASE
  WHEN COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0) < 0
  THEN (COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0)) * - 1
  ELSE 0
  END AS cur_over_qty_test,
  CASE
  WHEN COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0) < 0
  THEN (COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0)) * - 1
  ELSE 0
  END AS eom_over_qty_test,
  CASE
  WHEN COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0) < 0
  THEN (COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0)) * - 1
  ELSE 0
  END AS eom_over_qty,
  CASE
  WHEN CASE
    WHEN COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0) < 0
    THEN (COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0)) * - 1
    ELSE 0
    END > m_receipt_qty
  THEN m_receipt_qty
  ELSE CASE
   WHEN COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0) < 0
   THEN (COALESCE(eom_quantity_ordered, 0) - COALESCE(eom_receipt_qty, 0)) * - 1
   ELSE 0
   END
  END AS m_over_qty,
  CASE
  WHEN c_vasn_qty > CASE
    WHEN COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0) > 0 AND CURRENT_DATE > cur_otb_eow_date
    THEN COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0)
    ELSE 0
    END
  THEN CASE
   WHEN COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0) > 0 AND CURRENT_DATE > cur_otb_eow_date
   THEN COALESCE(cur_quantity_ordered, 0) - COALESCE(cur_receipt_qty, 0)
   ELSE 0
   END
  ELSE 0
  END AS c_vasn_qty_dedupe
FROM staging2 AS fct;


CREATE TEMPORARY TABLE IF NOT EXISTS staging4
CLUSTER BY purchase_order_number, vpn, channel
AS
SELECT month_label,
 otb_month_label,
 purchase_order_number,
 purchase_type,
 approval_date,
 original_end_ship_date,
 written_date,
 latest_edi_date,
 po_vasn_signal,
 po_type,
 order_type,
 import_country,
 recon_month_start_day_date,
 recon_month_end_day_date,
 bom_otb_eow_date,
 bom_status,
 eom_otb_eow_date,
 eom_status,
 eom_start_ship_date,
 eom_end_ship_date,
 cur_otb_eow_date,
 cur_status,
 cur_start_ship_date,
 cur_end_ship_date,
 edi_ind,
 npg_ind,
 internal_po_ind,
 exclude_from_backorder_avail_ind,
 xref_po_ind,
 parent_po_ind,
 parent_po,
 banner_country,
 channel,
 division,
 subdivision,
 department,
 class,
 subclass,
 vpn,
 style_desc,
 vpn_label,
 nrf_color_code,
 color_desc,
 supplier_color,
 prmy_supp_num,
 supplier,
 manufacturer,
 smart_sample_ind,
 gwp_ind,
 prepaid_supplier_ind,
 MAX(ownership_price_amt) AS ownership_price_amt,
 MAX(bom_unit_cost) AS bom_unit_cost,
 MAX(eom_unit_cost) AS eom_unit_cost,
 MAX(cur_unit_cost) AS cur_unit_cost,
 SUM(original_quantity_ordered) AS original_quantity_ordered,
 SUM(bom_quantity_ordered) AS bom_quantity_ordered,
 SUM(bom_quantity_canceled) AS bom_quantity_canceled,
 SUM(bom_receipt_qty) AS bom_receipt_qty,
 SUM(bom_vasn_qty) AS bom_vasn_qty,
 SUM(eom_quantity_ordered) AS eom_quantity_ordered,
 SUM(eom_quantity_canceled) AS eom_quantity_canceled,
 SUM(eom_receipt_qty) AS eom_receipt_qty,
 SUM(eom_vasn_qty) AS eom_vasn_qty,
 SUM(cur_quantity_ordered) AS cur_quantity_ordered,
 SUM(cur_quantity_canceled) AS cur_quantity_canceled,
 SUM(cur_receipt_qty) AS cur_receipt_qty,
 SUM(cur_vasn_qty) AS cur_vasn_qty,
 SUM(m_quantity_ordered) AS m_quantity_ordered,
 SUM(m_quantity_canceled) AS m_quantity_canceled,
 SUM(m_receipt_qty) AS m_receipt_qty,
 SUM(m_vasn_qty) AS m_vasn_qty,
 SUM(c_quantity_ordered) AS c_quantity_ordered,
 SUM(c_quantity_canceled) AS c_quantity_canceled,
 SUM(c_receipt_qty) AS c_receipt_qty,
 SUM(c_vasn_qty) AS c_vasn_qty,
 SUM(eom_net_receipts_qty) AS eom_net_receipts_qty,
 SUM(cur_net_receipts_qty) AS cur_net_receipts_qty,
 SUM(cur_open_qty) AS cur_open_qty,
 SUM(cur_over_qty_test) AS cur_over_qty_test,
 SUM(eom_over_qty_test) AS eom_over_qty_test,
 SUM(eom_over_qty) AS eom_over_qty,
 SUM(m_over_qty) AS m_over_qty,
 SUM(c_vasn_qty_dedupe) AS c_vasn_qty_dedupe
FROM staging3 AS fct
GROUP BY month_label,
 otb_month_label,
 purchase_order_number,
 purchase_type,
 approval_date,
 original_end_ship_date,
 written_date,
 latest_edi_date,
 po_vasn_signal,
 po_type,
 order_type,
 import_country,
 recon_month_start_day_date,
 recon_month_end_day_date,
 bom_otb_eow_date,
 bom_status,
 eom_otb_eow_date,
 eom_status,
 eom_start_ship_date,
 eom_end_ship_date,
 cur_otb_eow_date,
 cur_status,
 cur_start_ship_date,
 cur_end_ship_date,
 edi_ind,
 npg_ind,
 internal_po_ind,
 exclude_from_backorder_avail_ind,
 xref_po_ind,
 parent_po_ind,
 parent_po,
 banner_country,
 channel,
 division,
 subdivision,
 department,
 class,
 subclass,
 vpn,
 style_desc,
 vpn_label,
 nrf_color_code,
 color_desc,
 supplier_color,
 prmy_supp_num,
 supplier,
 manufacturer,
 smart_sample_ind,
 gwp_ind,
 prepaid_supplier_ind;


--COLLECT STATS  	PRIMARY INDEX (purchase_order_number, vpn, channel) 	,COLUMN (bom_status) 	,COLUMN (bom_otb_eow_date) 	,COLUMN (recon_month_start_day_date) 	,COLUMN (recon_month_end_day_date) 	,COLUMN (cur_status) 	,COLUMN (cur_otb_eow_date) 	,COLUMN (eom_status) 	,COLUMN (eom_otb_eow_date) 	,COLUMN (internal_po_ind) 	,COLUMN (approval_date) 		ON staging4


CREATE TEMPORARY TABLE IF NOT EXISTS outbound_receipt_data
CLUSTER BY purchase_order_num, vpn, month_label
AS
SELECT (SELECT DISTINCT month_label
  FROM date_filter
  WHERE month_rank = 2) AS month_label,
 otbdt.month_label AS otb_month_label,
 a.purchase_order_num,
 a.vpn,
 a.color_num,
 TRIM(FORMAT('%11d', CAST(a.channel AS INT64))) AS channel,
 TRIM(FORMAT('%11d',CAST(a.division AS INT64))) AS division,
 TRIM(FORMAT('%11d',CAST(a.subdivision AS INT64))) AS subdivision,
 TRIM(FORMAT('%11d',CAST(a.department AS INT64))) AS department,
 TRIM(FORMAT('%11d',CAST(a.class AS INT64))) AS class,
 TRIM(FORMAT('%11d',CAST(a.subclass AS INT64))) AS subclass,
 a.supp_num,
 a.supp_color,
 a.style_desc,
 MAX(a.otb_eow_date) AS otb_eow_date,
 SUM(a.receipt_cost) AS store_receipt_cost,
 SUM(a.receipt_retail) AS store_receipt_retail,
 SUM(a.receipt_units) AS store_quantity_received,
 SUM(a.store_quantity_open) AS store_quantity_open,
 SUM(a.c_vasn_qty_dedupe_outbound) AS c_vasn_qty_dedupe_outbound
FROM (SELECT str.month_idnt,
    str.otb_month_idnt,
    str.purchase_order_num,
    str.vpn,
    str.color_num,
    str.supp_num,
    str.supp_color,
    CONCAT(str.channel_num, ', ', str.channel_desc) AS channel,
    CONCAT(str.division_num, ', ', str.division_name) AS division,
    CONCAT(str.subdivision_num, ', ', str.subdivision_name) AS subdivision,
    CONCAT(str.dept_num, ', ', str.dept_name) AS department,
    CONCAT(str.class_num, ', ', str.class_desc) AS class,
    CONCAT(str.sbclass_num, ', ', str.sbclass_desc) AS subclass,
    str.style_desc,
    str.rcpt_cost AS receipt_cost,
    str.rcpt_retail AS receipt_retail,
    str.rcpt_units AS receipt_units,
    str.otb_eow_date,
    0 AS store_quantity_open,
    0 AS c_vasn_qty_dedupe_outbound
   FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail AS str
    INNER JOIN date_filter AS dt ON str.month_idnt = dt.month_idnt
   WHERE dt.month_rank = 2
   UNION ALL
   SELECT str0.month_idnt,
    str0.otb_month_idnt,
    str0.purchase_order_num,
    str0.vpn,
    str0.color_num,
    str0.supp_num,
    str0.supp_color,
    CONCAT(str0.channel_num, ', ', str0.channel_desc) AS channel,
    CONCAT(str0.division_num, ', ', str0.division_name) AS division,
    CONCAT(str0.subdivision_num, ', ', str0.subdivision_name) AS subdivision,
    CONCAT(str0.dept_num, ', ', str0.dept_name) AS department,
    CONCAT(str0.class_num, ', ', str0.class_desc) AS class,
    CONCAT(str0.sbclass_num, ', ', str0.sbclass_desc) AS subclass,
    str0.style_desc,
    0 AS receipt_cost,
    0 AS receipt_retail,
    0 AS receipt_units,
    str0.otb_eow_date,
    str0.quantity_open AS store_quantity_open,
     CASE
     WHEN str0.vasn_sku_qty > str0.quantity_open
     THEN str0.quantity_open
     ELSE 0
     END AS c_vasn_qty_dedupe_outbound
   FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail AS str0
    INNER JOIN date_filter AS dt0 ON str0.month_idnt = dt0.month_idnt
   WHERE dt0.month_rank = 1) AS a
 INNER JOIN (SELECT DISTINCT month_idnt,
   month_label
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS otbdt ON a.otb_month_idnt = otbdt.month_idnt
GROUP BY month_label,
 otb_month_label,
 a.purchase_order_num,
 a.vpn,
 a.color_num,
 channel,
 division,
 subdivision,
 department,
 class,
 subclass,
 a.supp_num,
 a.supp_color,
 a.style_desc;


CREATE TEMPORARY TABLE IF NOT EXISTS staging5
CLUSTER BY purchase_order_number, vpn, month_label
AS
SELECT COALESCE(fct.month_label, str.month_label) AS month_label,
 COALESCE(fct.otb_month_label, str.otb_month_label) AS otb_month_label,
 COALESCE(fct.purchase_order_number, str.purchase_order_num) AS purchase_order_number,
 fct.purchase_type,
 fct.approval_date,
 fct.original_end_ship_date,
 fct.ownership_price_amt,
 fct.written_date,
 fct.latest_edi_date,
 fct.po_vasn_signal,
 fct.po_type,
 fct.order_type,
 fct.import_country,
 fct.recon_month_start_day_date,
 fct.recon_month_end_day_date,
 fct.bom_otb_eow_date,
 fct.bom_status,
 fct.bom_unit_cost,
 fct.eom_otb_eow_date,
 fct.eom_status,
 fct.eom_unit_cost,
 fct.eom_start_ship_date,
 fct.eom_end_ship_date,
 COALESCE(fct.cur_otb_eow_date, str.otb_eow_date) AS cur_otb_eow_date,
 fct.cur_status,
 fct.cur_unit_cost,
 fct.cur_start_ship_date,
 fct.cur_end_ship_date,
 fct.edi_ind,
 fct.npg_ind,
 fct.internal_po_ind,
 fct.exclude_from_backorder_avail_ind,
 fct.xref_po_ind,
 fct.parent_po_ind,
 fct.parent_po,
 fct.banner_country,
 COALESCE(fct.channel, str.channel) AS channel,
 COALESCE(fct.division, str.division) AS division,
 COALESCE(fct.subdivision, str.subdivision) AS subdivision,
 COALESCE(fct.department, str.department) AS department,
 COALESCE(fct.class, str.class) AS class,
 COALESCE(fct.subclass, str.subclass) AS subclass,
 COALESCE(fct.vpn, str.vpn) AS vpn,
 fct.style_desc,
 fct.vpn_label,
 COALESCE(fct.nrf_color_code, str.color_num) AS nrf_color_code,
 fct.color_desc,
 fct.supplier_color,
 fct.prmy_supp_num,
 fct.supplier,
 fct.manufacturer,
 fct.smart_sample_ind,
 fct.gwp_ind,
 fct.prepaid_supplier_ind,
 fct.original_quantity_ordered,
 fct.bom_quantity_ordered,
 fct.bom_quantity_canceled,
 fct.bom_receipt_qty,
 fct.bom_vasn_qty,
 fct.eom_quantity_ordered,
 fct.eom_quantity_canceled,
 fct.eom_receipt_qty,
 fct.eom_vasn_qty,
 fct.cur_quantity_ordered,
 fct.cur_quantity_canceled,
 fct.cur_receipt_qty,
 fct.cur_vasn_qty,
 fct.m_quantity_ordered,
 fct.m_quantity_canceled,
 fct.m_receipt_qty,
 fct.m_vasn_qty,
 fct.c_quantity_ordered,
 fct.c_quantity_canceled,
 fct.c_receipt_qty,
 fct.c_vasn_qty,
 fct.eom_net_receipts_qty,
 fct.cur_net_receipts_qty,
 fct.cur_open_qty,
 fct.cur_over_qty_test,
 fct.eom_over_qty_test,
 fct.eom_over_qty,
 fct.m_over_qty,
 fct.c_vasn_qty_dedupe,
 COALESCE(str.store_receipt_cost, 0) AS store_receipt_cost,
 COALESCE(str.store_receipt_retail, 0) AS store_receipt_retail,
 COALESCE(str.store_quantity_received, 0) AS store_quantity_received,
 COALESCE(str.store_quantity_open, 0) AS store_quantity_open,
 COALESCE(str.c_vasn_qty_dedupe_outbound, 0) AS c_vasn_qty_dedupe_outbound,
 COALESCE(fct.eom_quantity_ordered - str.store_quantity_received, 0) AS net_store_receipts,
 COALESCE(CASE
   WHEN COALESCE(fct.eom_quantity_ordered - str.store_quantity_received, 0) < 0
   THEN COALESCE(fct.eom_quantity_ordered - str.store_quantity_received, 0) * - 1
   ELSE 0
   END, 0) AS store_over_qty
FROM staging4 AS fct
 FULL JOIN outbound_receipt_data AS str ON LOWER(fct.month_label) = LOWER(str.month_label) AND LOWER(fct.otb_month_label
             ) = LOWER(str.otb_month_label) AND LOWER(fct.purchase_order_number) = LOWER(str.purchase_order_num) AND
          LOWER(fct.vpn) = LOWER(str.vpn) AND LOWER(fct.nrf_color_code) = LOWER(str.color_num) AND LOWER(fct.prmy_supp_num
         ) = LOWER(str.supp_num) AND LOWER(COALESCE(fct.supplier_color, '')) = LOWER(COALESCE(str.supp_color, '')) AND
      LOWER(fct.channel) = LOWER(TRIM(str.channel)) AND LOWER(fct.class) = LOWER(TRIM(str.class)) AND LOWER(fct.subclass
     ) = LOWER(TRIM(str.subclass)) AND LOWER(fct.style_desc) = LOWER(str.style_desc);

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.receipt_recon_daily
;

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.receipt_recon_daily
(SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS refresh_timestamp,
  month_label,
  otb_month_label,
  recon_month_start_day_date,
  recon_month_end_day_date,
  purchase_order_number,
  purchase_type,
  approval_date,
  original_end_ship_date,
  ownership_price_amt,
  written_date,
  latest_edi_date,
  po_vasn_signal,
  po_type,
  order_type,
  bom_otb_eow_date,
  bom_status,
  eom_otb_eow_date,
  eom_status,
  eom_start_ship_date,
  eom_end_ship_date,
  cur_otb_eow_date,
  cur_status,
  cur_start_ship_date,
  cur_end_ship_date,
  edi_ind,
  npg_ind,
  internal_po_ind,
  exclude_from_backorder_avail_ind,
  xref_po_ind,
  parent_po_ind,
  parent_po,
  banner_country,
  channel,
  division,
  subdivision,
  department,
  class,
  subclass,
  vpn,
  TRIM(style_desc) AS style_desc,
  TRIM(vpn_label) AS vpn_label,
  nrf_color_code,
  color_desc,
  supplier_color,
  prmy_supp_num,
  supplier,
  manufacturer,
  smart_sample_ind,
  gwp_ind,
  prepaid_supplier_ind,
   CASE
   WHEN LOWER(bom_status) IN (LOWER('APPROVED'), LOWER('SUBMITTED'), LOWER('WORKSHEET')) AND bom_otb_eow_date BETWEEN
      recon_month_start_day_date AND recon_month_end_day_date AND (LOWER(internal_po_ind) <> LOWER('Y') OR LOWER(internal_po_ind
         ) = LOWER('Y') AND eom_otb_eow_date >= DATE_ADD(recon_month_start_day_date, INTERVAL - 12 MONTH))
   THEN 1
   ELSE 0
   END AS moi_bom_ind,
   CASE
   WHEN LOWER(eom_status) IN (LOWER('APPROVED'), LOWER('SUBMITTED'), LOWER('WORKSHEET'), LOWER('CLOSED')) AND
      eom_otb_eow_date BETWEEN recon_month_start_day_date AND recon_month_end_day_date AND (LOWER(internal_po_ind) <>
       LOWER('Y') OR LOWER(internal_po_ind) = LOWER('Y') AND eom_otb_eow_date >= DATE_ADD(recon_month_start_day_date,
         INTERVAL - 12 MONTH))
   THEN 1
   ELSE 0
   END AS moi_eom_ind,
   CASE
   WHEN LOWER(eom_status) IN (LOWER('APPROVED'), LOWER('SUBMITTED'), LOWER('WORKSHEET'), LOWER('CLOSED')) AND
       eom_otb_eow_date BETWEEN recon_month_start_day_date AND recon_month_end_day_date AND approval_date BETWEEN
      recon_month_start_day_date AND recon_month_end_day_date AND (LOWER(internal_po_ind) <> LOWER('Y') OR LOWER(internal_po_ind
         ) = LOWER('Y') AND eom_otb_eow_date >= DATE_ADD(recon_month_start_day_date, INTERVAL - 12 MONTH))
   THEN 1
   ELSE 0
   END AS moi_add_ind,
   CASE
   WHEN LOWER(eom_status) IN (LOWER('APPROVED'), LOWER('SUBMITTED'), LOWER('WORKSHEET'), LOWER('CLOSED')) AND
       eom_otb_eow_date BETWEEN recon_month_start_day_date AND recon_month_end_day_date AND bom_otb_eow_date NOT BETWEEN
      recon_month_start_day_date AND recon_month_end_day_date AND (LOWER(internal_po_ind) <> LOWER('Y') OR LOWER(internal_po_ind
         ) = LOWER('Y') AND eom_otb_eow_date >= DATE_ADD(recon_month_start_day_date, INTERVAL - 12 MONTH))
   THEN 1
   ELSE 0
   END AS moi_moved_in_ind,
   CASE
   WHEN LOWER(eom_status) IN (LOWER('APPROVED'), LOWER('SUBMITTED'), LOWER('WORKSHEET'), LOWER('CLOSED')) AND
       bom_otb_eow_date BETWEEN recon_month_start_day_date AND recon_month_end_day_date AND eom_otb_eow_date NOT BETWEEN
      recon_month_start_day_date AND recon_month_end_day_date AND (LOWER(internal_po_ind) <> LOWER('Y') OR LOWER(internal_po_ind
         ) = LOWER('Y') AND eom_otb_eow_date >= DATE_ADD(recon_month_start_day_date, INTERVAL - 12 MONTH))
   THEN 1
   ELSE 0
   END AS moi_moved_out_ind,
   CASE
   WHEN LOWER(cur_status) IN (LOWER('APPROVED'), LOWER('SUBMITTED'), LOWER('WORKSHEET')) AND store_quantity_open > 0 AND
     cur_otb_eow_date BETWEEN recon_month_start_day_date AND recon_month_end_day_date
   THEN 1
   ELSE 0
   END AS oo_in_mth_ind,
   CASE
   WHEN LOWER(cur_status) IN (LOWER('APPROVED'), LOWER('SUBMITTED'), LOWER('WORKSHEET')) AND store_quantity_open > 0 AND
     cur_otb_eow_date < recon_month_start_day_date
   THEN 1
   ELSE 0
   END AS oo_prior_mth_ind,
   CASE
   WHEN LOWER(eom_status) = LOWER('CLOSED') AND LOWER(bom_status) <> LOWER('CLOSED') AND m_quantity_canceled > 0
   THEN 1
   ELSE 0
   END AS cancel_all_ind,
   CASE
   WHEN LOWER(eom_status) = LOWER('CLOSED') AND LOWER(bom_status) <> LOWER('CLOSED') AND m_quantity_canceled > 0 AND
     eom_otb_eow_date BETWEEN recon_month_start_day_date AND recon_month_end_day_date
   THEN 1
   ELSE 0
   END AS cancel_in_mth_ind,
   CASE
   WHEN LOWER(eom_status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'), LOWER('SUBMITTED'), LOWER('CLOSED')) AND
     store_quantity_received > 0
   THEN 1
   ELSE 0
   END AS rcpt_total_ind,
   CASE
   WHEN LOWER(eom_status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'), LOWER('SUBMITTED'), LOWER('CLOSED')) AND
      store_quantity_received > 0 AND eom_otb_eow_date BETWEEN recon_month_start_day_date AND recon_month_end_day_date
   THEN 1
   ELSE 0
   END AS rcpt_in_mth_ind,
   CASE
   WHEN LOWER(internal_po_ind) = LOWER('Y') AND store_quantity_received > 0 AND eom_otb_eow_date BETWEEN
     recon_month_start_day_date AND recon_month_end_day_date
   THEN 1
   ELSE 0
   END AS rcpt_xref_ind,
   CASE
   WHEN store_quantity_received > 0 AND eom_otb_eow_date < recon_month_start_day_date
   THEN 1
   ELSE 0
   END AS rcpt_shift_in_prior_ind,
   CASE
   WHEN store_quantity_received > 0 AND eom_otb_eow_date > recon_month_end_day_date
   THEN 1
   ELSE 0
   END AS rcpt_shift_in_future_ind,
   CASE
   WHEN store_quantity_received > 0 AND store_over_qty > 0
   THEN 1
   ELSE 0
   END AS rcpt_over_ind,
  SUM(original_quantity_ordered) AS original_orders_u,
  SUM(original_quantity_ordered * ownership_price_amt) AS original_orders_r,
  CAST(SUM(original_quantity_ordered * cur_unit_cost) AS NUMERIC) AS original_orders_c,
  SUM(bom_quantity_ordered) AS bom_orders_u,
  SUM(bom_quantity_ordered * ownership_price_amt) AS bom_orders_r,
  CAST(SUM(bom_quantity_ordered * cur_unit_cost) AS NUMERIC) AS bom_orders_c,
  SUM(eom_quantity_ordered) AS eom_orders_u,
  SUM(eom_quantity_ordered * ownership_price_amt) AS eom_orders_r,
  CAST(SUM(eom_quantity_ordered * cur_unit_cost) AS NUMERIC) AS eom_orders_c,
  SUM(cur_quantity_ordered) AS cur_orders_u,
  SUM(cur_quantity_ordered * ownership_price_amt) AS cur_orders_r,
  CAST(SUM(cur_quantity_ordered * cur_unit_cost) AS NUMERIC) AS cur_orders_c,
  SUM(cur_open_qty) AS open_u,
  SUM(cur_open_qty * ownership_price_amt) AS open_r,
  CAST(SUM(cur_open_qty * cur_unit_cost) AS NUMERIC) AS open_c,
  SUM(m_over_qty) AS over_u,
  SUM(m_over_qty * ownership_price_amt) AS over_r,
  CAST(SUM(m_over_qty * cur_unit_cost) AS NUMERIC) AS over_c,
  SUM(m_over_qty) AS m_over_u,
  SUM(m_over_qty * ownership_price_amt) AS m_over_r,
  CAST(SUM(m_over_qty * cur_unit_cost) AS NUMERIC) AS m_over_c,
  SUM(m_quantity_canceled) AS m_cancels_u,
  SUM(m_quantity_canceled * ownership_price_amt) AS m_cancels_r,
  CAST(SUM(m_quantity_canceled * cur_unit_cost) AS NUMERIC) AS m_cancels_c,
  SUM(m_receipt_qty) AS m_receipts_u,
  SUM(m_receipt_qty * ownership_price_amt) AS m_receipts_r,
  CAST(SUM(m_receipt_qty * cur_unit_cost) AS NUMERIC) AS m_receipts_c,
  SUM(c_quantity_canceled) AS c_cancels_u,
  SUM(c_quantity_canceled * ownership_price_amt) AS c_cancels_r,
  CAST(SUM(c_quantity_canceled * cur_unit_cost) AS NUMERIC) AS c_cancels_c,
  SUM(c_vasn_qty_dedupe) AS c_vasn_u,
  SUM(c_vasn_qty_dedupe * ownership_price_amt) AS c_vasn_r,
  CAST(SUM(c_vasn_qty_dedupe * cur_unit_cost) AS NUMERIC) AS c_vasn_c,
  SUM(c_receipt_qty) AS c_receipts_u,
  SUM(c_receipt_qty * ownership_price_amt) AS c_receipts_r,
  CAST(SUM(c_receipt_qty * cur_unit_cost) AS NUMERIC) AS c_receipts_c,
  ROUND(CAST(SUM(store_quantity_open * cur_unit_cost) AS NUMERIC), 2) AS outbound_open_c,
  CAST(trunc(SUM(store_quantity_open)) AS INTEGER) AS outbound_open_u,
  ROUND(CAST(SUM(store_quantity_open * ownership_price_amt) AS NUMERIC), 2) AS outbound_open_r,
  ROUND(CAST(SUM(store_receipt_cost) AS NUMERIC), 2) AS outbound_receipts_c,
  CAST(trunc(SUM(store_quantity_received)) AS INTEGER) AS outbound_receipts_u,
  ROUND(CAST(SUM(store_receipt_retail) AS NUMERIC), 2) AS outbound_receipts_r,
  CAST(SUM(store_over_qty) AS NUMERIC) AS outbound_over_u,
  CAST(trunc(SUM(store_over_qty * cur_unit_cost)) AS INTEGER) AS outbound_over_c,
  CAST(SUM(store_over_qty * ownership_price_amt) AS NUMERIC) AS outbound_over_r,
  CAST(trunc(SUM(c_vasn_qty_dedupe_outbound)) AS INTEGER) AS c_vasn_u_outbound,
  CAST(SUM(c_vasn_qty_dedupe_outbound * ownership_price_amt) AS NUMERIC) AS c_vasn_r_outbound,
  CAST(SUM(c_vasn_qty_dedupe_outbound * cur_unit_cost) AS NUMERIC) AS c_vasn_c_outbound
 FROM staging5 AS fct
 GROUP BY refresh_timestamp,
  month_label,
  otb_month_label,
  recon_month_start_day_date,
  recon_month_end_day_date,
  purchase_order_number,
  purchase_type,
  approval_date,
  original_end_ship_date,
  ownership_price_amt,
  written_date,
  latest_edi_date,
  po_vasn_signal,
  po_type,
  order_type,
  bom_otb_eow_date,
  bom_status,
  eom_otb_eow_date,
  eom_status,
  eom_start_ship_date,
  eom_end_ship_date,
  cur_otb_eow_date,
  cur_status,
  cur_start_ship_date,
  cur_end_ship_date,
  edi_ind,
  npg_ind,
  internal_po_ind,
  exclude_from_backorder_avail_ind,
  xref_po_ind,
  parent_po_ind,
  parent_po,
  banner_country,
  channel,
  division,
  subdivision,
  department,
  class,
  subclass,
  vpn,
  style_desc,
  vpn_label,
  nrf_color_code,
  color_desc,
  supplier_color,
  prmy_supp_num,
  supplier,
  manufacturer,
  smart_sample_ind,
  gwp_ind,
  prepaid_supplier_ind,
  moi_bom_ind,
  moi_eom_ind,
  moi_add_ind,
  moi_moved_in_ind,
  oo_in_mth_ind,
  oo_prior_mth_ind,
  cancel_all_ind,
  cancel_in_mth_ind,
  rcpt_total_ind,
  rcpt_in_mth_ind,
  rcpt_xref_ind,
  rcpt_shift_in_prior_ind,
  rcpt_shift_in_future_ind,
  rcpt_over_ind,
  moi_moved_out_ind);