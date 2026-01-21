
-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=merch_double_booked_on_order_load;
-- Task_Name=merch_on_order_load_stage_fact_load;'
-- FOR SESSION VOLATILE;
-- ET;

------------------------------------------------------------
-- 1. clear MERCH_ON_ORDER_DOUBLE_BOOKED_WRK
-- 2. select from MERCH_ON_ORDER_FACT_VW with filters into MERCH_ON_ORDER_DOUBLE_BOOKED_WRK
------------------------------------------------------------
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_double_booked_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_double_booked_wrk (purchase_order_number, rms_sku_num, supp_num, dept_num,
 epm_sku_num, store_num, week_num, channel_num, cross_ref_id, ship_location_id, rms_casepack_num, epm_casepack_num,
 order_from_vendor_id, anticipated_price_type, order_category_code, po_type, order_type, purchase_type, status,
 cancel_reason, start_ship_date, end_ship_date, otb_eow_date, first_approval_date, latest_approval_date, written_date,
 first_approval_event_tmstp_pacific,first_approval_event_tmstp_pacific_tz, latest_approval_event_tmstp_pacific,latest_approval_event_tmstp_pacific_tz, anticipated_retail_amt,
 total_expenses_per_unit_currency, quantity_ordered, quantity_canceled, quantity_received, quantity_open, unit_cost_amt
 , total_expenses_per_unit_amt, total_duty_per_unit_amt, unit_estimated_landing_cost_amt,
 total_estimated_landing_cost_amt, total_anticipated_retail_amt, dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT moofv.purchase_order_number,
  moofv.rms_sku_num,
  prod.prmy_supp_num AS supp_num,
  prod.dept_num,
  moofv.epm_sku_num,
  moofv.store_num,
  moofv.week_num,
  sd.channel_num,
  moofv.cross_ref_id,
  moofv.ship_location_id,
  moofv.rms_casepack_num,
  moofv.epm_casepack_num,
  moofv.order_from_vendor_id,
  moofv.anticipated_price_type,
  moofv.order_category_code,
  moofv.po_type,
  moofv.order_type,
  moofv.purchase_type,
  moofv.status,
  moofv.cancel_reason,
  moofv.start_ship_date,
  moofv.end_ship_date,
  moofv.otb_eow_date,
  moofv.first_approval_date,
  moofv.latest_approval_date,
  moofv.written_date,
  moofv.first_approval_event_tmstp_pacific_utc,
  moofv.first_approval_event_tmstp_pacific_tz,
  moofv.latest_approval_event_tmstp_pacific_utc,
  moofv.latest_approval_event_tmstp_pacific_tz,
  moofv.anticipated_retail_amt,
  moofv.total_expenses_per_unit_currency,
  moofv.quantity_ordered,
  moofv.quantity_canceled,
  moofv.quantity_received,
  moofv.quantity_open,
  moofv.unit_cost_amt,
  moofv.total_expenses_per_unit_amt,
  moofv.total_duty_per_unit_amt,
  moofv.unit_estimated_landing_cost AS unit_estimated_landing_cost_amt,
  moofv.total_estimated_landing_cost AS total_estimated_landing_cost_amt,
  moofv.total_anticipated_retail_amt,
  moofv.dw_batch_date,
 
  moofv.dw_sys_load_tmstp_utc,
  moofv.dw_sys_load_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_on_order_fact_vw AS moofv
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS sd ON moofv.store_num = sd.store_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_product_sku_dim_as_is_vw AS prod ON LOWER(moofv.rms_sku_num) = LOWER(prod.rms_sku_num
     ) AND LOWER(sd.store_country_code) = LOWER(prod.channel_country));



------------------------------------------------------------
-- 3a. select from MERCH_ON_ORDER_DOUBLE_BOOKED_WRK into MERCH_PO_PARENT_XREF_GTT
-- 3b. select from MERCH_PO_PARENT_XREF_GTT into MERCH_PO_PARENT_XREF_WRK
------------------------------------------------------------ 
	 INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt
(SELECT DISTINCT purchase_order_number,
  cross_ref_id,
  written_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_double_booked_wrk AS o);

 ------------------------------------------------------------
-- 4. clear MERCH_PO_PARENT_XREF_WRK
-- 5. select MERCH_PARENT_XREF data from GTT into WRK
------------------------------------------------------------
 
 TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_wrk
(SELECT GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(GREATEST(COALESCE(CASE
            WHEN DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                      WHEN voh.written_date IS NULL
                      THEN 0
                      ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                      END, CASE
                      WHEN voh2.written_date IS NULL
                      THEN 0
                      ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                      END), CASE
                     WHEN voh3.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
                     END), CASE
                    WHEN voh4.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
                    END), CASE
                   WHEN voh5.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
                   END), CASE
                  WHEN voh6.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
                  END), CASE
                 WHEN voh7.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END), CASE
                WHEN voh8.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh9.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END)
            THEN voh.purchase_order_number
            ELSE NULL
            END, FORMAT('%4d', 0)), COALESCE(CASE
            WHEN DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                      WHEN voh.written_date IS NULL
                      THEN 0
                      ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                      END, CASE
                      WHEN voh2.written_date IS NULL
                      THEN 0
                      ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                      END), CASE
                     WHEN voh3.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
                     END), CASE
                    WHEN voh4.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
                    END), CASE
                   WHEN voh5.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
                   END), CASE
                  WHEN voh6.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
                  END), CASE
                 WHEN voh7.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END), CASE
                WHEN voh8.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh9.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END)
            THEN voh2.purchase_order_number
            ELSE NULL
            END, FORMAT('%4d', 0))), COALESCE(CASE
           WHEN DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                     WHEN voh.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                     END, CASE
                     WHEN voh2.written_date IS NULL
                     THEN 0
                     ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                     END), CASE
                    WHEN voh3.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
                    END), CASE
                   WHEN voh4.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
                   END), CASE
                  WHEN voh5.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
                  END), CASE
                 WHEN voh6.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END), CASE
                WHEN voh7.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh8.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END), CASE
              WHEN voh9.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
              END)
           THEN voh3.purchase_order_number
           ELSE NULL
           END, FORMAT('%4d', 0))), COALESCE(CASE
          WHEN DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                    WHEN voh.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                    END, CASE
                    WHEN voh2.written_date IS NULL
                    THEN 0
                    ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                    END), CASE
                   WHEN voh3.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
                   END), CASE
                  WHEN voh4.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
                  END), CASE
                 WHEN voh5.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END), CASE
                WHEN voh6.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh7.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END), CASE
              WHEN voh8.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
              END), CASE
             WHEN voh9.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
             END)
          THEN voh4.purchase_order_number
          ELSE NULL
          END, FORMAT('%4d', 0))), COALESCE(CASE
         WHEN DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                   WHEN voh.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                   END, CASE
                   WHEN voh2.written_date IS NULL
                   THEN 0
                   ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                   END), CASE
                  WHEN voh3.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
                  END), CASE
                 WHEN voh4.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END), CASE
                WHEN voh5.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh6.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END), CASE
              WHEN voh7.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
              END), CASE
             WHEN voh8.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
             END), CASE
            WHEN voh9.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
            END)
         THEN voh5.purchase_order_number
         ELSE NULL
         END, FORMAT('%4d', 0))), COALESCE(CASE
        WHEN DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                  WHEN voh.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                  END, CASE
                  WHEN voh2.written_date IS NULL
                  THEN 0
                  ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                  END), CASE
                 WHEN voh3.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END), CASE
                WHEN voh4.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh5.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END), CASE
              WHEN voh6.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
              END), CASE
             WHEN voh7.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
             END), CASE
            WHEN voh8.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
            END), CASE
           WHEN voh9.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
           END)
        THEN voh6.purchase_order_number
        ELSE NULL
        END, FORMAT('%4d', 0))), COALESCE(CASE
       WHEN DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                 WHEN voh.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END, CASE
                 WHEN voh2.written_date IS NULL
                 THEN 0
                 ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                 END), CASE
                WHEN voh3.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh4.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END), CASE
              WHEN voh5.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
              END), CASE
             WHEN voh6.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
             END), CASE
            WHEN voh7.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
            END), CASE
           WHEN voh8.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
           END), CASE
          WHEN voh9.written_date IS NULL
          THEN 0
          ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
          END)
       THEN voh7.purchase_order_number
       ELSE NULL
       END, FORMAT('%4d', 0))), COALESCE(CASE
      WHEN DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
                WHEN voh.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END, CASE
                WHEN voh2.written_date IS NULL
                THEN 0
                ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
                END), CASE
               WHEN voh3.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END), CASE
              WHEN voh4.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
              END), CASE
             WHEN voh5.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
             END), CASE
            WHEN voh6.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
            END), CASE
           WHEN voh7.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
           END), CASE
          WHEN voh8.written_date IS NULL
          THEN 0
          ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
          END), CASE
         WHEN voh9.written_date IS NULL
         THEN 0
         ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
         END)
      THEN voh8.purchase_order_number
      ELSE NULL
      END, FORMAT('%4d', 0))), COALESCE(CASE
     WHEN DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY) = LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(LEAST(CASE
               WHEN voh.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END, CASE
               WHEN voh2.written_date IS NULL
               THEN 0
               ELSE DATE_DIFF(voh2.written_date, CURRENT_DATE('PST8PDT'), DAY)
               END), CASE
              WHEN voh3.written_date IS NULL
              THEN 0
              ELSE DATE_DIFF(voh3.written_date, CURRENT_DATE('PST8PDT'), DAY)
              END), CASE
             WHEN voh4.written_date IS NULL
             THEN 0
             ELSE DATE_DIFF(voh4.written_date, CURRENT_DATE('PST8PDT'), DAY)
             END), CASE
            WHEN voh5.written_date IS NULL
            THEN 0
            ELSE DATE_DIFF(voh5.written_date, CURRENT_DATE('PST8PDT'), DAY)
            END), CASE
           WHEN voh6.written_date IS NULL
           THEN 0
           ELSE DATE_DIFF(voh6.written_date, CURRENT_DATE('PST8PDT'), DAY)
           END), CASE
          WHEN voh7.written_date IS NULL
          THEN 0
          ELSE DATE_DIFF(voh7.written_date, CURRENT_DATE('PST8PDT'), DAY)
          END), CASE
         WHEN voh8.written_date IS NULL
         THEN 0
         ELSE DATE_DIFF(voh8.written_date, CURRENT_DATE('PST8PDT'), DAY)
         END), CASE
        WHEN voh9.written_date IS NULL
        THEN 0
        ELSE DATE_DIFF(voh9.written_date, CURRENT_DATE('PST8PDT'), DAY)
        END)
     THEN voh9.purchase_order_number
     ELSE NULL
     END, FORMAT('%4d', 0))) AS parent_po_num,
  voh.purchase_order_number AS xref_po_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh2 ON LOWER(voh.cross_ref_id) = LOWER(voh2.purchase_order_number)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh3 ON LOWER(CASE
     WHEN LOWER(voh2.cross_ref_id) = LOWER(voh.purchase_order_number)
     THEN NULL
     ELSE voh2.cross_ref_id
     END) = LOWER(voh3.purchase_order_number)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh4 ON LOWER(CASE
     WHEN LOWER(voh3.cross_ref_id) = LOWER(voh2.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh3.cross_ref_id) = LOWER(voh.purchase_order_number)
     THEN NULL
     ELSE voh3.cross_ref_id
     END) = LOWER(voh4.purchase_order_number)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh5 ON LOWER(CASE
     WHEN LOWER(voh4.cross_ref_id) = LOWER(voh3.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh4.cross_ref_id) = LOWER(voh2.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh4.cross_ref_id) = LOWER(voh.purchase_order_number)
     THEN NULL
     ELSE voh4.cross_ref_id
     END) = LOWER(voh5.purchase_order_number)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh6 ON LOWER(CASE
     WHEN LOWER(voh5.cross_ref_id) = LOWER(voh4.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh5.cross_ref_id) = LOWER(voh3.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh5.cross_ref_id) = LOWER(voh2.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh5.cross_ref_id) = LOWER(voh.purchase_order_number)
     THEN NULL
     ELSE voh5.cross_ref_id
     END) = LOWER(voh6.purchase_order_number)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh7 ON LOWER(CASE
     WHEN LOWER(voh6.cross_ref_id) = LOWER(voh5.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh6.cross_ref_id) = LOWER(voh4.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh6.cross_ref_id) = LOWER(voh3.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh6.cross_ref_id) = LOWER(voh2.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh6.cross_ref_id) = LOWER(voh.purchase_order_number)
     THEN NULL
     ELSE voh6.cross_ref_id
     END) = LOWER(voh7.purchase_order_number)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh8 ON LOWER(CASE
     WHEN LOWER(voh7.cross_ref_id) = LOWER(voh6.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh7.cross_ref_id) = LOWER(voh5.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh7.cross_ref_id) = LOWER(voh4.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh7.cross_ref_id) = LOWER(voh3.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh7.cross_ref_id) = LOWER(voh2.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh7.cross_ref_id) = LOWER(voh.purchase_order_number)
     THEN NULL
     ELSE voh7.cross_ref_id
     END) = LOWER(voh8.purchase_order_number)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_gtt AS voh9 ON LOWER(CASE
     WHEN LOWER(voh8.cross_ref_id) = LOWER(voh7.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh8.cross_ref_id) = LOWER(voh6.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh8.cross_ref_id) = LOWER(voh5.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh8.cross_ref_id) = LOWER(voh4.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh8.cross_ref_id) = LOWER(voh3.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh8.cross_ref_id) = LOWER(voh2.purchase_order_number)
     THEN NULL
     WHEN LOWER(voh8.cross_ref_id) = LOWER(voh.purchase_order_number)
     THEN NULL
     ELSE voh8.cross_ref_id
     END) = LOWER(voh9.purchase_order_number));
	 
	
--------------------------------------------------------
-- 6. select from MERCH_PO_PARENT_XREF_WRK into MERCH_PO_PARENT_GTT
------------------------------------------------------------

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_gtt
(SELECT DISTINCT parent_po_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_wrk
 WHERE LOWER(parent_po_num) <> LOWER(xref_po_num));

------------------------------------------------------------
-- 7. select from MERCH_PO_PARENT_XREF_WRK into MERCH_PO_XREF_GTT
------------------------------------------------------------

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_xref_gtt
(SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_xref_wrk
 WHERE LOWER(parent_po_num) <> LOWER(xref_po_num));

 ------------------------------------------------------------
-- 8. clear MERCH_PO_SKU_ON_ORDER_FACT
-- 9. select from MERCH_PO_XREF_GTT, MERCH_PARENT_GTT, and MERCH_ON_ORDER_DOUBLE_BOOKED_WRK into MERCH_PO_SKU_ON_ORDER_FACT
------------------------------------------------------------
 
 TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_po_sku_on_order_fact;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_po_sku_on_order_fact (purchase_order_number, rms_sku_num, supp_num, dept_num, week_num,
 status, cancel_reason, end_ship_date, otb_eow_date, xref_po_ind, parent_po_ind, parent_po_num, qty_ordered,
 qty_received, qty_canceled, qty_open, total_qty_received, total_qty_canceled, total_qty_open,
 unit_estimated_landing_cost_amt, unit_anticipated_retail_amt, total_estimated_landing_cost_amt,
 total_anticipated_retail_amt, max_estimated_landing_cost_amt, dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT t0.purchase_order_number,
  t0.rms_sku_num,
  t0.supp_num,
  t0.dept_num,
  t0.week_num,
  t0.status,
  t0.cancel_reason,
  t0.end_ship_date,
  t0.otb_eow_date,
  t0.xref_po_ind,
  t0.parent_po_ind,
  t0.parent_po_num,
  t0.quantity_ordered AS qty_ordered,
  t0.quantity_received AS qty_received,
  t0.quantity_canceled AS qty_canceled,
  t0.quantity_open AS qty_open,
  SUM(t0.quantity_received) OVER (PARTITION BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind
   ORDER BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING) AS total_qty_received,
  SUM(t0.quantity_canceled) OVER (PARTITION BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind
   ORDER BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING) AS total_qty_canceled,
  SUM(t0.quantity_open) OVER (PARTITION BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind ORDER BY
     t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING) AS total_qty_open,
  t0.unit_estimated_landing_cost_amt,
  t0.anticipated_retail_amt AS unit_anticipated_retail_amt,
  SUM(t0.unit_estimated_landing_cost_amt) OVER (PARTITION BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind
     ORDER BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING) AS total_estimated_landing_cost_amt,
  SUM(t0.anticipated_retail_amt) OVER (PARTITION BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind
   ORDER BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING) AS total_anticipated_retail_amt,
  MAX(t0.unit_estimated_landing_cost_amt) OVER (PARTITION BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind
     ORDER BY t0.rms_sku_num, t0.parent_po_num, t0.xref_po_ind, t0.parent_po_ind RANGE BETWEEN UNBOUNDED PRECEDING AND
   UNBOUNDED FOLLOWING) AS
  xref_po_ind_parent_po_ind_unit_estimated_landing_cost_amt_rms_sku_num_rms_sku_num_parent_po_num_parent_po_ind_parent_po_num_xref_po_ind
  ,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)  AS TIMESTAMP) AS
  dw_sys_load_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz
 FROM (SELECT o.purchase_order_number,
    o.rms_sku_num,
    o.supp_num,
    o.dept_num,
    o.week_num,
    o.status,
    o.end_ship_date,
    o.otb_eow_date,
     CASE
     WHEN xref.xref_po_num IS NOT NULL
     THEN 'Y'
     ELSE 'N'
     END AS xref_po_ind,
     CASE
     WHEN par.parent_po_num IS NOT NULL
     THEN 'Y'
     ELSE 'N'
     END AS parent_po_ind,
    COALESCE(par.parent_po_num, xref.parent_po_num) AS parent_po_num,
    MAX(CASE
      WHEN o.quantity_canceled <> 0
      THEN COALESCE(o.cancel_reason, 'NOT_SPECIFIED')
      ELSE o.cancel_reason
      END) AS cancel_reason,
    SUM(o.quantity_ordered) AS quantity_ordered,
    SUM(o.quantity_received) AS quantity_received,
    SUM(o.quantity_canceled) AS quantity_canceled,
    SUM(o.quantity_open) AS quantity_open,
    MAX(o.unit_estimated_landing_cost_amt) AS unit_estimated_landing_cost_amt,
    MAX(o.anticipated_retail_amt) AS anticipated_retail_amt,
    MAX(o.total_estimated_landing_cost_amt) AS total_estimated_landing_cost_amt,
    MAX(o.total_anticipated_retail_amt) AS total_anticipated_retail_amt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_on_order_double_booked_wrk AS o
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_xref_gtt AS xref ON LOWER(o.purchase_order_number) = LOWER(xref.xref_po_num)
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_po_parent_gtt AS par ON LOWER(o.purchase_order_number) = LOWER(par.parent_po_num)
   GROUP BY o.purchase_order_number,
    o.rms_sku_num,
    o.supp_num,
    o.dept_num,
    o.week_num,
    o.status,
    o.end_ship_date,
    o.otb_eow_date,
    xref_po_ind,
    parent_po_ind,
    parent_po_num) AS t0);
	
	
-- 	ET;

-- SET QUERY_BAND = NONE FOR SESSION;
-- ET;