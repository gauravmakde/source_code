/***********************************************************/
/******************* FILTER AND LOOKUP TABLES **************/
/***********************************************************/


/******************* HIERARCHY ********************/

--DROP TABLE hier;



--prepaid suppliers from Vanessa Priddy

BEGIN

CREATE TEMPORARY TABLE IF NOT EXISTS hier
CLUSTER BY rms_sku_num
AS
SELECT sku.rms_sku_num,
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
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dept ON sku.dept_num = dept.dept_num
WHERE LOWER(sku.channel_country) = LOWER('US');


--COLLECT STATS  	PRIMARY INDEX (rms_sku_num) 	,COLUMN (rms_sku_num) 		ON hier


/******************* LOCATIONS ********************/


--DROP TABLE dist_org;


CREATE TEMPORARY TABLE IF NOT EXISTS dist_org
CLUSTER BY store_num
AS
SELECT DISTINCT store_num,
 banner_country,
 TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel
FROM `{{params.gcp_project_id}}`.t2dl_das_phase_zero.ban_cntry_store_dim_vw;


--COLLECT STATS  	PRIMARY INDEX (store_num) 	,COLUMN (store_num) 		ON dist_org


/******************* HISTORICAL FILTER (temp) ********************/


--These are POs that closed before the historical cutoff (4/23) from Jonny Siu


--Could be removed once historical backfill is complete


--DROP TABLE hist_po;


--	AND open_to_buy_endofweek_date BETWEEN '2022-07-03' AND '2022-07-30'


CREATE TEMPORARY TABLE IF NOT EXISTS hist_po
CLUSTER BY purchase_order_num
AS
SELECT DISTINCT purchase_order_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact
WHERE close_date < DATE '2022-04-23'
 AND (LOWER(dropship_ind) = LOWER('f') OR dropship_ind IS NULL)
 AND LOWER(sendonly_ticket_partner_edi_ind) = LOWER('f');


--COLLECT STATS  	PRIMARY INDEX (purchase_order_num) 	,COLUMN (purchase_order_num) 		ON hist_po


/***********************************************************/


/****************** STAGING MAIN SOURCE TABLE **************/


/***********************************************************/


--DROP TABLE main;



CREATE TEMPORARY TABLE main
  AS

SELECT
	 purchase_order_num,
   TRIM(FORMAT('%11d', channel_num)) || ', ' || TRIM( channel_desc) AS channel_num,
   TRIM( channel_brand)  AS channel_brand,
   TRIM(FORMAT('%11d', division_num)) || ', ' || TRIM( division_name) AS division,
   TRIM(FORMAT('%11d', subdivision_num)) || ', ' || TRIM( subdivision_name) AS subdivision,
   TRIM(FORMAT('%11d', dept_num)) || ', ' || TRIM( dept_name) AS department,
   TRIM(FORMAT('%11d', class_num)) || ', ' || TRIM( class_desc) AS class,
   TRIM(FORMAT('%11d', sbclass_num)) || ', ' || TRIM( sbclass_desc) AS subclass,
   TRIM( supp_num)  AS supplier_number,
	  supp_name as supplier_name
	 , vpn 
	 , style_desc 
	 , color_num 
	 , supp_color 
	 , customer_choice 
	 , o.STATUS as po_status
	 , comments
	 , latest_close_event_tmstp_pacific
	 , o.edi_ind 
	 , o.start_ship_date 
	 , o.end_ship_date
	 , o.otb_eow_date 
	 , o.latest_approval_date 
	 , o.otb_month 
	 , o.otb_month_idnt 
	 , o.order_type 
	 , o.internal_po_ind 
	 , o.npg_ind 
	 , o.po_type 
	 , o.purchase_type 
	 , o.month_idnt 
	 , Month 
	 , DENSE_RANK() OVER (PARTITION BY purchase_order_num, vpn, color_num, supp_color, style_desc, TRIM(supp_num), TRIM(FORMAT('%11d', division_num)) || ', ' || TRIM( division_name), TRIM(FORMAT('%11d', class_num)) || ', ' || TRIM( class_desc), TRIM(FORMAT('%11d', sbclass_num)) || ', ' || TRIM( sbclass_desc) ORDER BY month_idnt) as month_join
	 , SUM(RCPT_COST) AS RCPT_COST
	 , SUM(RCPT_RETAIL) AS RCPT_RETAIL
	 , SUM(RCPT_UNITS) AS RCPT_UNITS
	 , SUM(QUANTITY_ORDERED) AS QUANTITY_ORDERED
	 , SUM(QUANTITY_RECEIVED) AS  QUANTITY_RECEIVED
	 , SUM(QUANTITY_CANCELED) AS QUANTITY_CANCELED
	 , SUM(QUANTITY_OPEN) AS QUANTITY_OPEN
	 , MAX(UNIT_COST_AMT) AS UNIT_COST_AMT
	 , SUM(TOTAL_ANTICIPATED_RETAIL_AMT) AS TOTAL_ANTICIPATED_RETAIL_AMT
	 , SUM(vasn_sku_qty) AS vasn_sku_qty
	 , SUM(vasn_cost) AS vasn_cost
	 , SUM(dc_received_qty) as DC_RCPT_UNITS
	 , SUM(dc_received_c) AS DC_RCPT_COST
	FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.po_otb_detail o
	LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.PURCHASE_ORDER_HEADER_FACT h
	ON o.purchase_order_num = h.purchase_order_number 
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,supp_num,division_num,division_name,class_num,class_desc,sbclass_num,sbclass_desc
;


--COLLECT STATS  	PRIMARY INDEX (otb_eow_date, purchase_order_num, vpn) 	,COLUMN (otb_eow_date) 	,COLUMN (purchase_order_num) 		ON main


/***********************************************************/


/******************* INTERNAL XREF ORDERS ******************/


/***********************************************************/


--DROP TABLE voh;


--ADDED BY ME FOR CPU ISSUES


CREATE TEMPORARY TABLE IF NOT EXISTS voh
CLUSTER BY order_no
AS
SELECT purchase_order_num AS order_no,
 crossreference_external_id AS rms_xref_po,
 written_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact
WHERE written_date >= DATE_SUB(CURRENT_DATE, INTERVAL 365 DAY);


--COLLECT STATS  	PRIMARY INDEX (order_no) 	,COLUMN (rms_xref_po) 	,COLUMN (written_date) 		ON voh


/***********************************************************/


/******************* INTERNAL ORDERS STAGE *****************/


/***********************************************************/


--Creates table of internal POs, with their parent PO numbers. Commented out original code with CTE instead of temp table


--INCLUDES POS WITH PARENT_PO AND INTERNAL PO THAT ARE THE SAME NUMBER SO EXCLUDING BELOW


--DROP TABLE internal_orders_stage;


--with voh as 


--(


--	SELECT purchase_order_num as order_no, crossreference_external_id as rms_xref_po, written_date


--	from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact


--)


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





/***********************************************************/


/**************** PARENT ORDERS BASE *********************/


/***********************************************************/


--Creates a distinct list of Parent POs


--DROP TABLE parent_orders_base;


CREATE TEMPORARY TABLE IF NOT EXISTS parent_orders_base
CLUSTER BY parent_po
AS
SELECT DISTINCT parent_po
FROM internal_orders_stage
WHERE LOWER(parent_po) <> LOWER(order_no);





--COLLECT STATS  	PRIMARY INDEX (parent_po) 		ON parent_orders_base


/***********************************************************/


/**************** INTERNAL ORDERS BASE *********************/


/***********************************************************/


--Filters out where parent and internal are the same


--DROP TABLE internal_orders_base;


CREATE TEMPORARY TABLE IF NOT EXISTS internal_orders_base
CLUSTER BY order_no
AS
SELECT *
FROM internal_orders_stage
WHERE LOWER(parent_po) <> LOWER(order_no);





--COLLECT STATS  	PRIMARY INDEX (order_no) 	,COLUMN (parent_po) 		ON internal_orders_base


/***********************************************************/


/************************ DATES ****************************/


/***********************************************************/


--rolls dates up to week so we can join to PO table


--DROP TABLE po_dates


CREATE TEMPORARY TABLE IF NOT EXISTS po_dates
CLUSTER BY month_idnt
AS
SELECT month_idnt,
 month_label,
 month_end_day_date,
  CASE
  WHEN month_start_day_date <= CURRENT_DATE AND month_end_day_date >= CURRENT_DATE
  THEN 1
  ELSE 0
  END AS curr_month_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
GROUP BY month_idnt,
 month_label,
 month_end_day_date,
 curr_month_ind;


--COLLECT STATS  	PRIMARY INDEX (month_idnt) 	,COLUMN (month_idnt) 		ON po_dates


/***********************************************************/


/****************** Total Quantities ***********************/


/***********************************************************/


--grabs the values from the RR table that we can't get from Outbound table


--DROP TABLE rr_quantity


CREATE TEMPORARY TABLE IF NOT EXISTS rr_quantity
CLUSTER BY month_end_day_date, purchase_order_number, rms_sku_num
AS
SELECT rr.eom_status,
 rr.month_end_day_date,
 rr.internal_po_ind,
 rr.purchase_order_number,
 rr.rms_sku_num,
 rr.latest_edi_date,
 rr.po_vasn_signal,
 SUM(rr.eom_quantity_ordered) AS eom_quantity_ordered,
 SUM(rr.eom_quantity_ordered * rr.eom_unit_cost) AS eom_quantity_ordered_c,
 SUM(rr.original_quantity_ordered) AS original_quantity_ordered,
 SUM(rr.eom_receipt_qty) AS eom_receipt_qty,
 SUM(rr.eom_receipt_qty * rr.eom_unit_cost) AS eom_receipt_c,
 MAX(rr.ownership_price_amt) AS ownership_price_amt,
 MAX(rr.eom_unit_cost) AS eom_unit_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.receipt_recon_fact_vw AS rr
 INNER JOIN po_dates AS d ON rr.month_end_day_date = d.month_end_day_date AND d.curr_month_ind = 1
GROUP BY rr.eom_status,
 rr.month_end_day_date,
 rr.internal_po_ind,
 rr.purchase_order_number,
 rr.rms_sku_num,
 rr.latest_edi_date,
 rr.po_vasn_signal;


--COLLECT STATS  	PRIMARY INDEX (month_end_day_date, purchase_order_number, rms_sku_num) 	,COLUMN (month_end_day_date) 	,COLUMN (purchase_order_number) 		ON rr_quantity


/***********************************************************/


/********************* PO SKU Table ************************/


/***********************************************************/


--joins outbound sku table to RR metrics and internal / parent PO tables


--DROP Table po_sku_table


CREATE TEMPORARY TABLE IF NOT EXISTS po_sku_table
CLUSTER BY purchase_order_num, sku_num
AS
SELECT s.purchase_order_num,
 s.sku_num,
 rr.eom_status,
 rr.internal_po_ind,
 rr.eom_unit_cost,
 rr.ownership_price_amt,
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
 SUM(s.rcpt_cost) AS rcpt_c,
 SUM(s.rcpt_units) AS rcpt_u,
 SUM(s.quantity_open * rr.eom_unit_cost) AS oo_c,
 SUM(s.quantity_open) AS oo_u,
 SUM(s.quantity_ordered) AS ordered_u,
 SUM(rr.eom_quantity_ordered) AS ordered_u_rr,
 SUM(rr.original_quantity_ordered) AS original_quantity_ordered
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.outbound_po_sku_lkp AS s
 LEFT JOIN internal_orders_base AS inx ON LOWER(s.purchase_order_num) = LOWER(inx.order_no)
 LEFT JOIN parent_orders_base AS par ON LOWER(s.purchase_order_num) = LOWER(par.parent_po)
 LEFT JOIN rr_quantity AS rr ON LOWER(s.sku_num) = LOWER(rr.rms_sku_num) AND LOWER(s.purchase_order_num) = LOWER(rr.purchase_order_number
    )
GROUP BY s.purchase_order_num,
 s.sku_num,
 rr.eom_status,
 rr.internal_po_ind,
 rr.eom_unit_cost,
 rr.ownership_price_amt,
 xref_po_ind,
 parent_po_ind,
 parent_po;


--COLLECT STATS      PRIMARY INDEX (purchase_order_num, sku_num) 	,COLUMN (purchase_order_num) 	,COLUMN (sku_num) 		ON po_sku_table


/***********************************************************/


/******************** Parent PO Table **********************/


/***********************************************************/


--Grabs just the parent PO's in worksheet and approved status


--DROP table parent_po_test


CREATE TEMPORARY TABLE IF NOT EXISTS parent_po
CLUSTER BY sku_num, purchase_order_num
AS
SELECT *
FROM po_sku_table
WHERE LOWER(parent_po_ind) = LOWER('Y')
 AND (LOWER(eom_status) = LOWER('WORKSHEET') OR LOWER(eom_status) = LOWER('APPROVED'));


--COLLECT STATS  	PRIMARY INDEX (sku_num, purchase_order_num) 	,COLUMN (sku_num) 	,COLUMN (purchase_order_num) 		ON parent_po


/***********************************************************/


/********************* XREF PO Table ***********************/


/***********************************************************/


--Grabs just the XREF PO's 


--DROP TABLE xref_po_test


CREATE TEMPORARY TABLE IF NOT EXISTS xref_po
CLUSTER BY sku_num, purchase_order_num
AS
SELECT *
FROM po_sku_table
WHERE LOWER(xref_po_ind) = LOWER('Y')
 OR LOWER(internal_po_ind) = LOWER('T');


--COLLECT STATS  	PRIMARY INDEX (sku_num, purchase_order_num)     ,COLUMN (sku_num) 	,COLUMN (purchase_order_num) 		ON xref_po


/***********************************************************/


/****************** Double Booking Table *******************/


/***********************************************************/


--Joins parent and XREF po tables to get the double booking values 


--DROP table double_booking_cases;


CREATE TEMPORARY TABLE IF NOT EXISTS double_booking_cases
CLUSTER BY xref_po_num, parent_po
AS
SELECT x.purchase_order_num AS xref_po_num,
 x.parent_po,
 x.sku_num,
 p.eom_status,
 x.rcpt_c AS xref_rcpt_c,
 p.rcpt_c AS parent_rcpt_c,
 x.rcpt_u AS xref_rcpt_u,
 p.rcpt_u AS parent_rcpt_u,
 x.oo_c AS xref_oo_c,
 p.oo_c AS parent_oo_c,
 x.oo_u AS xref_oo_u,
 p.oo_u AS parent_oo_u,
 COALESCE(x.ordered_u_rr - x.rcpt_u, 0) AS net_store_receipts,
 COALESCE(CASE
   WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
   THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
   ELSE 0
   END, 0) AS store_over_u,
  x.rcpt_u - COALESCE(CASE
    WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
    THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
    ELSE 0
    END, 0) AS rcpt_u_less_over_u,
  CASE
  WHEN p.oo_u > 0 AND x.rcpt_u - COALESCE(CASE
       WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
       THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
       ELSE 0
       END, 0) > 0
  THEN x.rcpt_u - COALESCE(CASE
     WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
     THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
     ELSE 0
     END, 0)
  ELSE 0
  END AS parent_po_open_w_xref_receipts_u,
  CASE
  WHEN p.oo_u > 0 AND x.rcpt_u - COALESCE(CASE
       WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
       THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
       ELSE 0
       END, 0) > 0
  THEN (x.rcpt_u - COALESCE(CASE
       WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
       THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
       ELSE 0
       END, 0)) * x.eom_unit_cost
  ELSE 0
  END AS parent_po_open_w_xref_receipts_c,
  CASE
  WHEN p.oo_u > 0 AND x.rcpt_u - COALESCE(CASE
       WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
       THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
       ELSE 0
       END, 0) > 0
  THEN (x.rcpt_u - COALESCE(CASE
       WHEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) < 0
       THEN COALESCE(x.ordered_u_rr - x.rcpt_u, 0) * - 1
       ELSE 0
       END, 0)) * x.ownership_price_amt
  ELSE 0
  END AS parent_po_open_w_xref_receipts_r,
  CASE
  WHEN p.oo_u > 0 AND x.oo_u > 0 AND LOWER(x.eom_status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))
  THEN x.oo_u
  ELSE 0
  END AS parent_po_open_w_xref_open_u,
  CASE
  WHEN p.oo_u > 0 AND x.oo_u > 0 AND LOWER(x.eom_status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))
  THEN x.oo_c
  ELSE 0
  END AS parent_po_open_w_xref_open_c,
  CASE
  WHEN p.oo_u > 0 AND x.oo_u > 0 AND LOWER(x.eom_status) IN (LOWER('APPROVED'), LOWER('WORKSHEET'))
  THEN x.oo_u * x.ownership_price_amt
  ELSE 0
  END AS parent_po_open_w_xref_open_r
FROM xref_po AS x
 INNER JOIN parent_po AS p ON LOWER(x.parent_po) = LOWER(p.purchase_order_num) AND LOWER(x.sku_num) = LOWER(p.sku_num);


--COLLECT STATS  	PRIMARY INDEX (xref_po_num, parent_po) 	,COLUMN (xref_po_num) 	,COLUMN (parent_po) 		ON double_booking_cases


/***********************************************************/


/********************** RR Rollups *************************/


/***********************************************************/


--Rolls the RR table up to month and VPN


--drop table rr_rollup;


CREATE TEMPORARY TABLE IF NOT EXISTS rr_rollup
CLUSTER BY purchase_order_number, vpn
AS
SELECT s.eom_status,
 s.internal_po_ind,
 s.purchase_order_number,
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
 hier.gwp_ind,
 s.po_vasn_signal,
 s.latest_edi_date,
 SUM(s.eom_quantity_ordered) AS eom_quantity_ordered,
 SUM(s.original_quantity_ordered) AS original_quantity_ordered,
 SUM(s.eom_receipt_qty) AS eom_receipt_qty,
 MAX(s.ownership_price_amt) AS ownership_price_amt,
 MAX(s.eom_unit_cost) AS eom_unit_cost
FROM rr_quantity AS s
 LEFT JOIN hier ON LOWER(s.rms_sku_num) = LOWER(hier.rms_sku_num)
GROUP BY s.eom_status,
 s.internal_po_ind,
 s.purchase_order_number,
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
 hier.gwp_ind,
 s.po_vasn_signal,
 s.latest_edi_date;


--COLLECT STATS  	PRIMARY INDEX (purchase_order_number, vpn) 	,COLUMN (purchase_order_number) 	,COLUMN (vpn) 		ON rr_rollup


/***********************************************************/


/********************* Staging Table ***********************/


/***********************************************************/


--Joins hierarchy to the double booking values to get ready to join to main table


--drop table staging_1


CREATE TEMPORARY TABLE IF NOT EXISTS staging_1
CLUSTER BY xref_po_num, parent_po, vpn
AS
SELECT d.xref_po_num,
 d.parent_po,
 d.eom_status,
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
 SUM(d.xref_rcpt_c) AS xref_rcpt_c,
 SUM(d.parent_rcpt_c) AS parent_rcpt_c,
 SUM(d.xref_rcpt_u) AS xref_rcpt_u,
 SUM(d.parent_rcpt_u) AS parent_rcpt_u,
 SUM(d.xref_oo_c) AS xref_oo_c,
 SUM(d.parent_oo_c) AS parent_oo_c,
 SUM(d.xref_oo_u) AS xref_oo_u,
 SUM(d.parent_oo_u) AS parent_oo_u,
 SUM(d.parent_po_open_w_xref_receipts_u) AS parent_po_open_w_xref_receipts_u,
 SUM(d.parent_po_open_w_xref_receipts_c) AS parent_po_open_w_xref_receipts_c,
 SUM(d.parent_po_open_w_xref_receipts_r) AS parent_po_open_w_xref_receipts_r,
 SUM(d.parent_po_open_w_xref_open_u) AS parent_po_open_w_xref_open_u,
 SUM(d.parent_po_open_w_xref_open_c) AS parent_po_open_w_xref_open_c,
 SUM(d.parent_po_open_w_xref_open_r) AS parent_po_open_w_xref_open_r
FROM double_booking_cases AS d
 LEFT JOIN hier ON LOWER(d.sku_num) = LOWER(hier.rms_sku_num)
GROUP BY d.xref_po_num,
 d.parent_po,
 d.eom_status,
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
 hier.prmy_supp_num;


--COLLECT STATS  	PRIMARY INDEX (xref_po_num, parent_po, vpn) 	,COLUMN (xref_po_num) 	,COLUMN (parent_po)     ,COLUMN (vpn) 		ON staging_1


/***********************************************************/


/*********************** Full Data *************************/


/***********************************************************/


--Joins double booked values to the main table to get the full data set


--{env_suffix}



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.xref_po_daily{{params.env_suffix}};


--{env_suffix}



INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.xref_po_daily{{params.env_suffix}} 

SELECT
    cast(timestamp(current_datetime('PST8PDT')) as datetime)  AS refresh_timestamp,
    o.purchase_order_num,
    o.channel_num,
    o.channel_brand,
    o.division,
    o.subdivision,
    o.department,
    o.class,
    o.subclass,
    o.supplier_number,
    o.supplier_name,
    o.vpn,
    o.style_desc,
    o.color_num,
    o.supp_color,
    o.customer_choice,
    o.po_status,
    CASE
      WHEN upper(rtrim(o.edi_ind, ' ')) = 'T' THEN 'Y'
      ELSE 'N'
    END AS edi_ind,
    o.otb_eow_date,
    o.otb_month,
    o.otb_month_idnt,
    CASE
      WHEN coalesce(par.parent_po, inx.parent_po) IS NOT NULL THEN otb_month_idnt
    END AS parent_otb_month_idnt,
    o.start_ship_date,
    o.end_ship_date,
    o.latest_approval_date,
    o.latest_close_event_tmstp_pacific,
    o.comments,
    o.order_type,
    o.internal_po_ind,
    CASE
      WHEN inx.order_no IS NOT NULL THEN 'Y'
      ELSE 'N'
    END AS xref_po_ind,
    CASE
      WHEN par.parent_po IS NOT NULL THEN 'Y'
      ELSE 'N'
    END AS parent_po_ind,
    coalesce(par.parent_po, inx.parent_po) AS parent_po,
    CASE
      WHEN upper(rtrim(o.npg_ind, ' ')) = 'T' THEN 'Y'
      ELSE 'N'
    END AS npg_ind,
    r.gwp_ind,
    o.po_type,
    o.purchase_type,
    o.month_idnt,
    o.month,
    cast(o.rcpt_cost as numeric),
    cast(o.rcpt_retail as numeric),
    cast(o.rcpt_units as int64),
    cast(o.quantity_ordered as int64),
    cast(o.quantity_received as int64),
    cast(o.quantity_canceled as int64),
    cast(o.quantity_open as int64),
    cast(o.unit_cost_amt as int64),
    cast(o.total_anticipated_retail_amt as numeric),
    r.eom_receipt_qty AS dc_rcpt_units,
    r.eom_receipt_qty * eom_unit_cost AS dc_rcpt_cost,
    o.vasn_sku_qty,
    cast(o.vasn_cost as numeric),
    CASE
      WHEN o.vasn_sku_qty > 0 THEN 'Y'
      ELSE 'N'
    END AS vasn_ind,
    cast(xref_rcpt_c as numeric),
    cast(parent_rcpt_c as numeric),
    cast(xref_rcpt_u as int64),
    cast(parent_rcpt_u as int64),
    cast(xref_oo_c as int64),
    cast(parent_oo_c as int64),
    cast(xref_oo_u as int64),
    cast(parent_oo_u as int64),
    cast(COALESCE(eom_quantity_ordered - rcpt_units,0) as int64) AS net_store_receipts,
    cast(COALESCE(CASE WHEN cast(COALESCE(eom_quantity_ordered - rcpt_units,0) as int64) < 0 THEN cast(COALESCE(eom_quantity_ordered - rcpt_units,0) as int64) * -1 ELSE 0 END,0) as int64) AS store_over_u,
    cast(COALESCE(CASE WHEN cast(COALESCE(eom_quantity_ordered - rcpt_units,0) as int64) < 0 THEN cast(COALESCE(eom_quantity_ordered - rcpt_units,0) as int64) * -1 ELSE 0 END,0)  * eom_unit_cost as numeric) AS store_over_c,
    cast(parent_po_open_w_xref_receipts_u as int64),
    cast(parent_po_open_w_xref_receipts_c as numeric),
    cast(parent_po_open_w_xref_receipts_r as numeric),
    cast(parent_po_open_w_xref_open_u as int64),
    cast(parent_po_open_w_xref_open_c as numeric),
    cast(parent_po_open_w_xref_open_r as numeric),
    cast(o.quantity_canceled as int64) AS canceled_u,
    cast(o.quantity_canceled * unit_cost_amt as numeric) AS canceled_c,
    cast(o.quantity_canceled * ownership_price_amt as numeric) AS canceled_r,
    cast(o.quantity_open as int64) AS open_u,
    cast(o.quantity_open * unit_cost_amt as numeric) AS open_c,
    cast(o.quantity_open * ownership_price_amt as numeric) AS open_r,
    cast(eom_quantity_ordered as int64) AS eom_ordered_u,
    cast(eom_quantity_ordered * eom_unit_cost as numeric) AS eom_ordered_c,
    cast(eom_quantity_ordered * ownership_price_amt as numeric) AS eom_ordered_r,
    cast(original_quantity_ordered as int64) AS og_ordered_u,
    cast(original_quantity_ordered * eom_unit_cost as numeric) AS og_ordered_c,
    cast(original_quantity_ordered * ownership_price_amt as numeric) AS og_ordered_r
  FROM
    main AS o
    LEFT OUTER JOIN internal_orders_base AS inx ON o.purchase_order_num = inx.order_no
    LEFT OUTER JOIN parent_orders_base AS par ON o.purchase_order_num = par.parent_po
    LEFT OUTER JOIN staging_1 AS s ON s.xref_po_num = o.purchase_order_num
     AND s.vpn = o.vpn
     AND s.nrf_color_code = o.color_num
     AND s.supplier_color = o.supp_color
     AND s.style_desc = o.style_desc
     AND s.prmy_supp_num = o.supplier_number
     AND s.division = o.division
     AND s.class = o.class
     AND s.subclass = o.subclass
     AND o.month_join = 1
    LEFT OUTER JOIN rr_rollup AS r ON r.purchase_order_number = o.purchase_order_num
     AND r.vpn = o.vpn
     AND r.nrf_color_code = o.color_num
     AND r.supplier_color = o.supp_color
     AND r.style_desc = o.style_desc
     AND r.prmy_supp_num = o.supplier_number
     AND r.division = o.division
     AND r.class = o.class
     AND r.subclass = o.subclass
     AND o.month_join = 1
;



END