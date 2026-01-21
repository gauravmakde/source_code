/*------------------------------------------------------
 Merch Customer Reporting
 Purpose: Provide customer trends by product hierarchy at a monthly grain
 
 Last Update: 11/29/23 Rasagnya Avala - ETL Pipeline creation
 Contact for logic/code: Rasagnya Avala (Merch Analytics)
 Contact for ETL: Rasagnya Avala (Merch Analytics)
 --------------------------------------------------------*/

-- Date Lookup



CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup AS
SELECT MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt,
 MIN(day_date_last_year_realigned) AS ly_start_dt,
 MAX(day_date_last_year_realigned) AS ly_end_dt,
 MIN(lly_day_date) AS lly_start_dt,
 MAX(lly_day_date) AS lly_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt = (SELECT DISTINCT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 10 DAY));


CREATE TEMPORARY TABLE IF NOT EXISTS tran_base_ty
AS
SELECT 'TY' AS ty_ly_ind,
  CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
 tran.acp_id,
 tran.sku_num,
 tran.trip_id,
 tran.store_num,
  CASE
  WHEN ntn.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS ntn_flg,
 MIN(tran.sale_date) AS sale_date,
 SUM(CASE
   WHEN tran.gc_flag = 0
   THEN tran.net_amt
   ELSE 0
   END) AS net_spend,
 SUM(CASE
   WHEN tran.gc_flag = 0
   THEN tran.gross_amt
   ELSE 0
   END) AS gross_spend,
 SUM(tran.gross_items) AS gross_items,
 SUM(tran.net_items) AS net_items
FROM (SELECT acp_id,
   sku_num,
   global_tran_id,
   business_unit_desc,
    shipped_usd_sales - return_usd_amt AS net_amt,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_usd_sales
    ELSE NULL
    END AS gross_amt,
    CASE
    WHEN LOWER(nonmerch_fee_code) = LOWER('6666')
    THEN 1
    ELSE 0
    END AS gc_flag,
    CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
    THEN 867
    ELSE intent_store_num
    END AS store_num,
   COALESCE(order_date, tran_date) AS sale_date,
    CASE
    WHEN shipped_usd_sales > 0
    THEN SUBSTR(acp_id || '_' || FORMAT('%11d', CASE
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
           THEN 808
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
           THEN 867
           ELSE intent_store_num
           END) || '_' || '_' || CAST(COALESCE(order_date, tran_date) AS STRING), 1, 150)
    ELSE NULL
    END AS trip_id,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_qty
    ELSE NULL
    END AS gross_items,
    CASE
    WHEN shipped_usd_sales < 0
    THEN shipped_qty * - 1
    WHEN shipped_usd_sales = 0
    THEN 0
    ELSE shipped_qty
    END AS net_items
  FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact AS dtl
  WHERE COALESCE(order_date, tran_date) >= (SELECT ty_start_dt
     FROM date_lookup)
   AND COALESCE(order_date, tran_date) <= (SELECT ty_end_dt
     FROM date_lookup)
   AND CASE
     WHEN shipped_usd_sales < 0
     THEN shipped_qty * - 1
     WHEN shipped_usd_sales = 0
     THEN 0
     ELSE shipped_qty
     END > 0
   AND LOWER(transaction_type) = LOWER('retail')
   AND business_day_date BETWEEN (SELECT ty_start_dt
     FROM date_lookup) AND (SELECT ty_end_dt
     FROM date_lookup)
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str 
 ON tran.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE' ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_fact AS ntn ON tran.global_tran_id = ntn.ntn_global_tran_id AND LOWER(tran.sku_num ) = LOWER(ntn.sku_num)
WHERE LOWER(str.store_country_code) = LOWER('US')
GROUP BY ty_ly_ind,
 channel,
 tran.acp_id,
 tran.sku_num,
 tran.trip_id,
 tran.store_num,
 ntn_flg;

CREATE TEMPORARY TABLE IF NOT EXISTS tran_base_ly
AS
SELECT 'LY' AS ty_ly_ind,
  CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
 tran.acp_id,
 tran.sku_num,
 tran.trip_id,
 tran.store_num,
  CASE
  WHEN ntn.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS ntn_flg,
 MIN(tran.sale_date) AS sale_date,
 SUM(CASE
   WHEN tran.gc_flag = 0
   THEN tran.net_amt
   ELSE 0
   END) AS net_spend,
 SUM(CASE
   WHEN tran.gc_flag = 0
   THEN tran.gross_amt
   ELSE 0
   END) AS gross_spend,
 SUM(tran.gross_items) AS gross_items,
 SUM(tran.net_items) AS net_items
FROM (SELECT acp_id,
   sku_num,
   global_tran_id,
   business_unit_desc,
    shipped_usd_sales - return_usd_amt AS net_amt,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_usd_sales
    ELSE NULL
    END AS gross_amt,
    CASE
    WHEN LOWER(nonmerch_fee_code) = LOWER('6666')
    THEN 1
    ELSE 0
    END AS gc_flag,
    CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
    THEN 867
    ELSE intent_store_num
    END AS store_num,
   COALESCE(order_date, tran_date) AS sale_date,
    CASE
    WHEN shipped_usd_sales > 0
    THEN SUBSTR(acp_id || '_' || FORMAT('%11d', CASE
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
           THEN 808
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
           THEN 867
           ELSE intent_store_num
           END) || '_' || '_' || CAST(COALESCE(order_date, tran_date) AS STRING), 1, 150)
    ELSE NULL
    END AS trip_id,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_qty
    ELSE NULL
    END AS gross_items,
    CASE
    WHEN shipped_usd_sales < 0
    THEN shipped_qty * - 1
    WHEN shipped_usd_sales = 0
    THEN 0
    ELSE shipped_qty
    END AS net_items
  FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact AS dtl
  WHERE COALESCE(order_date, tran_date) >= (SELECT ly_start_dt
     FROM date_lookup)
   AND COALESCE(order_date, tran_date) <= (SELECT ly_end_dt
     FROM date_lookup)
   AND CASE
     WHEN shipped_usd_sales < 0
     THEN shipped_qty * - 1
     WHEN shipped_usd_sales = 0
     THEN 0
     ELSE shipped_qty
     END > 0
   AND LOWER(transaction_type) = LOWER('retail')
   AND business_day_date BETWEEN (SELECT ly_start_dt
     FROM date_lookup) AND (SELECT ly_end_dt
     FROM date_lookup)
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str 
 ON tran.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE' ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_fact AS ntn 
 ON tran.global_tran_id = ntn.ntn_global_tran_id AND LOWER(tran.sku_num) = LOWER(ntn.sku_num)
WHERE LOWER(str.store_country_code) = LOWER('US')
GROUP BY ty_ly_ind,
 channel,
 tran.acp_id,
 tran.sku_num,
 tran.trip_id,
 tran.store_num,
 ntn_flg;

CREATE TEMPORARY TABLE IF NOT EXISTS anniv_skus
AS
SELECT DISTINCT rms_sku_num AS sku_idnt,
 'Anniversary' AS event_type,
 EXTRACT(YEAR FROM enticement_start_tmstp) AS event_year
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_promotion_timeline_dim AS pptd
WHERE LOWER(channel_country) = LOWER('US')
 AND LOWER(enticement_tags) LIKE LOWER('%ANNIVERSARY_SALE%')
 AND EXTRACT(YEAR FROM enticement_start_tmstp) <= EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT'))
 AND EXTRACT(YEAR FROM enticement_start_tmstp) >= EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT')) - 1;

CREATE TEMPORARY TABLE IF NOT EXISTS cyber_skus_2023
AS
SELECT sku_idnt,
 banner,
 'US' AS country,
  CASE
  WHEN LOWER(banner) = LOWER('RACK') AND udig_rank = 3
  THEN '1, HOL23_NR_CYBER_SPECIAL PURCHASE'
  WHEN LOWER(banner) = LOWER('NORDSTROM') AND udig_rank = 1
  THEN '1, HOL23_N_CYBER_SPECIAL PURCHASE'
  WHEN LOWER(banner) = LOWER('NORDSTROM') AND udig_rank = 2
  THEN '2, CYBER 2023_US'
  ELSE NULL
  END AS udig,
 udig_event_type AS event_type,
 2023 AS cyber_year
FROM (SELECT sku_idnt,
   banner,
   'Cyber' AS udig_event_type,
   MIN(CASE
     WHEN LOWER(item_group) = LOWER('1, HOL23_N_CYBER_SPECIAL PURCHASE')
     THEN 1
     WHEN LOWER(item_group) = LOWER('2, CYBER 2023_US')
     THEN 2
     WHEN LOWER(item_group) = LOWER('1, HOL23_NR_CYBER_SPECIAL PURCHASE')
     THEN 3
     ELSE NULL
     END) AS udig_rank
  FROM (SELECT DISTINCT sku_idnt,
      CASE
      WHEN CAST(udig_colctn_idnt AS FLOAT64) = 50009
      THEN 'RACK'
      ELSE 'NORDSTROM'
      END AS banner,
       udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_usr_vws.udig_itm_grp_sku_lkup
    WHERE LOWER(udig_colctn_idnt) IN (LOWER('50008'), LOWER('50009'), LOWER('25032'))
     AND LOWER(udig_itm_grp_nm) IN (LOWER('HOL23_NR_CYBER_SPECIAL PURCHASE'), LOWER('HOL23_N_CYBER_SPECIAL PURCHASE'),
       LOWER('CYBER 2023_US'))) AS t2
  GROUP BY sku_idnt,
   banner,
   udig_event_type) AS rnk;


CREATE TEMPORARY TABLE IF NOT EXISTS cyber_skus_2022
AS
SELECT sku_idnt,
 banner,
 country,
  CASE
  WHEN udig_rank = 1
  THEN '1, HOL22_N_CYBER_SPECIAL PURCHASE'
  WHEN udig_rank = 2
  THEN '1, HOL22_NR_CYBER_SPECIAL PURCHASE'
  WHEN udig_rank = 3
  THEN '2, CYBER 2022_US'
  WHEN udig_rank = 4
  THEN '6, STOCKING STUFFERS 2022_US'
  WHEN udig_rank = 5
  THEN '1, GIFTS & GLAM 2022_US'
  WHEN udig_rank = 6
  THEN '3, HOLIDAY TREATMENT 2022_US'
  WHEN udig_rank = 7
  THEN '4, PALETTES 2022_US'
  WHEN udig_rank = 8
  THEN '5, HOLIDAY FRAGRANCE 2022_US'
  WHEN udig_rank = 9
  THEN '7, FRAGRANCE STOCKING STUFFERS 2022_US'
  ELSE NULL
  END AS udig,
 udig_event_type AS event_type,
 2022 AS cyber_year
FROM (SELECT sku_idnt,
   country,
   banner,
   'Cyber' AS udig_event_type,
   MIN(CASE
     WHEN LOWER(item_group) = LOWER('1, HOL22_N_CYBER_SPECIAL PURCHASE')
     THEN 1
     WHEN LOWER(item_group) = LOWER('1, HOL22_NR_CYBER_SPECIAL PURCHASE')
     THEN 2
     WHEN LOWER(item_group) = LOWER('2, CYBER 2022_US')
     THEN 3
     WHEN LOWER(item_group) = LOWER('6, STOCKING STUFFERS 2022_US')
     THEN 4
     WHEN LOWER(item_group) = LOWER('1, GIFTS & GLAM 2022_US')
     THEN 5
     WHEN LOWER(item_group) = LOWER('3, HOLIDAY TREATMENT 2022_US')
     THEN 6
     WHEN LOWER(item_group) = LOWER('4, PALETTES 2022_US')
     THEN 7
     WHEN LOWER(item_group) = LOWER('5, HOLIDAY FRAGRANCE 2022_US')
     THEN 8
     WHEN LOWER(item_group) = LOWER('7, FRAGRANCE STOCKING STUFFERS 2022_US')
     THEN 9
     ELSE NULL
     END) AS udig_rank
  FROM (SELECT DISTINCT sku_idnt,
        udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group,
      'US' AS country,
       CASE
       WHEN CAST(udig_colctn_idnt AS FLOAT64) = 50009
       THEN 'RACK'
       ELSE 'NORDSTROM'
       END AS banner
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_usr_vws.udig_itm_grp_sku_lkup
     WHERE LOWER(udig_colctn_nm) IN (LOWER('HOLIDAY 2022_RACK'), LOWER('HOLIDAY 2022_NORDSTROM'), LOWER('US BEAUTY 22 HOLIDAY REPORTING'
         ))
     UNION ALL
     SELECT DISTINCT sku_idnt,
        udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group,
      'CA' AS country,
       CASE
       WHEN CAST(udig_colctn_idnt AS FLOAT64) = 50009
       THEN 'RACK'
       ELSE 'NORDSTROM'
       END AS banner
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_usr_vws.udig_itm_grp_sku_lkup
     WHERE LOWER(udig_colctn_nm) IN (LOWER('HOLIDAY 2022_RACK'), LOWER('HOLIDAY 2022_NORDSTROM'))) 
  GROUP BY sku_idnt,
   country,
   banner,
   udig_event_type) AS rnk;

CREATE TEMPORARY TABLE IF NOT EXISTS cyber_skus
AS
SELECT *
FROM cyber_skus_2023
UNION ALL
SELECT *
FROM cyber_skus_2022;

CREATE TEMPORARY TABLE IF NOT EXISTS events_data
AS
SELECT DISTINCT tran_base_ty.acp_id,
 tran_base_ty.sale_date,
  'Anniversary - ' || FORMAT('%20d', anniv_skus.event_year) AS event_type
FROM tran_base_ty
 INNER JOIN anniv_skus ON LOWER(anniv_skus.sku_idnt) = LOWER(tran_base_ty.sku_num) AND anniv_skus.event_year = EXTRACT(YEAR
    FROM CURRENT_DATE('PST8PDT'))
WHERE tran_base_ty.sale_date BETWEEN (SELECT MIN(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE LOWER(ty_ly_lly) = LOWER('TY')
    AND anniv_ind = 1
    AND LOWER(event_type) NOT IN (LOWER('Non-Event'), LOWER('Post'))) AND (SELECT MAX(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE LOWER(ty_ly_lly) = LOWER('TY')
    AND anniv_ind = 1
    AND LOWER(event_type) NOT IN (LOWER('Non-Event'), LOWER('Post')))
UNION ALL
SELECT DISTINCT tran_base_ly.acp_id,
 tran_base_ly.sale_date,
  'Anniversary - ' || FORMAT('%20d', anniv_skus0.event_year) AS event_type
FROM tran_base_ly
 INNER JOIN anniv_skus AS anniv_skus0 ON LOWER(anniv_skus0.sku_idnt) = LOWER(tran_base_ly.sku_num) AND anniv_skus0.event_year
    = EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT')) - 1
WHERE tran_base_ly.sale_date BETWEEN (SELECT MIN(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE LOWER(ty_ly_lly) = LOWER('LY')
    AND anniv_ind = 1
    AND LOWER(event_type) NOT IN (LOWER('Non-Event'), LOWER('Post'))) AND (SELECT MAX(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE LOWER(ty_ly_lly) = LOWER('LY')
    AND anniv_ind = 1
    AND LOWER(event_type) NOT IN (LOWER('Non-Event'), LOWER('Post')))
UNION ALL
SELECT DISTINCT tran_base_ly0.acp_id,
 tran_base_ly0.sale_date,
 'Cyber - 2022' AS event_type
FROM tran_base_ly AS tran_base_ly0
 INNER JOIN cyber_skus ON LOWER(cyber_skus.sku_idnt) = LOWER(tran_base_ly0.sku_num) AND LOWER(cyber_skus.country) =
    LOWER('US') AND cyber_skus.cyber_year = 2022
WHERE tran_base_ly0.sale_date BETWEEN (SELECT MIN(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE yr_idnt = 2022
    AND cyber_ind = 1) AND (SELECT MAX(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE yr_idnt = 2022
    AND cyber_ind = 1)
UNION ALL
SELECT DISTINCT tran_base_ty0.acp_id,
 tran_base_ty0.sale_date,
 'Cyber - 2023' AS event_type
FROM tran_base_ty AS tran_base_ty0
 INNER JOIN cyber_skus AS cyber_skus0 ON LOWER(cyber_skus0.sku_idnt) = LOWER(tran_base_ty0.sku_num) AND LOWER(cyber_skus0
     .country) = LOWER('US') AND cyber_skus0.cyber_year = 2023
WHERE tran_base_ty0.sale_date BETWEEN (SELECT MIN(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE yr_idnt = 2023
    AND cyber_ind = 1) AND (SELECT MAX(day_dt)
   FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.scaled_event_dates
   WHERE yr_idnt = 2023
    AND cyber_ind = 1);

CREATE TEMPORARY TABLE IF NOT EXISTS cm_lyl_status_ty
AS
SELECT 'TY' AS ty_ly_ind,
 acp_id,
  CASE
  WHEN cardmember_fl = 1
  THEN 'cardmember'
  WHEN member_fl = 1
  THEN 'member'
  ELSE 'non-member'
  END AS loyalty_status,
  CASE
  WHEN rewards_level = 1
  THEN '1-member'
  WHEN rewards_level = 2
  THEN '2-insider'
  WHEN rewards_level = 3
  THEN '3-influencer'
  WHEN rewards_level = 4
  THEN '4-ambassador'
  WHEN rewards_level = 5
  THEN '5-icon'
  WHEN rewards_level = 0
  THEN CASE
   WHEN LOWER(CASE
      WHEN cardmember_fl = 1
      THEN 'cardmember'
      WHEN member_fl = 1
      THEN 'member'
      ELSE 'non-member'
      END) = LOWER('cardmember')
   THEN '1-member'
   WHEN LOWER(CASE
      WHEN cardmember_fl = 1
      THEN 'cardmember'
      WHEN member_fl = 1
      THEN 'member'
      ELSE 'non-member'
      END) = LOWER('member')
   THEN '1-member'
   WHEN LOWER(CASE
      WHEN cardmember_fl = 1
      THEN 'cardmember'
      WHEN member_fl = 1
      THEN 'member'
      ELSE 'non-member'
      END) = LOWER('non-member')
   THEN '0-nonmember'
   ELSE NULL
   END
  ELSE '0-nonmember'
  END AS nordy_level
FROM (SELECT ac.acp_id,
    CASE
    WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt
       FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt
         FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS cardmember_fl,
    CASE
    WHEN CASE
        WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt
           FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt
             FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
        THEN 1
        ELSE 0
        END = 0 AND lmd.member_enroll_date <= (SELECT ty_end_dt
        FROM date_lookup) AND (lmd.member_close_date >= (SELECT ty_end_dt
         FROM date_lookup) OR lmd.member_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS member_fl,
   MAX(CASE
     WHEN LOWER(t1.rewards_level) IN (LOWER('MEMBER'))
     THEN 1
     WHEN LOWER(t1.rewards_level) IN (LOWER('INSIDER'), LOWER('L1'))
     THEN 2
     WHEN LOWER(t1.rewards_level) IN (LOWER('INFLUENCER'), LOWER('L2'))
     THEN 3
     WHEN LOWER(t1.rewards_level) IN (LOWER('AMBASSADOR'), LOWER('L3'))
     THEN 4
     WHEN LOWER(t1.rewards_level) IN (LOWER('ICON'), LOWER('L4'))
     THEN 5
     ELSE 0
     END) AS rewards_level
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd ON LOWER(ac.acp_loyalty_id) = LOWER(lmd.loyalty_id)
   LEFT JOIN (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
    WHERE start_day_date <= (SELECT ty_end_dt FROM date_lookup)
     AND end_day_date > (SELECT ty_end_dt
       FROM date_lookup)) AS t1 ON LOWER(ac.acp_loyalty_id) = LOWER(t1.loyalty_id)
  GROUP BY ac.acp_id,
   cardmember_fl,
   member_fl) AS lyl_ty;

CREATE TEMPORARY TABLE IF NOT EXISTS cm_lyl_status_ly
AS
SELECT 'LY' AS ty_ly_ind,
 acp_id,
  CASE
  WHEN cardmember_fl = 1
  THEN 'cardmember'
  WHEN member_fl = 1
  THEN 'member'
  ELSE 'non-member'
  END AS loyalty_status,
  CASE
  WHEN rewards_level = 1
  THEN '1-member'
  WHEN rewards_level = 2
  THEN '2-insider'
  WHEN rewards_level = 3
  THEN '3-influencer'
  WHEN rewards_level = 4
  THEN '4-ambassador'
  WHEN rewards_level = 5
  THEN '5-icon'
  WHEN rewards_level = 0
  THEN CASE
   WHEN LOWER(CASE
      WHEN cardmember_fl = 1
      THEN 'cardmember'
      WHEN member_fl = 1
      THEN 'member'
      ELSE 'non-member'
      END) = LOWER('cardmember')
   THEN '1-member'
   WHEN LOWER(CASE
      WHEN cardmember_fl = 1
      THEN 'cardmember'
      WHEN member_fl = 1
      THEN 'member'
      ELSE 'non-member'
      END) = LOWER('member')
   THEN '1-member'
   WHEN LOWER(CASE
      WHEN cardmember_fl = 1
      THEN 'cardmember'
      WHEN member_fl = 1
      THEN 'member'
      ELSE 'non-member'
      END) = LOWER('non-member')
   THEN '0-nonmember'
   ELSE NULL
   END
  ELSE '0-nonmember'
  END AS nordy_level
FROM (SELECT ac.acp_id,
    CASE
    WHEN lmd.cardmember_enroll_date <= (SELECT ly_end_dt
       FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ly_end_dt
         FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS cardmember_fl,
    CASE
    WHEN CASE
        WHEN lmd.cardmember_enroll_date <= (SELECT ly_end_dt
           FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ly_end_dt
             FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
        THEN 1
        ELSE 0
        END = 0 AND lmd.member_enroll_date <= (SELECT ly_end_dt
        FROM date_lookup) AND (lmd.member_close_date >= (SELECT ly_end_dt
         FROM date_lookup) OR lmd.member_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS member_fl,
   MAX(CASE
     WHEN LOWER(t1.rewards_level) IN (LOWER('MEMBER'))
     THEN 1
     WHEN LOWER(t1.rewards_level) IN (LOWER('INSIDER'), LOWER('L1'))
     THEN 2
     WHEN LOWER(t1.rewards_level) IN (LOWER('INFLUENCER'), LOWER('L2'))
     THEN 3
     WHEN LOWER(t1.rewards_level) IN (LOWER('AMBASSADOR'), LOWER('L3'))
     THEN 4
     WHEN LOWER(t1.rewards_level) IN (LOWER('ICON'), LOWER('L4'))
     THEN 5
     ELSE 0
     END) AS rewards_level
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd ON LOWER(ac.acp_loyalty_id) = LOWER(lmd.loyalty_id)
   LEFT JOIN (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
    WHERE start_day_date <= (SELECT ly_end_dt
       FROM date_lookup)
     AND end_day_date > (SELECT ly_end_dt FROM date_lookup)) AS t1
	 ON LOWER(ac.acp_loyalty_id) = LOWER(t1.loyalty_id)
  GROUP BY ac.acp_id,
   cardmember_fl,
   member_fl) AS lyl_ty;

CREATE TEMPORARY TABLE IF NOT EXISTS activated_ty
AS
SELECT DISTINCT ca.acp_id,
 COALESCE(CASE
   WHEN ac.ca_dma_code IS NOT NULL
   THEN 'CA'
   ELSE NULL
   END, CASE
   WHEN ac.us_dma_code IS NOT NULL
   THEN 'US'
   ELSE NULL
   END) AS customer_country,
  CASE
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('US') AND LOWER(ca.activated_channel) = LOWER('FLS')
  THEN '110'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('CA') AND LOWER(ca.activated_channel) = LOWER('FLS')
  THEN '111'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('US') AND LOWER(ca.activated_channel) = LOWER('NCOM')
  THEN '120'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('CA') AND LOWER(ca.activated_channel) = LOWER('NCOM')
  THEN '121'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('US') AND LOWER(ca.activated_channel) = LOWER('RACK')
  THEN '210'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('CA') AND LOWER(ca.activated_channel) = LOWER('RACK')
  THEN '211'
  WHEN LOWER(ca.activated_channel) = LOWER('NRHL')
  THEN '250'
  ELSE NULL
  END AS activation_channel,
 ca.activated_channel
FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.customer_activation AS ca
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac ON LOWER(ca.acp_id) = LOWER(ac.acp_id)
WHERE ca.activated_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup) AND (SELECT ty_end_dt
   FROM date_lookup);


CREATE TEMPORARY TABLE IF NOT EXISTS activated_ly
AS
SELECT DISTINCT ca.acp_id,
 COALESCE(CASE
   WHEN ac.ca_dma_code IS NOT NULL
   THEN 'CA'
   ELSE NULL
   END, CASE
   WHEN ac.us_dma_code IS NOT NULL
   THEN 'US'
   ELSE NULL
   END) AS customer_country,
  CASE
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('US') AND LOWER(ca.activated_channel) = LOWER('FLS')
  THEN '110'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('CA') AND LOWER(ca.activated_channel) = LOWER('FLS')
  THEN '111'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('US') AND LOWER(ca.activated_channel) = LOWER('NCOM')
  THEN '120'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('CA') AND LOWER(ca.activated_channel) = LOWER('NCOM')
  THEN '121'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('US') AND LOWER(ca.activated_channel) = LOWER('RACK')
  THEN '210'
  WHEN LOWER(COALESCE(CASE
       WHEN ac.ca_dma_code IS NOT NULL
       THEN 'CA'
       ELSE NULL
       END, CASE
       WHEN ac.us_dma_code IS NOT NULL
       THEN 'US'
       ELSE NULL
       END)) = LOWER('CA') AND LOWER(ca.activated_channel) = LOWER('RACK')
  THEN '211'
  WHEN LOWER(ca.activated_channel) = LOWER('NRHL')
  THEN '250'
  ELSE NULL
  END AS activation_channel,
 ca.activated_channel
FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.customer_activation AS ca
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac ON LOWER(ca.acp_id) = LOWER(ac.acp_id)
WHERE ca.activated_date BETWEEN (SELECT ly_start_dt
   FROM date_lookup) AND (SELECT ly_end_dt
   FROM date_lookup);

CREATE TEMPORARY TABLE IF NOT EXISTS cm_customer
AS
SELECT DISTINCT ex.acp_id,
 ex.age_value,
 ex.gender
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS ex 
 ON LOWER(ac.acp_id) = LOWER(ex.acp_id
   );

CREATE TEMPORARY TABLE IF NOT EXISTS retained_ty
AS
SELECT DISTINCT tran.acp_id,
 'TY' AS ty_ly_ind
FROM (SELECT acp_id,
   sku_num,
   global_tran_id,
   business_unit_desc,
    shipped_usd_sales - return_usd_amt AS net_amt,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_usd_sales
    ELSE NULL
    END AS gross_amt,
    CASE
    WHEN LOWER(nonmerch_fee_code) = LOWER('6666')
    THEN 1
    ELSE 0
    END AS gc_flag,
    CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
    THEN 867
    ELSE intent_store_num
    END AS store_num,
   COALESCE(order_date, tran_date) AS sale_date,
    CASE
    WHEN shipped_usd_sales > 0
    THEN SUBSTR(acp_id || '_' || FORMAT('%11d', CASE
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
           THEN 808
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
           THEN 867
           ELSE intent_store_num
           END) || '_' || '_' || CAST(COALESCE(order_date, tran_date) AS STRING), 1, 150)
    ELSE NULL
    END AS trip_id,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_qty
    ELSE NULL
    END AS gross_items,
    CASE
    WHEN shipped_usd_sales < 0
    THEN shipped_qty * - 1
    WHEN shipped_usd_sales = 0
    THEN 0
    ELSE shipped_qty
    END AS net_items
  FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact AS dtl
  WHERE COALESCE(order_date, tran_date) >= (SELECT ly_start_dt
     FROM date_lookup)
   AND COALESCE(order_date, tran_date) <= (SELECT ly_end_dt
     FROM date_lookup)
   AND CASE
     WHEN shipped_usd_sales < 0
     THEN shipped_qty * - 1
     WHEN shipped_usd_sales = 0
     THEN 0
     ELSE shipped_qty
     END > 0
   AND LOWER(transaction_type) = LOWER('retail')
   AND business_day_date BETWEEN (SELECT ly_start_dt
     FROM date_lookup) AND (SELECT ly_end_dt
     FROM date_lookup)
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str 
 ON tran.store_num = str.store_num 
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE' ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA' ), LOWER('TRUNK CLUB'))
WHERE LOWER(str.store_country_code) = LOWER('US');

CREATE TEMPORARY TABLE IF NOT EXISTS retained_ly
AS
SELECT DISTINCT tran.acp_id,
 'LY' AS ty_ly_ind
FROM (SELECT acp_id,
   sku_num,
   global_tran_id,
   business_unit_desc,
    shipped_usd_sales - return_usd_amt AS net_amt,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_usd_sales
    ELSE NULL
    END AS gross_amt,
    CASE
    WHEN LOWER(nonmerch_fee_code) = LOWER('6666')
    THEN 1
    ELSE 0
    END AS gc_flag,
    CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp' ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp' ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
    THEN 867
    ELSE intent_store_num
    END AS store_num,
   COALESCE(order_date, tran_date) AS sale_date,
    CASE
    WHEN shipped_usd_sales > 0
    THEN SUBSTR(acp_id || '_' || FORMAT('%11d', CASE
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE')
           THEN 808
           WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
               ) AND LOWER(business_unit_desc) = LOWER('FULL LINE CANADA')
           THEN 867
           ELSE intent_store_num
           END) || '_' || '_' || CAST(COALESCE(order_date, tran_date) AS STRING), 1, 150)
    ELSE NULL
    END AS trip_id,
    CASE
    WHEN shipped_usd_sales > 0
    THEN shipped_qty
    ELSE NULL
    END AS gross_items,
    CASE
    WHEN shipped_usd_sales < 0
    THEN shipped_qty * - 1
    WHEN shipped_usd_sales = 0
    THEN 0
    ELSE shipped_qty
    END AS net_items
  FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact AS dtl
  WHERE COALESCE(order_date, tran_date) >= (SELECT lly_start_dt
     FROM date_lookup)
   AND COALESCE(order_date, tran_date) <= (SELECT lly_end_dt
     FROM date_lookup)
   AND CASE
     WHEN shipped_usd_sales < 0
     THEN shipped_qty * - 1
     WHEN shipped_usd_sales = 0
     THEN 0
     ELSE shipped_qty
     END > 0
   AND LOWER(transaction_type) = LOWER('retail')
   AND business_day_date BETWEEN (SELECT lly_start_dt
     FROM date_lookup) AND (SELECT lly_end_dt
     FROM date_lookup)
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON tran.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE' ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
WHERE LOWER(str.store_country_code) = LOWER('US');

CREATE TEMPORARY TABLE IF NOT EXISTS full_data_ty
AS
SELECT e.fiscal_year_num AS year_idnt,
 e.fiscal_halfyear_num AS half_idnt,
 e.quarter_idnt,
 e.month_idnt,
 e.week_idnt,
   TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' H' || SUBSTR(RPAD(CAST(e.fiscal_halfyear_num AS STRING), 5, ' '), -1) AS
 half_label,
 e.quarter_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ' ' || TRIM(e.month_abrv
   ) AS month_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ', ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ', Wk ' || TRIM(FORMAT('%11d'
    , e.week_num_of_fiscal_month)) AS week_label,
 a.channel,
 a.ty_ly_ind,
 a.acp_id,
 events_data.acp_id AS event_acp_id,
 events_data.event_type,
 act.acp_id AS activated_acp_id,
 a.ntn_flg,
 a.trip_id,
 a.store_num,
  CASE
  WHEN c.loyalty_status IS NULL
  THEN 'non-member'
  ELSE c.loyalty_status
  END AS loyalty_status,
 d.brand_name,
 d.div_num,
 d.div_desc,
 d.dept_num,
 d.dept_desc,
 d.grp_num,
 d.grp_desc,
 d.class_num,
 d.class_desc,
 d.sbclass_num,
 d.sbclass_desc,
 catg.category,
 supp.vendor_name AS supplier,
 d.npg_ind,
 cust.age_value,
 a.net_spend,
 a.gross_spend,
 a.net_items,
 a.gross_items,
 ROW_NUMBER() OVER (PARTITION BY d.npg_ind, supp.vendor_name, a.ty_ly_ind, d.web_style_num, d.rms_sku_num, catg.category
    , d.div_num, d.dept_num, d.grp_num, d.class_num, d.sbclass_num, a.trip_id, a.acp_id, d.brand_name, c.loyalty_status
    , a.channel ORDER BY a.acp_id) AS rn
FROM tran_base_ty AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d ON LOWER(a.sku_num) = LOWER(d.rms_sku_num) AND LOWER(d.channel_country) = LOWER('US')
 LEFT JOIN cm_customer AS cust ON LOWER(cust.acp_id) = LOWER(a.acp_id)
 LEFT JOIN cm_lyl_status_ty AS c ON LOWER(c.acp_id) = LOWER(a.acp_id)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS e ON a.sale_date = e.day_date
 LEFT JOIN activated_ty AS act ON LOWER(act.acp_id) = LOWER(a.acp_id) AND LOWER(act.activated_channel) = LOWER(a.channel )
 LEFT JOIN events_data ON LOWER(events_data.acp_id) = LOWER(a.acp_id) AND e.day_date = events_data.sale_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS catg ON d.dept_num = catg.dept_num AND CAST(catg.class_num AS FLOAT64) = d.class_num AND CAST(catg.sbclass_num AS FLOAT64) = d.sbclass_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(d.prmy_supp_num) = LOWER(supp.vendor_num);


CREATE TEMPORARY TABLE IF NOT EXISTS full_data_ly
AS
SELECT e.fiscal_year_num AS year_idnt,
 e.fiscal_halfyear_num AS half_idnt,
 e.quarter_idnt,
 e.month_idnt,
 e.week_idnt,
   TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' H' || SUBSTR(RPAD(CAST(e.fiscal_halfyear_num AS STRING), 5, ' '), -1) AS
 half_label,
 e.quarter_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ' ' || TRIM(e.month_abrv
   ) AS month_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ', ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ', Wk ' || TRIM(FORMAT('%11d'
    , e.week_num_of_fiscal_month)) AS week_label,
 a.channel,
 a.ty_ly_ind,
 a.acp_id,
 events_data.acp_id AS event_acp_id,
 events_data.event_type,
 act.acp_id AS activated_acp_id,
 a.ntn_flg,
 a.trip_id,
 a.store_num,
  CASE
  WHEN c.loyalty_status IS NULL
  THEN 'non-member'
  ELSE c.loyalty_status
  END AS loyalty_status,
 d.brand_name,
 d.div_num,
 d.div_desc,
 d.dept_num,
 d.dept_desc,
 d.grp_num,
 d.grp_desc,
 d.class_num,
 d.class_desc,
 d.sbclass_num,
 d.sbclass_desc,
 catg.category,
 supp.vendor_name AS supplier,
 d.npg_ind,
 cust.age_value,
 a.net_spend,
 a.gross_spend,
 a.net_items,
 a.gross_items,
 ROW_NUMBER() OVER (PARTITION BY d.npg_ind, supp.vendor_name, a.ty_ly_ind, d.web_style_num, d.rms_sku_num, catg.category
    , d.div_num, d.dept_num, d.grp_num, d.class_num, d.sbclass_num, a.trip_id, a.acp_id, d.brand_name, c.loyalty_status
    , a.channel ORDER BY a.acp_id) AS rn
FROM tran_base_ly AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d ON LOWER(a.sku_num) = LOWER(d.rms_sku_num) AND LOWER(d.channel_country ) = LOWER('US')
 LEFT JOIN cm_customer AS cust ON LOWER(cust.acp_id) = LOWER(a.acp_id)
 LEFT JOIN cm_lyl_status_ly AS c ON LOWER(c.acp_id) = LOWER(a.acp_id)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS e ON a.sale_date = e.day_date_last_year_realigned
 LEFT JOIN activated_ly AS act ON LOWER(act.acp_id) = LOWER(a.acp_id) AND LOWER(act.activated_channel) = LOWER(a.channel)
 LEFT JOIN events_data ON LOWER(events_data.acp_id) = LOWER(a.acp_id) AND e.day_date = events_data.sale_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS catg ON d.dept_num = catg.dept_num AND CAST(catg.class_num AS FLOAT64)= d.class_num AND CAST(catg.sbclass_num AS FLOAT64) = d.sbclass_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(d.prmy_supp_num) = LOWER(supp.vendor_num);

CREATE TEMPORARY TABLE IF NOT EXISTS full_data
AS
SELECT *
FROM full_data_ty
UNION ALL
SELECT *
FROM full_data_ly;

CREATE TEMPORARY TABLE IF NOT EXISTS prefinal
AS
SELECT f.year_idnt,
 f.half_idnt,
 f.quarter_idnt,
 f.month_idnt,
 f.half_label,
 f.quarter_label,
 f.month_label,
  CASE
  WHEN LOWER(f.channel) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'Nordstrom'
  ELSE 'Rack'
  END AS banner,
 f.channel,
 f.ntn_flg,
 f.ty_ly_ind,
 f.loyalty_status,
 f.brand_name,
 f.div_num,
 f.div_desc,
 f.dept_num,
 f.dept_desc,
 f.grp_num,
 f.grp_desc,
 f.class_num,
 f.class_desc,
 f.sbclass_num,
 f.sbclass_desc,
 f.category,
 f.supplier,
 f.trip_id,
 f.acp_id,
 f.event_acp_id,
 f.event_type,
 f2.acp_id AS retained_acp_id,
 f.activated_acp_id,
 f.npg_ind,
 f.age_value,
 SUM(f.net_spend) AS net_spend,
 SUM(f.gross_spend) AS gross_spend,
 SUM(f.net_items) AS net_items,
 SUM(f.gross_items) AS gross_items
FROM full_data AS f
 LEFT JOIN tran_base_ly AS f2 ON LOWER(f2.acp_id) = LOWER(f.acp_id)
WHERE f.rn = 1
GROUP BY f.year_idnt,
 f.half_idnt,
 f.quarter_idnt,
 f.month_idnt,
 f.half_label,
 f.quarter_label,
 f.month_label,
 banner,
 f.channel,
 f.ntn_flg,
 f.ty_ly_ind,
 f.loyalty_status,
 f.brand_name,
 f.div_num,
 f.div_desc,
 f.dept_num,
 f.dept_desc,
 f.grp_num,
 f.grp_desc,
 f.class_num,
 f.class_desc,
 f.sbclass_num,
 f.sbclass_desc,
 f.category,
 f.supplier,
 f.trip_id,
 f.acp_id,
 f.event_acp_id,
 f.event_type,
 retained_acp_id,
 f.activated_acp_id,
 f.npg_ind,
 f.age_value;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_brand_base_nordstrom
AS
SELECT 'Brand                                                               ' AS level,
 brand_name AS dim,
 CAST('' AS string) AS subdim,
 cast(ty_ly_ind as string) as ty_ly_ind,
 '     ' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Brand x Division' AS level,
 brand_name  AS dim,
 cast(CONCAT(div_num, ', ', div_desc) as string) AS subdim,
 cast(ty_ly_ind as string) as ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Brand x Department' AS level,
 brand_name AS dim,
 CONCAT(dept_num, ', ', dept_desc) AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Brand X Subdivision' AS level,
 brand_name AS dim,
 CONCAT(grp_num, ', ', grp_desc) AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_brand_base_nordstrom1
AS
SELECT 'Total                                           ' AS level,
 CAST('TOTAL' AS string) AS dim,
 '                                      ' AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT cast('Loyalty Status' as string) AS level,
 CAST('loyalty_status' AS string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT cast('Division' as string) AS level,
 cast(CONCAT(div_num, ', ', div_desc) as string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Subdivision' AS level,
 cast(CONCAT(grp_num, ', ', grp_desc) as string) AS dim,
 '' AS sub_dim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 sub_dim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Department' AS level,
 cast(CONCAT(dept_num, ', ', dept_desc) as string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Channel' AS level,
 CAST(channel AS string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Class' AS level,
 cast(CONCAT(class_num, ', ', class_desc) as string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_brand_base_nordstrom2
AS
SELECT 'Subclass                  ' AS level,
 CONCAT(sbclass_num, ', ', sbclass_desc) AS dim,
 '                  ' AS subdim,
 ty_ly_ind,
 '  ' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Supplier' AS level,
 CAST(supplier AS string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Category' AS level,
 CAST(category AS string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Nordstrom')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_brand_base_rack
AS
SELECT 'Brand                                                               ' AS level,
 brand_name AS dim,
 CAST('                               ' AS string) AS subdim,
 ty_ly_ind,
 '     ' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Brand x Division' AS level,
 brand_name AS dim,
 cast(CONCAT(div_num, ', ', div_desc) as string) AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Brand x Department' AS level,
 brand_name AS dim,
 CONCAT(dept_num, ', ', dept_desc) AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_brand_base_rack1
AS
SELECT 'Brand x Subdivision' AS level,
 brand_name AS dim,
 cast(CONCAT(grp_num, ', ', grp_desc) as string) AS subdim,
 ty_ly_ind,
 ' ' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Loyalty Status' AS level,
 loyalty_status  AS dim,
 CAST('' AS string) AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Total' AS level,
 'Total' AS dim,
 CAST('' AS string) AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 dim;


CREATE TEMPORARY TABLE IF NOT EXISTS customer_brand_base_rack2
AS
SELECT 'Division    ' AS level,
 CONCAT(div_num, ', ', div_desc) AS dim,
 '  ' AS subdim,
 ty_ly_ind,
 '  ' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Subdivision' AS level,
 CONCAT(grp_num, ', ', grp_desc) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Department' AS level,
 CONCAT(dept_num, ', ', dept_desc) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_brand_base_rack3
AS
SELECT 'Class' AS level,
 cast(CONCAT(class_num, ', ', class_desc) as string) AS dim,
 '     ' AS subdim,
 ty_ly_ind,
 '   ' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label
UNION ALL
SELECT 'Subclass' AS level,
 cast(CONCAT(sbclass_num, ', ', sbclass_desc) as string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Channel' AS level,
 CAST(channel AS string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Supplier' AS level,
 CAST(supplier AS string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind
UNION ALL
SELECT 'Category' AS level,
 CAST(category AS string) AS dim,
 '' AS subdim,
 ty_ly_ind,
 '' AS npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 COUNT(DISTINCT acp_id) AS customers,
 COUNT(DISTINCT retained_acp_id) AS retained_customers,
 COUNT(DISTINCT activated_acp_id) AS activated_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('non-member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) = LOWER('member')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_member_customers,
 COUNT(DISTINCT CASE
   WHEN LOWER(loyalty_status) <> LOWER('cardmember')
   THEN acp_id
   ELSE NULL
   END) AS loyalty_cardmember_customers,
 AVG(age_value) AS avg_age,
 COUNT(DISTINCT trip_id) AS trips,
 COUNT(DISTINCT CASE
   WHEN ntn_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_customers,
 SUM(gross_items) AS gross_items,
 SUM(net_items) AS net_items,
 SUM(gross_spend) AS gross_sales,
 SUM(net_spend) AS net_sales
FROM prefinal
WHERE LOWER(banner) = LOWER('Rack')
GROUP BY level,
 dim,
 subdim,
 ty_ly_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 npg_ind;

CREATE TEMPORARY TABLE IF NOT EXISTS merch_customer_brand_base_n
AS
SELECT level,
 CAST(dim AS string) AS dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 customers,
 retained_customers,
 activated_customers,
 loyalty_customers,
 loyalty_member_customers,
 loyalty_cardmember_customers,
 avg_age,
 trips,
 ntn_customers,
 gross_items,
 net_items,
 gross_sales,
 net_sales
FROM customer_brand_base_nordstrom
UNION ALL
SELECT level,
 cast(dim as string) as dim,
 CAST(subdim AS string) AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 customers,
 retained_customers,
 activated_customers,
 loyalty_customers,
 loyalty_member_customers,
 loyalty_cardmember_customers,
 avg_age,
 trips,
 ntn_customers,
 gross_items,
 net_items,
 gross_sales,
 net_sales
FROM customer_brand_base_nordstrom1
UNION ALL
SELECT level,
 cast( dim as string) as dim,
 CAST(subdim AS string) AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 customers,
 retained_customers,
 activated_customers,
 loyalty_customers,
 loyalty_member_customers,
 loyalty_cardmember_customers,
 avg_age,
 trips,
 ntn_customers,
 gross_items,
 net_items,
 gross_sales,
 net_sales
FROM customer_brand_base_nordstrom2;

CREATE TEMPORARY TABLE IF NOT EXISTS merch_customer_brand_base_r
AS
SELECT level,
 CAST(dim AS string) AS dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 customers,
 retained_customers,
 activated_customers,
 loyalty_customers,
 loyalty_member_customers,
 loyalty_cardmember_customers,
 avg_age,
 trips,
 ntn_customers,
 gross_items,
 net_items,
 gross_sales,
 net_sales
FROM customer_brand_base_rack
UNION ALL
SELECT level,
 CAST(dim AS string) AS dim,
 subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 customers,
 retained_customers,
 activated_customers,
 loyalty_customers,
 loyalty_member_customers,
 loyalty_cardmember_customers,
 avg_age,
 trips,
 ntn_customers,
 gross_items,
 net_items,
 gross_sales,
 net_sales
FROM customer_brand_base_rack1
UNION ALL
SELECT level,
 dim,
 CAST(subdim AS string) AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 customers,
 retained_customers,
 activated_customers,
 loyalty_customers,
 loyalty_member_customers,
 loyalty_cardmember_customers,
 avg_age,
 trips,
 ntn_customers,
 gross_items,
 net_items,
 gross_sales,
 net_sales
FROM customer_brand_base_rack2
UNION ALL
SELECT level,
 dim,
 CAST(subdim AS string) AS subdim,
 ty_ly_ind,
 npg_ind,
 banner,
 channel,
 event_type,
 year_idnt,
 half_idnt,
 quarter_idnt,
 month_idnt,
 half_label,
 quarter_label,
 month_label,
 customers,
 retained_customers,
 activated_customers,
 loyalty_customers,
 loyalty_member_customers,
 loyalty_cardmember_customers,
 avg_age,
 trips,
 ntn_customers,
 gross_items,
 net_items,
 gross_sales,
 net_sales
FROM customer_brand_base_rack3;

DELETE FROM `{{params.gcp_project_id}}`.t2dl_das_cus.merch_customer_brand_base
WHERE month_idnt = (SELECT DISTINCT month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 10 DAY));


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_cus.merch_customer_brand_base
(SELECT level,
  CAST(dim AS STRING) AS dim,
  CAST(subdim AS STRING) AS subdim,
  ty_ly_ind,
  npg_ind,
  banner,
  channel,
  event_type,
  year_idnt,
  half_idnt,
  quarter_idnt,
  month_idnt,
  half_label,
  quarter_label,
  month_label,
  customers,
  retained_customers,
  activated_customers,
  loyalty_customers,
  loyalty_member_customers,
  loyalty_cardmember_customers,
  CAST(avg_age AS BIGNUMERIC) AS avg_age,
  trips,
  ntn_customers,
  gross_items,
  net_items,
  gross_sales,
  CAST(net_sales AS NUMERIC) AS net_sales
 FROM (SELECT *
    FROM merch_customer_brand_base_r
    UNION ALL
    SELECT *
    FROM merch_customer_brand_base_n) AS t);

DROP TABLE IF EXISTS cm_customer;

CREATE TEMPORARY TABLE IF NOT EXISTS cm_customer
AS
SELECT DISTINCT ex.acp_id,
 ex.age_value,
 ex.gender,
 ac.us_dma_code,
 ac.us_dma_desc,
 COALESCE(zip.urbanicity, 'Unknown') AS us_urbanicity,
 ex.household_estimated_income_range
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS ex ON LOWER(ac.acp_id) = LOWER(ex.acp_id
   )
 LEFT JOIN `{{params.gcp_project_id}}`.t3dl_ace_cai.zip_code_urbanicity AS zip ON LOWER(ac.billing_postal_code) = LOWER(zip.zip_code);

DROP TABLE IF EXISTS full_data_ty;

CREATE TEMPORARY TABLE IF NOT EXISTS full_data_ty
AS
SELECT e.fiscal_year_num AS year_idnt,
 e.fiscal_halfyear_num AS half_idnt,
 e.quarter_idnt,
 e.month_idnt,
 e.week_idnt,
   TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' H' || SUBSTR(RPAD(CAST(e.fiscal_halfyear_num AS STRING), 5, ' '), -1) AS
 half_label,
 e.quarter_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ' ' || TRIM(e.month_abrv
   ) AS month_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ', ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ', Wk ' || TRIM(FORMAT('%11d'
    , e.week_num_of_fiscal_month)) AS week_label,
 a.channel,
 a.ty_ly_ind,
 a.acp_id,
 a.ntn_flg,
 a.trip_id,
 d.brand_name,
 d.div_num,
 d.div_desc,
 d.dept_num,
 d.dept_desc,
 d.grp_num,
 d.grp_desc,
 a.net_spend,
 a.gross_spend,
 a.net_items,
 a.gross_items,
 cust.age_value,
 cust.gender,
 cust.household_estimated_income_range,
 CONCAT(cust.us_dma_code, ', ', cust.us_dma_desc) AS dma,
 cust.us_urbanicity,
 ROW_NUMBER() OVER (PARTITION BY d.npg_ind, a.ty_ly_ind, d.web_style_num, d.rms_sku_num, d.div_num, d.dept_num, d.grp_num
    , a.trip_id, a.acp_id, a.channel ORDER BY a.acp_id) AS rn
FROM tran_base_ty AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d ON LOWER(a.sku_num) = LOWER(d.rms_sku_num)
 LEFT JOIN cm_customer AS cust ON LOWER(cust.acp_id) = LOWER(a.acp_id)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS e ON a.sale_date = e.day_date;

DROP TABLE IF EXISTS full_data_ly;

CREATE TEMPORARY TABLE IF NOT EXISTS full_data_ly
AS
SELECT e.fiscal_year_num AS year_idnt,
 e.fiscal_halfyear_num AS half_idnt,
 e.quarter_idnt,
 e.month_idnt,
 e.week_idnt,
   TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' H' || SUBSTR(RPAD(CAST(e.fiscal_halfyear_num AS STRING), 5, ' '), -1) AS
 half_label,
 e.quarter_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ' ' || TRIM(e.month_abrv
   ) AS month_label,
     TRIM(FORMAT('%11d', e.fiscal_year_num)) || ', ' || TRIM(FORMAT('%11d', e.fiscal_month_num)) || ', Wk ' || TRIM(FORMAT('%11d'
    , e.week_num_of_fiscal_month)) AS week_label,
 a.channel,
 a.ty_ly_ind,
 a.acp_id,
 a.ntn_flg,
 a.trip_id,
 d.brand_name,
 d.div_num,
 d.div_desc,
 d.dept_num,
 d.dept_desc,
 d.grp_num,
 d.grp_desc,
 a.net_spend,
 a.gross_spend,
 a.net_items,
 a.gross_items,
 cust.age_value,
 cust.gender,
 cust.household_estimated_income_range,
 CONCAT(cust.us_dma_code, ', ', cust.us_dma_desc) AS dma,
 cust.us_urbanicity,
 ROW_NUMBER() OVER (PARTITION BY d.npg_ind, a.ty_ly_ind, d.web_style_num, d.rms_sku_num, d.div_num, d.dept_num, d.grp_num
    , a.trip_id, a.acp_id, a.channel ORDER BY a.acp_id) AS rn
FROM tran_base_ly AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d ON LOWER(a.sku_num) = LOWER(d.rms_sku_num)
 LEFT JOIN cm_customer AS cust ON LOWER(cust.acp_id) = LOWER(a.acp_id)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS e ON a.sale_date = e.day_date_last_year_realigned;

DROP TABLE IF EXISTS full_data;

CREATE TEMPORARY TABLE IF NOT EXISTS full_data
AS
SELECT *
FROM full_data_ty
UNION ALL
SELECT *
FROM full_data_ly;

CREATE TEMPORARY TABLE IF NOT EXISTS brand_top_10_markets
AS
SELECT brand_name,
 dma,
 ROW_NUMBER() OVER (PARTITION BY brand_name ORDER BY COUNT(DISTINCT acp_id) DESC) AS market_rk
FROM full_data
GROUP BY brand_name,
 dma
HAVING dma <> CAST('Not Defined' AS string);

CREATE TEMPORARY TABLE IF NOT EXISTS div_top_10_markets
AS
SELECT div_num,
 div_desc,
 dma,
 ROW_NUMBER() OVER (PARTITION BY div_num, div_desc ORDER BY COUNT(DISTINCT acp_id) DESC) AS market_rk
FROM full_data
GROUP BY div_num,
 div_desc,
 dma
HAVING dma <> CAST('Not Defined' AS string);

CREATE TEMPORARY TABLE IF NOT EXISTS dept_top_10_markets
AS
SELECT dept_num,
 dept_desc,
 dma,
 ROW_NUMBER() OVER (PARTITION BY dept_num, dept_desc ORDER BY COUNT(DISTINCT acp_id) DESC) AS market_rk
FROM full_data
GROUP BY dept_num,
 dept_desc,
 dma
HAVING dma <> CAST('Not Defined' AS string);

CREATE TEMPORARY TABLE IF NOT EXISTS subdiv_top_10_markets
AS
SELECT grp_num,
 grp_desc,
 dma,
 ROW_NUMBER() OVER (PARTITION BY grp_num, grp_desc ORDER BY COUNT(DISTINCT acp_id) DESC) AS market_rk
FROM full_data
GROUP BY grp_num,
 grp_desc,
 dma
HAVING dma <> CAST('Not Defined' AS string);

CREATE TEMPORARY TABLE IF NOT EXISTS prefinal_brands AS
SELECT
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    CASE
        WHEN channel IN ('FLS', 'NCOM') THEN 'Nordstrom'
        ELSE 'Rack'
    END AS banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    f.brand_name,
    acp_id,
    age_value AS age,
    CASE
        WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
        WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
        WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
        WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
        WHEN age_value >= 65 THEN 'Retired'
        ELSE 'Unknown'
    END AS age_value,
    CASE
        WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
        WHEN age_value < 18 THEN 'Younger'
        WHEN age_value >= 30 THEN 'Older'
        ELSE 'Unknown'
    END AS lifestage_sample,
    gender,
    CASE
        WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
        WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
        WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
        WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
        WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
        WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
        WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
        WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
        WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
        WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
        WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
        WHEN household_estimated_income_range = '$250,000+' THEN 350000
        ELSE NULL
    END AS estimated_income_range_midpoint,
    bm.dma,
    us_urbanicity,
    CASE WHEN age_value < 44 THEN 1 END AS age_under_44_flag,
    AVG(age_value) OVER (PARTITION BY f.brand_name, acp_id, channel, channel) AS avg_age,
    AVG(age_value) OVER (PARTITION BY f.brand_name, acp_id, channel, channel) AS avg_income,
    SUM(net_spend) AS net_spend,
    SUM(gross_spend) AS gross_spend,
    SUM(net_items) AS net_items,
    SUM(gross_items) AS gross_items
FROM
    full_data AS f
LEFT JOIN
    brand_top_10_markets bm
    ON bm.brand_name = f.brand_name
    AND bm.dma = f.dma
    AND bm.market_rk <= 10
GROUP BY
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    f.brand_name,
    acp_id,
    age_value,
    gender,
    household_estimated_income_range,
    bm.dma,
    us_urbanicity,
    age;

CREATE TEMPORARY TABLE IF NOT EXISTS prefinal_div AS
SELECT
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    CASE
        WHEN channel IN ('FLS', 'NCOM') THEN 'Nordstrom'
        ELSE 'Rack'
    END AS banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    f.div_num,
    f.div_desc,
    acp_id,
    age_value AS age,
    CASE
        WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
        WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
        WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
        WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
        WHEN age_value >= 65 THEN 'Retired'
        ELSE 'Unknown'
    END AS age_value,
    CASE
        WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
        WHEN age_value < 18 THEN 'Younger'
        WHEN age_value >= 30 THEN 'Older'
        ELSE 'Unknown'
    END AS lifestage_sample,
    gender,
    CASE
        WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
        WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
        WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
        WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
        WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
        WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
        WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
        WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
        WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
        WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
        WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
        WHEN household_estimated_income_range = '$250,000+' THEN 350000
        ELSE NULL
    END AS estimated_income_range_midpoint,
    div.dma,
    us_urbanicity,
    CASE WHEN age_value < 44 THEN 1 END AS age_under_44_flag,
    AVG(age_value) OVER (PARTITION BY f.div_num, acp_id, channel, channel) AS avg_age,
    AVG(age_value) OVER (PARTITION BY f.div_num, acp_id, channel, channel) AS avg_income,
    SUM(net_spend) AS net_spend,
    SUM(gross_spend) AS gross_spend,
    SUM(net_items) AS net_items,
    SUM(gross_items) AS gross_items
FROM
    full_data AS f
LEFT JOIN
    div_top_10_markets div
    ON div.div_num = f.div_num
    AND div.dma = f.dma
    AND div.market_rk <= 10
GROUP BY
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    f.div_num,
    f.div_desc,
    acp_id,
    age_value,
    gender,
    household_estimated_income_range,
    div.dma,
    us_urbanicity,
    age;

CREATE TEMPORARY TABLE IF NOT EXISTS prefinal_subdiv AS
SELECT
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    CASE
        WHEN channel IN ('FLS', 'NCOM') THEN 'Nordstrom'
        ELSE 'Rack'
    END AS banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    brand_name,
    div_num,
    div_desc,
    dept_num,
    dept_desc,
    f.grp_num,
    f.grp_desc,
    acp_id,
    age_value AS age,
    CASE
        WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
        WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
        WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
        WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
        WHEN age_value >= 65 THEN 'Retired'
        ELSE 'Unknown'
    END AS age_value,
    CASE
        WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
        WHEN age_value < 18 THEN 'Younger'
        WHEN age_value >= 30 THEN 'Older'
        ELSE 'Unknown'
    END AS lifestage_sample,
    gender,
    CASE
        WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
        WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
        WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
        WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
        WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
        WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
        WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
        WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
        WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
        WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
        WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
        WHEN household_estimated_income_range = '$250,000+' THEN 350000
        ELSE NULL
    END AS estimated_income_range_midpoint,
    subdiv.dma,
    us_urbanicity,
    CASE WHEN age_value < 44 THEN 1 END AS age_under_44_flag,
    AVG(age_value) OVER (PARTITION BY brand_name, acp_id, channel, channel) AS avg_age,
    AVG(age_value) OVER (PARTITION BY brand_name, acp_id, channel, channel) AS avg_income,
    SUM(net_spend) AS net_spend,
    SUM(gross_spend) AS gross_spend,
    SUM(net_items) AS net_items,
    SUM(gross_items) AS gross_items
FROM
    full_data AS f
LEFT JOIN
    subdiv_top_10_markets subdiv
    ON subdiv.grp_num = f.grp_num
    AND subdiv.dma = f.dma
    AND subdiv.market_rk <= 10
GROUP BY
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    brand_name,
    div_num,
    div_desc,
    dept_num,
    dept_desc,
    f.grp_num,
    f.grp_desc,
    acp_id,
    age_value,
    gender,
    household_estimated_income_range,
    subdiv.dma,
    us_urbanicity,
    age;

CREATE TEMPORARY TABLE IF NOT EXISTS prefinal_dept AS
SELECT
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    CASE
        WHEN channel IN ('FLS', 'NCOM') THEN 'Nordstrom'
        ELSE 'Rack'
    END AS banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    brand_name,
    div_num,
    div_desc,
    f.dept_num,
    f.dept_desc,
    grp_num,
    grp_desc,
    acp_id,
    age_value AS age,
    CASE
        WHEN age_value BETWEEN 14 AND 22 THEN 'Young Adult'
        WHEN age_value BETWEEN 23 AND 29 THEN 'Early Career'
        WHEN age_value BETWEEN 30 AND 44 THEN 'Mid Career'
        WHEN age_value BETWEEN 45 AND 64 THEN 'Late Career'
        WHEN age_value >= 65 THEN 'Retired'
        ELSE 'Unknown'
    END AS age_value,
    CASE
        WHEN age_value BETWEEN 18 AND 29 THEN 'Core Customer Target Age'
        WHEN age_value < 18 THEN 'Younger'
        WHEN age_value >= 30 THEN 'Older'
        ELSE 'Unknown'
    END AS lifestage_sample,
    gender,
    CASE
        WHEN household_estimated_income_range = '$1,000-$14,999' THEN 8000
        WHEN household_estimated_income_range = '$15,000-$24,999' THEN 20000
        WHEN household_estimated_income_range = '$25,000-$34,999' THEN 30000
        WHEN household_estimated_income_range = '$35,000-$49,999' THEN 42500
        WHEN household_estimated_income_range = '$50,000-$74,999' THEN 62500
        WHEN household_estimated_income_range = '$75,000-$99,999' THEN 87500
        WHEN household_estimated_income_range = '$100,000-$124,999' THEN 112500
        WHEN household_estimated_income_range = '$125,000-$149,999' THEN 137500
        WHEN household_estimated_income_range = '$150,000-$174,999' THEN 162500
        WHEN household_estimated_income_range = '$175,000-$199,999' THEN 187500
        WHEN household_estimated_income_range = '$200,000-$249,999' THEN 212500
        WHEN household_estimated_income_range = '$250,000+' THEN 350000
        ELSE NULL
    END AS estimated_income_range_midpoint,
    dept.dma,
    us_urbanicity,
    CASE WHEN age_value < 44 THEN 1 END AS age_under_44_flag,
    AVG(age_value) OVER (PARTITION BY f.brand_name, acp_id, channel, channel) AS avg_age,
    AVG(age_value) OVER (PARTITION BY f.brand_name, acp_id, channel, channel) AS avg_income,
    SUM(net_spend) AS net_spend,
    SUM(gross_spend) AS gross_spend,
    SUM(net_items) AS net_items,
    SUM(gross_items) AS gross_items
FROM
    full_data AS f
LEFT JOIN
    dept_top_10_markets dept
    ON dept.dept_num = f.dept_num
    AND dept.dma = f.dma
    AND dept.market_rk <= 10
GROUP BY
    year_idnt,
    half_idnt,
    quarter_idnt,
    month_idnt,
    half_label,
    quarter_label,
    month_label,
    banner,
    channel,
    ntn_flg,
    ty_ly_ind,
    brand_name,
    div_num,
    div_desc,
    f.dept_num,
    f.dept_desc,
    grp_num,
    grp_desc,
    acp_id,
    age_value,
    gender,
    household_estimated_income_range,
    dept.dma,
    us_urbanicity,
    age;

CREATE TEMPORARY TABLE IF NOT EXISTS brand_cte AS
SELECT 
    'Brand                          ' AS product_attribute,
    brand_name AS dimension,
    dma,
    ty_ly_ind,
    banner,
    channel,
    year_idnt,
    cast(half_idnt as string) as half_idnt,
    cast(quarter_idnt as string) as  quarter_idnt ,
    cast(month_idnt as string) as month_idnt ,
    half_label,
    quarter_label,
    month_label,
    cast(TRUNC(AVG(avg_age)) as int64) AS avg_age,
    cast(TRUNC(AVG(CASE WHEN ntn_flg = 1 THEN avg_age END)) as int64) AS avg_ntn_age,
    cast(TRUNC(AVG(avg_income)) as int64) AS avg_income,
    COUNT(DISTINCT acp_id) AS customers,
    COUNT(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id END) AS customer_under_44,
    COUNT(DISTINCT CASE WHEN gender = 'Male' THEN acp_id END) AS male_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Female' THEN acp_id END) AS female_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id END) AS unknown_gender_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers,
    SUM(gross_items) AS gross_items,
    SUM(gross_spend) AS gross_spend,
    COUNT(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM 
    prefinal_brands
GROUP BY 
    product_attribute, 
    brand_name, 
    dma, 
    ty_ly_ind, 
    banner, 
    channel, 
    year_idnt, 
    half_idnt, 
    quarter_idnt, 
    month_idnt, 
    half_label, 
    quarter_label, 
    month_label;

CREATE TEMPORARY TABLE IF NOT EXISTS div_cte AS
SELECT 
    'Division                          ' AS product_attribute,
    CONCAT(div_num, ', ', div_desc) AS dimension,
    dma,
    ty_ly_ind,
    banner,
    channel,
    year_idnt,
    cast(half_idnt as string) as half_idnt ,
    cast(quarter_idnt as string) as quarter_idnt,
    cast(month_idnt as string) as month_idnt ,
    half_label,
    quarter_label,
    month_label,
    cast(TRUNC(AVG(avg_age)) as int64) AS avg_age,
    cast(TRUNC(AVG(CASE WHEN ntn_flg = 1 THEN avg_age END)) as int64) AS avg_ntn_age,
    cast(TRUNC(AVG(avg_income)) as int64) AS avg_income,
    COUNT(DISTINCT acp_id) AS customers,
    COUNT(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id END) AS customer_under_44,
    COUNT(DISTINCT CASE WHEN gender = 'Male' THEN acp_id END) AS male_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Female' THEN acp_id END) AS female_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id END) AS unknown_gender_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers,
    SUM(gross_items) AS gross_items,
    SUM(gross_spend) AS gross_spend,
    COUNT(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM 
    prefinal_div
GROUP BY 
    product_attribute, 
    div_num, 
    div_desc, 
    dma, 
    ty_ly_ind, 
    banner, 
    channel, 
    year_idnt, 
    half_idnt, 
    quarter_idnt, 
    month_idnt, 
    half_label, 
    quarter_label, 
    month_label;

CREATE TEMPORARY TABLE IF NOT EXISTS subdiv_cte AS
SELECT 
    'Subdivision                          ' AS product_attribute,
    CONCAT(grp_num, ', ', grp_desc) AS dimension,
    dma,
    ty_ly_ind,
    banner,
    channel,
    year_idnt,
    cast(half_idnt as string) as half_idnt ,
    cast(quarter_idnt as string) as quarter_idnt ,
    cast(month_idnt as string) as month_idnt ,
    half_label,
    quarter_label,
    month_label,
    cast(TRUNC(AVG(avg_age)) as int64) AS avg_age,
    cast(TRUNC(AVG(CASE WHEN ntn_flg = 1 THEN avg_age END)) as int64) AS avg_ntn_age,
    cast(TRUNC(AVG(avg_income)) as int64) AS avg_income,
    COUNT(DISTINCT acp_id) AS customers,
    COUNT(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id END) AS customer_under_44,
    COUNT(DISTINCT CASE WHEN gender = 'Male' THEN acp_id END) AS male_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Female' THEN acp_id END) AS female_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id END) AS unknown_gender_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers,
    SUM(gross_items) AS gross_items,
    SUM(gross_spend) AS gross_spend,
    COUNT(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM 
    prefinal_subdiv
GROUP BY 
    product_attribute, 
    grp_num, 
    grp_desc, 
    dma, 
    ty_ly_ind, 
    banner, 
    channel, 
    year_idnt, 
    half_idnt, 
    quarter_idnt, 
    month_idnt, 
    half_label, 
    quarter_label, 
    month_label;

CREATE TEMPORARY TABLE IF NOT EXISTS dept_cte AS
SELECT 
    'Department                          ' AS product_attribute,
    CONCAT(dept_num, ', ', dept_desc) AS dimension,
    dma,
    ty_ly_ind,
    banner,
    channel,
    year_idnt,
    cast(half_idnt as string) as  half_idnt,
    cast(quarter_idnt as string) as quarter_idnt,
    cast(month_idnt as string) as month_idnt,
    half_label,
    quarter_label,
    month_label,
    cast(TRUNC(AVG(avg_age)) as int64) AS avg_age,
    cast(TRUNC(AVG(CASE WHEN ntn_flg = 1 THEN avg_age END)) as int64) AS avg_ntn_age,
    cast(TRUNC(AVG(avg_income)) as int64) AS avg_income,
    COUNT(DISTINCT acp_id) AS customers,
    COUNT(DISTINCT CASE WHEN age_under_44_flag = 1 THEN acp_id END) AS customer_under_44,
    COUNT(DISTINCT CASE WHEN gender = 'Male' THEN acp_id END) AS male_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Female' THEN acp_id END) AS female_customers,
    COUNT(DISTINCT CASE WHEN gender = 'Unknown' THEN acp_id END) AS unknown_gender_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Young Adult' THEN acp_id END) AS young_adult_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Early Career' THEN acp_id END) AS early_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Mid Career' THEN acp_id END) AS mid_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Late Career' THEN acp_id END) AS late_career_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Retired' THEN acp_id END) AS retired_customers,
    COUNT(DISTINCT CASE WHEN age_value = 'Unknown' THEN acp_id END) AS unknown_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Core Customer Target Age' THEN acp_id END) AS core_target_age_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Higher' THEN acp_id END) AS core_target_higher_customers,
    COUNT(DISTINCT CASE WHEN lifestage_sample = 'Lower' THEN acp_id END) AS core_target_lower_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Rural' THEN acp_id END) AS rural_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Urban' THEN acp_id END) AS urban_customers,
    COUNT(DISTINCT CASE WHEN us_urbanicity = 'Suburban' THEN acp_id END) AS suburban_customers,
    SUM(gross_items) AS gross_items,
    SUM(gross_spend) AS gross_spend,
    COUNT(DISTINCT CASE WHEN ntn_flg = 1 THEN acp_id END) AS ntn_count
FROM 
    prefinal_dept
GROUP BY 
    product_attribute, 
    dept_num, 
    dept_desc, 
    dma, 
    ty_ly_ind, 
    banner, 
    channel, 
    year_idnt, 
    half_idnt, 
    quarter_idnt, 
    month_idnt, 
    half_label, 
    quarter_label, 
    month_label;


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_customer_demographic
WHERE CAST(month_idnt AS FLOAT64) = (SELECT DISTINCT month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 10 DAY));




INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_customer_demographic 
SELECT * FROM brand_cte
UNION ALL 
SELECT * FROM div_cte
UNION ALL
SELECT * FROM subdiv_cte
UNION ALL
SELECT * FROM dept_cte;

