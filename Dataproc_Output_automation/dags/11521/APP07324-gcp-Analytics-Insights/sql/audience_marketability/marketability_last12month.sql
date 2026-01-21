CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup AS
SELECT MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1461 DAY) AS pre_start_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS pre_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 YEAR) AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY));


SELECT *
FROM date_lookup;


CREATE TEMPORARY TABLE IF NOT EXISTS pre_purchase
AS
SELECT DISTINCT CASE
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  ELSE intent_store_num
  END AS store_num,
 acp_id,
 COALESCE(order_date, tran_date) AS sale_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
WHERE COALESCE(order_date, tran_date) >= (SELECT pre_start_dt
   FROM date_lookup)
 AND COALESCE(order_date, tran_date) <= (SELECT pre_end_dt
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
 AND acp_id IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS pre_purchase_channel
AS
SELECT CASE
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
 pre_purchase.acp_id,
 MAX(pre_purchase.sale_date) AS latest_sale_date
FROM pre_purchase
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON pre_purchase.store_num = str.store_num AND LOWER(str.business_unit_desc
    ) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'
     ), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
GROUP BY channel,
 pre_purchase.acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS ty_purchase
AS
SELECT DISTINCT CASE
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  ELSE intent_store_num
  END AS store_num,
 acp_id,
 COALESCE(order_date, tran_date) AS sale_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
WHERE COALESCE(order_date, tran_date) >= (SELECT ty_start_dt
   FROM date_lookup)
 AND COALESCE(order_date, tran_date) <= (SELECT ty_end_dt
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
 AND acp_id IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS ty_purchase_channel
AS
SELECT CASE
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
 ty_purchase.acp_id,
 MIN(ty_purchase.sale_date) AS first_sale_date
FROM ty_purchase
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON ty_purchase.store_num = str.store_num AND LOWER(str.business_unit_desc)
   IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'
     ), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
GROUP BY channel,
 ty_purchase.acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS existing_cust
AS
SELECT ty.acp_id,
 ty.channel,
  CASE
  WHEN acq.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS new_customer_flg,
  CASE
  WHEN pre.latest_sale_date IS NOT NULL AND DATE_DIFF(ty.first_sale_date, pre.latest_sale_date, DAY) < 1461
  THEN 1
  ELSE 0
  END AS old_channel_flg
FROM ty_purchase_channel AS ty
 LEFT JOIN (SELECT acp_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
  WHERE aare_status_date BETWEEN (SELECT ty_start_dt
     FROM date_lookup) AND (SELECT ty_end_dt
     FROM date_lookup)) AS acq ON LOWER(ty.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN pre_purchase_channel AS pre ON LOWER(ty.acp_id) = LOWER(pre.acp_id) AND LOWER(ty.channel) = LOWER(pre.channel
    );


CREATE TEMPORARY TABLE IF NOT EXISTS new_cust
AS
SELECT ty.acp_id,
 ty.channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS acq
 INNER JOIN ty_purchase_channel AS ty ON LOWER(acq.acp_id) = LOWER(ty.acp_id) AND LOWER(acq.aare_chnl_code) <> LOWER(ty
    .channel)
 INNER JOIN (SELECT acp_id
  FROM ty_purchase_channel
  GROUP BY acp_id
  HAVING COUNT(DISTINCT channel) > 1) AS t1 ON LOWER(acq.acp_id) = LOWER(t1.acp_id)
WHERE acq.aare_status_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup) AND (SELECT ty_end_dt
   FROM date_lookup);


CREATE TEMPORARY TABLE IF NOT EXISTS ltm_engaged_customer
AS
SELECT 'LTM' AS report_period,
 acp_id,
 channel AS engaged_to_channel
FROM existing_cust
WHERE old_channel_flg = 0
 AND new_customer_flg = 0
UNION ALL
SELECT 'LTM' AS report_period,
 acp_id,
 channel AS engaged_to_channel
FROM new_cust;


DROP TABLE IF EXISTS date_lookup;


CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup AS
SELECT MIN(last_year_day_date_realigned) AS ly_start_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS ly_end_dt,
 MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 YEAR) AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY));


CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive
AS
SELECT CASE
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
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
     THEN 'FLS'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
     THEN 'NCOM'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
     THEN 'RACK'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
     THEN 'NRHL'
     ELSE NULL
     END) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'FP'
  ELSE 'OP'
  END AS brand,
 t2.acp_id,
 SUM(t2.gross_amt) AS gross_spend,
 SUM(t2.non_gc_amt) AS non_gc_spend,
 COUNT(DISTINCT t2.trip_id) AS trips,
 SUM(t2.items) AS items
FROM (SELECT acp_id,
   line_net_usd_amt AS gross_amt,
    CASE
    WHEN LOWER(nonmerch_fee_code) = LOWER('6666')
    THEN 0
    ELSE line_net_usd_amt
    END AS non_gc_amt,
    CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
    THEN 867
    ELSE intent_store_num
    END AS store_num,
   COALESCE(order_date, tran_date) AS sale_date,
   SUBSTR(acp_id || FORMAT('%11d', store_num) || CAST(COALESCE(order_date, tran_date) AS STRING), 1, 150) AS trip_id,
   line_item_quantity AS items
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
  WHERE COALESCE(order_date, tran_date) >= (SELECT ty_start_dt
     FROM date_lookup)
   AND COALESCE(order_date, tran_date) <= (SELECT ty_end_dt
     FROM date_lookup)
   AND line_net_usd_amt > 0
   AND acp_id IS NOT NULL) AS t2
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON t2.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'
     ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
GROUP BY channel,
 brand,
 t2.acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS ty_negative
AS
SELECT CASE
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
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
     THEN 'FLS'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
     THEN 'NCOM'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
     THEN 'RACK'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
     THEN 'NRHL'
     ELSE NULL
     END) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'FP'
  ELSE 'OP'
  END AS brand,
 t2.acp_id,
 SUM(t2.line_net_usd_amt) AS return_spend
FROM (SELECT acp_id,
   line_net_usd_amt,
    CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
    THEN 867
    ELSE intent_store_num
    END AS store_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
  WHERE tran_date >= (SELECT ty_start_dt
     FROM date_lookup)
   AND tran_date <= (SELECT ty_end_dt
     FROM date_lookup)
   AND line_net_usd_amt <= 0
   AND acp_id IS NOT NULL) AS t2
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON t2.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'
     ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
GROUP BY channel,
 brand,
 t2.acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS ty
AS
SELECT COALESCE(a.acp_id, b.acp_id) AS acp_id,
 COALESCE(a.channel, b.channel) AS channel,
 COALESCE(a.brand, b.brand) AS brand,
 COALESCE(a.gross_spend, 0) AS gross_spend,
  COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend, 0) AS net_spend,
 COALESCE(a.trips, 0) AS trips,
 COALESCE(a.items, 0) AS items
FROM ty_positive AS a
 FULL JOIN ty_negative AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.brand
    ) = LOWER(b.brand);


CREATE TEMPORARY TABLE IF NOT EXISTS ly_positive
AS
SELECT DISTINCT dtl.acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON dtl.intent_store_num = str.store_num
WHERE COALESCE(dtl.order_date, dtl.tran_date) >= (SELECT ly_start_dt
   FROM date_lookup)
 AND COALESCE(dtl.order_date, dtl.tran_date) <= (SELECT ly_end_dt
   FROM date_lookup)
 AND dtl.line_net_usd_amt > 0
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND dtl.acp_id IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS ly_negative
AS
SELECT DISTINCT dtl.acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON dtl.intent_store_num = str.store_num
WHERE dtl.tran_date >= (SELECT ly_start_dt
   FROM date_lookup)
 AND dtl.tran_date <= (SELECT ly_end_dt
   FROM date_lookup)
 AND dtl.line_net_usd_amt <= 0
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND dtl.acp_id IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS ly
AS
SELECT *
FROM ly_positive
UNION DISTINCT
SELECT *
FROM ly_negative;


CREATE TEMPORARY TABLE IF NOT EXISTS new_ty
AS
SELECT acp_id,
 aare_chnl_code AS acquired_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
WHERE aare_status_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup) AND (SELECT ty_end_dt
   FROM date_lookup);


CREATE TEMPORARY TABLE IF NOT EXISTS activated_ty
AS
SELECT acp_id,
 activated_channel
FROM dl_cma_cmbr.customer_activation
WHERE activated_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup) AND (SELECT ty_end_dt
   FROM date_lookup);


CREATE TEMPORARY TABLE IF NOT EXISTS temp_customer_summary
AS
SELECT ty.brand,
 ty.channel,
 ty.acp_id,
 ty.gross_spend,
 ty.net_spend,
 ty.trips,
 ty.items,
  CASE
  WHEN acq.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS new_flg,
  CASE
  WHEN ly.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS retained_flg,
  CASE
  WHEN eng.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS eng_flg,
  CASE
  WHEN act.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS act_flg
FROM ty
 LEFT JOIN ly ON LOWER(ty.acp_id) = LOWER(ly.acp_id)
 LEFT JOIN ltm_engaged_customer AS eng ON LOWER(ty.acp_id) = LOWER(eng.acp_id) AND LOWER(ty.channel) = LOWER(eng.engaged_to_channel
    )
 LEFT JOIN new_ty AS acq ON LOWER(ty.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN activated_ty AS act ON LOWER(ty.acp_id) = LOWER(act.acp_id) AND LOWER(ty.channel) = LOWER(act.activated_channel
    );

CREATE TEMPORARY TABLE IF NOT EXISTS purchase_channel
  AS
    WITH cte AS (
      SELECT
          temp_customer_summary.acp_id,
          count(DISTINCT temp_customer_summary.channel) AS channel_cnt
        FROM
          temp_customer_summary
        GROUP BY 1
    )
    SELECT
        temp_customer_summary.acp_id,
        rtrim(substr(string_agg(concat(trim(temp_customer_summary.channel, ' '), ','), ' ' ORDER BY temp_customer_summary.channel), 1, 10000), ',') AS channel_ind
      FROM
        --              array_agg(channel order by channel asc,NEW store_array()) as channel_ind
        temp_customer_summary
      WHERE temp_customer_summary.acp_id IN(
        SELECT DISTINCT
            cte.acp_id
          FROM
            cte
          WHERE cte.channel_cnt = 4
      )
      GROUP BY 1
    UNION ALL
    SELECT
        temp_customer_summary.acp_id,
        rtrim(substr(string_agg(concat(trim(temp_customer_summary.channel, ' '), ','), ' ' ORDER BY temp_customer_summary.channel), 1, 10000), ',') AS channel_ind
      FROM
        temp_customer_summary
      WHERE temp_customer_summary.acp_id IN(
        SELECT DISTINCT
            cte.acp_id
          FROM
            cte
          WHERE cte.channel_cnt = 3
      )
      GROUP BY 1
    UNION ALL
    SELECT
        temp_customer_summary.acp_id,
        rtrim(substr(string_agg(concat(trim(temp_customer_summary.channel, ' '), ','), ' ' ORDER BY temp_customer_summary.channel), 1, 10000), ',') AS channel_ind
      FROM
        temp_customer_summary
      WHERE temp_customer_summary.acp_id IN(
        SELECT DISTINCT
            cte.acp_id
          FROM
            cte
          WHERE cte.channel_cnt = 2
      )
      GROUP BY 1
    UNION ALL
    SELECT
        temp_customer_summary.acp_id,
        rtrim(substr(string_agg(concat(trim(temp_customer_summary.channel, ' '), ','), ' ' ORDER BY temp_customer_summary.channel), 1, 10000), ',') AS channel_ind
      FROM
        temp_customer_summary
      WHERE temp_customer_summary.acp_id IN(
        SELECT DISTINCT
            cte.acp_id
          FROM
            cte
          WHERE cte.channel_cnt = 1
      )
      GROUP BY 1
;
-- If you want to create a new table with the result
CREATE TEMPORARY TABLE IF NOT EXISTS acp_aare AS
SELECT a.*,
       b.channel_ind,
       CASE 
           WHEN new_flg = 1 AND retained_flg = 0 AND eng_flg = 0 AND act_flg = 0 THEN 'Acquired'
           WHEN new_flg = 1 AND retained_flg = 0 AND eng_flg = 0 AND act_flg = 1 THEN 'Acquired'
           WHEN new_flg = 1 AND retained_flg = 0 AND eng_flg = 1 AND act_flg = 1 THEN 'Acquired'
           WHEN new_flg = 0 AND retained_flg = 1 AND eng_flg = 0 AND act_flg = 0 THEN 'Retained'
           WHEN new_flg = 0 AND retained_flg = 1 AND eng_flg = 1 AND act_flg = 0 THEN 'Retained'
           WHEN new_flg = 0 AND retained_flg = 1 AND eng_flg = 0 AND act_flg = 1 THEN 'Retained'
           WHEN new_flg = 0 AND retained_flg = 1 AND eng_flg = 1 AND act_flg = 1 THEN 'Retained'
           ELSE 'Reactivated'
       END AS aare
FROM temp_customer_summary a
LEFT JOIN purchase_channel b 
  ON a.acp_id = b.acp_id;





CREATE TEMPORARY TABLE IF NOT EXISTS email_distinct
AS
SELECT acp_id,
 MAX(CASE
   WHEN LOWER(fp_ind) = LOWER('Y')
   THEN 1
   ELSE 0
   END) AS fp_email_marketable_idnt,
 MAX(CASE
   WHEN LOWER(op_ind) = LOWER('Y')
   THEN 1
   ELSE 0
   END) AS op_email_marketable_idnt
FROM t2dl_das_mktg_operations.email_marketability_vw
WHERE LOWER(marketability_ind) = LOWER('Y')
 AND acp_id IS NOT NULL
GROUP BY acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS site_marketable
AS
SELECT xref.session_shopper_id AS shopper_id,
 xref.session_icon_id AS icon_id,
 xref.session_acp_id AS acp_id,
 f.channel,
 f.experience,
 f.channelcountry,
 COUNT(DISTINCT f.session_id) AS session_cnt,
 SUM(f.product_views) AS product_views,
 SUM(f.cart_adds) AS cart_adds,
 SUM(f.web_orders) AS web_orders,
 SUM(f.web_ordered_units) AS order_units,
 SUM(f.web_demand_usd) AS demand
FROM (SELECT session_id,
   MAX(shopper_id) AS session_shopper_id,
   MAX(ent_cust_id) AS session_icon_id,
   MAX(acp_id) AS session_acp_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_session_xref
  WHERE LOWER(ent_cust_id) LIKE LOWER('icon::%')
  GROUP BY session_id) AS xref
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_session_fact AS f ON LOWER(xref.session_id) = LOWER(f.session_id)
WHERE f.activity_date_pacific BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 YEAR) AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY
    ))
 AND LOWER(f.experience) IN (LOWER('DESKTOP_WEB'), LOWER('MOBILE_WEB'))
GROUP BY shopper_id,
 icon_id,
 acp_id,
 f.channel,
 f.experience,
 f.channelcountry;


CREATE TEMPORARY TABLE IF NOT EXISTS sm_banner_by_shopper
AS
SELECT icon_id,
 acp_id,
 RTRIM(STRING_AGG(TRIM(site_ind) || ','), ',') AS site_ind
FROM (SELECT DISTINCT icon_id,
   acp_id,
    CASE
    WHEN LOWER(experience) IN (LOWER('DESKTOP_WEB')) AND LOWER(channel) = LOWER('NORDSTROM')
    THEN 'n.com site'
    WHEN LOWER(experience) IN (LOWER('DESKTOP_WEB')) AND LOWER(channel) = LOWER('NORDSTROM_RACK')
    THEN 'r.com site'
    WHEN LOWER(experience) IN (LOWER('MOBILE_WEB')) AND LOWER(channel) = LOWER('NORDSTROM')
    THEN 'n.com mow'
    WHEN LOWER(experience) IN (LOWER('MOBILE_WEB')) AND LOWER(channel) = LOWER('NORDSTROM_RACK')
    THEN 'r.com mow'
    WHEN LOWER(experience) IN (LOWER('IOS_APP'), LOWER('ANDROID_APP')) AND LOWER(channel) = LOWER('NORDSTROM')
    THEN 'n.com app'
    WHEN LOWER(experience) IN (LOWER('IOS_APP'), LOWER('ANDROID_APP')) AND LOWER(channel) = LOWER('NORDSTROM_RACK')
    THEN 'r.com app'
    ELSE NULL
    END AS site_ind
  FROM site_marketable) AS sub
GROUP BY icon_id,
 acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS sm_channel_by_shopper
AS
SELECT icon_id,
 acp_id,
 RTRIM(STRING_AGG(TRIM(channel) || ','), ',') AS site_channel_ind
FROM (SELECT DISTINCT icon_id,
   acp_id,
   channel
  FROM site_marketable) AS sub
GROUP BY icon_id,
 acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS hqyy_site_marketability_0
AS
SELECT sm.shopper_id,
 sm.icon_id,
 sm.acp_id,
 sm.channel,
 sm.experience,
 sm.channelcountry,
 sm.session_cnt,
 sm.product_views,
 sm.cart_adds,
 sm.web_orders,
 sm.order_units,
 sm.demand,
 sbbs.site_ind,
 scbs.site_channel_ind
FROM site_marketable AS sm
 LEFT JOIN sm_banner_by_shopper AS sbbs ON LOWER(sm.icon_id) = LOWER(sbbs.icon_id)
 LEFT JOIN sm_channel_by_shopper AS scbs ON LOWER(sm.icon_id) = LOWER(scbs.icon_id);


CREATE TEMPORARY TABLE IF NOT EXISTS acp_fy_cohort_19_thru_22
AS
SELECT acp_id,
  CASE
  WHEN LOWER(engagement_cohort) IN (LOWER('Acquire & Activate'), LOWER('Acquired Mid-Qtr'))
  THEN 'Acquire & Activate'
  ELSE engagement_cohort
  END AS engagement_cohort
FROM t2dl_das_aec.audience_engagement_cohorts
WHERE execution_qtr IN (SELECT MAX(execution_qtr) AS A1317208987
   FROM t2dl_das_aec.audience_engagement_cohorts);





CREATE TEMPORARY TABLE IF NOT EXISTS hqyy_site_marketability AS
WITH aare_dat AS (
    SELECT acp_id,
           channel_ind,
           aare,
           SUM(COALESCE(gross_spend, 0)) AS gross_spend,
           SUM(trips) AS total_trips
    FROM acp_aare
    GROUP BY acp_id, channel_ind, aare
)
SELECT DISTINCT *
FROM (
    SELECT sm.*,
           squad.engagement_cohort,
           a.channel_ind AS historic_shopping_channel,
           a.aare,

           -- Customer attributes
           cal.loyalty_level,
           cal.is_loyalty_member,
           cal.is_cardmember,
           cal.member_enroll_country_code
    FROM hqyy_site_marketability_0 sm
    LEFT JOIN acp_fy_cohort_19_thru_22 squad
        ON sm.acp_id = squad.acp_id
    LEFT JOIN aare_dat a
        ON a.acp_id = sm.acp_id
    LEFT JOIN T2DL_das_cal.customer_attributes_loyalty cal
        ON sm.acp_id = cal.acp_id
) sub;




CREATE TEMPORARY TABLE IF NOT EXISTS sm_distinct as (
select distinct acp_id,
                site_ind,
                site_channel_ind,
                session_cnt,
                product_views,
                cart_adds,
                web_orders,
                order_units,
                demand
from  hqyy_site_marketability
    );




CREATE TEMPORARY TABLE IF NOT EXISTS cust_l1y
AS
SELECT DISTINCT acp_id
FROM t2dl_das_cal.customer_attributes_transactions
WHERE net_spend_1year_jwn > 0;


CREATE TEMPORARY TABLE IF NOT EXISTS cust_l2y
AS
SELECT DISTINCT acp_id
FROM t2dl_das_cal.customer_attributes_transactions
WHERE net_spend_2year_jwn > 0;


CREATE TEMPORARY TABLE IF NOT EXISTS cust_l4y
AS
SELECT DISTINCT acp_id
FROM t2dl_das_cal.customer_attributes_transactions
WHERE net_spend_4year_jwn > 0;


CREATE TEMPORARY TABLE IF NOT EXISTS ltm_shopping_history
AS
SELECT acp_id,
 REPLACE(TRIM(TRIM(TRIM(COALESCE(CASE
             WHEN active_1year_ncom = 1
             THEN 'NCOM'
             ELSE NULL
             END, '') || ' ' || COALESCE(CASE
            WHEN active_1year_nstores = 1
            THEN 'FLS'
            ELSE NULL
            END, '')) || ' ' || COALESCE(CASE
         WHEN active_1year_rcom = 1
         THEN 'NRHL'
         ELSE NULL
         END, '')) || ' ' || COALESCE(CASE
      WHEN active_1year_rstores = 1
      THEN 'RACK'
      ELSE NULL
      END, '')), ' ', ',') AS historic_shopping_channel,
 SUM(trips_1year_jwn) AS ltm_total_trips,
 SUM(gross_spend_1year_jwn) AS ltm_gross_spend
FROM t2dl_das_cal.customer_attributes_transactions
WHERE net_spend_1year_jwn > 0
GROUP BY acp_id,
 historic_shopping_channel;



CREATE TEMPORARY TABLE IF NOT EXISTS icon_list
  AS
    SELECT DISTINCT
        hqyy_site_marketability.acp_id
      FROM
        hqyy_site_marketability
    UNION DISTINCT
    SELECT DISTINCT
        mktg_dm_customer_audience.acp_id
      FROM
        `{{params.gcp_project_id}}`.{{params.mktg_audience_t2_schema}}.mktg_dm_customer_audience
    UNION DISTINCT
    SELECT DISTINCT
        email_distinct.acp_id
      FROM
        email_distinct
    UNION DISTINCT
    SELECT DISTINCT
        paid_marketability.acp_id
      FROM
        t2dl_das_mktg_audience.paid_marketability
    UNION DISTINCT
    SELECT DISTINCT
        cust_l1y.acp_id
      FROM
        cust_l1y
    UNION DISTINCT
    SELECT DISTINCT
        cust_l2y.acp_id
      FROM
        cust_l2y
    UNION DISTINCT
    SELECT DISTINCT
        cust_l4y.acp_id
      FROM
        cust_l4y
;



-- Create or replace a permanent table in BigQuery
CREATE TEMPORARY TABLE IF NOT EXISTS multi_media_base AS
WITH sm AS (
    -- Aggregating site visit data (sessions, pageviews, etc.)
    SELECT acp_id,
           site_ind,
           site_channel_ind,
           SUM(session_cnt) AS total_sessions,
           SUM(product_views) AS total_pv,
           SUM(cart_adds) AS total_atb,
           SUM(web_orders) AS total_orders,
           SUM(order_units) AS total_units,
           SUM(demand) AS total_demand
    FROM sm_distinct
    GROUP BY acp_id, site_ind, site_channel_ind
)
SELECT main.acp_id,
       -- Last 1 Year
       CASE WHEN l1y.acp_id IS NOT NULL THEN 1 ELSE 0 END AS l1y_purchase_idnt,
       -- Last 2 Year
       CASE WHEN l2y.acp_id IS NOT NULL THEN 1 ELSE 0 END AS l2y_purchase_idnt,
       -- Last 4 Year
       CASE WHEN l4y.acp_id IS NOT NULL THEN 1 ELSE 0 END AS l4y_purchase_idnt,
       -- Direct Mail
       CASE WHEN dm.acp_id IS NOT NULL THEN 1 ELSE 0 END AS directmail_idnt,
       -- Site visit attributes
       CASE WHEN sm.acp_id IS NOT NULL THEN sm.site_channel_ind ELSE NULL END AS site_visited_channel,
       CASE WHEN sm.acp_id IS NOT NULL THEN sm.site_ind ELSE NULL END AS site_visited_experience,
       CASE WHEN sm.acp_id IS NOT NULL THEN sm.total_sessions ELSE 0 END AS site_visited_sessions,
       CASE WHEN sm.acp_id IS NOT NULL THEN sm.total_pv ELSE 0 END AS site_visited_pageviews,
       CASE WHEN sm.acp_id IS NOT NULL THEN sm.total_atb ELSE 0 END AS site_visited_atb,
       CASE WHEN sm.acp_id IS NOT NULL THEN sm.total_orders ELSE 0 END AS site_converted_orders,
       CASE WHEN sm.acp_id IS NOT NULL THEN sm.total_demand ELSE 0 END AS site_converted_demand,
       -- Paid Media
       CASE WHEN paid.acp_id IS NOT NULL THEN 1 ELSE 0 END AS paid_idnt,
       0 AS paid_marketable_idnt,  -- Placeholder for paid marketable identifier
       -- Email
       CASE WHEN email.acp_id IS NOT NULL THEN 1 ELSE 0 END AS email_idnt,
       CASE WHEN email.acp_id IS NOT NULL THEN email.op_email_marketable_idnt ELSE 0 END AS op_email_marketable_idnt,
       CASE WHEN email.acp_id IS NOT NULL THEN email.fp_email_marketable_idnt ELSE 0 END AS fp_email_marketable_idnt,
       CASE WHEN er.acp_id IS NOT NULL THEN 1 ELSE 0 END AS email_reached_idnt,
       CASE WHEN ep.acp_id IS NOT NULL THEN 1 ELSE 0 END AS email_purchased_idnt
FROM icon_list main
LEFT JOIN cust_l1y l1y ON l1y.acp_id = main.acp_id
LEFT JOIN cust_l2y l2y ON l2y.acp_id = main.acp_id
LEFT JOIN cust_l4y l4y ON l4y.acp_id = main.acp_id
LEFT JOIN `{{params.gcp_project_id}}`.{{params.mktg_audience_t2_schema}}.mktg_dm_customer_audience dm ON main.acp_id = dm.acp_id
LEFT JOIN sm ON sm.acp_id = main.acp_id
LEFT JOIN email_distinct  email ON email.acp_id = main.acp_id
LEFT JOIN (
    SELECT DISTINCT acp_id 
    FROM T2DL_DAS_MKTG_AUDIENCE.paid_marketability
) paid ON paid.acp_id = main.acp_id
LEFT JOIN (
    SELECT DISTINCT acp_id 
    FROM T2DL_DAS_MKTG_AUDIENCE.email_marketability
    WHERE utm_medium = 'email'
) er ON er.acp_id = main.acp_id
LEFT JOIN (
    SELECT DISTINCT acp_id 
    FROM T2DL_DAS_MKTG_AUDIENCE.email_marketability 
    WHERE utm_medium = 'email' AND converted = 'Y'
) ep ON ep.acp_id = main.acp_id;


CREATE TEMPORARY TABLE IF NOT EXISTS hqyy_marketability
  AS
    WITH aare_dat AS (
      SELECT
          acp_aare.acp_id,
          acp_aare.channel_ind,
          acp_aare.aare,
          sum(coalesce(acp_aare.gross_spend, 0)) AS gross_spend,
          sum(acp_aare.trips) AS total_trips
        FROM
          acp_aare
        GROUP BY 1, 2, 3
    )
    SELECT
        base.*,
        -- - customer loyalty
        --                  case when obj.unique_source_id is not null then 1 else 0 end as is_loyalty_member,
        cal.is_loyalty_member AS is_loyalty_member,
        cal.is_cardmember,
        CASE
          WHEN cal.is_cardmember = 1 THEN 'CARDMEMBER'
          WHEN upper(rtrim(cal.loyalty_level, ' ')) = 'ICON' THEN 'CARDMEMBER'
          WHEN cal.is_loyalty_member = 1 THEN 'MEMBER'
          ELSE 'NON-LOYALTY'
        END AS loyalty_status,
        cal.member_enroll_country_code,
        cal.loyalty_level,
        -- - anniversary
        an.anniversary_flag,
        -- - customer squad
        squad.engagement_cohort,
        -- - customer aare
        ltm.historic_shopping_channel AS historic_shopping_channel,
        a.aare,
        ltm.ltm_gross_spend AS ltm_gross_spend,
        ltm.ltm_total_trips AS ltm_total_trips,
        -- - LiveRamp reserved space
        CAST(0 as integer) AS lr_custom_column1,
        --  for aec
        CAST(0 as integer) AS lr_custom_column2,
        --  for loyalty
        CAST(0 as integer) AS lr_custom_column3
      FROM
        --  for cardmember
        multi_media_base AS base
        LEFT OUTER JOIN t2dl_das_cal.customer_attributes_loyalty AS cal ON base.acp_id = cal.acp_id
        LEFT OUTER JOIN acp_fy_cohort_19_thru_22 AS squad ON base.acp_id = squad.acp_id
        LEFT OUTER JOIN aare_dat AS a ON a.acp_id = base.acp_id
        LEFT OUTER JOIN ltm_shopping_history AS ltm ON base.acp_id = ltm.acp_id
        LEFT OUTER JOIN (
          SELECT DISTINCT
              customer_attributes_transactions.acp_id,
              CASE
                WHEN customer_attributes_transactions.shopped_ly_anniversary = 1
                 OR customer_attributes_transactions.shopped_ty_anniversary = 1
                 OR customer_attributes_transactions.shopped_lly_anniversary = 1 THEN 'Anniversary Shopper'
                ELSE 'Non Anniversary Shopper'
              END AS anniversary_flag
            FROM
              t2dl_das_cal.customer_attributes_transactions
        ) AS an ON an.acp_id = base.acp_id
;
insert into hqyy_marketability
    select a.acp_id,
           l1y_purchase_idnt,
           l2y_purchase_idnt,
           l4y_purchase_idnt,
           a.directmail_idnt,
           a.site_visited_channel,
           site_visited_experience,
           site_visited_sessions,
           site_visited_pageviews,
           site_visited_atb,
           site_converted_orders,
           site_converted_demand,
           paid_idnt,
           paid_marketable_idnt,
           email_idnt,
           op_email_marketable_idnt,            --- Added Oct 6, 2023
           fp_email_marketable_idnt,            --- Added Oct 6, 2023
           email_reached_idnt,
           email_purchased_idnt,
           is_loyalty_member,
           is_cardmember,
           loyalty_status,
           member_enroll_country_code,
           loyalty_level,
           'Non Anniversary Shopper' as anniversary_flag,
           engagement_cohort,
           historic_shopping_channel,
           aare,
           ltm_gross_spend,
           ltm_total_trips,
           cast(trunc(lr_custom_column1) as INT64) lr_custom_column1,
           cast(trunc(lr_custom_column2) as  INT64) lr_custom_column2,
           cast(trunc(lr_custom_column3) as  INT64) lr_custom_column3
    from `{{params.gcp_project_id}}`.{{params.mktg_audience_t2_schema}}.liveramp_table_last12month a;


   CREATE TEMPORARY TABLE IF NOT EXISTS hqyy_marketability_final
  AS
    SELECT
        sub.*,
        -- - custom column
        CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
      FROM
        (
          SELECT DISTINCT
              *
            FROM
              hqyy_marketability
        ) AS sub
;

truncate table `{{params.gcp_project_id}}`.{{params.mktg_audience_t2_schema}}.customer_marketability_ltm;

insert into `{{params.gcp_project_id}}`.{{params.mktg_audience_t2_schema}}.customer_marketability_ltm
  select * from hqyy_marketability_final;
