--CREATE OR REPLACE PROCEDURE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.raven_source_bteq_30()
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP09044;
DAG_ID=kpi_buyerflow_11521_ACE_ENG;
---     Task_Name=kpi_buyerflow;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup

AS
SELECT b.ty_start_dt,
 c.ty_end_dt,
 TRIM(CASE
   WHEN CAST(b.report AS DATE) =  {{params.report_period}}
   THEN SUBSTR(CAST(CONCAT(a.fiscal_year_num, '_', a.month_abrv) AS STRING), 1, 18)
   ELSE {{params.report_period}}
   END) AS report_period,
 DATE_SUB(b.ty_start_dt, INTERVAL 1461 DAY) AS pre_start_dt,
 DATE_SUB(c.ty_end_dt, INTERVAL 1 DAY) AS pre_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
 LEFT JOIN (SELECT month_start_day_date AS ty_start_dt,
   '1234' AS report
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date =  {{params.start_date}}) AS b ON TRUE
 LEFT JOIN (SELECT month_end_day_date AS ty_end_dt,
   '1234' AS report
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date =  {{params.end_date}}) AS c ON TRUE
WHERE a.day_date = (SELECT month_start_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date =  {{params.start_date}})
GROUP BY b.ty_start_dt,
 c.ty_end_dt,
 report_period,
 pre_start_dt,
 pre_end_dt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
  HAVING COUNT(DISTINCT channel) > 1) AS multi ON LOWER(acq.acp_id) = LOWER(multi.acp_id)
WHERE acq.aare_status_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup) AND (SELECT ty_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS engaged_customer

AS
SELECT (SELECT report_period
  FROM date_lookup) AS report_period,
 acp_id,
 channel AS engaged_to_channel
FROM existing_cust
WHERE old_channel_flg = 0
 AND new_customer_flg = 0
UNION ALL
SELECT (SELECT report_period
  FROM date_lookup) AS report_period,
 acp_id,
 channel AS engaged_to_channel
FROM new_cust;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup_ly

AS
SELECT TRIM(CASE
   WHEN CAST(b.report AS DATE) =  {{params.report_period}}
   THEN SUBSTR(CAST(CONCAT(a.fiscal_year_num, '_', a.month_abrv) AS STRING), 1, 18)
   ELSE {{params.report_period}}
   END) AS report_period,
 MIN(a.day_date_last_year_realigned) AS ly_start_dt,
 DATE_SUB(a.month_start_day_date, INTERVAL 1 DAY) AS ly_end_dt,
 a.month_start_day_date AS ty_start_dt,
 a.month_end_day_date AS ty_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
 LEFT JOIN (SELECT month_start_day_date AS ty_start_dt,
   '1234' AS report
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = {{params.start_date}}) AS b ON a.month_start_day_date = b.ty_start_dt
WHERE a.day_date = {{params.end_date}}
GROUP BY report_period,
 ly_end_dt,
 ty_start_dt,
 ty_end_dt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
     END) IN (LOWER('FLS'), LOWER('RACK'))
  THEN 'STORE'
  ELSE 'DIGITAL'
  END AS digital_store,
 tran.acp_id,
 SUM(tran.gross_amt) AS gross_spend,
 SUM(tran.non_gc_amt) AS non_gc_spend,
 COUNT(DISTINCT tran.trip_id) AS trips,
 SUM(tran.items) AS items
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
     FROM date_lookup_ly)
   AND COALESCE(order_date, tran_date) <= (SELECT ty_end_dt
     FROM date_lookup_ly)
   AND line_net_usd_amt > 0
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON tran.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'
     ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
GROUP BY channel,
 brand,
 digital_store,
 tran.acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
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
     END) IN (LOWER('FLS'), LOWER('RACK'))
  THEN 'STORE'
  ELSE 'DIGITAL'
  END AS digital_store,
 tran.acp_id,
 SUM(tran.line_net_usd_amt) AS return_spend
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
     FROM date_lookup_ly)
   AND tran_date <= (SELECT ty_end_dt
     FROM date_lookup_ly)
   AND line_net_usd_amt <= 0
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON tran.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'
     ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
GROUP BY channel,
 brand,
 digital_store,
 tran.acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty

AS
SELECT COALESCE(a.acp_id, b.acp_id) AS acp_id,
 COALESCE(a.channel, b.channel) AS channel,
 COALESCE(a.brand, b.brand) AS brand,
 COALESCE(a.digital_store, b.digital_store) AS digital_store,
 COALESCE(a.gross_spend, 0) AS gross_spend,
  COALESCE(a.non_gc_spend, 0) + COALESCE(b.return_spend, 0) AS net_spend,
 COALESCE(a.trips, 0) AS trips,
 COALESCE(a.items, 0) AS items
FROM ty_positive AS a
 FULL JOIN ty_negative AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.brand
     ) = LOWER(b.brand) AND LOWER(a.digital_store) = LOWER(b.digital_store);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly_positive

AS
SELECT DISTINCT dtl.acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON dtl.intent_store_num = str.store_num
WHERE COALESCE(dtl.order_date, dtl.tran_date) >= (SELECT ly_start_dt
   FROM date_lookup_ly)
 AND COALESCE(dtl.order_date, dtl.tran_date) <= (SELECT ly_end_dt
   FROM date_lookup_ly)
 AND dtl.line_net_usd_amt > 0
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND dtl.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly_negative

AS
SELECT DISTINCT dtl.acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON dtl.intent_store_num = str.store_num
WHERE dtl.tran_date >= (SELECT ly_start_dt
   FROM date_lookup_ly)
 AND dtl.tran_date <= (SELECT ly_end_dt
   FROM date_lookup_ly)
 AND dtl.line_net_usd_amt <= 0
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
 AND dtl.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly

AS
SELECT *
FROM ly_positive
UNION DISTINCT
SELECT *
FROM ly_negative;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS new_ty

AS
SELECT acp_id,
 aare_chnl_code AS acquired_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
WHERE aare_status_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup_ly) AND (SELECT ty_end_dt
   FROM date_lookup_ly);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS activated_ty

AS
SELECT acp_id,
 activated_channel
FROM dl_cma_cmbr.customer_activation
WHERE activated_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup_ly) AND (SELECT ty_end_dt
   FROM date_lookup_ly);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS lyl_status

AS
SELECT acp_id,
  CASE
  WHEN cardmember_fl = 1
  THEN 'cardmember'
  WHEN member_fl = 1
  THEN 'member'
  ELSE 'non-member'
  END AS loyalty_status,
 rewards_level
FROM (SELECT ac.acp_id,
    CASE
    WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt
       FROM date_lookup_ly) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt
         FROM date_lookup_ly) OR lmd.cardmember_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS cardmember_fl,
    CASE
    WHEN CASE
        WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt
           FROM date_lookup_ly) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt
             FROM date_lookup_ly) OR lmd.cardmember_close_date IS NULL)
        THEN 1
        ELSE 0
        END = 0 AND lmd.member_enroll_date <= (SELECT ty_end_dt
        FROM date_lookup_ly) AND (lmd.member_close_date >= (SELECT ty_end_dt
         FROM date_lookup_ly) OR lmd.member_close_date IS NULL)
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
    WHERE start_day_date <= (SELECT ty_end_dt
       FROM date_lookup_ly)
     AND end_day_date > (SELECT ty_end_dt
       FROM date_lookup_ly)) AS t1 ON LOWER(ac.acp_loyalty_id) = LOWER(t1.loyalty_id)
  GROUP BY ac.acp_id,
   cardmember_fl,
   member_fl) AS lyl;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_summary

AS
SELECT ty.brand,
 ty.channel,
 ty.digital_store,
 ty.acp_id,
  CASE
  WHEN x.predicted_ct_segment IS NULL
  THEN 'unknown'
  ELSE x.predicted_ct_segment
  END AS predicted_segment,
 ac.country,
  CASE
  WHEN lyl.loyalty_status IS NULL
  THEN 'non-member'
  ELSE lyl.loyalty_status
  END AS loyalty_status,
  CASE
  WHEN lyl.rewards_level = 1
  THEN '1-member'
  WHEN lyl.rewards_level = 2
  THEN '2-insider'
  WHEN lyl.rewards_level = 3
  THEN '3-influencer'
  WHEN lyl.rewards_level = 4
  THEN '4-ambassador'
  WHEN lyl.rewards_level = 5
  THEN '5-icon'
  WHEN lyl.rewards_level = 0
  THEN CASE
   WHEN LOWER(lyl.loyalty_status) = LOWER('cardmember')
   THEN '1-member'
   WHEN LOWER(lyl.loyalty_status) = LOWER('member')
   THEN '1-member'
   WHEN LOWER(lyl.loyalty_status) = LOWER('non-member')
   THEN '0-nonmember'
   ELSE NULL
   END
  ELSE '0-nonmember'
  END AS nordy_level,
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
 LEFT JOIN engaged_customer AS eng ON LOWER(ty.acp_id) = LOWER(eng.acp_id) AND LOWER(ty.channel) = LOWER(eng.engaged_to_channel
    )
 LEFT JOIN new_ty AS acq ON LOWER(ty.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN activated_ty AS act ON LOWER(ty.acp_id) = LOWER(act.acp_id) AND LOWER(ty.channel) = LOWER(act.activated_channel
    )
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment AS x ON LOWER(ty
   .acp_id) = LOWER(x.acp_id)
 LEFT JOIN lyl_status AS lyl ON LOWER(ty.acp_id) = LOWER(lyl.acp_id)
 LEFT JOIN (SELECT acp_id,
    CASE
    WHEN us_dma_code = - 1
    THEN NULL
    ELSE us_dma_desc
    END AS us_dma,
    CASE
    WHEN CASE
     WHEN us_dma_code = - 1
     THEN NULL
     ELSE us_dma_desc
     END IS NOT NULL
    THEN 'US'
    WHEN ca_dma_code IS NOT NULL
    THEN 'CA'
    ELSE 'unknown'
    END AS country
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer) AS ac ON LOWER(ty.acp_id) = LOWER(ac.acp_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics on customer_summary column (acp_id);
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS temporary_buyerflowplus_table

AS
SELECT (SELECT report_period
  FROM date_lookup_ly) AS report_period,
 channel,
 predicted_segment,
 country,
 loyalty_status,
 nordy_level,
 COUNT(DISTINCT acp_id) AS total_customers,
 SUM(gross_spend) AS total_gross_spend,
 SUM(net_spend) AS total_net_spend,
 SUM(trips) AS total_trips,
 SUM(items) AS total_items,
 COUNT(DISTINCT CASE
   WHEN retained_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS retained,
 SUM(CASE
   WHEN retained_flg = 1
   THEN gross_spend
   ELSE 0
   END) AS retained_gross_spend,
 SUM(CASE
   WHEN retained_flg = 1
   THEN net_spend
   ELSE 0
   END) AS retained_net_spend,
 SUM(CASE
   WHEN retained_flg = 1
   THEN cast(trunc(CAST(trips as float64))AS INTEGER)
   ELSE 0
   END) AS retained_trips,
 SUM(CASE
   WHEN retained_flg = 1
   THEN items
   ELSE 0
   END) AS retained_items,
 COUNT(DISTINCT CASE
   WHEN new_flg = 1
   THEN acp_id
   ELSE NULL
   END) AS acquired,
 SUM(CASE
   WHEN new_flg = 1
   THEN gross_spend
   ELSE 0
   END) AS new_gross_spend,
 SUM(CASE
   WHEN new_flg = 1
   THEN net_spend
   ELSE 0
   END) AS new_net_spend,
 SUM(CASE
   WHEN new_flg = 1
   THEN  cast(trunc(CAST(trips as float64))AS INTEGER)
   ELSE 0
   END) AS new_trips,
 SUM(CASE
   WHEN new_flg = 1
   THEN items
   ELSE 0
   END) AS new_items,
   COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
     WHEN new_flg = 1
     THEN acp_id
     ELSE NULL
     END) - COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS reactivated,
   SUM(gross_spend) - SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) - SUM(CASE
    WHEN new_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS reactivated_gross_spend,
   SUM(net_spend) - SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) - SUM(CASE
    WHEN new_flg = 1
    THEN net_spend
    ELSE 0
    END) AS reactivated_net_spend,
   SUM(trips) - SUM(CASE
     WHEN retained_flg = 1
     THEN cast(trunc(CAST(trips as float64))AS INTEGER)
     ELSE 0
     END) - SUM(CASE
    WHEN new_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS reactivated_trips,
   SUM(items) - SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) - SUM(CASE
    WHEN new_flg = 1
    THEN items
    ELSE 0
    END) AS reactivated_items,
 SUM(act_flg) AS activated,
 SUM(CASE
   WHEN act_flg = 1
   THEN gross_spend
   ELSE 0
   END) AS activated_gross_spend,
 SUM(CASE
   WHEN act_flg = 1
   THEN net_spend
   ELSE 0
   END) AS activated_net_spend,
 SUM(CASE
   WHEN act_flg = 1
   THEN cast(trunc(CAST(trips as float64))AS INTEGER)
   ELSE 0
   END) AS activated_trips,
 SUM(CASE
   WHEN act_flg = 1
   THEN items
   ELSE 0
   END) AS activated_items,
 SUM(eng_flg) AS engaged,
 SUM(CASE
   WHEN eng_flg = 1
   THEN gross_spend
   ELSE 0
   END) AS engaged_gross_spend,
 SUM(CASE
   WHEN eng_flg = 1
   THEN net_spend
   ELSE 0
   END) AS engaged_net_spend,
 SUM(CASE
   WHEN eng_flg = 1
   THEN cast(trunc(CAST(trips as float64))AS INTEGER)
   ELSE 0
   END) AS engaged_trips,
 SUM(CASE
   WHEN eng_flg = 1
   THEN items
   ELSE 0
   END) AS engaged_items,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)  as current_timesta
FROM customer_summary
GROUP BY report_period,
 channel,
 predicted_segment,
 country,
 loyalty_status,
 nordy_level;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO temporary_buyerflowplus_table
(SELECT (SELECT report_period
   FROM date_lookup_ly) AS report_period,
  brand,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customers,
  SUM(gross_spend) AS total_gross_spend,
  SUM(net_spend) AS total_net_spend,
  SUM(trips) AS total_trips,
  SUM(items) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained,
  SUM(CASE
    WHEN retained_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS retained_gross_spend,
  SUM(CASE
    WHEN retained_flg = 1
    THEN net_spend
    ELSE 0
    END) AS retained_net_spend,
  SUM(CASE
    WHEN retained_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS retained_trips,
  SUM(CASE
    WHEN retained_flg = 1
    THEN items
    ELSE 0
    END) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS acquired,
  SUM(CASE
    WHEN new_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS new_gross_spend,
  SUM(CASE
    WHEN new_flg = 1
    THEN net_spend
    ELSE 0
    END) AS new_net_spend,
  SUM(CASE
    WHEN new_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS new_trips,
  SUM(CASE
    WHEN new_flg = 1
    THEN items
    ELSE 0
    END) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS reactivated,
    SUM(gross_spend) - SUM(CASE
      WHEN retained_flg = 1
      THEN gross_spend
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS reactivated_gross_spend,
    SUM(net_spend) - SUM(CASE
      WHEN retained_flg = 1
      THEN net_spend
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS reactivated_net_spend,
    SUM(trips) - SUM(CASE
      WHEN retained_flg = 1
      THEN cast(trunc(CAST(trips as float64))AS INTEGER)
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN cast(trunc(CAST(trips as float64))AS INTEGER)
     ELSE 0
     END) AS reactivated_trips,
    SUM(items) - SUM(CASE
      WHEN retained_flg = 1
      THEN items
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS reactivated_items,
  SUM(act_flg) AS activated,
  SUM(CASE
    WHEN act_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS activated_gross_spend,
  SUM(CASE
    WHEN act_flg = 1
    THEN net_spend
    ELSE 0
    END) AS activated_net_spend,
  SUM(CASE
    WHEN act_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS activated_trips,
  SUM(CASE
    WHEN act_flg = 1
    THEN items
    ELSE 0
    END) AS activated_items,
  COUNT(DISTINCT CASE
    WHEN eng_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS engaged,
  SUM(CASE
    WHEN eng_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS engaged_gross_spend,
  SUM(CASE
    WHEN eng_flg = 1
    THEN net_spend
    ELSE 0
    END) AS engaged_net_spend,
  SUM(CASE
    WHEN eng_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS engaged_trips,
  SUM(CASE
    WHEN eng_flg = 1
    THEN items
    ELSE 0
    END) AS engaged_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM customer_summary
 GROUP BY brand,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO temporary_buyerflowplus_table
(SELECT (SELECT report_period
   FROM date_lookup_ly) AS report_period,
  digital_store,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customers,
  SUM(gross_spend) AS total_gross_spend,
  SUM(net_spend) AS total_net_spend,
  SUM(trips) AS total_trips,
  SUM(items) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained,
  SUM(CASE
    WHEN retained_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS retained_gross_spend,
  SUM(CASE
    WHEN retained_flg = 1
    THEN net_spend
    ELSE 0
    END) AS retained_net_spend,
  SUM(CASE
    WHEN retained_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS retained_trips,
  SUM(CASE
    WHEN retained_flg = 1
    THEN items
    ELSE 0
    END) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS acquired,
  SUM(CASE
    WHEN new_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS new_gross_spend,
  SUM(CASE
    WHEN new_flg = 1
    THEN net_spend
    ELSE 0
    END) AS new_net_spend,
  SUM(CASE
    WHEN new_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS new_trips,
  SUM(CASE
    WHEN new_flg = 1
    THEN items
    ELSE 0
    END) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS reactivated,
    SUM(gross_spend) - SUM(CASE
      WHEN retained_flg = 1
      THEN gross_spend
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS reactivated_gross_spend,
    SUM(net_spend) - SUM(CASE
      WHEN retained_flg = 1
      THEN net_spend
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS reactivated_net_spend,
    SUM(trips) - SUM(CASE
      WHEN retained_flg = 1
      THEN cast(trunc(CAST(trips as float64))AS INTEGER)
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN cast(trunc(CAST(trips as float64))AS INTEGER)
     ELSE 0
     END) AS reactivated_trips,
    SUM(items) - SUM(CASE
      WHEN retained_flg = 1
      THEN items
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS reactivated_items,
  SUM(act_flg) AS activated,
  SUM(CASE
    WHEN act_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS activated_gross_spend,
  SUM(CASE
    WHEN act_flg = 1
    THEN net_spend
    ELSE 0
    END) AS activated_net_spend,
  SUM(CASE
    WHEN act_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS activated_trips,
  SUM(CASE
    WHEN act_flg = 1
    THEN items
    ELSE 0
    END) AS activated_items,
  COUNT(DISTINCT CASE
    WHEN eng_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS engaged,
  SUM(CASE
    WHEN eng_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS engaged_gross_spend,
  SUM(CASE
    WHEN eng_flg = 1
    THEN net_spend
    ELSE 0
    END) AS engaged_net_spend,
  SUM(CASE
    WHEN eng_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS engaged_trips,
  SUM(CASE
    WHEN eng_flg = 1
    THEN items
    ELSE 0
    END) AS engaged_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM customer_summary
 GROUP BY digital_store,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO temporary_buyerflowplus_table
(SELECT (SELECT report_period
   FROM date_lookup_ly) AS report_period,
  'JWN',
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customers,
  SUM(gross_spend) AS total_gross_spend,
  SUM(net_spend) AS total_net_spend,
  SUM(trips) AS total_trips,
  SUM(items) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained,
  SUM(CASE
    WHEN retained_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS retained_gross_spend,
  SUM(CASE
    WHEN retained_flg = 1
    THEN net_spend
    ELSE 0
    END) AS retained_net_spend,
  SUM(CASE
    WHEN retained_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS retained_trips,
  SUM(CASE
    WHEN retained_flg = 1
    THEN items
    ELSE 0
    END) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS acquired,
  SUM(CASE
    WHEN new_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS new_gross_spend,
  SUM(CASE
    WHEN new_flg = 1
    THEN net_spend
    ELSE 0
    END) AS new_net_spend,
  SUM(CASE
    WHEN new_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS new_trips,
  SUM(CASE
    WHEN new_flg = 1
    THEN items
    ELSE 0
    END) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS reactivated,
    SUM(gross_spend) - SUM(CASE
      WHEN retained_flg = 1
      THEN gross_spend
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS reactivated_gross_spend,
    SUM(net_spend) - SUM(CASE
      WHEN retained_flg = 1
      THEN net_spend
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS reactivated_net_spend,
    SUM(trips) - SUM(CASE
      WHEN retained_flg = 1
      THEN cast(trunc(CAST(trips as float64))AS INTEGER)
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN cast(trunc(CAST(trips as float64))AS INTEGER)
     ELSE 0
     END) AS reactivated_trips,
    SUM(items) - SUM(CASE
      WHEN retained_flg = 1
      THEN items
      ELSE 0
      END) - SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS reactivated_items,
  SUM(act_flg) AS activated,
  SUM(CASE
    WHEN act_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS activated_gross_spend,
  SUM(CASE
    WHEN act_flg = 1
    THEN net_spend
    ELSE 0
    END) AS activated_net_spend,
  SUM(CASE
    WHEN act_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS activated_trips,
  SUM(CASE
    WHEN act_flg = 1
    THEN items
    ELSE 0
    END) AS activated_items,
  COUNT(DISTINCT CASE
    WHEN eng_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS engaged,
  SUM(CASE
    WHEN eng_flg = 1
    THEN gross_spend
    ELSE 0
    END) AS engaged_gross_spend,
  SUM(CASE
    WHEN eng_flg = 1
    THEN net_spend
    ELSE 0
    END) AS engaged_net_spend,
  SUM(CASE
    WHEN eng_flg = 1
    THEN cast(trunc(CAST(trips as float64))AS INTEGER)
    ELSE 0
    END) AS engaged_trips,
  SUM(CASE
    WHEN eng_flg = 1
    THEN items
    ELSE 0
    END) AS engaged_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM customer_summary
 GROUP BY predicted_segment,
  country,
  loyalty_status,
  nordy_level);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS final_table

AS
SELECT TRIM(report_period) AS report_period,
 CAST(trunc(cast(CASE
   WHEN SUBSTR(report_period, 1, STRPOS(LOWER(report_period), LOWER('_')) - 1) = ''
   THEN '0'
   ELSE SUBSTR(report_period, 1, STRPOS(LOWER(report_period), LOWER('_')) - 1)
   END as float64)) AS INTEGER) AS report_year,
 SUBSTR(report_period, STRPOS(LOWER(report_period), LOWER('_')) + 1) AS report_month,
 channel,
 country,
 SUM(total_customers) AS total_customers,
 SUM(total_trips) AS total_trips,
 SUM(retained) AS retained,
 SUM(retained_trips) AS retained_trips,
 SUM(acquired) AS acquired,
 SUM(new_trips) AS new_trips,
 SUM(reactivated) AS reactivated,
 SUM(reactivated_trips) AS reactivated_trips,
 SUM(activated) AS activated,
 SUM(activated_trips) AS activated_trips,
 SUM(engaged) AS engaged,
 SUM(engaged_trips) AS engaged_trips,
 SUM(total_net_spend) AS total_net_spend,
 SUM(retained_net_spend) AS retained_net_spend,
 SUM(new_net_spend) AS new_net_spend,
 SUM(reactivated_net_spend) AS reactivated_net_spend,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM temporary_buyerflowplus_table
GROUP BY report_period,
 report_year,
 report_month,
 channel,
 country;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.kpi_scorecard_t2_schema}}.kpi_buyerflow
WHERE LOWER(report_period) = LOWER((SELECT report_period
            FROM date_lookup));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.kpi_scorecard_t2_schema}}.kpi_buyerflow
(SELECT a.report_period,
  CAST(a.report_year AS STRING) AS report_year,
  a.report_month,
  a.channel,
  a.country,
  SUM(a.total_customers) AS total_customers,
  SUM(b.total_customers) AS ly_total_customers,
  SUM(a.total_trips) AS total_trips,
  SUM(b.total_trips) AS ly_total_trips,
  SUM(a.retained) AS retained,
  SUM(b.retained) AS ly_retained,
  SUM(a.retained_trips) AS retained_trips,
  SUM(b.retained_trips) AS ly_retained_trips,
  SUM(a.acquired) AS acquired,
  SUM(b.acquired) AS ly_acquired,
  SUM(a.new_trips) AS new_trips,
  SUM(b.new_trips) AS ly_new_trips,
  SUM(a.reactivated) AS reactivated,
  SUM(b.reactivated) AS ly_reactivated,
  SUM(a.reactivated_trips) AS reactivated_trips,
  SUM(b.reactivated_trips) AS ly_reactivated_trips,
  SUM(a.activated) AS activated,
  SUM(b.activated) AS ly_activated,
  SUM(a.activated_trips) AS activated_trips,
  SUM(b.activated_trips) AS ly_activated_trips,
  SUM(a.engaged) AS engaged,
  SUM(b.engaged) AS ly_engaged,
  SUM(a.engaged_trips) AS engaged_trips,
  SUM(b.engaged_trips) AS ly_engaged_trips,
  CAST(trunc(cast(SUM(a.total_net_spend)as float64)) AS INTEGER) AS total_net_spend,
  SUM(b.total_net_spend) AS ly_total_net_spend,
  CAST(trunc(cast(SUM(a.retained_net_spend)as float64)) AS INTEGER) AS retained_net_spend,
  SUM(b.retained_net_spend) AS ly_retained_net_spend,
  CAST(trunc(cast(SUM(a.new_net_spend)as float64)) AS INTEGER) AS new_net_spend,
  SUM(b.new_net_spend) AS ly_new_net_spend,
  CAST(trunc(cast(SUM(a.reactivated_net_spend)as float64)) AS INTEGER) AS reactivated_net_spend,
  SUM(b.reactivated_net_spend) AS ly_reactivated_net_spend,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM final_table AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.kpi_scorecard_t2_schema}}.kpi_buyerflow AS b ON CAST(b.report_year AS FLOAT64) = a.report_year - 1 AND LOWER(TRIM(b.report_month
        )) = LOWER(TRIM(a.report_month)) AND LOWER(b.channel) = LOWER(a.channel) AND LOWER(a.country) = LOWER(b.country
     )
 GROUP BY a.report_period,
  a.report_year,
  a.report_month,
  a.channel,
  a.country);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
