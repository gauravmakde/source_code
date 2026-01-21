BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP09037;
DAG_ID=mothership_ets_primary_11521_ACE_ENG;
---Task_Name=ets_primary;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT CASE
  WHEN month_num >= 202301
  THEN month_num
  ELSE NULL
  END AS month_num,
 month_454_num,
 quarter_num,
 halfyear_num,
 year_num,
 MIN(day_date) AS start_dt,
 MAX(day_date) AS end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
WHERE month_num BETWEEN 202401 AND (CAST((SELECT MAX(report_period)
     FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.month_num_buyerflow) AS INTEGER))
GROUP BY month_num,
 month_454_num,
 quarter_num,
 halfyear_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_positive

AS
SELECT CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA')) THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB')) THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA')) THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE')) THEN 'NRHL'
  ELSE NULL
  END AS channel,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA')) THEN 'FLS'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB')) THEN 'NCOM'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA')) THEN 'RACK'
     WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE')) THEN 'NRHL'
     ELSE NULL
     END) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'FP'
  ELSE 'OP'
  END AS brand,
 tran.acp_id,
 tran.ytd_flag,
 tran.qtd_q1_flag,
 tran.qtd_q2_flag,
 tran.qtd_q3_flag,
 tran.qtd_q4_flag,
 tran.ly_flag,
 tran.h1_flag,
 tran.q1_flag,
 tran.feb_flag,
 COUNT(DISTINCT CASE
   WHEN tran.ytd_flag = 1 
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_ytd,
 COUNT(DISTINCT CASE
   WHEN tran.qtd_q1_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_qtd_q1,
 COUNT(DISTINCT CASE
   WHEN tran.qtd_q2_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_qtd_q2,
 COUNT(DISTINCT CASE
   WHEN tran.qtd_q3_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_qtd_q3,
 COUNT(DISTINCT CASE
   WHEN tran.qtd_q4_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_qtd_q4,
 COUNT(DISTINCT CASE
   WHEN tran.ly_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_ly,
 COUNT(DISTINCT CASE
   WHEN tran.h1_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_h1,
 COUNT(DISTINCT CASE
   WHEN tran.q1_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_q1,
 COUNT(DISTINCT CASE
   WHEN tran.feb_flag = 1
   THEN tran.trip_id
   ELSE NULL
   END) AS trips_feb
FROM (SELECT acp_id,
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
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
      FROM date_lookup
      WHERE year_num = 2024)
    THEN 1
    ELSE 0
    END AS ytd_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202401, 202402)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202401, 202402))
    THEN 1
    ELSE 0
    END AS qtd_q1_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202404, 202405)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202404, 202405))
    THEN 1
    ELSE 0
    END AS qtd_q2_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202407, 202408)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202407, 202408))
    THEN 1
    ELSE 0
    END AS qtd_q3_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202410, 202411)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202410, 202411))
    THEN 1
    ELSE 0
    END AS qtd_q4_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE year_num = 2023) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE year_num = 2023)
    THEN 1
    ELSE 0
    END AS ly_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE halfyear_num = 20241) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE halfyear_num = 20241)
    THEN 1
    ELSE 0
    END AS h1_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE quarter_num = 20241) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE quarter_num = 20241)
    THEN 1
    ELSE 0
    END AS q1_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num = 202401) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num = 202401)
    THEN 1
    ELSE 0
    END AS feb_flag,
   SUBSTR(acp_id || FORMAT('%11d', store_num) || CAST(COALESCE(order_date, tran_date) AS STRING), 1, 150) AS trip_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
  WHERE COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
     FROM date_lookup)
   AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
     FROM date_lookup)
   AND line_net_usd_amt > 0
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON tran.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'
     ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
GROUP BY channel,
 brand,
 tran.acp_id,
 tran.ytd_flag,
 tran.qtd_q1_flag,
 tran.qtd_q2_flag,
 tran.qtd_q3_flag,
 tran.qtd_q4_flag,
 tran.ly_flag,
 tran.h1_flag,
 tran.q1_flag,
 tran.feb_flag;
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
  WHEN LOWER(
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
     END) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'FP'
  ELSE 'OP'
  END AS brand,
 tran.acp_id,
 tran.ytd_flag,
 tran.qtd_q1_flag,
 tran.qtd_q2_flag,
 tran.qtd_q3_flag,
 tran.qtd_q4_flag,
 tran.ly_flag,
 tran.h1_flag,
 tran.q1_flag,
 tran.feb_flag
FROM (SELECT acp_id,
    CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
    THEN 867
    ELSE intent_store_num
    END AS store_num,
    CASE
    WHEN tran_date >= (SELECT MIN(start_dt)
      FROM date_lookup
      WHERE year_num = 2024)
    THEN 1
    ELSE 0
    END AS ytd_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202401, 202402)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202401, 202402))
    THEN 1
    ELSE 0
    END AS qtd_q1_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202404, 202405)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202404, 202405))
    THEN 1
    ELSE 0
    END AS qtd_q2_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202407, 202408)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202407, 202408))
    THEN 1
    ELSE 0
    END AS qtd_q3_flag,
    CASE
    WHEN COALESCE(order_date, tran_date) >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num IN (202410, 202411)) AND COALESCE(order_date, tran_date) <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num IN (202410, 202411))
    THEN 1
    ELSE 0
    END AS qtd_q4_flag,
    CASE
    WHEN tran_date >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE year_num = 2022) AND tran_date <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE year_num = 2022)
    THEN 1
    ELSE 0
    END AS ly_flag,
    CASE
    WHEN tran_date >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE halfyear_num = 20241) AND tran_date <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE halfyear_num = 20241)
    THEN 1
    ELSE 0
    END AS h1_flag,
    CASE
    WHEN tran_date >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE quarter_num = 20241) AND tran_date <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE quarter_num = 20241)
    THEN 1
    ELSE 0
    END AS q1_flag,
    CASE
    WHEN tran_date >= (SELECT MIN(start_dt)
       FROM date_lookup
       WHERE month_num = 202401) AND tran_date <= (SELECT MAX(end_dt)
       FROM date_lookup
       WHERE month_num = 202401)
    THEN 1
    ELSE 0
    END AS feb_flag
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
  WHERE tran_date >= (SELECT MIN(start_dt)
     FROM date_lookup)
   AND tran_date <= (SELECT MAX(end_dt)
     FROM date_lookup)
   AND line_net_usd_amt <= 0
   AND acp_id IS NOT NULL) AS tran
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON tran.store_num = str.store_num AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'
     ), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'
     ), LOWER('TRUNK CLUB'))
GROUP BY channel,
 brand,
 tran.acp_id,
 tran.ytd_flag,
 tran.qtd_q1_flag,
 tran.qtd_q2_flag,
 tran.qtd_q3_flag,
 tran.qtd_q4_flag,
 tran.ly_flag,
 tran.h1_flag,
 tran.q1_flag,
 tran.feb_flag;
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
 COALESCE(a.ytd_flag, b.ytd_flag) AS ytd_flag,
 COALESCE(a.qtd_q1_flag, b.qtd_q1_flag) AS qtd_q1_flag,
 COALESCE(a.qtd_q2_flag, b.qtd_q2_flag) AS qtd_q2_flag,
 COALESCE(a.qtd_q3_flag, b.qtd_q3_flag) AS qtd_q3_flag,
 COALESCE(a.qtd_q4_flag, b.qtd_q4_flag) AS qtd_q4_flag,
 COALESCE(a.ly_flag, b.ly_flag) AS ly_flag,
 COALESCE(a.h1_flag, b.h1_flag) AS h1_flag,
 COALESCE(a.q1_flag, b.q1_flag) AS q1_flag,
 COALESCE(a.feb_flag, b.feb_flag) AS feb_flag,
 COALESCE(a.trips_ytd, 0) AS trips_ytd,
 COALESCE(a.trips_qtd_q1, 0) AS trips_qtd_q1,
 COALESCE(a.trips_qtd_q2, 0) AS trips_qtd_q2,
 COALESCE(a.trips_qtd_q3, 0) AS trips_qtd_q3,
 COALESCE(a.trips_qtd_q4, 0) AS trips_qtd_q4,
 COALESCE(a.trips_ly, 0) AS trips_ly,
 COALESCE(a.trips_h1, 0) AS trips_h1,
 COALESCE(a.trips_q1, 0) AS trips_q1,
 COALESCE(a.trips_feb, 0) AS trips_feb
FROM ty_positive AS a
 FULL JOIN ty_negative AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.channel) = LOWER(b.channel) AND LOWER(a.brand
             ) = LOWER(b.brand) AND a.ytd_flag = b.ytd_flag AND a.qtd_q1_flag = b.qtd_q1_flag AND a.qtd_q2_flag = b.qtd_q2_flag
          AND a.qtd_q3_flag = b.qtd_q3_flag AND a.qtd_q4_flag = b.qtd_q4_flag AND a.ly_flag = b.ly_flag AND a.h1_flag =
     b.h1_flag AND a.q1_flag = b.q1_flag AND a.feb_flag = b.feb_flag;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS new_ty
AS
SELECT acp_id,
 aare_chnl_code AS acquired_channel,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
    FROM date_lookup
    WHERE year_num = 2024)
  THEN 1
  ELSE 0
  END AS ytd_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE month_num IN (202401, 202402)) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE month_num IN (202401, 202402))
  THEN 1
  ELSE 0
  END AS qtd_q1_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE month_num IN (202404, 202405)) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE month_num IN (202404, 202405))
  THEN 1
  ELSE 0
  END AS qtd_q2_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE month_num IN (202407, 202408)) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE month_num IN (202407, 202408))
  THEN 1
  ELSE 0
  END AS qtd_q3_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE month_num IN (202410, 202411)) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE month_num IN (202410, 202411))
  THEN 1
  ELSE 0
  END AS qtd_q4_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE year_num = 2023) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE year_num = 2023)
  THEN 1
  ELSE 0
  END AS ly_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE halfyear_num = 20241) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE halfyear_num = 20241)
  THEN 1
  ELSE 0
  END AS h1_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE quarter_num = 20241) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE quarter_num = 20241)
  THEN 1
  ELSE 0
  END AS q1_flag,
  CASE
  WHEN aare_status_date >= (SELECT MIN(start_dt)
     FROM date_lookup
     WHERE month_num = 202401) AND aare_status_date <= (SELECT MAX(end_dt)
     FROM date_lookup
     WHERE month_num = 202401)
  THEN 1
  ELSE 0
  END AS feb_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
WHERE aare_status_date BETWEEN (SELECT MIN(start_dt)
   FROM date_lookup) AND (SELECT MAX(end_dt)
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_summary
AS
SELECT ty.channel,
 ty.acp_id,
 ac.country,
 ty.brand,
 ty.trips_ytd,
 ty.trips_qtd_q1,
 ty.trips_qtd_q2,
 ty.trips_qtd_q3,
 ty.trips_qtd_q4,
 ty.trips_ly,
 ty.trips_h1,
 ty.trips_q1,
 ty.trips_feb,
 ty.ytd_flag,
 ty.qtd_q1_flag,
 ty.qtd_q2_flag,
 ty.qtd_q3_flag,
 ty.qtd_q4_flag,
 ty.ly_flag,
 ty.h1_flag,
 ty.q1_flag,
 ty.feb_flag,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.ytd_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_ytd,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.qtd_q1_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_qtd_q1,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.qtd_q2_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_qtd_q2,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.qtd_q3_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_qtd_q3,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.qtd_q4_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_qtd_q4,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.ly_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_ly,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.h1_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_h1,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.q1_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_q1,
  CASE
  WHEN acq.acp_id IS NOT NULL AND acq.feb_flag = 1
  THEN 1
  ELSE 0
  END AS new_flg_feb
FROM ty
 LEFT JOIN new_ty AS acq ON LOWER(ty.acp_id) = LOWER(acq.acp_id) AND ty.ytd_flag = acq.ytd_flag AND ty.qtd_q1_flag = acq
          .qtd_q1_flag AND ty.qtd_q2_flag = acq.qtd_q2_flag AND ty.qtd_q3_flag = acq.qtd_q3_flag AND ty.qtd_q4_flag =
       acq.qtd_q4_flag AND ty.ly_flag = acq.ly_flag AND ty.h1_flag = acq.h1_flag AND ty.q1_flag = acq.q1_flag AND ty.feb_flag
    = acq.feb_flag
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
--collect statistics on customer_summary column (acp_id,channel,brand);
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS temporary_buyerflowplus_table
AS
SELECT '202423_YTD' AS report_period,
 cust.channel,
 ebh.total_customers_ly,
 ebh.total_trips_ly,
 ebh.acquired_ly,
 COUNT(DISTINCT CASE
   WHEN cust.ytd_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_ytd,
 COUNT(DISTINCT CASE
   WHEN cust.qtd_q1_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_qtd_q1,
 COUNT(DISTINCT CASE
   WHEN cust.qtd_q2_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_qtd_q2,
 COUNT(DISTINCT CASE
   WHEN cust.qtd_q3_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_qtd_q3,
 COUNT(DISTINCT CASE
   WHEN cust.qtd_q4_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_qtd_q4,
 COUNT(DISTINCT CASE
   WHEN cust.h1_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_h1,
 COUNT(DISTINCT CASE
   WHEN cust.q1_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_q1,
 COUNT(DISTINCT CASE
   WHEN cust.feb_flag = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS total_customers_feb,
 SUM(cust.trips_ytd) AS total_trips_ytd,
 SUM(cust.trips_qtd_q1) AS total_trips_qtd_q1,
 SUM(cust.trips_qtd_q2) AS total_trips_qtd_q2,
 SUM(cust.trips_qtd_q3) AS total_trips_qtd_q3,
 SUM(cust.trips_qtd_q4) AS total_trips_qtd_q4,
 SUM(cust.trips_h1) AS total_trips_h1,
 SUM(cust.trips_q1) AS total_trips_q1,
 SUM(cust.trips_feb) AS total_trips_feb,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_ytd = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_ytd,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_qtd_q1 = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_qtd_q1,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_qtd_q2 = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_qtd_q2,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_qtd_q3 = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_qtd_q3,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_qtd_q4 = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_qtd_q4,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_h1 = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_h1,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_q1 = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_q1,
 COUNT(DISTINCT CASE
   WHEN cust.new_flg_feb = 1
   THEN cust.acp_id
   ELSE NULL
   END) AS acquired_feb,
 ROUND(SUM(cust.trips_ytd) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.ytd_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_ytd,
 ROUND(SUM(cust.trips_qtd_q1) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.qtd_q1_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q1,
 ROUND(SUM(cust.trips_qtd_q2) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.qtd_q2_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q2,
 ROUND(SUM(cust.trips_qtd_q3) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.qtd_q3_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q3,
 ROUND(SUM(cust.trips_qtd_q4) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.qtd_q4_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q4,
  ebh.total_trips_ly / NULLIF(CAST(ebh.total_customers_ly AS NUMERIC), 0) AS trips_per_cust_ly,
 ROUND(SUM(cust.trips_h1) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.h1_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_h1,
 ROUND(SUM(cust.trips_q1) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.q1_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_q1,
 ROUND(SUM(cust.trips_feb) / NULLIF(CAST(COUNT(DISTINCT CASE
       WHEN cust.feb_flag = 1
       THEN cust.acp_id
       ELSE NULL
       END) AS NUMERIC), 0), 4) AS trips_per_cust_feb,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS current_timesta
FROM customer_summary AS cust
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mothership.ets_buyerflow_hist AS ebh ON LOWER(cust.channel) = LOWER(ebh.channel)
WHERE LOWER(cust.country) <> LOWER('CA')
GROUP BY report_period,
 cust.channel,
 ebh.total_customers_ly,
 ebh.total_trips_ly,
 ebh.acquired_ly;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO temporary_buyerflowplus_table
(SELECT '202423_YTD',
  cust.brand,
  ebh.total_customers_ly,
  ebh.total_trips_ly,
  ebh.acquired_ly,
  COUNT(DISTINCT CASE
    WHEN cust.ytd_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_ytd,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q1_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q1,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q2_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q2,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q3_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q3,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q4_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q4,
  COUNT(DISTINCT CASE
    WHEN cust.h1_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_h1,
  COUNT(DISTINCT CASE
    WHEN cust.q1_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_q1,
  COUNT(DISTINCT CASE
    WHEN cust.feb_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_feb,
  SUM(cust.trips_ytd) AS total_trips_ytd,
  SUM(cust.trips_qtd_q1) AS total_trips_qtd_q1,
  SUM(cust.trips_qtd_q2) AS total_trips_qtd_q2,
  SUM(cust.trips_qtd_q3) AS total_trips_qtd_q3,
  SUM(cust.trips_qtd_q4) AS total_trips_qtd_q4,
  SUM(cust.trips_h1) AS total_trips_h1,
  SUM(cust.trips_q1) AS total_trips_q1,
  SUM(cust.trips_feb) AS total_trips_feb,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_ytd = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_ytd,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q1 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q1,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q2 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q2,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q3 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q3,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q4 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q4,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_h1 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_h1,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_q1 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_q1,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_feb = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_feb,
  ROUND(SUM(cust.trips_ytd) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.ytd_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_ytd,
  ROUND(SUM(cust.trips_qtd_q1) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q1_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q1,
  ROUND(SUM(cust.trips_qtd_q2) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q2_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q2,
  ROUND(SUM(cust.trips_qtd_q3) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q3_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q3,
  ROUND(SUM(cust.trips_qtd_q4) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q4_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q4,
   ebh.total_trips_ly / NULLIF(CAST(ebh.total_customers_ly AS NUMERIC), 0) AS trips_per_cust_ly,
  ROUND(SUM(cust.trips_h1) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.h1_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_h1,
  ROUND(SUM(cust.trips_q1) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.q1_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_q1,
  ROUND(SUM(cust.trips_feb) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.feb_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_feb,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM customer_summary AS cust
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mothership.ets_buyerflow_hist AS ebh ON LOWER(cust.brand) = LOWER(ebh.channel)
 WHERE LOWER(cust.country) <> LOWER('CA')
 GROUP BY 1,
  cust.brand,
  ebh.total_customers_ly,
  ebh.total_trips_ly,
  ebh.acquired_ly);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO temporary_buyerflowplus_table
(SELECT '202423_YTD',
  'JWN',
  ebh.total_customers_ly,
  ebh.total_trips_ly,
  ebh.acquired_ly,
  COUNT(DISTINCT CASE
    WHEN cust.ytd_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_ytd,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q1_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q1,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q2_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q2,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q3_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q3,
  COUNT(DISTINCT CASE
    WHEN cust.qtd_q4_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_qtd_q4,
  COUNT(DISTINCT CASE
    WHEN cust.h1_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_h1,
  COUNT(DISTINCT CASE
    WHEN cust.q1_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_q1,
  COUNT(DISTINCT CASE
    WHEN cust.feb_flag = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS total_customers_feb,
  SUM(cust.trips_ytd) AS total_trips_ytd,
  SUM(cust.trips_qtd_q1) AS total_trips_qtd_q1,
  SUM(cust.trips_qtd_q2) AS total_trips_qtd_q2,
  SUM(cust.trips_qtd_q3) AS total_trips_qtd_q3,
  SUM(cust.trips_qtd_q4) AS total_trips_qtd_q4,
  SUM(cust.trips_h1) AS total_trips_h1,
  SUM(cust.trips_q1) AS total_trips_q1,
  SUM(cust.trips_feb) AS total_trips_feb,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_ytd = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_ytd,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q1 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q1,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q2 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q2,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q3 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q3,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_qtd_q4 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_qtd_q4,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_h1 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_h1,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_q1 = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_q1,
  COUNT(DISTINCT CASE
    WHEN cust.new_flg_feb = 1
    THEN cust.acp_id
    ELSE NULL
    END) AS acquired_feb,
  ROUND(SUM(cust.trips_ytd) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.ytd_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_ytd,
  ROUND(SUM(cust.trips_qtd_q1) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q1_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q1,
  ROUND(SUM(cust.trips_qtd_q2) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q2_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q2,
  ROUND(SUM(cust.trips_qtd_q3) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q3_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q3,
  ROUND(SUM(cust.trips_qtd_q4) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.qtd_q4_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_qtd_q4,
   ebh.total_trips_ly / NULLIF(CAST(ebh.total_customers_ly AS NUMERIC), 0) AS trips_per_cust_ly,
  ROUND(SUM(cust.trips_h1) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.h1_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_h1,
  ROUND(SUM(cust.trips_q1) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.q1_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_q1,
  ROUND(SUM(cust.trips_feb) / NULLIF(CAST(COUNT(DISTINCT CASE
        WHEN cust.feb_flag = 1
        THEN cust.acp_id
        ELSE NULL
        END) AS NUMERIC), 0), 4) AS trips_per_cust_feb,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM customer_summary AS cust
  LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mothership.ets_buyerflow_hist AS ebh ON LOWER(ebh.channel) = LOWER('JWN')
 WHERE LOWER(cust.country) <> LOWER('CA')
 GROUP BY 1,
  2,
  ebh.total_customers_ly,
  ebh.total_trips_ly,
  ebh.acquired_ly);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS month_range
AS
SELECT DISTINCT month_idnt AS month_num,
 month_start_day_date,
 month_end_day_date,
 month_abrv,
 quarter_abrv,
 quarter_idnt,
 fiscal_halfyear_num,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt BETWEEN 202301 AND 202412;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_fm
AS
SELECT b.report_period AS month_num,
 b.box,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 202401
  THEN CAST(tbt.total_customers_feb AS INTEGER)
  ELSE b.total_customer
  END AS total_customer,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 202401
  THEN CAST(tbt.total_trips_feb AS BIGNUMERIC)
  ELSE b.total_trips
  END AS total_trips,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 202401
  THEN CAST(tbt.acquired_feb AS INTEGER)
  ELSE b.new_customer
  END AS new_customer,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 202401
  THEN tbt.trips_per_cust_feb
  ELSE b.total_trips / b.total_customer
  END AS trips_per_cust
FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.month_num_buyerflow AS b
 LEFT JOIN temporary_buyerflowplus_table AS tbt ON LOWER(b.box) = LOWER(tbt.channel)
WHERE CAST(b.report_period AS INTEGER) IN (SELECT month_num
   FROM month_range
   WHERE fiscal_year_num = 2024)
 AND CAST(b.report_period AS FLOAT64) < (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_fq
AS
SELECT b.report_period AS quarter_num,
 b.box,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN CAST(tbt.total_customers_q1 AS INTEGER)
  ELSE b.total_customer
  END AS total_customer,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN CAST(tbt.total_trips_q1 AS BIGNUMERIC)
  ELSE b.total_trips
  END AS total_trips,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN CAST(tbt.acquired_q1 AS INTEGER)
  ELSE b.new_customer
  END AS new_customer,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN tbt.trips_per_cust_q1
  ELSE b.total_trips / b.total_customer
  END AS trips_per_cust
FROM dl_cma_cmbr.quarter_num_buyerflow AS b
 LEFT JOIN temporary_buyerflowplus_table AS tbt ON LOWER(b.box) = LOWER(tbt.channel)
WHERE CAST(b.report_period AS INTEGER) IN (SELECT quarter_idnt
   FROM month_range
   WHERE fiscal_year_num = 2024)
 AND CAST(b.report_period AS FLOAT64) < (SELECT DISTINCT quarter_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
UNION ALL
SELECT 'qtd_q1' AS quarter_num,
 channel AS box,
 total_customers_qtd_q1 AS total_customer,
 total_trips_qtd_q1 AS total_trips,
 acquired_qtd_q1 AS new_customer,
 trips_per_cust_qtd_q1 AS trips_per_cust
FROM temporary_buyerflowplus_table
UNION ALL
SELECT 'qtd_q2' AS quarter_num,
 channel AS box,
 total_customers_qtd_q2 AS total_customer,
 total_trips_qtd_q2 AS total_trips,
 acquired_qtd_q2 AS new_customer,
 trips_per_cust_qtd_q2 AS trips_per_cust
FROM temporary_buyerflowplus_table
UNION ALL
SELECT 'qtd_q3' AS quarter_num,
 channel AS box,
 total_customers_qtd_q3 AS total_customer,
 total_trips_qtd_q3 AS total_trips,
 acquired_qtd_q3 AS new_customer,
 trips_per_cust_qtd_q3 AS trips_per_cust
FROM temporary_buyerflowplus_table
UNION ALL
SELECT 'qtd_q4' AS quarter_num,
 channel AS box,
 total_customers_qtd_q4 AS total_customer,
 total_trips_qtd_q4 AS total_trips,
 acquired_qtd_q4 AS new_customer,
 trips_per_cust_qtd_q4 AS trips_per_cust
FROM temporary_buyerflowplus_table
UNION ALL
SELECT SUBSTR(CAST(month_range0.quarter_idnt AS STRING), 1, 5) AS quarter_num,
 cust_fm.box,
 CAST(NULL AS BIGINT) AS total_customer,
 CAST(NULL AS BIGNUMERIC) AS total_trips,
 CAST(NULL AS BIGINT) AS new_customer,
 CAST(NULL AS BIGNUMERIC) AS trips_per_cust
FROM cust_fm
 INNER JOIN month_range AS month_range0 ON TRUE
WHERE month_range0.quarter_idnt > CAST((SELECT MAX(report_period)
    FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.quarter_num_buyerflow) AS FLOAT64)
GROUP BY quarter_num,
 cust_fm.box;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_fh
AS
SELECT b.report_period AS half_num,
 b.box,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN CAST(tbt.total_customers_h1 AS INTEGER)
  ELSE b.total_customer
  END AS total_customer,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN CAST(tbt.total_trips_h1 AS BIGNUMERIC)
  ELSE b.total_trips
  END AS total_trips,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN CAST(tbt.acquired_h1 AS INTEGER)
  ELSE b.new_customer
  END AS new_customer,
  CASE
  WHEN CAST(b.report_period AS FLOAT64) = 20241
  THEN tbt.trips_per_cust_h1
  ELSE b.total_trips / b.total_customer
  END AS trips_per_cust
FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.halfyear_num_buyerflow AS b
 LEFT JOIN temporary_buyerflowplus_table AS tbt ON LOWER(b.box) = LOWER(tbt.channel)
WHERE CAST(b.report_period AS INTEGER) IN (SELECT fiscal_halfyear_num
   FROM month_range
   WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num) AS A3977768
      FROM month_range))
 AND CAST(b.report_period AS FLOAT64) < (SELECT DISTINCT halfyear_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
UNION ALL
SELECT SUBSTR(CAST(month_range1.fiscal_halfyear_num AS STRING), 1, 5) AS half_num,
 cust_fm.box,
 CAST(NULL AS INTEGER) AS total_customer,
 CAST(NULL AS BIGNUMERIC) AS total_trips,
 CAST(NULL AS INTEGER) AS new_customer,
 CAST(NULL AS BIGNUMERIC) AS trips_per_cust
FROM cust_fm
 INNER JOIN month_range AS month_range1 ON TRUE
WHERE month_range1.fiscal_halfyear_num > CAST((SELECT MAX(report_period)
    FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.halfyear_num_buyerflow) AS FLOAT64)
GROUP BY half_num,
 cust_fm.box;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_fy
AS
SELECT '2023' AS year_num,
 channel AS box,
 SUBSTR('ly', 1, 3) AS period_type,
 total_customers_ly AS total_customer,
 total_trips_ly AS total_trips,
 acquired_ly AS new_customer,
 trips_per_cust_ly AS trips_per_cust
FROM temporary_buyerflowplus_table
UNION ALL
SELECT '2024' AS year_num,
 channel AS box,
 'ytd' AS period_type,
 total_customers_ytd AS total_customer,
 total_trips_ytd AS total_trips,
 acquired_ytd AS new_customer,
 trips_per_cust_ytd AS trips_per_cust
FROM temporary_buyerflowplus_table
UNION ALL
SELECT DISTINCT '2024' AS year_num,
 box,
 'ty' AS period_type,
 CAST(NULL AS BIGINT) AS total_customer,
 CAST(NULL AS BIGINT) AS total_trips,
 CAST(NULL AS BIGINT) AS new_customer,
 CAST(NULL AS BIGNUMERIC) AS trips_per_cust
FROM `{{params.gcp_project_id}}`.dl_cma_cmbr.quarter_num_buyerflow;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_p50
AS
SELECT OL.order_num,
 OL.carrier_tracking_num,
 OL.source_channel_code,
 OL.order_tmstp_pacific,
 OL.order_date_pacific,
 OL.delivery_tmstp,
 OL.delivery_date,
 ddd.fiscal_year_num AS fiscal_year,
 ddd.fiscal_halfyear_num AS half_num,
 ddd.quarter_idnt,
  CASE
  WHEN ddd.month_idnt IN (202401, 202402)
  THEN 'qtd_q1'
  WHEN ddd.month_idnt IN (202404, 202405)
  THEN 'qtd_q2'
  WHEN ddd.month_idnt IN (202407, 202408)
  THEN 'qtd_q3'
  WHEN ddd.month_idnt IN (202410, 202411)
  THEN 'qtd_q4'
  ELSE NULL
  END AS qtd_idnt,
 ddd.month_idnt,
  CASE
  WHEN OL.click_to_delivery_seconds IS NULL
  THEN NULL
  WHEN CAST(OL.click_to_delivery_seconds AS FLOAT64) / 86400 > 20
  THEN 20
  WHEN CAST(OL.click_to_delivery_seconds AS FLOAT64) / 86400 <= 0
  THEN NULL
  ELSE ROUND(CAST(OL.click_to_delivery_seconds AS FLOAT64) / 86400, 2)
  END AS click_to_delivery
FROM (SELECT order_num,
   carrier_tracking_num,
   source_channel_code,
   order_tmstp_pacific,
   order_date_pacific,
   MAX(shipped_tmstp_pacific) AS shipped_tmstp_pacific,
   MAX(shipped_date_pacific) AS shipped_date_pacific,
   MAX(CAST(carrier_first_attempted_delivery_tmstp_pacific AS DATE)) AS delivery_date,
   MAX(carrier_first_attempted_delivery_tmstp_pacific) AS delivery_tmstp,
   MAX(CAST(TIMESTAMP_DIFF(carrier_first_attempted_delivery_tmstp_pacific, order_tmstp_pacific, DAY) AS BIGINT) * 86400
       + (EXTRACT(HOUR FROM carrier_first_attempted_delivery_tmstp_pacific) - EXTRACT(HOUR FROM order_tmstp_pacific)) *
        3600 + (EXTRACT(MINUTE FROM carrier_first_attempted_delivery_tmstp_pacific) - EXTRACT(MINUTE FROM
          order_tmstp_pacific)) * 60 + (EXTRACT(SECOND FROM carrier_first_attempted_delivery_tmstp_pacific) - EXTRACT(SECOND
        FROM order_tmstp_pacific))) AS click_to_delivery_seconds
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf
  WHERE LOWER(shipped_node_type_code) = LOWER('FC')
   AND EXTRACT(YEAR FROM carrier_first_attempted_delivery_tmstp_pacific) >= 2022
   AND carrier_first_attempted_delivery_tmstp_pacific > shipped_tmstp_pacific
   AND shipped_tmstp_pacific > order_tmstp_pacific
   AND CAST(carrier_first_attempted_delivery_tmstp_pacific AS DATE) <= CURRENT_DATE('PST8PDT')
  GROUP BY order_num,
   carrier_tracking_num,
   source_channel_code,
   order_tmstp_pacific,
   order_date_pacific) AS OL
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_sca_vws.day_cal_454_dim_vw AS ddd ON OL.delivery_date = ddd.day_date
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_sca_vws.day_cal_454_dim_vw AS aaa ON aaa.day_date = CURRENT_DATE('PST8PDT')
WHERE ddd.fiscal_year_num IN (2023, 2024)
 AND ddd.month_idnt < aaa.month_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS p50_fm
AS 
SELECT 
    'JWN' AS channel,
    fiscal_year,
    half_num,
    quarter_idnt AS quarter,
    month_idnt AS fm,
    CAST(CAST(TRUNC(1000 * PERCENTILE_DISC(CLICK_TO_DELIVERY, 0.5) OVER (PARTITION BY pre_p50.fiscal_year,pre_p50.half_num,pre_p50.qtd_idnt,pre_p50.month_idnt)) AS INT64) AS NUMERIC) / 1000 AS p50_fm
  FROM pre_p50;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS p50_fq
AS
SELECT 'JWN' AS channel,
 fiscal_year,
 half_num,
 quarter_idnt AS quarter,
 'All' AS fm,
CAST(CAST(TRUNC(1000 * PERCENTILE_DISC(CLICK_TO_DELIVERY, 0.5) OVER (PARTITION BY pre_p50.fiscal_year,pre_p50.half_num,pre_p50.qtd_idnt,pre_p50.month_idnt)) AS INT64) AS NUMERIC) / 1000 AS p50_fq
FROM pre_p50;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS p50_qtd
AS
SELECT 'JWN' AS channel,
 fiscal_year,
 half_num,
 qtd_idnt AS qtd,
 'All' AS fm,
CAST(CAST(TRUNC(1000 * PERCENTILE_DISC(CLICK_TO_DELIVERY, 0.5) OVER (PARTITION BY pre_p50.fiscal_year,pre_p50.half_num,pre_p50.qtd_idnt,pre_p50.month_idnt)) AS INT64) AS NUMERIC) / 1000 AS p50_fq
FROM pre_p50
WHERE qtd_idnt IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS p50_fh
AS
SELECT 'JWN' AS channel,
 fiscal_year,
 half_num,
 'All' AS quarter,
 'All' AS fm,
 CAST(CAST(TRUNC(1000 * PERCENTILE_DISC(CLICK_TO_DELIVERY, 0.5) OVER (PARTITION BY pre_p50.fiscal_year,pre_p50.half_num,pre_p50.qtd_idnt,pre_p50.month_idnt)) AS INT64) AS NUMERIC) / 1000 AS p50_fh
FROM pre_p50;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS p50_fy
AS
SELECT 'JWN' AS channel,
 fiscal_year,
 'All' AS quarter,
 'All' AS fm,
  CAST(CAST(TRUNC(1000 * PERCENTILE_DISC(CLICK_TO_DELIVERY, 0.5) OVER (PARTITION BY pre_p50.fiscal_year,pre_p50.half_num,pre_p50.qtd_idnt,pre_p50.month_idnt)) AS INT64) AS NUMERIC) / 1000 AS p50_fy
FROM pre_p50;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_sbrands
AS
SELECT CASE
  WHEN LOWER(ws.chnl_label) = LOWER('210, RACK STORES')
  THEN 'RACK'
  ELSE 'NRHL'
  END AS chnl,
 dc.month_num,
 dc.quarter_num,
 dc.halfyear_num AS half_num,
 dc.year_num,
  CASE
  WHEN dc.month_num IN (202401, 202402)
  THEN 'qtd_q1'
  WHEN dc.month_num IN (202404, 202405)
  THEN 'qtd_q2'
  WHEN dc.month_num IN (202407, 202408)
  THEN 'qtd_q3'
  WHEN dc.month_num IN (202410, 202411)
  THEN 'qtd_q4'
  ELSE NULL
  END AS qtd_num,
 SUM(CASE
   WHEN sb.brand IS NOT NULL
   THEN ws.sales_dollars
   ELSE 0
   END) AS sb_sales,
 SUM(ws.sales_dollars) AS total_sales_for_sb
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.wbr_supplier AS ws
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_mothership.ets_rsb_lookup AS sb ON LOWER(ws.brand) = LOWER(sb.brand)
 LEFT JOIN (SELECT DISTINCT week_num,
   month_num,
   quarter_num,
   halfyear_num,
   year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal) AS dc ON ws.week_idnt = dc.week_num
WHERE dc.month_num IN (SELECT month_num
   FROM month_range)
 AND dc.month_num < (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND LOWER(ws.chnl_label) IN (LOWER('210, RACK STORES'), LOWER('250, OFFPRICE ONLINE'))
GROUP BY chnl,
 dc.month_num,
 dc.quarter_num,
 half_num,
 dc.year_num,
 qtd_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sbrands_fm
AS
SELECT chnl,
 month_num,
 quarter_num,
 qtd_num,
 half_num,
 year_num,
 sb_sales,
 total_sales_for_sb,
  sb_sales / IF(total_sales_for_sb = 0, NULL, total_sales_for_sb) AS pct_sb_sales
FROM pre_sbrands
UNION ALL
SELECT CASE
  WHEN chnl IS NOT NULL
  THEN 'OP'
  ELSE NULL
  END AS chnl,
 month_num,
 quarter_num,
 qtd_num,
 half_num,
 year_num,
 SUM(sb_sales) AS sb_sales,
 SUM(total_sales_for_sb) AS total_sales_for_sb,
  SUM(sb_sales) / IF(SUM(total_sales_for_sb) = 0, NULL, SUM(total_sales_for_sb)) AS pct_sb_sales
FROM pre_sbrands
GROUP BY chnl,
 month_num,
 quarter_num,
 qtd_num,
 half_num,
 year_num
UNION ALL
SELECT CASE
  WHEN chnl IS NOT NULL
  THEN 'JWN'
  ELSE NULL
  END AS chnl,
 month_num,
 quarter_num,
 qtd_num,
 half_num,
 year_num,
 SUM(sb_sales) AS sb_sales,
 SUM(total_sales_for_sb) AS total_sales_for_sb,
  SUM(sb_sales) / IF(SUM(total_sales_for_sb) = 0, NULL, SUM(total_sales_for_sb)) AS pct_sb_sales
FROM pre_sbrands
GROUP BY chnl,
 month_num,
 quarter_num,
 qtd_num,
 half_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sbrands_fq
AS
SELECT chnl,
 quarter_num,
 half_num,
 year_num,
 SUM(sb_sales) AS sb_sales,
 SUM(total_sales_for_sb) AS total_sales_for_sb,
  SUM(sb_sales) / IF(SUM(total_sales_for_sb) = 0, NULL, SUM(total_sales_for_sb)) AS pct_sb_sales
FROM sbrands_fm
GROUP BY chnl,
 quarter_num,
 half_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sbrands_qtd
AS
SELECT chnl,
 qtd_num,
 half_num,
 year_num,
 SUM(sb_sales) AS sb_sales,
 SUM(total_sales_for_sb) AS total_sales_for_sb,
  SUM(sb_sales) / IF(SUM(total_sales_for_sb) = 0, NULL, SUM(total_sales_for_sb)) AS pct_sb_sales
FROM sbrands_fm
GROUP BY chnl,
 qtd_num,
 half_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sbrands_fh
AS
SELECT chnl,
 half_num,
 year_num,
 SUM(sb_sales) AS sb_sales,
 SUM(total_sales_for_sb) AS total_sales_for_sb,
  SUM(sb_sales) / IF(SUM(total_sales_for_sb) = 0, NULL, SUM(total_sales_for_sb)) AS pct_sb_sales
FROM sbrands_fq
GROUP BY chnl,
 half_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sbrands_fy
AS
SELECT chnl,
 year_num,
 SUM(sb_sales) AS sb_sales,
  SUM(sb_sales) / IF(SUM(total_sales_for_sb) = 0, NULL, SUM(total_sales_for_sb)) AS pct_sb_sales
FROM sbrands_fh
GROUP BY chnl,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_merch
AS
SELECT t7.banner,
 t7.week_num,
 t7.month_num,
 t7.qtd_num,
 t7.quarter_num,
 t7.halfyear_num,
 t7.year_num,
 t7.cad_conv,
 t7.merch_margin_topside,
  CASE
  WHEN t7.month_num < (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
    WHERE day_date = CURRENT_DATE('PST8PDT'))
  THEN 1
  ELSE 0
  END AS ptd_flag,
 SUM(CASE
   WHEN LOWER(t7.country) = LOWER('CA')
   THEN t7.ty_merch_margin_amt * t7.cad_conv
   ELSE t7.ty_merch_margin_amt
   END) AS ty_merch_margin_retail_amt,
 SUM(CASE
   WHEN LOWER(t7.country) = LOWER('CA')
   THEN t7.ty_net_sales_retail_amt * t7.cad_conv
   ELSE t7.ty_net_sales_retail_amt
   END) AS ty_net_sales_retail_amt,
 SUM(CASE
   WHEN LOWER(t7.fulfill_type_desc) <> LOWER('DROPSHIP') AND LOWER(t7.country) = LOWER('CA')
   THEN t7.ty_net_sales_cost_amt * 0.745101
   WHEN LOWER(t7.fulfill_type_desc) <> LOWER('DROPSHIP')
   THEN t7.ty_net_sales_cost_amt
   ELSE 0
   END) AS ty_net_sales_cost_amt_no_ds,
 SUM(CASE
   WHEN LOWER(t7.country) = LOWER('CA')
   THEN t7.ty_eop_tot_cost_amt * t7.cad_conv
   ELSE t7.ty_eop_tot_cost_amt
   END) AS ty_eop_tot_cost_amt,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA')
   THEN t7.ty_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1
   THEN t7.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_m,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA') AND t7.month_454_num IN (1, 4, 7, 10)
   THEN t7.ty_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1 AND t7.month_454_num IN (1, 4, 7, 10)
   THEN t7.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_q,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA') AND t7.month_454_num IN (1, 7)
   THEN t7.ty_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1 AND t7.month_454_num IN (1, 7)
   THEN t7.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_h,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA') AND t7.month_454_num IN (1)
   THEN t7.ty_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1 AND t7.month_454_num IN (1)
   THEN t7.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_y,
 SUM(CASE
   WHEN LOWER(t7.country) = LOWER('CA')
   THEN t7.op_merch_margin_amt * t7.cad_conv
   ELSE t7.op_merch_margin_amt
   END) AS op_merch_margin_retail_amt,
 SUM(CASE
   WHEN LOWER(t7.country) = LOWER('CA')
   THEN t7.op_net_sales_retail_amt * t7.cad_conv
   ELSE t7.op_net_sales_retail_amt
   END) AS op_net_sales_retail_amt,
 SUM(CASE
   WHEN LOWER(t7.fulfill_type_desc) <> LOWER('DROPSHIP') AND LOWER(t7.country) = LOWER('CA')
   THEN t7.op_net_sales_cost_amt * t7.cad_conv
   WHEN LOWER(t7.fulfill_type_desc) <> LOWER('DROPSHIP')
   THEN t7.op_net_sales_cost_amt
   ELSE 0
   END) AS op_net_sales_cost_amt_no_ds,
 SUM(CASE
   WHEN LOWER(t7.country) = LOWER('CA')
   THEN t7.op_eop_tot_cost_amt * t7.cad_conv
   ELSE t7.op_eop_tot_cost_amt
   END) AS op_eop_tot_cost_amt,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA')
   THEN t7.op_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1
   THEN t7.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_m,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA') AND t7.month_454_num IN (1, 4, 7, 10)
   THEN t7.op_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1 AND t7.month_454_num IN (1, 4, 7, 10)
   THEN t7.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_q,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA') AND t7.month_454_num IN (1, 7)
   THEN t7.op_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1 AND t7.month_454_num IN (1, 7)
   THEN t7.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_h,
 SUM(CASE
   WHEN t7.week_454_num = 1 AND LOWER(t7.country) = LOWER('CA') AND t7.month_454_num IN (1)
   THEN t7.op_bop_tot_cost_amt * t7.cad_conv
   WHEN t7.week_454_num = 1 AND t7.month_454_num IN (1)
   THEN t7.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_y
FROM (SELECT 'JWN' AS banner,
   dc.week_num,
   dc.month_num,
    CASE
    WHEN dc.month_num IN (202401, 202402)
    THEN 'qtd_q1'
    WHEN dc.month_num IN (202404, 202405)
    THEN 'qtd_q2'
    WHEN dc.month_num IN (202407, 202408)
    THEN 'qtd_q3'
    WHEN dc.month_num IN (202410, 202411)
    THEN 'qtd_q4'
    ELSE NULL
    END AS qtd_num,
   dc.quarter_num,
   dc.halfyear_num,
   dc.year_num,
    CASE
    WHEN dc.month_num = 202201
    THEN 0.785546
    WHEN dc.month_num = 202202
    THEN 0.790326
    WHEN dc.month_num = 202203
    THEN 0.791828
    WHEN dc.month_num = 202204
    THEN 0.777424
    WHEN dc.month_num = 202205
    THEN 0.781372
    WHEN dc.month_num = 202206
    THEN 0.772917
    WHEN dc.month_num = 202207
    THEN 0.775675
    WHEN dc.month_num = 202208
    THEN 0.753012
    WHEN dc.month_num = 202209
    THEN 0.728916
    WHEN dc.month_num = 202210
    THEN 0.743329
    WHEN dc.month_num = 202211
    THEN 0.735565
    WHEN dc.month_num = 202212
    THEN 0.744048
    WHEN dc.month_num = 202301
    THEN 0.745101
    ELSE NULL
    END AS cad_conv,
    CASE
    WHEN dc.week_454_num = 1 AND dc.month_num = 202301
    THEN 34912771
    WHEN dc.week_454_num = 1 AND dc.month_num = 202302
    THEN 9070807
    WHEN dc.week_454_num = 1 AND dc.month_num = 202303
    THEN 1763254
    WHEN dc.week_454_num = 1 AND dc.month_num = 202304
    THEN 745788
    WHEN dc.week_454_num = 1 AND dc.month_num = 202305
    THEN - 4141405
    WHEN dc.week_454_num = 1 AND dc.month_num = 202306
    THEN - 1752272
    ELSE 0
    END AS merch_margin_topside,
    CASE
    WHEN dc.month_num < (SELECT DISTINCT month_num
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
      WHERE day_date = CURRENT_DATE('PST8PDT'))
    THEN 1
    ELSE 0
    END AS ptd_flag,
   mfp.country,
   mfp.ty_merch_margin_amt,
    CASE
    WHEN dc.month_num = 202201
    THEN 0.785546
    WHEN dc.month_num = 202202
    THEN 0.790326
    WHEN dc.month_num = 202203
    THEN 0.791828
    WHEN dc.month_num = 202204
    THEN 0.777424
    WHEN dc.month_num = 202205
    THEN 0.781372
    WHEN dc.month_num = 202206
    THEN 0.772917
    WHEN dc.month_num = 202207
    THEN 0.775675
    WHEN dc.month_num = 202208
    THEN 0.753012
    WHEN dc.month_num = 202209
    THEN 0.728916
    WHEN dc.month_num = 202210
    THEN 0.743329
    WHEN dc.month_num = 202211
    THEN 0.735565
    WHEN dc.month_num = 202212
    THEN 0.744048
    WHEN dc.month_num = 202301
    THEN 0.745101
    ELSE NULL
    END,
   mfp.ty_net_sales_retail_amt,
   mfp.ty_net_sales_cost_amt,
   mfp.fulfill_type_desc,
   mfp.ty_eop_tot_cost_amt,
   mfp.ty_bop_tot_cost_amt,
   dc.week_454_num,
   dc.month_454_num,
   mfp.op_merch_margin_amt,
   mfp.op_net_sales_retail_amt,
   mfp.op_net_sales_cost_amt,
   mfp.op_eop_tot_cost_amt,
   mfp.op_bop_tot_cost_amt
  FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.mfp_banner_country_channel_stg AS mfp
   LEFT JOIN (SELECT DISTINCT week_454_num,
     month_454_num,
     week_num,
     month_num,
     quarter_num,
     halfyear_num,
     year_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal) AS dc ON mfp.week_num = dc.week_num
  WHERE (LOWER(mfp.country) = LOWER('US') OR LOWER(mfp.country) = LOWER('CA') AND mfp.week_num <= 202304)
   AND dc.month_num IN (SELECT month_num
     FROM month_range
     WHERE dc.year_num >= 2023)
   AND LOWER(mfp.division) <> LOWER('600, ALTERNATE MODELS')) AS t7
GROUP BY t7.banner,
 t7.week_num,
 t7.month_num,
 t7.qtd_num,
 t7.quarter_num,
 t7.halfyear_num,
 t7.year_num,
 t7.cad_conv,
 t7.merch_margin_topside
UNION ALL
SELECT CASE
  WHEN LOWER(mfp0.banner) = LOWER('NORDSTROM RACK')
  THEN 'OP'
  WHEN LOWER(mfp0.banner) = LOWER('NORDSTROM')
  THEN 'FP'
  ELSE NULL
  END AS banner,
 dc.week_num,
 dc.month_num,
  CASE
  WHEN dc.month_num IN (202401, 202402)
  THEN 'qtd_q1'
  WHEN dc.month_num IN (202404, 202405)
  THEN 'qtd_q2'
  WHEN dc.month_num IN (202407, 202408)
  THEN 'qtd_q3'
  WHEN dc.month_num IN (202410, 202411)
  THEN 'qtd_q4'
  ELSE NULL
  END AS qtd_num,
 dc.quarter_num,
 dc.halfyear_num,
 dc.year_num,
  CASE
  WHEN dc.month_num = 202301
  THEN 0.745101
  ELSE NULL
  END AS cad_conv,
  CASE
  WHEN dc.week_454_num = 1 AND dc.month_num = 202301
  THEN 34912771
  WHEN dc.week_454_num = 1 AND dc.month_num = 202302
  THEN 9070807
  WHEN dc.week_454_num = 1 AND dc.month_num = 202303
  THEN 1763254
  WHEN dc.week_454_num = 1 AND dc.month_num = 202304
  THEN 745788
  WHEN dc.week_454_num = 1 AND dc.month_num = 202305
  THEN - 4141405
  WHEN dc.week_454_num = 1 AND dc.month_num = 202306
  THEN - 1752272
  ELSE 0
  END AS merch_margin_topside,
  CASE
  WHEN dc.month_num < (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
    WHERE day_date = CURRENT_DATE('PST8PDT'))
  THEN 1
  ELSE 0
  END AS ptd_flag,
 SUM(CASE
   WHEN LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.ty_merch_margin_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   ELSE mfp0.ty_merch_margin_amt
   END) AS ty_merch_margin_retail_amt,
 SUM(CASE
   WHEN LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.ty_net_sales_retail_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   ELSE mfp0.ty_net_sales_retail_amt
   END) AS ty_net_sales_retail_amt,
 SUM(CASE
   WHEN LOWER(mfp0.fulfill_type_desc) <> LOWER('DROPSHIP') AND LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.ty_net_sales_cost_amt * 0.745101
   WHEN LOWER(mfp0.fulfill_type_desc) <> LOWER('DROPSHIP')
   THEN mfp0.ty_net_sales_cost_amt
   ELSE 0
   END) AS ty_net_sales_cost_amt_no_ds,
 SUM(CASE
   WHEN LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.ty_eop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   ELSE mfp0.ty_eop_tot_cost_amt
   END) AS ty_eop_tot_cost_amt,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.ty_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1
   THEN mfp0.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_m,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA') AND dc.month_454_num IN (1, 4, 7, 10)
   THEN mfp0.ty_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1 AND dc.month_454_num IN (1, 4, 7, 10)
   THEN mfp0.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_q,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA') AND dc.month_454_num IN (1, 7)
   THEN mfp0.ty_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1 AND dc.month_454_num IN (1, 7)
   THEN mfp0.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_h,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA') AND dc.month_454_num IN (1)
   THEN mfp0.ty_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1 AND dc.month_454_num IN (1)
   THEN mfp0.ty_bop_tot_cost_amt
   ELSE 0
   END) AS ty_bop_tot_cost_amt_y,
 SUM(CASE
   WHEN LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.op_merch_margin_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   ELSE mfp0.op_merch_margin_amt
   END) AS op_merch_margin_retail_amt,
 SUM(CASE
   WHEN LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.op_net_sales_retail_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   ELSE mfp0.op_net_sales_retail_amt
   END) AS op_net_sales_retail_amt,
 SUM(CASE
   WHEN LOWER(mfp0.fulfill_type_desc) <> LOWER('DROPSHIP') AND LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.op_net_sales_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN LOWER(mfp0.fulfill_type_desc) <> LOWER('DROPSHIP')
   THEN mfp0.op_net_sales_cost_amt
   ELSE 0
   END) AS op_net_sales_cost_amt_no_ds,
 SUM(CASE
   WHEN LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.op_eop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   ELSE mfp0.op_eop_tot_cost_amt
   END) AS op_eop_tot_cost_amt,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA')
   THEN mfp0.op_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1
   THEN mfp0.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_m,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA') AND dc.month_454_num IN (1, 4, 7, 10)
   THEN mfp0.op_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1 AND dc.month_454_num IN (1, 4, 7, 10)
   THEN mfp0.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_q,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA') AND dc.month_454_num IN (1, 7)
   THEN mfp0.op_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1 AND dc.month_454_num IN (1, 7)
   THEN mfp0.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_h,
 SUM(CASE
   WHEN dc.week_454_num = 1 AND LOWER(mfp0.country) = LOWER('CA') AND dc.month_454_num IN (1)
   THEN mfp0.op_bop_tot_cost_amt * CASE
     WHEN dc.month_num = 202301
     THEN 0.745101
     ELSE NULL
     END
   WHEN dc.week_454_num = 1 AND dc.month_454_num IN (1)
   THEN mfp0.op_bop_tot_cost_amt
   ELSE 0
   END) AS op_bop_tot_cost_amt_y
FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.mfp_banner_country_channel_stg AS mfp0
 LEFT JOIN (SELECT DISTINCT week_454_num,
   month_454_num,
   week_num,
   month_num,
   quarter_num,
   halfyear_num,
   year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal) AS dc ON mfp0.week_num = dc.week_num
WHERE (LOWER(mfp0.country) = LOWER('US') OR LOWER(mfp0.country) = LOWER('CA') AND mfp0.week_num <= 202304)
 AND dc.month_num IN (SELECT month_num
   FROM month_range
   WHERE dc.year_num >= 2023)
 AND LOWER(mfp0.division) <> LOWER('600, ALTERNATE MODELS')
GROUP BY banner,
 dc.week_num,
 dc.month_num,
 qtd_num,
 dc.quarter_num,
 dc.halfyear_num,
 dc.year_num,
 cad_conv,
 merch_margin_topside;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_fm
AS
SELECT banner,
 month_num,
 quarter_num,
 halfyear_num,
 year_num,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_merch_margin_retail_amt + merch_margin_topside
     ELSE 0
     END) / NULLIF(SUM(ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_m) / (COUNT(ty_eop_tot_cost_amt) + 1), 0), 0) AS mmroi
 ,
  SUM(CASE
    WHEN ptd_flag = 1
    THEN ty_merch_margin_retail_amt + merch_margin_topside
    ELSE 0
    END) / NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_retail_amt
     ELSE 0
     END), 0) AS merch_margin,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_cost_amt_no_ds
     ELSE 0
     END) / NULLIF(SUM(ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_m) / (COUNT(ty_eop_tot_cost_amt) + 1), 0), 0) AS turn,
  SUM(op_net_sales_cost_amt_no_ds) / NULLIF(SUM(op_eop_tot_cost_amt + op_bop_tot_cost_amt_m) / (COUNT(op_eop_tot_cost_amt
       ) + 1), 0) AS turn_plan
FROM pre_merch
GROUP BY banner,
 month_num,
 quarter_num,
 halfyear_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_fq
AS
SELECT banner,
 quarter_num,
 halfyear_num,
 year_num,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_merch_margin_retail_amt + merch_margin_topside
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_q
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS mmroi,
  SUM(CASE
    WHEN ptd_flag = 1
    THEN ty_merch_margin_retail_amt + merch_margin_topside
    ELSE 0
    END) / NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_retail_amt
     ELSE 0
     END), 0) AS merch_margin,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_cost_amt_no_ds
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_q
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS turn,
  SUM(op_net_sales_cost_amt_no_ds) / NULLIF(SUM(op_eop_tot_cost_amt + op_bop_tot_cost_amt_q) / (COUNT(op_eop_tot_cost_amt
       ) + 1), 0) AS turn_plan
FROM pre_merch
GROUP BY banner,
 quarter_num,
 halfyear_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_qtd
AS
SELECT banner,
 qtd_num,
 halfyear_num,
 year_num,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_merch_margin_retail_amt + merch_margin_topside
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_q
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS mmroi,
  SUM(CASE
    WHEN ptd_flag = 1
    THEN ty_merch_margin_retail_amt + merch_margin_topside
    ELSE 0
    END) / NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_retail_amt
     ELSE 0
     END), 0) AS merch_margin,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_cost_amt_no_ds
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_q
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS turn,
  SUM(op_net_sales_cost_amt_no_ds) / NULLIF(SUM(op_eop_tot_cost_amt + op_bop_tot_cost_amt_q) / (COUNT(op_eop_tot_cost_amt
       ) + 1), 0) AS turn_plan
FROM pre_merch
WHERE qtd_num IS NOT NULL
GROUP BY banner,
 qtd_num,
 halfyear_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_fh
AS
SELECT banner,
 halfyear_num AS half_num,
 year_num,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_merch_margin_retail_amt + merch_margin_topside
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_h
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS mmroi,
  SUM(CASE
    WHEN ptd_flag = 1
    THEN ty_merch_margin_retail_amt + merch_margin_topside
    ELSE 0
    END) / NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_retail_amt
     ELSE 0
     END), 0) AS merch_margin,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_cost_amt_no_ds
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_h
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS turn,
  SUM(op_net_sales_cost_amt_no_ds) / NULLIF(SUM(op_eop_tot_cost_amt + op_bop_tot_cost_amt_h) / (COUNT(op_eop_tot_cost_amt
       ) + 1), 0) AS turn_plan
FROM pre_merch
GROUP BY banner,
 half_num,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_fy
AS
SELECT banner,
 year_num,
  CASE
  WHEN year_num = (SELECT MAX(fiscal_year_num) - 1
    FROM month_range)
  THEN 'ly'
  ELSE 'ytd'
  END AS period_type,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_merch_margin_retail_amt + merch_margin_topside
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_y
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS mmroi,
  SUM(CASE
    WHEN ptd_flag = 1
    THEN ty_merch_margin_retail_amt + merch_margin_topside
    ELSE 0
    END) / NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_retail_amt
     ELSE 0
     END), 0) AS merch_margin,
 NULLIF(SUM(CASE
     WHEN ptd_flag = 1
     THEN ty_net_sales_cost_amt_no_ds
     ELSE 0
     END) / NULLIF(SUM(CASE
       WHEN ptd_flag = 1
       THEN ty_eop_tot_cost_amt + ty_bop_tot_cost_amt_y
       ELSE NULL
       END) / (COUNT(CASE
         WHEN ptd_flag = 1
         THEN ty_eop_tot_cost_amt
         ELSE NULL
         END) + 1), 0), 0) AS turn,
  SUM(op_net_sales_cost_amt_no_ds) / NULLIF(SUM(op_eop_tot_cost_amt + op_bop_tot_cost_amt_y) / (COUNT(op_eop_tot_cost_amt
       ) + 1), 0) AS turn_plan
FROM pre_merch
WHERE ptd_flag = 1
GROUP BY banner,
 year_num,
 period_type
UNION ALL
SELECT banner,
 year_num,
 'ty' AS period_type,
 CAST(NULL AS BIGNUMERIC) AS mmroi,
 CAST(NULL AS BIGNUMERIC) AS merch_margin,
 CAST(NULL AS BIGNUMERIC) AS turn,
  SUM(op_net_sales_cost_amt_no_ds) / NULLIF(SUM(op_eop_tot_cost_amt + op_bop_tot_cost_amt_y) / (COUNT(op_eop_tot_cost_amt
       ) + 1), 0) AS turn_plan
FROM pre_merch
WHERE year_num = (SELECT MAX(fiscal_year_num)
   FROM month_range)
GROUP BY banner,
 year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt
AS
SELECT mr.month_num,
 mr.quarter_idnt,
  CASE
  WHEN mr.month_num IN (202401, 202402)
  THEN 'qtd_q1'
  WHEN mr.month_num IN (202404, 202405)
  THEN 'qtd_q2'
  WHEN mr.month_num IN (202407, 202408)
  THEN 'qtd_q3'
  WHEN mr.month_num IN (202410, 202411)
  THEN 'qtd_q4'
  ELSE NULL
  END AS qtd_num,
 rcpt.half_num,
 mr.fiscal_year_num,
 'JWN' AS banner,
 SUM(CASE
   WHEN LOWER(sd.store_type_code) IN (LOWER('FL'), LOWER('RK'))
   THEN rcpt.receipts_po_units_ty
   ELSE NULL
   END) AS rcpts_store,
 SUM(CASE
   WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('OC'))
   THEN rcpt.receipts_po_units_ty
   ELSE NULL
   END) AS rcpts_fc,
 SUM(CASE
   WHEN LOWER(sd.store_type_code) IN (LOWER('FC'), LOWER('OF'), LOWER('OC'), LOWER('FL'), LOWER('RK'))
   THEN rcpt.receipts_po_units_ty
   ELSE NULL
   END) AS rcpts_total
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_transaction_sbclass_store_week_agg_fact_vw AS rcpt
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON rcpt.store_num = sd.store_num
 INNER JOIN month_range AS mr ON rcpt.month_num = mr.month_num
WHERE LOWER(rcpt.store_country_code) = LOWER('US')
GROUP BY mr.month_num,
 mr.quarter_idnt,
 qtd_num,
 rcpt.half_num,
 mr.fiscal_year_num
UNION ALL
SELECT mr0.month_num,
 mr0.quarter_idnt,
  CASE
  WHEN mr0.month_num IN (202401, 202402)
  THEN 'qtd_q1'
  WHEN mr0.month_num IN (202404, 202405)
  THEN 'qtd_q2'
  WHEN mr0.month_num IN (202407, 202408)
  THEN 'qtd_q3'
  WHEN mr0.month_num IN (202410, 202411)
  THEN 'qtd_q4'
  ELSE NULL
  END AS qtd_num,
 mr0.fiscal_halfyear_num AS half_num,
 mr0.fiscal_year_num,
 'JWN' AS banner,
 CAST(NULL AS INTEGER) AS rcpts_store,
 CAST(NULL AS INTEGER) AS rcpts_fc,
 CAST(NULL AS INTEGER) AS rcpts_total
FROM `{{params.gcp_project_id}}`.t2dl_das_apt.dept_chnl_split_intentional AS rcpt0
 INNER JOIN month_range AS mr0 ON rcpt0.mth_idnt = mr0.month_num
WHERE rcpt0.mth_idnt > (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
 AND rcpt0.chnl_idnt IN (110, 210, 120, 250)
GROUP BY mr0.month_num,
 mr0.quarter_idnt,
 qtd_num,
 half_num,
 mr0.fiscal_year_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt_plan
AS
SELECT mr.month_num,
 mr.quarter_idnt,
  CASE
  WHEN mr.month_num IN (202401, 202402)
  THEN 'qtd_q1'
  WHEN mr.month_num IN (202404, 202405)
  THEN 'qtd_q2'
  WHEN mr.month_num IN (202407, 202408)
  THEN 'qtd_q3'
  WHEN mr.month_num IN (202410, 202411)
  THEN 'qtd_q4'
  ELSE NULL
  END AS qtd_num,
 mr.fiscal_halfyear_num AS half_num,
 mr.fiscal_year_num,
 'JWN' AS banner,
 CAST(SUM(CASE
    WHEN rcpt.chnl_idnt IN (110, 210)
    THEN rcpt.plan_rcpt_u
    ELSE NULL
    END) AS NUMERIC) AS rcpts_store_plan,
 CAST(SUM(CASE
    WHEN rcpt.chnl_idnt IN (110, 210)
    THEN rcpt.plan_rcpt_u
    ELSE NULL
    END) AS NUMERIC) AS rcpts_fc_plan,
 CAST(SUM(rcpt.plan_rcpt_u) AS NUMERIC) AS rcpts_total_plan
FROM `{{params.gcp_project_id}}`.t2dl_das_apt.dept_chnl_split_intentional AS rcpt
 INNER JOIN month_range AS mr ON rcpt.mth_idnt = mr.month_num
WHERE rcpt.mth_idnt = rcpt.plan_cycle
 OR rcpt.mth_idnt >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
    WHERE day_date = CURRENT_DATE('PST8PDT')) AND rcpt.chnl_idnt IN (110, 210, 120, 250)
GROUP BY mr.month_num,
 mr.quarter_idnt,
 qtd_num,
 half_num,
 mr.fiscal_year_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt_fm
AS
SELECT rcpt.month_num,
 'JWN' AS banner,
  rcpt.rcpts_fc / CAST(rcpt.rcpts_total AS NUMERIC) AS rcpt_split_fc,
  rcpt.rcpts_store / CAST(rcpt.rcpts_total AS NUMERIC) AS rcpt_split_store,
  rcpt_plan.rcpts_fc_plan / CAST(rcpt_plan.rcpts_total_plan AS NUMERIC) AS rcpt_split_fc_plan,
  rcpt_plan.rcpts_store_plan / CAST(rcpt_plan.rcpts_total_plan AS NUMERIC) AS rcpt_split_store_plan
FROM rcpt
 LEFT JOIN rcpt_plan ON rcpt.month_num = rcpt_plan.month_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt_fq
AS
SELECT rcpt.quarter_idnt,
 'JWN' AS banner,
  SUM(rcpt.rcpts_fc) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_fc,
  SUM(rcpt.rcpts_store) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_store,
  SUM(rcpt_plan.rcpts_fc_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_fc_plan,
  SUM(rcpt_plan.rcpts_store_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_store_plan
FROM rcpt
 LEFT JOIN rcpt_plan ON rcpt.month_num = rcpt_plan.month_num
GROUP BY rcpt.quarter_idnt,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt_qtd
AS
SELECT rcpt.qtd_num,
 'JWN' AS banner,
  SUM(rcpt.rcpts_fc) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_fc,
  SUM(rcpt.rcpts_store) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_store,
  SUM(rcpt_plan.rcpts_fc_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_fc_plan,
  SUM(rcpt_plan.rcpts_store_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_store_plan
FROM rcpt
 LEFT JOIN rcpt_plan ON rcpt.month_num = rcpt_plan.month_num
GROUP BY rcpt.qtd_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt_fh
AS
SELECT rcpt.half_num,
 'JWN' AS banner,
  SUM(rcpt.rcpts_fc) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_fc,
  SUM(rcpt.rcpts_store) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_store,
  SUM(rcpt_plan.rcpts_fc_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_fc_plan,
  SUM(rcpt_plan.rcpts_store_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_store_plan
FROM rcpt
 LEFT JOIN rcpt_plan ON rcpt.month_num = rcpt_plan.month_num
GROUP BY rcpt.half_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt_fy
AS
SELECT rcpt.fiscal_year_num,
 'JWN' AS banner,
  CASE
  WHEN rcpt.fiscal_year_num = (SELECT MAX(fiscal_year_num) - 1
    FROM month_range)
  THEN 'ly'
  ELSE 'ytd'
  END AS period_type,
  SUM(rcpt.rcpts_fc) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_fc,
  SUM(rcpt.rcpts_store) / CAST(SUM(rcpt.rcpts_total) AS NUMERIC) AS rcpt_split_store,
  SUM(rcpt_plan.rcpts_fc_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_fc_plan,
  SUM(rcpt_plan.rcpts_store_plan) / CAST(SUM(rcpt_plan.rcpts_total_plan) AS NUMERIC) AS rcpt_split_store_plan
FROM rcpt
 LEFT JOIN rcpt_plan ON rcpt.month_num = rcpt_plan.month_num
WHERE rcpt.month_num < (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
GROUP BY rcpt.fiscal_year_num,
 banner,
 period_type
UNION ALL
SELECT rcpt0.fiscal_year_num,
 'JWN' AS banner,
 'ty' AS period_type,
 CAST(NULL AS BIGNUMERIC) AS rcpt_split_fc,
 CAST(NULL AS BIGNUMERIC) AS rcpt_split_store,
  SUM(rcpt_plan0.rcpts_fc_plan) / CAST(SUM(rcpt_plan0.rcpts_total_plan) AS NUMERIC) AS rcpt_split_fc_plan,
  SUM(rcpt_plan0.rcpts_store_plan) / CAST(SUM(rcpt_plan0.rcpts_total_plan) AS NUMERIC) AS rcpt_split_store_plan
FROM rcpt AS rcpt0
 LEFT JOIN rcpt_plan AS rcpt_plan0 ON rcpt0.month_num = rcpt_plan0.month_num
WHERE rcpt0.fiscal_year_num = (SELECT MAX(fiscal_year_num)
   FROM month_range)
GROUP BY rcpt0.fiscal_year_num,
 banner,
 period_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
------------------- Merch Net Sales Addition -------------------
CREATE TEMPORARY TABLE IF NOT EXISTS pre_merch_net_sales
AS
SELECT mr.month_num,
 mr.quarter_idnt,
  CASE
  WHEN mr.month_num IN (202401, 202402)
  THEN 'qtd_q1'
  WHEN mr.month_num IN (202404, 202405)
  THEN 'qtd_q2'
  WHEN mr.month_num IN (202407, 202408)
  THEN 'qtd_q3'
  WHEN mr.month_num IN (202410, 202411)
  THEN 'qtd_q4'
  ELSE NULL
  END AS qtd_num,
 mr.fiscal_halfyear_num AS half_num,
 mr.fiscal_year_num,
 'JWN' AS banner,
 SUM(mfp.ty_net_sales_retail_amt) AS merch_net_sales,
 SUM(mfp.op_net_sales_retail_amt) AS merch_net_sales_plan
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_channel_fact AS mfp
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.org_channel_dim AS ocd ON mfp.channel_num = ocd.channel_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.department_dim AS dd ON mfp.dept_num = dd.dept_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS mwc ON mfp.week_num = mwc.week_idnt
 INNER JOIN month_range AS mr ON mwc.month_idnt = mr.month_num
WHERE ocd.banner_country_num IN (1, 3)
 AND dd.division_num IN (10, 12, 14, 20, 30, 40, 45, 51, 53, 54, 55, 60, 70, 90, 91, 92, 93, 94, 95, 96, 97, 99, 100,
   120, 200, 310, 340, 345, 351, 360, 365, 444, 700, 900, 980, 990)
 AND mr.month_num IN (SELECT DISTINCT month_num
   FROM month_range)
GROUP BY mr.month_num,
 mr.quarter_idnt,
 qtd_num,
 half_num,
 mr.fiscal_year_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_net_sales_fm
AS
SELECT month_num,
 banner,
 merch_net_sales,
 merch_net_sales_plan
FROM pre_merch_net_sales AS pmns;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_net_sales_fq
AS
SELECT quarter_idnt,
 banner,
 SUM(merch_net_sales) AS merch_net_sales,
 SUM(merch_net_sales_plan) AS merch_net_sales_plan
FROM pre_merch_net_sales AS pmns
GROUP BY quarter_idnt,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_net_sales_qtd
AS
SELECT qtd_num,
 banner,
 SUM(merch_net_sales) AS merch_net_sales,
 SUM(merch_net_sales_plan) AS merch_net_sales_plan
FROM pre_merch_net_sales AS pmns
GROUP BY qtd_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_net_sales_fh
AS
SELECT half_num,
 banner,
 SUM(merch_net_sales) AS merch_net_sales,
 SUM(merch_net_sales_plan) AS merch_net_sales_plan
FROM pre_merch_net_sales AS pmns
GROUP BY half_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS merch_net_sales_fy
AS
SELECT fiscal_year_num,
 banner,
  CASE
  WHEN fiscal_year_num = (SELECT MAX(fiscal_year_num) - 1
    FROM month_range)
  THEN 'ly'
  ELSE 'ytd'
  END AS period_type,
 SUM(merch_net_sales) AS merch_net_sales,
 SUM(merch_net_sales_plan) AS merch_net_sales_plan
FROM pre_merch_net_sales AS pmns
WHERE month_num < (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
   WHERE day_date = CURRENT_DATE('PST8PDT'))
GROUP BY fiscal_year_num,
 banner,
 period_type
UNION ALL
SELECT fiscal_year_num,
 banner,
 'ty' AS period_type,
 SUM(merch_net_sales) AS merch_net_sales,
 SUM(merch_net_sales_plan) AS merch_net_sales_plan
FROM pre_merch_net_sales AS pmns
WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num)
   FROM month_range)
GROUP BY fiscal_year_num,
 banner,
 period_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
-- eop is a snapshot metric, so data is measured as the last weekly value of a given time period
-- for example, Q3 EOP Units is defined as EOP units during the last week of Q3
CREATE TEMPORARY TABLE IF NOT EXISTS pre_eop
AS
SELECT t8.banner,
 t8.month_num,
 t8.fiscal_month_num,
 t8.month_end_week_idnt,
 t8.qtd_num,
 t8.week_num0 AS week_num,
 t8.quarter_num,
 t8.fiscal_quarter_num,
 t8.quarter_end_week_idnt,
 t8.halfyear_num,
 t8.fiscal_year_num,
 t8.ptd_flag,
 t8.eop_inv_cost,
 t8.eop_inv_units,
 t8.eop_inv_cost_plan,
 t8.eop_inv_units_plan
FROM (SELECT 'JWN' AS banner,
   dc.month_idnt AS month_num,
   dc.fiscal_month_num,
   dc.month_end_week_idnt,
    CASE
    WHEN dc.month_idnt IN (202402)
    THEN 'qtd_q1'
    WHEN dc.month_idnt IN (202405)
    THEN 'qtd_q2'
    WHEN dc.month_idnt IN (202408)
    THEN 'qtd_q3'
    WHEN dc.month_idnt IN (202411)
    THEN 'qtd_q4'
    ELSE NULL
    END AS qtd_num,
   dc.quarter_idnt AS quarter_num,
   dc.fiscal_quarter_num,
   dc.quarter_end_week_idnt,
   dc.fiscal_halfyear_num AS halfyear_num,
   dc.fiscal_year_num,
    CASE
    WHEN dc.month_idnt < (SELECT DISTINCT month_num
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
      WHERE day_date = CURRENT_DATE('PST8PDT'))
    THEN 1
    ELSE 0
    END AS ptd_flag,
   dc.week_idnt AS week_num0,
   SUM(CASE
     WHEN mfp.week_num = dc.month_end_week_idnt
     THEN mfp.ty_eop_tot_cost_amt
     ELSE 0
     END) AS eop_inv_cost,
   SUM(CASE
     WHEN mfp.week_num = dc.month_end_week_idnt
     THEN CAST(mfp.ty_eop_tot_units AS INTEGER)
     ELSE 0
     END) AS eop_inv_units,
   SUM(CASE
     WHEN mfp.week_num = dc.month_end_week_idnt
     THEN mfp.op_eop_tot_cost_amt
     ELSE 0
     END) AS eop_inv_cost_plan,
   SUM(CASE
     WHEN mfp.week_num = dc.month_end_week_idnt
     THEN CAST(mfp.op_eop_tot_units AS INTEGER)
     ELSE 0
     END) AS eop_inv_units_plan
  FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.mfp_banner_country_channel_stg AS mfp
   LEFT JOIN (SELECT DISTINCT fiscal_week_num,
     fiscal_month_num,
     month_end_week_idnt,
     week_idnt,
     month_idnt,
     quarter_idnt,
     fiscal_quarter_num,
     quarter_end_week_idnt,
     fiscal_halfyear_num,
     fiscal_year_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS dc ON mfp.week_num = dc.week_idnt
  WHERE (LOWER(mfp.country) = LOWER('US') OR LOWER(mfp.country) = LOWER('CA') AND mfp.week_num <= 202304)
   AND dc.month_idnt IN (SELECT month_num
     FROM month_range
     WHERE fiscal_year_num >= 2023)
   AND LOWER(mfp.division) <> LOWER('600, ALTERNATE MODELS')
  GROUP BY banner,
   month_num,
   dc.fiscal_month_num,
   dc.month_end_week_idnt,
   qtd_num,
   quarter_num,
   dc.fiscal_quarter_num,
   dc.quarter_end_week_idnt,
   halfyear_num,
   dc.fiscal_year_num,
   ptd_flag,
   week_num0) AS t8;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS eop_fm
AS
SELECT month_num,
 banner,
 SUM(eop_inv_cost) AS eop_inv_cost,
 SUM(eop_inv_units) AS eop_inv_units,
 SUM(eop_inv_cost_plan) AS eop_inv_cost_plan,
 SUM(eop_inv_units_plan) AS eop_inv_units_plan
FROM pre_eop AS pe
GROUP BY month_num,
 banner;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS eop_qtd
AS
SELECT banner,
 qtd_num,
 SUM(CASE
   WHEN week_num = month_end_week_idnt AND qtd_num IS NOT NULL
   THEN eop_inv_cost
   ELSE 0
   END) AS eop_inv_cost,
 SUM(CASE
   WHEN week_num = month_end_week_idnt AND qtd_num IS NOT NULL
   THEN eop_inv_units
   ELSE 0
   END) AS eop_inv_units,
 SUM(CASE
   WHEN week_num = month_end_week_idnt AND qtd_num IS NOT NULL
   THEN eop_inv_cost_plan
   ELSE 0
   END) AS eop_inv_cost_plan,
 SUM(CASE
   WHEN week_num = month_end_week_idnt AND qtd_num IS NOT NULL
   THEN eop_inv_units_plan
   ELSE 0
   END) AS eop_inv_units_plan
FROM pre_eop AS pe
GROUP BY banner,
 qtd_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS eop_fq
AS
SELECT banner,
 quarter_num,
 quarter_end_week_idnt,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt
   THEN eop_inv_cost
   ELSE 0
   END) AS eop_inv_cost,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt
   THEN eop_inv_units
   ELSE 0
   END) AS eop_inv_units,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt
   THEN eop_inv_cost_plan
   ELSE 0
   END) AS eop_inv_cost_plan,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt
   THEN eop_inv_units_plan
   ELSE 0
   END) AS eop_inv_units_plan
FROM pre_eop AS pe
GROUP BY banner,
 quarter_num,
 quarter_end_week_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS eop_fh
AS
SELECT banner,
 halfyear_num AS half_num,
 fiscal_year_num,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num IN (2, 4)
   THEN eop_inv_cost
   ELSE 0
   END) AS eop_inv_cost,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num IN (2, 4)
   THEN eop_inv_units
   ELSE 0
   END) AS eop_inv_units,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num IN (2, 4)
   THEN eop_inv_cost_plan
   ELSE 0
   END) AS eop_inv_cost_plan,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num IN (2, 4)
   THEN eop_inv_units_plan
   ELSE 0
   END) AS eop_inv_units_plan
FROM pre_eop AS pe
GROUP BY banner,
 half_num,
 fiscal_year_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS eop_fy
AS
SELECT fiscal_year_num,
 banner,
 SUBSTR('ly', 1, 10) AS period_type,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_cost
   ELSE 0
   END) AS eop_inv_cost,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_units
   ELSE 0
   END) AS eop_inv_units,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_cost_plan
   ELSE 0
   END) AS eop_inv_cost_plan,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_units_plan
   ELSE 0
   END) AS eop_inv_units_plan
FROM pre_eop AS pe
WHERE month_num = (SELECT MAX(month_num)
   FROM month_range
   WHERE fiscal_year_num = (SELECT MIN(fiscal_year_num)
      FROM month_range))
GROUP BY fiscal_year_num,
 banner,
 period_type
UNION ALL
SELECT fiscal_year_num,
 banner,
 SUBSTR('ytd', 1, 10) AS period_type,
 SUM(CASE
   WHEN week_num = month_end_week_idnt
   THEN eop_inv_cost
   ELSE 0
   END) AS eop_inv_cost,
 SUM(CASE
   WHEN week_num = month_end_week_idnt
   THEN eop_inv_units
   ELSE 0
   END) AS eop_inv_units,
 SUM(CASE
   WHEN week_num = month_end_week_idnt
   THEN eop_inv_cost_plan
   ELSE 0
   END) AS eop_inv_cost_plan,
 SUM(CASE
   WHEN week_num = month_end_week_idnt
   THEN eop_inv_units_plan
   ELSE 0
   END) AS eop_inv_units_plan
FROM pre_eop AS pe
WHERE month_num = (SELECT MAX(month_num)
   FROM eop_fm
   WHERE eop_inv_cost > 0
    AND eop_inv_units > 0)
GROUP BY fiscal_year_num,
 banner,
 period_type
UNION ALL
SELECT fiscal_year_num,
 banner,
 SUBSTR('ty', 1, 10) AS period_type,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_cost
   ELSE 0
   END) AS eop_inv_cost,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_units
   ELSE 0
   END) AS eop_inv_units,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_cost_plan
   ELSE 0
   END) AS eop_inv_cost_plan,
 SUM(CASE
   WHEN week_num = quarter_end_week_idnt AND fiscal_quarter_num = 4
   THEN eop_inv_units_plan
   ELSE 0
   END) AS eop_inv_units_plan
FROM pre_eop AS pe
WHERE month_num = (SELECT MAX(month_num)
   FROM month_range
   WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num)
      FROM month_range))
GROUP BY fiscal_year_num,
 banner,
 period_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS et_fm
AS
SELECT cust_fm.box,
 'fm' AS period_type,
 cust_fm.month_num AS period_value,
 cust_fm.total_trips,
 cust_fm.total_customer,
 cust_fm.new_customer,
 cust_fm.trips_per_cust,
 p50_fm.p50_fm AS p50,
 sbrands_fm.sb_sales,
 sbrands_fm.pct_sb_sales,
 merch_fm.mmroi,
 merch_fm.merch_margin,
 merch_fm.turn,
 NULL AS sellthru,
 merch_fm.turn_plan,
 NULL AS sellthru_plan,
 rcpt_fm.rcpt_split_fc,
 rcpt_fm.rcpt_split_store,
 rcpt_fm.rcpt_split_fc_plan,
 rcpt_fm.rcpt_split_store_plan,
 merch_net_sales_fm.merch_net_sales,
 merch_net_sales_fm.merch_net_sales_plan,
 eop_fm.eop_inv_cost,
 eop_fm.eop_inv_units,
 eop_fm.eop_inv_cost_plan,
 eop_fm.eop_inv_units_plan
FROM cust_fm
 LEFT JOIN p50_fm AS p50_fm ON CAST(cust_fm.month_num AS FLOAT64) = p50_fm.fm AND LOWER(cust_fm.box) = LOWER(p50_fm.channel
    )
 LEFT JOIN sbrands_fm ON CAST(cust_fm.month_num AS FLOAT64) = sbrands_fm.month_num AND LOWER(cust_fm.box) = LOWER(sbrands_fm
    .chnl)
 LEFT JOIN merch_fm ON CAST(cust_fm.month_num AS FLOAT64) = merch_fm.month_num AND LOWER(cust_fm.box) = LOWER(merch_fm.banner
    )
 LEFT JOIN rcpt_fm ON CAST(cust_fm.month_num AS FLOAT64) = rcpt_fm.month_num AND LOWER(cust_fm.box) = LOWER(rcpt_fm.banner
    )
 LEFT JOIN merch_net_sales_fm ON CAST(cust_fm.month_num AS FLOAT64) = merch_net_sales_fm.month_num AND LOWER(cust_fm.box
    ) = LOWER(merch_net_sales_fm.banner)
 LEFT JOIN eop_fm ON CAST(cust_fm.month_num AS FLOAT64) = eop_fm.month_num AND LOWER(cust_fm.box) = LOWER(eop_fm.banner
    );
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS et_qtd
AS
SELECT cust_fq.box,
 'qtd' AS period_type,
 cust_fq.quarter_num AS period_value,
 cust_fq.total_trips,
 cust_fq.total_customer,
 cust_fq.new_customer,
 cust_fq.trips_per_cust,
 p50_qtd.p50_fq AS p50,
 sbrands_qtd.sb_sales,
 sbrands_qtd.pct_sb_sales,
 merch_qtd.mmroi,
 merch_qtd.merch_margin,
 merch_qtd.turn,
 NULL AS sellthru,
 merch_qtd.turn_plan,
 NULL AS sellthru_plan,
 rcpt_qtd.rcpt_split_fc,
 rcpt_qtd.rcpt_split_store,
 rcpt_qtd.rcpt_split_fc_plan,
 rcpt_qtd.rcpt_split_store_plan,
 merch_net_sales_qtd.merch_net_sales,
 merch_net_sales_qtd.merch_net_sales_plan,
 eop_qtd.eop_inv_cost,
 eop_qtd.eop_inv_units,
 eop_qtd.eop_inv_cost_plan,
 eop_qtd.eop_inv_units_plan
FROM cust_fq
 LEFT JOIN p50_qtd ON LOWER(cust_fq.quarter_num) = LOWER(p50_qtd.qtd) AND LOWER(cust_fq.box) = LOWER(p50_qtd.channel)
 LEFT JOIN sbrands_qtd ON LOWER(cust_fq.quarter_num) = LOWER(sbrands_qtd.qtd_num) AND LOWER(cust_fq.box) = LOWER(sbrands_qtd
    .chnl)
 LEFT JOIN merch_qtd ON LOWER(cust_fq.quarter_num) = LOWER(merch_qtd.qtd_num) AND LOWER(cust_fq.box) = LOWER(merch_qtd.banner
    )
 LEFT JOIN rcpt_qtd ON LOWER(cust_fq.quarter_num) = LOWER(rcpt_qtd.qtd_num) AND LOWER(cust_fq.box) = LOWER(rcpt_qtd.banner
    )
 LEFT JOIN merch_net_sales_qtd ON LOWER(cust_fq.quarter_num) = LOWER(merch_net_sales_qtd.qtd_num) AND LOWER(cust_fq.box
    ) = LOWER(merch_net_sales_qtd.banner)
 LEFT JOIN eop_qtd ON LOWER(cust_fq.quarter_num) = LOWER(eop_qtd.qtd_num) AND LOWER(cust_fq.box) = LOWER(eop_qtd.banner
    )
WHERE LOWER(cust_fq.quarter_num) LIKE LOWER('qtd%');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS et_fq
AS
SELECT cust_fq.box,
 'fq' AS period_type,
 cust_fq.quarter_num AS period_value,
 cust_fq.total_trips,
 cust_fq.total_customer,
 cust_fq.new_customer,
 cust_fq.trips_per_cust,
 p50_fq.p50_fq AS p50,
 sbrands_fq.sb_sales,
 sbrands_fq.pct_sb_sales,
 merch_fq.mmroi,
 merch_fq.merch_margin,
 merch_fq.turn,
 NULL AS sellthru,
 merch_fq.turn_plan,
 NULL AS sellthru_plan,
 rcpt_fq.rcpt_split_fc,
 rcpt_fq.rcpt_split_store,
 rcpt_fq.rcpt_split_fc_plan,
 rcpt_fq.rcpt_split_store_plan,
 merch_net_sales_fq.merch_net_sales,
 merch_net_sales_fq.merch_net_sales_plan,
 eop_fq.eop_inv_cost,
 eop_fq.eop_inv_units,
 eop_fq.eop_inv_cost_plan,
 eop_fq.eop_inv_units_plan
FROM cust_fq
 LEFT JOIN p50_fq AS p50_fq ON CAST(cust_fq.quarter_num AS FLOAT64) = p50_fq.quarter AND LOWER(cust_fq.box) = LOWER(p50_fq
    .channel)
 LEFT JOIN sbrands_fq ON CAST(cust_fq.quarter_num AS FLOAT64) = sbrands_fq.quarter_num AND LOWER(cust_fq.box) = LOWER(sbrands_fq
    .chnl)
 LEFT JOIN merch_fq ON CAST(cust_fq.quarter_num AS FLOAT64) = merch_fq.quarter_num AND LOWER(cust_fq.box) = LOWER(merch_fq
    .banner)
 LEFT JOIN rcpt_fq ON CAST(cust_fq.quarter_num AS FLOAT64) = rcpt_fq.quarter_idnt AND LOWER(cust_fq.box) = LOWER(rcpt_fq
    .banner)
 LEFT JOIN merch_net_sales_fq ON CAST(cust_fq.quarter_num AS FLOAT64) = merch_net_sales_fq.quarter_idnt AND LOWER(cust_fq
    .box) = LOWER(merch_net_sales_fq.banner)
 LEFT JOIN eop_fq ON CAST(cust_fq.quarter_num AS FLOAT64) = eop_fq.quarter_num AND LOWER(cust_fq.box) = LOWER(eop_fq.banner
    )
WHERE LOWER(cust_fq.quarter_num) NOT LIKE LOWER('qtd%');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS et_fh
AS
SELECT cust_fh.box,
 'fh' AS period_type,
 cust_fh.half_num AS period_value,
 cust_fh.total_trips,
 cust_fh.total_customer,
 cust_fh.new_customer,
 cust_fh.trips_per_cust,
 p50_fh.p50_fh AS p50,
 sbrands_fh.sb_sales,
 sbrands_fh.pct_sb_sales,
 merch_fh.mmroi,
 merch_fh.merch_margin,
 merch_fh.turn,
 NULL AS sellthru,
 merch_fh.turn_plan,
 NULL AS sellthru_plan,
 rcpt_fh.rcpt_split_fc,
 rcpt_fh.rcpt_split_store,
 rcpt_fh.rcpt_split_fc_plan,
 rcpt_fh.rcpt_split_store_plan,
 merch_net_sales_fh.merch_net_sales,
 merch_net_sales_fh.merch_net_sales_plan,
 eop_fh.eop_inv_cost,
 eop_fh.eop_inv_units,
 eop_fh.eop_inv_cost_plan,
 eop_fh.eop_inv_units_plan
FROM cust_fh
 LEFT JOIN p50_fh AS p50_fh ON CAST(cust_fh.half_num AS FLOAT64) = p50_fh.half_num AND LOWER(cust_fh.box) = LOWER(p50_fh
    .channel)
 LEFT JOIN sbrands_fh ON CAST(cust_fh.half_num AS FLOAT64) = sbrands_fh.half_num AND LOWER(cust_fh.box) = LOWER(sbrands_fh
    .chnl)
 LEFT JOIN merch_fh ON CAST(cust_fh.half_num AS FLOAT64) = merch_fh.half_num AND LOWER(cust_fh.box) = LOWER(merch_fh.banner
    )
 LEFT JOIN rcpt_fh ON CAST(cust_fh.half_num AS FLOAT64) = rcpt_fh.half_num AND LOWER(cust_fh.box) = LOWER(rcpt_fh.banner
    )
 LEFT JOIN merch_net_sales_fh ON CAST(cust_fh.half_num AS FLOAT64) = merch_net_sales_fh.half_num AND LOWER(cust_fh.box)
   = LOWER(merch_net_sales_fh.banner)
 LEFT JOIN eop_fh ON CAST(cust_fh.half_num AS FLOAT64) = eop_fh.half_num AND LOWER(cust_fh.box) = LOWER(eop_fh.banner);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS et_fy
AS
SELECT cust_fy.box,
 cust_fy.period_type,
 cust_fy.year_num AS period_value,
 cust_fy.total_trips,
 cust_fy.total_customer,
 cust_fy.new_customer,
 cust_fy.trips_per_cust,
 p50_fy.p50_fy AS p50,
 sbrands_fy.sb_sales,
 sbrands_fy.pct_sb_sales,
 merch_fy.mmroi,
 merch_fy.merch_margin,
 merch_fy.turn,
 NULL AS sellthru,
 merch_fy.turn_plan,
 NULL AS sellthru_plan,
 rcpt_fy.rcpt_split_fc,
 rcpt_fy.rcpt_split_store,
 rcpt_fy.rcpt_split_fc_plan,
 rcpt_fy.rcpt_split_store_plan,
 merch_net_sales_fy.merch_net_sales,
 merch_net_sales_fy.merch_net_sales_plan,
 eop_fy.eop_inv_cost,
 eop_fy.eop_inv_units,
 eop_fy.eop_inv_cost_plan,
 eop_fy.eop_inv_units_plan
FROM cust_fy
 LEFT JOIN p50_fy AS p50_fy ON CAST(cust_fy.year_num AS FLOAT64) = p50_fy.fiscal_year AND LOWER(cust_fy.box) = LOWER(p50_fy
     .channel) AND LOWER(cust_fy.period_type) <> LOWER('ty')
 LEFT JOIN sbrands_fy ON CAST(cust_fy.year_num AS FLOAT64) = sbrands_fy.year_num AND LOWER(cust_fy.box) = LOWER(sbrands_fy
     .chnl) AND LOWER(cust_fy.period_type) <> LOWER('ty')
 LEFT JOIN merch_fy ON CAST(cust_fy.year_num AS FLOAT64) = merch_fy.year_num AND LOWER(cust_fy.box) = LOWER(merch_fy.banner
     ) AND LOWER(cust_fy.period_type) = LOWER(merch_fy.period_type)
 LEFT JOIN rcpt_fy ON CAST(cust_fy.year_num AS FLOAT64) = rcpt_fy.fiscal_year_num AND LOWER(cust_fy.box) = LOWER(rcpt_fy
     .banner) AND LOWER(cust_fy.period_type) = LOWER(rcpt_fy.period_type)
 LEFT JOIN merch_net_sales_fy ON CAST(cust_fy.year_num AS FLOAT64) = merch_net_sales_fy.fiscal_year_num AND LOWER(cust_fy
     .box) = LOWER(merch_net_sales_fy.banner) AND LOWER(cust_fy.period_type) = LOWER(merch_net_sales_fy.period_type)
 LEFT JOIN eop_fy ON CAST(cust_fy.year_num AS FLOAT64) = eop_fy.fiscal_year_num AND LOWER(cust_fy.box) = LOWER(eop_fy.banner
     ) AND LOWER(cust_fy.period_type) = LOWER(eop_fy.period_type);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS manual_plan
AS
SELECT box,
  CASE
  WHEN LOWER(time_period) = LOWER('Total Year')
  THEN 'ty'
  WHEN LOWER(time_period) = LOWER('YTD')
  THEN 'ytd'
  WHEN LOWER(time_period) IN (LOWER('Q1'), LOWER('Q2'), LOWER('Q3'), LOWER('Q4'))
  THEN 'fq'
  WHEN LOWER(time_period) IN (LOWER('QTD Q1 Feb-Mar'), LOWER('QTD Q2 May-Jun'), LOWER('QTD Q3 Aug-Sep'), LOWER('QTD Q4 Nov-Dec'
     ))
  THEN 'qtd'
  WHEN LOWER(time_period) IN (LOWER('H1'), LOWER('H2'))
  THEN 'fh'
  ELSE 'fm'
  END AS period_type,
  CASE
  WHEN LOWER(time_period) IN (LOWER('YTD'), LOWER('Total Year'))
  THEN SUBSTR(CAST(fiscal_year AS STRING), 1, 20)
  WHEN LOWER(time_period) IN (LOWER('H1'), LOWER('H2'))
  THEN SUBSTR(CAST(CONCAT(fiscal_year, REPLACE(time_period, 'H', '')) AS STRING), 1, 20)
  WHEN LOWER(time_period) IN (LOWER('Q1'), LOWER('Q2'), LOWER('Q3'), LOWER('Q4'))
  THEN SUBSTR(CAST(CONCAT(fiscal_year, REPLACE(time_period, 'Q', '')) AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('jan')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '12') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('dec')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '11') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('nov')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '10') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('oct')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '09') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('sep')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '08') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('aug')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '07') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('jul')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '06') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('jun')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '05') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('may')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '04') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('apr')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '03') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('mar')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '02') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('feb')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '01') AS STRING), 1, 20)
  WHEN LOWER(time_period) LIKE LOWER('QTD Q1 Feb-%')
  THEN 'qtd_q1'
  WHEN LOWER(time_period) LIKE LOWER('QTD Q2 May-%')
  THEN 'qtd_q2'
  WHEN LOWER(time_period) LIKE LOWER('QTD Q3 Aug-%')
  THEN 'qtd_q3'
  WHEN LOWER(time_period) LIKE LOWER('QTD Q4 Nov-%')
  THEN 'qtd_q4'
  ELSE NULL
  END AS period_value,
  CASE
  WHEN LOWER(metric) LIKE LOWER('Variable Fulfillment Rate of Sales%')
  THEN 'fulfillment_ros_plan'
  WHEN LOWER(metric) LIKE LOWER('Variable Fulfillment Cost per Unit%')
  THEN 'fulfillment_cpu_plan'
  WHEN LOWER(metric) = LOWER('Strategic Brand Sales % (Rack Stores)')
  THEN 'sb_sales_plan'
  WHEN LOWER(metric) = LOWER('Strategic Brand Sales % (Rack)')
  THEN 'pct_sb_sales_plan'
  WHEN LOWER(metric) = LOWER('Budget Management (Cash excl Bonus)')
  THEN 'budget_mgmt_plan'
  WHEN LOWER(metric) = LOWER('Business Value Delivered')
  THEN 'bus_value_plan'
  WHEN LOWER(metric) = LOWER('Capitalization Rate (Labor)')
  THEN 'cap_rate_plan'
  WHEN LOWER(metric) = LOWER('Liquidity')
  THEN 'liquidity_plan'
  WHEN LOWER(metric) = LOWER('Leverage')
  THEN 'leverage_plan'
  WHEN LOWER(metric) = LOWER('Corporate OH Labor')
  THEN 'oh_labor_plan'
  WHEN LOWER(metric) = LOWER('Compliance (HR & Legal)')
  THEN 'compliance_plan'
  WHEN LOWER(metric) = LOWER('Merch Margin % (Cost)')
  THEN 'merch_margin_plan'
  WHEN LOWER(metric) = LOWER('MMROI (Cost)')
  THEN 'MMROI_plan'
  WHEN LOWER(metric) = LOWER('New Customer Count (Rack)')
  THEN 'new_customer_plan'
  WHEN LOWER(metric) LIKE LOWER('Customer Count%')
  THEN 'total_customer_plan'
  WHEN LOWER(metric) LIKE LOWER('Customer Trips%')
  THEN 'total_trips_plan'
  WHEN LOWER(metric) LIKE LOWER('Customer Spend%')
  THEN 'total_spend_plan'
  WHEN LOWER(metric) = LOWER('Headcount')
  THEN 'headcount_plan'
  WHEN LOWER(metric) = LOWER('Credit EBIT (excl losses)')
  THEN 'credit_ebit_pct_plan'
  WHEN LOWER(metric) = LOWER('Safety Injury Rate')
  THEN 'sfty_inj_rate_plan'
  WHEN LOWER(metric) = LOWER('Beauty Selling Cost')
  THEN 'beauty_selling_cost_plan'
  WHEN LOWER(metric) LIKE LOWER('Disaster Recovery%')
  THEN 'disaster_rec_testing_plan'
  WHEN LOWER(metric) = LOWER('Legal Budget')
  THEN 'legal_budget_plan'
  WHEN LOWER(metric) = LOWER('Avg VASN to Store Receive (ex NPG, Nike, HI/AK) Days')
  THEN 'avg_vasn_to_store_plan'
  WHEN LOWER(metric) = LOWER('Top Items NMS Coverage')
  THEN 'top_items_nms_plan'
  WHEN LOWER(metric) = LOWER('Supply Chain (FC/DC) Labor Productivity')
  THEN 'sup_chain_labor_prod_plan'
  WHEN LOWER(metric) = LOWER('Shrinkage %')
  THEN 'shrinkage_pct_plan'
  WHEN LOWER(metric) = LOWER('Corporate Overhead Labor $')
  THEN 'corp_oh_labor_plan'
  WHEN LOWER(metric) = LOWER('% of Stores Executing Weekly RFID Counts')
  THEN 'pct_stores_rfid_count_plan'
  WHEN LOWER(metric) = LOWER('Technology-enabled Sales/EBIT Improvements')
  THEN 'tech_sales_ebit_plan'
  WHEN LOWER(metric) = LOWER('NAP Data Discoverability ')
  THEN 'data_discoverability_plan'
  WHEN LOWER(metric) = LOWER('Store Shrink % (cost)')
  THEN 'store_shrink_cost_plan'
  WHEN LOWER(metric) = LOWER('FC/DC Shrink % (cost)')
  THEN 'fc_shrink_pct_plan'
  WHEN LOWER(metric) = LOWER('Technology Spend/Budget')
  THEN 'tech_budget_plan'
  WHEN LOWER(metric) = LOWER('op_gmv')
  THEN 'op_gmv_plan'
  WHEN LOWER(metric) = LOWER('comp_gmv')
  THEN 'comp_gmv_plan'
  WHEN LOWER(metric) = LOWER('Marketplace Op GMV')
  THEN 'marketplace_opgmv_plan'
  WHEN LOWER(metric) = LOWER('new_store_op_gmv')
  THEN 'new_store_op_gmv_plan'
  WHEN LOWER(metric) = LOWER('nyc_op_gmv')
  THEN 'nyc_op_gmv_plan'
  WHEN LOWER(metric) = LOWER('Customer Retention Rate')
  THEN 'retention_rate_plan'
  WHEN LOWER(metric) = LOWER('Free Cash Flow')
  THEN 'cash_flow_plan'
  ELSE metric
  END AS feature_name,
 plan AS feature_value
FROM --t2dl_das_mothership.ets_manual
`{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.ets_manual
WHERE box IS NOT NULL
 AND plan IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_insert
AS
SELECT et_fm.box,
 SUBSTR(et_fm.period_type, 1, 6) AS period_type,
 et_fm.period_value,
 t.feature_name,
 CAST(CASE
   WHEN t.feature_name = 'null'
   THEN et_fm.total_trips
   WHEN t.feature_name = 'null'
   THEN et_fm.total_customer
   WHEN t.feature_name = 'null'
   THEN et_fm.new_customer
   WHEN t.feature_name = 'null'
   THEN et_fm.trips_per_cust
   WHEN t.feature_name = 'null'
   THEN et_fm.p50
   WHEN t.feature_name = 'null'
   THEN et_fm.sb_sales
   WHEN t.feature_name = 'null'
   THEN et_fm.pct_sb_sales
   WHEN t.feature_name = 'null'
   THEN et_fm.mmroi
   WHEN t.feature_name = 'null'
   THEN et_fm.merch_margin
   WHEN t.feature_name = 'null'
   THEN et_fm.turn
   WHEN t.feature_name = 'null'
   THEN et_fm.turn_plan
   WHEN t.feature_name = 'null'
   THEN et_fm.sellthru_plan
   WHEN t.feature_name = 'null'
   THEN et_fm.rcpt_split_fc
   WHEN t.feature_name = 'null'
   THEN et_fm.rcpt_split_store
   WHEN t.feature_name = 'null'
   THEN et_fm.rcpt_split_fc_plan
   WHEN t.feature_name = 'null'
   THEN et_fm.rcpt_split_store_plan
   WHEN t.feature_name = 'null'
   THEN et_fm.merch_net_sales
   WHEN t.feature_name = 'null'
   THEN et_fm.merch_net_sales_plan
   WHEN t.feature_name = 'null'
   THEN et_fm.eop_inv_cost
   WHEN t.feature_name = 'null'
   THEN et_fm.eop_inv_units
   WHEN t.feature_name = 'null'
   THEN et_fm.eop_inv_cost_plan
   WHEN t.feature_name = 'null'
   THEN et_fm.eop_inv_units_plan
   ELSE NULL
   END AS BIGNUMERIC) AS feature_value
FROM et_fm
 INNER JOIN (SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name) AS t ON CASE
  WHEN t.feature_name = 'null'
  THEN et_fm.total_trips
  WHEN t.feature_name = 'null'
  THEN et_fm.total_customer
  WHEN t.feature_name = 'null'
  THEN et_fm.new_customer
  WHEN t.feature_name = 'null'
  THEN et_fm.trips_per_cust
  WHEN t.feature_name = 'null'
  THEN et_fm.p50
  WHEN t.feature_name = 'null'
  THEN et_fm.sb_sales
  WHEN t.feature_name = 'null'
  THEN et_fm.pct_sb_sales
  WHEN t.feature_name = 'null'
  THEN et_fm.mmroi
  WHEN t.feature_name = 'null'
  THEN et_fm.merch_margin
  WHEN t.feature_name = 'null'
  THEN et_fm.turn
  WHEN t.feature_name = 'null'
  THEN et_fm.turn_plan
  WHEN t.feature_name = 'null'
  THEN et_fm.sellthru_plan
  WHEN t.feature_name = 'null'
  THEN et_fm.rcpt_split_fc
  WHEN t.feature_name = 'null'
  THEN et_fm.rcpt_split_store
  WHEN t.feature_name = 'null'
  THEN et_fm.rcpt_split_fc_plan
  WHEN t.feature_name = 'null'
  THEN et_fm.rcpt_split_store_plan
  WHEN t.feature_name = 'null'
  THEN et_fm.merch_net_sales
  WHEN t.feature_name = 'null'
  THEN et_fm.merch_net_sales_plan
  WHEN t.feature_name = 'null'
  THEN et_fm.eop_inv_cost
  WHEN t.feature_name = 'null'
  THEN et_fm.eop_inv_units
  WHEN t.feature_name = 'null'
  THEN et_fm.eop_inv_cost_plan
  WHEN t.feature_name = 'null'
  THEN et_fm.eop_inv_units_plan
  ELSE NULL
  END IS NOT NULL
UNION ALL
SELECT et_fy.box,
 et_fy.period_type,
 et_fy.period_value,
 t2.feature_name,
 CAST(CASE
   WHEN t2.feature_name = 'null'
   THEN et_fy.total_trips
   WHEN t2.feature_name = 'null'
   THEN et_fy.total_customer
   WHEN t2.feature_name = 'null'
   THEN et_fy.new_customer
   WHEN t2.feature_name = 'null'
   THEN et_fy.trips_per_cust
   WHEN t2.feature_name = 'null'
   THEN et_fy.p50
   WHEN t2.feature_name = 'null'
   THEN et_fy.sb_sales
   WHEN t2.feature_name = 'null'
   THEN et_fy.pct_sb_sales
   WHEN t2.feature_name = 'null'
   THEN et_fy.mmroi
   WHEN t2.feature_name = 'null'
   THEN et_fy.merch_margin
   WHEN t2.feature_name = 'null'
   THEN et_fy.turn
   WHEN t2.feature_name = 'null'
   THEN et_fy.turn_plan
   WHEN t2.feature_name = 'null'
   THEN et_fy.sellthru_plan
   WHEN t2.feature_name = 'null'
   THEN et_fy.rcpt_split_fc
   WHEN t2.feature_name = 'null'
   THEN et_fy.rcpt_split_store
   WHEN t2.feature_name = 'null'
   THEN et_fy.rcpt_split_fc_plan
   WHEN t2.feature_name = 'null'
   THEN et_fy.rcpt_split_store_plan
   WHEN t2.feature_name = 'null'
   THEN et_fy.merch_net_sales
   WHEN t2.feature_name = 'null'
   THEN et_fy.merch_net_sales_plan
   WHEN t2.feature_name = 'null'
   THEN et_fy.eop_inv_cost
   WHEN t2.feature_name = 'null'
   THEN et_fy.eop_inv_units
   WHEN t2.feature_name = 'null'
   THEN et_fy.eop_inv_cost_plan
   WHEN t2.feature_name = 'null'
   THEN et_fy.eop_inv_units_plan
   ELSE NULL
   END AS BIGNUMERIC) AS feature_value
FROM et_fy
 INNER JOIN (SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name) AS t2 ON CASE
  WHEN t2.feature_name = 'null'
  THEN et_fy.total_trips
  WHEN t2.feature_name = 'null'
  THEN et_fy.total_customer
  WHEN t2.feature_name = 'null'
  THEN et_fy.new_customer
  WHEN t2.feature_name = 'null'
  THEN et_fy.trips_per_cust
  WHEN t2.feature_name = 'null'
  THEN et_fy.p50
  WHEN t2.feature_name = 'null'
  THEN et_fy.sb_sales
  WHEN t2.feature_name = 'null'
  THEN et_fy.pct_sb_sales
  WHEN t2.feature_name = 'null'
  THEN et_fy.mmroi
  WHEN t2.feature_name = 'null'
  THEN et_fy.merch_margin
  WHEN t2.feature_name = 'null'
  THEN et_fy.turn
  WHEN t2.feature_name = 'null'
  THEN et_fy.turn_plan
  WHEN t2.feature_name = 'null'
  THEN et_fy.sellthru_plan
  WHEN t2.feature_name = 'null'
  THEN et_fy.rcpt_split_fc
  WHEN t2.feature_name = 'null'
  THEN et_fy.rcpt_split_store
  WHEN t2.feature_name = 'null'
  THEN et_fy.rcpt_split_fc_plan
  WHEN t2.feature_name = 'null'
  THEN et_fy.rcpt_split_store_plan
  WHEN t2.feature_name = 'null'
  THEN et_fy.merch_net_sales
  WHEN t2.feature_name = 'null'
  THEN et_fy.merch_net_sales_plan
  WHEN t2.feature_name = 'null'
  THEN et_fy.eop_inv_cost
  WHEN t2.feature_name = 'null'
  THEN et_fy.eop_inv_units
  WHEN t2.feature_name = 'null'
  THEN et_fy.eop_inv_cost_plan
  WHEN t2.feature_name = 'null'
  THEN et_fy.eop_inv_units_plan
  ELSE NULL
  END IS NOT NULL
UNION ALL
SELECT et_fh.box,
 et_fh.period_type,
 et_fh.period_value,
 t5.feature_name,
 CAST(CASE
   WHEN t5.feature_name = 'null'
   THEN et_fh.total_trips
   WHEN t5.feature_name = 'null'
   THEN et_fh.total_customer
   WHEN t5.feature_name = 'null'
   THEN et_fh.new_customer
   WHEN t5.feature_name = 'null'
   THEN et_fh.trips_per_cust
   WHEN t5.feature_name = 'null'
   THEN et_fh.p50
   WHEN t5.feature_name = 'null'
   THEN et_fh.sb_sales
   WHEN t5.feature_name = 'null'
   THEN et_fh.pct_sb_sales
   WHEN t5.feature_name = 'null'
   THEN et_fh.mmroi
   WHEN t5.feature_name = 'null'
   THEN et_fh.merch_margin
   WHEN t5.feature_name = 'null'
   THEN et_fh.turn
   WHEN t5.feature_name = 'null'
   THEN et_fh.turn_plan
   WHEN t5.feature_name = 'null'
   THEN et_fh.sellthru_plan
   WHEN t5.feature_name = 'null'
   THEN et_fh.rcpt_split_fc
   WHEN t5.feature_name = 'null'
   THEN et_fh.rcpt_split_store
   WHEN t5.feature_name = 'null'
   THEN et_fh.rcpt_split_fc_plan
   WHEN t5.feature_name = 'null'
   THEN et_fh.rcpt_split_store_plan
   WHEN t5.feature_name = 'null'
   THEN et_fh.merch_net_sales
   WHEN t5.feature_name = 'null'
   THEN et_fh.merch_net_sales_plan
   WHEN t5.feature_name = 'null'
   THEN et_fh.eop_inv_cost
   WHEN t5.feature_name = 'null'
   THEN et_fh.eop_inv_units
   WHEN t5.feature_name = 'null'
   THEN et_fh.eop_inv_cost_plan
   WHEN t5.feature_name = 'null'
   THEN et_fh.eop_inv_units_plan
   ELSE NULL
   END AS BIGNUMERIC) AS feature_value
FROM et_fh
 INNER JOIN (SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name) AS t5 ON CASE
  WHEN t5.feature_name = 'null'
  THEN et_fh.total_trips
  WHEN t5.feature_name = 'null'
  THEN et_fh.total_customer
  WHEN t5.feature_name = 'null'
  THEN et_fh.new_customer
  WHEN t5.feature_name = 'null'
  THEN et_fh.trips_per_cust
  WHEN t5.feature_name = 'null'
  THEN et_fh.p50
  WHEN t5.feature_name = 'null'
  THEN et_fh.sb_sales
  WHEN t5.feature_name = 'null'
  THEN et_fh.pct_sb_sales
  WHEN t5.feature_name = 'null'
  THEN et_fh.mmroi
  WHEN t5.feature_name = 'null'
  THEN et_fh.merch_margin
  WHEN t5.feature_name = 'null'
  THEN et_fh.turn
  WHEN t5.feature_name = 'null'
  THEN et_fh.turn_plan
  WHEN t5.feature_name = 'null'
  THEN et_fh.sellthru_plan
  WHEN t5.feature_name = 'null'
  THEN et_fh.rcpt_split_fc
  WHEN t5.feature_name = 'null'
  THEN et_fh.rcpt_split_store
  WHEN t5.feature_name = 'null'
  THEN et_fh.rcpt_split_fc_plan
  WHEN t5.feature_name = 'null'
  THEN et_fh.rcpt_split_store_plan
  WHEN t5.feature_name = 'null'
  THEN et_fh.merch_net_sales
  WHEN t5.feature_name = 'null'
  THEN et_fh.merch_net_sales_plan
  WHEN t5.feature_name = 'null'
  THEN et_fh.eop_inv_cost
  WHEN t5.feature_name = 'null'
  THEN et_fh.eop_inv_units
  WHEN t5.feature_name = 'null'
  THEN et_fh.eop_inv_cost_plan
  WHEN t5.feature_name = 'null'
  THEN et_fh.eop_inv_units_plan
  ELSE NULL
  END IS NOT NULL
UNION ALL
SELECT et_fq.box,
 et_fq.period_type,
 et_fq.period_value,
 t8.feature_name,
 CAST(CASE
   WHEN t8.feature_name = 'null'
   THEN et_fq.total_trips
   WHEN t8.feature_name = 'null'
   THEN et_fq.total_customer
   WHEN t8.feature_name = 'null'
   THEN et_fq.new_customer
   WHEN t8.feature_name = 'null'
   THEN et_fq.trips_per_cust
   WHEN t8.feature_name = 'null'
   THEN et_fq.p50
   WHEN t8.feature_name = 'null'
   THEN et_fq.sb_sales
   WHEN t8.feature_name = 'null'
   THEN et_fq.pct_sb_sales
   WHEN t8.feature_name = 'null'
   THEN et_fq.mmroi
   WHEN t8.feature_name = 'null'
   THEN et_fq.merch_margin
   WHEN t8.feature_name = 'null'
   THEN et_fq.turn
   WHEN t8.feature_name = 'null'
   THEN et_fq.turn_plan
   WHEN t8.feature_name = 'null'
   THEN et_fq.sellthru_plan
   WHEN t8.feature_name = 'null'
   THEN et_fq.rcpt_split_fc
   WHEN t8.feature_name = 'null'
   THEN et_fq.rcpt_split_store
   WHEN t8.feature_name = 'null'
   THEN et_fq.rcpt_split_fc_plan
   WHEN t8.feature_name = 'null'
   THEN et_fq.rcpt_split_store_plan
   WHEN t8.feature_name = 'null'
   THEN et_fq.merch_net_sales
   WHEN t8.feature_name = 'null'
   THEN et_fq.merch_net_sales_plan
   WHEN t8.feature_name = 'null'
   THEN et_fq.eop_inv_cost
   WHEN t8.feature_name = 'null'
   THEN et_fq.eop_inv_units
   WHEN t8.feature_name = 'null'
   THEN et_fq.eop_inv_cost_plan
   WHEN t8.feature_name = 'null'
   THEN et_fq.eop_inv_units_plan
   ELSE NULL
   END AS BIGNUMERIC) AS feature_value
FROM et_fq
 INNER JOIN (SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name) AS t8 ON CASE
  WHEN t8.feature_name = 'null'
  THEN et_fq.total_trips
  WHEN t8.feature_name = 'null'
  THEN et_fq.total_customer
  WHEN t8.feature_name = 'null'
  THEN et_fq.new_customer
  WHEN t8.feature_name = 'null'
  THEN et_fq.trips_per_cust
  WHEN t8.feature_name = 'null'
  THEN et_fq.p50
  WHEN t8.feature_name = 'null'
  THEN et_fq.sb_sales
  WHEN t8.feature_name = 'null'
  THEN et_fq.pct_sb_sales
  WHEN t8.feature_name = 'null'
  THEN et_fq.mmroi
  WHEN t8.feature_name = 'null'
  THEN et_fq.merch_margin
  WHEN t8.feature_name = 'null'
  THEN et_fq.turn
  WHEN t8.feature_name = 'null'
  THEN et_fq.turn_plan
  WHEN t8.feature_name = 'null'
  THEN et_fq.sellthru_plan
  WHEN t8.feature_name = 'null'
  THEN et_fq.rcpt_split_fc
  WHEN t8.feature_name = 'null'
  THEN et_fq.rcpt_split_store
  WHEN t8.feature_name = 'null'
  THEN et_fq.rcpt_split_fc_plan
  WHEN t8.feature_name = 'null'
  THEN et_fq.rcpt_split_store_plan
  WHEN t8.feature_name = 'null'
  THEN et_fq.merch_net_sales
  WHEN t8.feature_name = 'null'
  THEN et_fq.merch_net_sales_plan
  WHEN t8.feature_name = 'null'
  THEN et_fq.eop_inv_cost
  WHEN t8.feature_name = 'null'
  THEN et_fq.eop_inv_units
  WHEN t8.feature_name = 'null'
  THEN et_fq.eop_inv_cost_plan
  WHEN t8.feature_name = 'null'
  THEN et_fq.eop_inv_units_plan
  ELSE NULL
  END IS NOT NULL
UNION ALL
SELECT et_qtd.box,
 et_qtd.period_type,
 et_qtd.period_value,
 t11.feature_name,
 CAST(CASE
   WHEN t11.feature_name = 'null'
   THEN et_qtd.total_trips
   WHEN t11.feature_name = 'null'
   THEN et_qtd.total_customer
   WHEN t11.feature_name = 'null'
   THEN et_qtd.new_customer
   WHEN t11.feature_name = 'null'
   THEN et_qtd.trips_per_cust
   WHEN t11.feature_name = 'null'
   THEN et_qtd.p50
   WHEN t11.feature_name = 'null'
   THEN et_qtd.sb_sales
   WHEN t11.feature_name = 'null'
   THEN et_qtd.pct_sb_sales
   WHEN t11.feature_name = 'null'
   THEN et_qtd.mmroi
   WHEN t11.feature_name = 'null'
   THEN et_qtd.merch_margin
   WHEN t11.feature_name = 'null'
   THEN et_qtd.turn
   WHEN t11.feature_name = 'null'
   THEN et_qtd.turn_plan
   WHEN t11.feature_name = 'null'
   THEN et_qtd.sellthru_plan
   WHEN t11.feature_name = 'null'
   THEN et_qtd.rcpt_split_fc
   WHEN t11.feature_name = 'null'
   THEN et_qtd.rcpt_split_store
   WHEN t11.feature_name = 'null'
   THEN et_qtd.rcpt_split_fc_plan
   WHEN t11.feature_name = 'null'
   THEN et_qtd.rcpt_split_store_plan
   WHEN t11.feature_name = 'null'
   THEN et_qtd.merch_net_sales
   WHEN t11.feature_name = 'null'
   THEN et_qtd.merch_net_sales_plan
   WHEN t11.feature_name = 'null'
   THEN et_qtd.eop_inv_cost
   WHEN t11.feature_name = 'null'
   THEN et_qtd.eop_inv_units
   WHEN t11.feature_name = 'null'
   THEN et_qtd.eop_inv_cost_plan
   WHEN t11.feature_name = 'null'
   THEN et_qtd.eop_inv_units_plan
   ELSE NULL
   END AS BIGNUMERIC) AS feature_value
FROM et_qtd
 INNER JOIN (SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name
   UNION ALL
   SELECT 'null' AS feature_name) AS t11 ON CASE
  WHEN t11.feature_name = 'null'
  THEN et_qtd.total_trips
  WHEN t11.feature_name = 'null'
  THEN et_qtd.total_customer
  WHEN t11.feature_name = 'null'
  THEN et_qtd.new_customer
  WHEN t11.feature_name = 'null'
  THEN et_qtd.trips_per_cust
  WHEN t11.feature_name = 'null'
  THEN et_qtd.p50
  WHEN t11.feature_name = 'null'
  THEN et_qtd.sb_sales
  WHEN t11.feature_name = 'null'
  THEN et_qtd.pct_sb_sales
  WHEN t11.feature_name = 'null'
  THEN et_qtd.mmroi
  WHEN t11.feature_name = 'null'
  THEN et_qtd.merch_margin
  WHEN t11.feature_name = 'null'
  THEN et_qtd.turn
  WHEN t11.feature_name = 'null'
  THEN et_qtd.turn_plan
  WHEN t11.feature_name = 'null'
  THEN et_qtd.sellthru_plan
  WHEN t11.feature_name = 'null'
  THEN et_qtd.rcpt_split_fc
  WHEN t11.feature_name = 'null'
  THEN et_qtd.rcpt_split_store
  WHEN t11.feature_name = 'null'
  THEN et_qtd.rcpt_split_fc_plan
  WHEN t11.feature_name = 'null'
  THEN et_qtd.rcpt_split_store_plan
  WHEN t11.feature_name = 'null'
  THEN et_qtd.merch_net_sales
  WHEN t11.feature_name = 'null'
  THEN et_qtd.merch_net_sales_plan
  WHEN t11.feature_name = 'null'
  THEN et_qtd.eop_inv_cost
  WHEN t11.feature_name = 'null'
  THEN et_qtd.eop_inv_units
  WHEN t11.feature_name = 'null'
  THEN et_qtd.eop_inv_cost_plan
  WHEN t11.feature_name = 'null'
  THEN et_qtd.eop_inv_units_plan
  ELSE NULL
  END IS NOT NULL
UNION ALL
SELECT box,
  CASE
  WHEN LOWER(time_period) = LOWER('Total Year')
  THEN 'ly'
  WHEN LOWER(time_period) = LOWER('YTD')
  THEN 'ytd'
  WHEN LOWER(time_period) IN (LOWER('Q1'), LOWER('Q2'), LOWER('Q3'), LOWER('Q4'))
  THEN 'fq'
  WHEN LOWER(time_period) IN (LOWER('QTD Q1 Feb-Mar'), LOWER('QTD Q2 May-Jun'), LOWER('QTD Q3 Aug-Sep'), LOWER('QTD Q4 Nov-Dec'
     ))
  THEN 'qtd'
  WHEN LOWER(time_period) IN (LOWER('H1'), LOWER('H2'))
  THEN 'fh'
  ELSE 'fm'
  END AS period_type,
  CASE
  WHEN LOWER(time_period) IN (LOWER('YTD'), LOWER('Total Year'))
  THEN SUBSTR(CAST(fiscal_year AS STRING), 1, 20)
  WHEN LOWER(time_period) IN (LOWER('H1'), LOWER('H2'))
  THEN SUBSTR(CAST(CONCAT(fiscal_year, REPLACE(time_period, 'H', '')) AS STRING), 1, 20)
  WHEN LOWER(time_period) IN (LOWER('Q1'), LOWER('Q2'), LOWER('Q3'), LOWER('Q4'))
  THEN SUBSTR(CAST(CONCAT(fiscal_year, REPLACE(time_period, 'Q', '')) AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('jan')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '12') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('dec')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '11') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('nov')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '10') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('oct')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '09') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('sep')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '08') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('aug')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '07') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('jul')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '06') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('jun')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '05') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('may')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '04') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('apr')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '03') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('mar')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '02') AS STRING), 1, 20)
  WHEN LOWER(time_period) = LOWER('feb')
  THEN SUBSTR(CAST(CONCAT(fiscal_year, '01') AS STRING), 1, 20)
  WHEN LOWER(time_period) LIKE LOWER('QTD Q1 Feb-%')
  THEN 'qtd_q1'
  WHEN LOWER(time_period) LIKE LOWER('QTD Q2 May-%')
  THEN 'qtd_q2'
  WHEN LOWER(time_period) LIKE LOWER('QTD Q3 Aug-%')
  THEN 'qtd_q3'
  WHEN LOWER(time_period) LIKE LOWER('QTD Q4 Nov-%')
  THEN 'qtd_q4'
  ELSE NULL
  END AS period_value,
  CASE
  WHEN LOWER(metric) LIKE LOWER('Variable Fulfillment Rate of Sales%')
  THEN 'fulfillment_ros'
  WHEN LOWER(metric) LIKE LOWER('Variable Fulfillment Cost per Unit%')
  THEN 'fulfillment_cpu'
  WHEN LOWER(metric) = LOWER('Budget Management (Cash excl Bonus)')
  THEN 'budget_mgmt'
  WHEN LOWER(metric) = LOWER('Business Value Delivered')
  THEN 'bus_value'
  WHEN LOWER(metric) = LOWER('Capitalization Rate (Labor)')
  THEN 'cap_rate'
  WHEN LOWER(metric) = LOWER('Liquidity')
  THEN 'liquidity'
  WHEN LOWER(metric) = LOWER('Leverage')
  THEN 'leverage'
  WHEN LOWER(metric) = LOWER('Corporate OH Labor')
  THEN 'oh_labor'
  WHEN LOWER(metric) = LOWER('Compliance (HR & Legal)')
  THEN 'compliance'
  WHEN LOWER(metric) = LOWER('Reg. Price Sell-thru')
  THEN 'sellthru'
  WHEN LOWER(metric) = LOWER('Headcount')
  THEN 'headcount'
  WHEN LOWER(metric) = LOWER('Credit EBIT (excl losses)')
  THEN 'credit_ebit_pct'
  WHEN LOWER(metric) = LOWER('Safety Injury Rate')
  THEN 'sfty_inj_rate'
  WHEN LOWER(metric) = LOWER('Beauty Selling Cost')
  THEN 'beauty_selling_cost'
  WHEN LOWER(metric) LIKE LOWER('Disaster Recovery%')
  THEN 'disaster_rec_testing'
  WHEN LOWER(metric) = LOWER('Legal Budget')
  THEN 'legal_budget'
  WHEN LOWER(metric) = LOWER('Avg VASN to Store Receive (ex NPG, Nike, HI/AK) Days')
  THEN 'avg_vasn_to_store'
  WHEN LOWER(metric) = LOWER('Top Items NMS Coverage')
  THEN 'top_items_nms'
  WHEN LOWER(metric) = LOWER('Supply Chain (FC/DC) Labor Productivity')
  THEN 'sup_chain_labor_prod'
  WHEN LOWER(metric) = LOWER('Shrinkage %')
  THEN 'shrinkage_pct'
  WHEN LOWER(metric) = LOWER('Corporate Overhead Labor $')
  THEN 'corp_oh_labor'
  WHEN LOWER(metric) = LOWER('% of Stores Executing Weekly RFID Counts')
  THEN 'pct_stores_rfid_count'
  WHEN LOWER(metric) = LOWER('Technology-enabled Sales/EBIT Improvements')
  THEN 'tech_sales_ebit'
  WHEN LOWER(metric) = LOWER('NAP Data Discoverability ')
  THEN 'data_discoverability'
  WHEN LOWER(metric) = LOWER('Store Shrink % (cost)')
  THEN 'store_shrink_cost'
  WHEN LOWER(metric) = LOWER('FC/DC Shrink % (cost)')
  THEN 'fc_shrink_pct'
  WHEN LOWER(metric) = LOWER('Technology Spend/Budget')
  THEN 'tech_budget'
  WHEN LOWER(metric) = LOWER('op_gmv')
  THEN 'op_gmv'
  WHEN LOWER(metric) = LOWER('comp_gmv')
  THEN 'comp_gmv'
  WHEN LOWER(metric) = LOWER('Marketplace Op GMV')
  THEN 'marketplace_opgmv'
  WHEN LOWER(metric) = LOWER('new_store_op_gmv')
  THEN 'new_store_op_gmv'
  WHEN LOWER(metric) = LOWER('nyc_op_gmv')
  THEN 'nyc_op_gmv'
  WHEN LOWER(metric) = LOWER('Customer Retention Rate')
  THEN 'retention_rate'
  WHEN LOWER(metric) = LOWER('Free Cash Flow')
  THEN 'cash_flow'
  ELSE metric
  END AS feature_name,
 actuals AS feature_value
FROM `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.ets_manual
WHERE box IS NOT NULL
 AND actuals IS NOT NULL
UNION ALL
SELECT box,
 period_type,
 period_value,
 feature_name,
 feature_value
FROM manual_plan
UNION ALL
SELECT t17.box,
 t17.period_type,
 t17.period_value,
 'trips_per_cust_plan' AS feature_name,
  t17.feature_value / t20.feature_value AS feature_value
FROM manual_plan AS t17
 LEFT JOIN manual_plan AS t20 ON LOWER(t17.box) = LOWER(t20.box) AND LOWER(t17.period_value) = LOWER(t20.period_value)
  AND LOWER(t17.period_type) = LOWER(t20.period_type)
WHERE LOWER(t17.feature_name) = LOWER('total_trips_plan')
 AND LOWER(t20.feature_name) = LOWER('total_customer_plan')
UNION ALL
SELECT t110.box,
 t110.period_type,
 t110.period_value,
 'spend_per_cust_plan' AS feature_name,
  t110.feature_value / t21.feature_value AS feature_value
FROM manual_plan AS t110
 LEFT JOIN manual_plan AS t21 ON LOWER(t110.box) = LOWER(t21.box) AND LOWER(t110.period_value) = LOWER(t21.period_value
     ) AND LOWER(t110.period_type) = LOWER(t21.period_type)
WHERE LOWER(t110.feature_name) = LOWER('total_spend_plan')
 AND LOWER(t21.feature_name) = LOWER('total_customer_plan')
UNION ALL
SELECT t111.box,
 t111.period_type,
 t111.period_value,
 'spend_per_trip_plan' AS feature_name,
  t111.feature_value / t24.feature_value AS feature_value
FROM manual_plan AS t111
 LEFT JOIN manual_plan AS t24 ON LOWER(t111.box) = LOWER(t24.box) AND LOWER(t111.period_value) = LOWER(t24.period_value
     ) AND LOWER(t111.period_type) = LOWER(t24.period_type)
WHERE LOWER(t111.feature_name) = LOWER('total_spend_plan')
 AND LOWER(t24.feature_name) = LOWER('total_trips_plan');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.ets_primary;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.mothership_t2_schema}}.ets_primary
--t2dl_das_mothership.ets_primary
(SELECT box,
  period_type,
  period_value,
  feature_name,
  CAST(feature_value AS NUMERIC) AS feature_value,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM pre_insert);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(box), COLUMN(period_type), COLUMN(period_value), COLUMN(feature_name) ON t2dl_das_mothership.ets_primary;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
