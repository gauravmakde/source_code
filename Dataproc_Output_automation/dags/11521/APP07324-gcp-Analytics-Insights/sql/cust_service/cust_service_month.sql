/* SET QUERY_BAND = 'App_ID=APP08159;
     DAG_ID=cust_service_11521_ACE_ENG;
     Task_Name=cust_service_month;'
     FOR SESSION VOLATILE;*/

/*
T2/Table Name: T2DL_DAS_SERVICE_ENG.cust_service_month
Team：Customer Analytics - Styling & Strategy
Date Created: Mar. 12th 2023 

Note:
-- Update Cadence: Monthly (after end of each fiscal month)

*/

/*----------------------------------------------------------------------
--A. Date Handle & Transactions / Returns & Services Tagging
----------------------------------------------------------------------*/

/***************************************************************************************/
/***************************************************************************************/
/********* Step 1: create realigned fiscal calendar                            *********/
/***************************************************************************************/
/***************************************************************************************/

/***************************************************************************************/
/********* Step 1-a: Going back to start of 2009, find all years with a "53rd week" *********/
/***************************************************************************************/
--drop table week_53_yrs;
CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yrs
CLUSTER BY year_num
AS
SELECT year_num,
 RANK() OVER (ORDER BY year_num DESC) AS recency_rank
FROM (SELECT DISTINCT year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE week_of_fyr = 53
   AND day_date >= DATE '2009-01-01'
   AND day_date <= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 365 DAY)) AS x;


CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yr_count
CLUSTER BY year_count
AS
SELECT COUNT(DISTINCT year_num) AS year_count
FROM week_53_yrs AS x;


CREATE TEMPORARY TABLE IF NOT EXISTS realigned_fiscal_calendar (
day_date DATE,
day_num INTEGER,
day_desc string,
week_num INTEGER,
week_desc string,
month_num INTEGER,
month_short_desc string,
quarter_num INTEGER,
halfyear_num INTEGER,
year_num INTEGER,
month_454_num INTEGER,
year_454_num INTEGER
) CLUSTER BY day_date;


INSERT INTO realigned_fiscal_calendar
(SELECT CASE
   WHEN year_num > (SELECT year_num
     FROM week_53_yrs
     WHERE recency_rank = 1)
   THEN day_date
   WHEN (SELECT *
      FROM week_53_yr_count) = 1 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 1)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 2 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 2 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 3 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 21 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 2)
   THEN DATE_ADD(day_date, INTERVAL 7 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 3)
   THEN DATE_ADD(day_date, INTERVAL 14 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num > (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 4)
   THEN DATE_ADD(day_date, INTERVAL 21 DAY)
   WHEN (SELECT *
      FROM week_53_yr_count) = 4 AND year_num <= (SELECT year_num
      FROM week_53_yrs
      WHERE recency_rank = 4)
   THEN DATE_ADD(day_date, INTERVAL 28 DAY)
   ELSE NULL
   END AS day_date,
  day_num,
  day_desc,
  week_num,
  week_desc,
  month_num,
  month_short_desc,
  quarter_num,
  halfyear_num,
  year_num,
  month_454_num,
  year_num AS year_454_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
 WHERE day_date >= DATE '2009-01-01'
  AND day_date <= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 365 DAY)
  AND week_of_fyr <> 53);


--COLLECT   STATISTICS COLUMN (MONTH_NUM) ON     realigned_fiscal_calendar


--COLLECT   STATISTICS COLUMN (DAY_DATE) ON     realigned_fiscal_calendar


CREATE TEMPORARY TABLE IF NOT EXISTS get_months
CLUSTER BY last_complete_mo
AS
SELECT DISTINCT earliest_minus_3,
 earliest_act_mo,
  last_complete_mo - 200 AS part1_end_mo,
  CASE
  WHEN MOD(last_complete_mo, 100) = 12
  THEN last_complete_mo - 111
  ELSE last_complete_mo - 199
  END AS part2_start_mo,
  last_complete_mo - 100 AS part2_end_mo,
  CASE
  WHEN MOD(last_complete_mo, 100) = 12
  THEN last_complete_mo - 11
  ELSE last_complete_mo - 99
  END AS part3_start_mo,
 last_complete_mo
FROM (SELECT DISTINCT CURRENT_DATE('PST8PDT') AS todays_date,
   month_num AS todays_month,
    CASE
    WHEN MOD(month_num, 100) = 1
    THEN month_num - 89
    ELSE month_num - 1
    END AS last_complete_mo,
    CASE
    WHEN MOD(month_num, 100) = 12
    THEN month_num - 211
    ELSE month_num - 299
    END AS earliest_act_mo,
    CASE
    WHEN MOD(month_num, 100) IN (1, 2)
    THEN month_num - 390
    ELSE month_num - 302
    END AS earliest_minus_3
  FROM realigned_fiscal_calendar AS a
  WHERE day_date = CURRENT_DATE('PST8PDT')) AS x;


CREATE TEMPORARY TABLE IF NOT EXISTS date_range
CLUSTER BY latest_mo_dt
AS
SELECT MIN(CASE
   WHEN month_num = (SELECT earliest_minus_3
     FROM get_months)
   THEN day_date
   ELSE NULL
   END) AS earliest_dt,
 MIN(CASE
   WHEN month_num = (SELECT earliest_act_mo
     FROM get_months)
   THEN day_date
   ELSE NULL
   END) AS earliest_act_dt,
 MAX(CASE
   WHEN month_num = (SELECT part1_end_mo
     FROM get_months)
   THEN day_date
   ELSE NULL
   END) AS part1_end_dt,
 MIN(CASE
   WHEN month_num = (SELECT part2_start_mo
     FROM get_months)
   THEN day_date
   ELSE NULL
   END) AS part2_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT part2_end_mo
     FROM get_months)
   THEN day_date
   ELSE NULL
   END) AS part2_end_dt,
 MIN(CASE
   WHEN month_num = (SELECT part3_start_mo
     FROM get_months)
   THEN day_date
   ELSE NULL
   END) AS part3_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT last_complete_mo
     FROM get_months)
   THEN day_date
   ELSE NULL
   END) AS latest_mo_dt
FROM realigned_fiscal_calendar;


CREATE TEMPORARY TABLE IF NOT EXISTS sls_olf_combined_pre
CLUSTER BY global_tran_id, line_item_seq_num
AS
SELECT DISTINCT a.global_tran_id,
 a.line_item_seq_num,
 LPAD(COALESCE(TRIM(a.sku_num), 'HI_MOM'), 15, '0') AS padded_sku,
 a.acp_id,
 COALESCE(a.order_date, a.tran_date) AS purch_dt,
 cal.month_num AS purch_mo,
 a.business_day_date,
 a.shipped_usd_sales AS gross_sales,
  CASE
  WHEN LOWER(a.line_item_order_type) LIKE LOWER('CustInit%') AND LOWER(a.line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(a.business_unit_desc) IN (LOWER('N.COM'), LOWER('N.CA'), LOWER('OFFPRICE ONLINE'), LOWER('TRUNK CLUB'
      ))
  THEN a.ringing_store_num
  ELSE a.intent_store_num
  END AS reporting_store_num,
  CASE
  WHEN LOWER(a.line_item_order_type) LIKE LOWER('CustInit%') AND LOWER(a.line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(a.business_unit_desc) IN (LOWER('N.COM'), LOWER('N.CA'), LOWER('OFFPRICE ONLINE'), LOWER('TRUNK CLUB'
      ))
  THEN '2) n.com'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN '1) NS'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN '2) n.com'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN '3) RS'
  WHEN LOWER(a.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN '4) nr.com'
  ELSE NULL
  END AS reporting_channel,
 a.intent_store_num,
 a.destination_node_num AS destination_store,
 a.destination_zip_code,
  CASE
  WHEN LOWER(UPPER(TRIM(a.business_unit_desc))) IN (LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('RACK CANADA'))
  THEN 'CA'
  ELSE 'US'
  END AS country,
 a.commission_slsprsn_num,
 COALESCE(a.order_num, '#' || FORMAT('%20d', a.global_tran_id)) AS order_num,
  CASE
  WHEN LOWER(a.nonmerch_fee_code) = LOWER('6666')
  THEN 1
  ELSE 0
  END AS bought_giftcard,
  CASE
  WHEN LOWER(boards.remote_sell_swimlane) IN (LOWER('STYLEBOARD_ATTRIBUTED'), LOWER('STYLEBOARD_PRIVATE_ATTRIBUTED'),
    LOWER('PERSONAL_REQUEST_A_LOOK_ATTRIB'), LOWER('PERSONAL_REQUEST_A_LOOK_ATTRIBUTED'), LOWER('REQUEST_A_LOOK_ATTRIBUTED'
     ), LOWER('CHAT_BOARD_ATTRIBUTED'))
  THEN 'A01) Style Boards'
  ELSE NULL
  END AS style_board_service,
  CASE
  WHEN LOWER(boards.remote_sell_swimlane) IN (LOWER('STYLELINK_ATTRIBUTED'), LOWER('STYLEBOARD_PUBLIC_ATTRIBUTED'))
  THEN 'A02) Style Links'
  ELSE NULL
  END AS style_link_service,
  CASE
  WHEN LOWER(boards.remote_sell_swimlane) IN (LOWER('PRIVATE_STYLING_ATTRIBUTED'), LOWER('ECF_ATTRIBUTED'))
  THEN 'A03) Style Board OR Style Link'
  ELSE NULL
  END AS private_styling_service,
  CASE
  WHEN LOWER(boards.remote_sell_swimlane) = LOWER('TRUNK_CLUB')
  THEN 'A04) Nordstrom Trunks'
  ELSE NULL
  END AS trunk_service,
  CASE
  WHEN a.shipped_usd_sales > 0
  THEN r.restaurant_service
  ELSE NULL
  END AS restaurant_service,
  CASE
  WHEN LOWER(a.upc_num) = LOWER('439027332977')
  THEN 'A05) Nordstrom To You'
  ELSE NULL
  END AS n2u_service,
  CASE
  WHEN LOWER(a.upc_num) = LOWER('439027332984')
  THEN 'A06) Nordstrom To You - Closet'
  ELSE NULL
  END AS n2u_closet_service,
  CASE
  WHEN LOWER(a.nonmerch_fee_code) IN (LOWER('1803'), LOWER('1926'))
  THEN 'C02) Non-Nordstrom Alterations'
  WHEN LOWER(a.nonmerch_fee_code) IN (LOWER('108'), LOWER('809'), LOWER('116'), LOWER('272'), LOWER('817'), LOWER('663'
     ))
  THEN 'C01) Nordstrom Alterations'
  WHEN LOWER(a.nonmerch_fee_code) IN (LOWER('3855'), LOWER('3786'))
  THEN 'C03) Rushed Alterations'
  WHEN LOWER(a.nonmerch_fee_code) = LOWER('647')
  THEN 'C04) Personalized Alterations'
  WHEN LOWER(a.nonmerch_fee_code) IN (LOWER('8571'), LOWER('8463'))
  THEN 'F04) Shoe Shine'
  ELSE NULL
  END AS fee_code_services,
 bsc.beauty_service,
  CASE
  WHEN LOWER(a.payroll_dept) IN (LOWER('1196'))
  THEN 'B04) Personal Stylist'
  WHEN LOWER(a.payroll_dept) IN (LOWER('1205'))
  THEN 'B01) Independent Stylist'
  WHEN LOWER(a.payroll_dept) IN (LOWER('1404'))
  THEN 'B02) Beauty Stylist'
  WHEN LOWER(a.hr_es_ind) IN (LOWER('Y'))
  THEN 'B03) Emerging Stylist'
  ELSE NULL
  END AS selling_service,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(CASE
           WHEN LOWER(a.payroll_dept) IN (LOWER('1196'))
           THEN 'B04) Personal Stylist'
           WHEN LOWER(a.payroll_dept) IN (LOWER('1205'))
           THEN 'B01) Independent Stylist'
           WHEN LOWER(a.payroll_dept) IN (LOWER('1404'))
           THEN 'B02) Beauty Stylist'
           WHEN LOWER(a.hr_es_ind) IN (LOWER('Y'))
           THEN 'B03) Emerging Stylist'
           ELSE NULL
           END, 'XYZ'), 1, 3)) IN (LOWER('B01'), LOWER('B03'), LOWER('B04')) AND (LOWER(COALESCE(a.line_item_order_type
           , 'XYZ')) <> LOWER('CustInitWebOrder') OR LOWER(COALESCE(a.line_item_fulfillment_type, 'XYZ')) <> LOWER('StorePickUp'
          )) AND a.shipped_usd_sales >= 0 AND LOWER(COALESCE(a.item_source, 'XYZ')) <> LOWER('SB_SALESEMPINIT') AND
    LOWER(SUBSTR(a.business_unit_desc, 1, 4)) IN (LOWER('FULL'))
  THEN 'A07) In-Store Styling'
  ELSE NULL
  END AS instore_styling_service,
  CASE
  WHEN LOWER(TRIM(UPPER(a.promise_type_code))) = LOWER('SAME_DAY_COURIER')
  THEN 'D04) Same Day Delivery'
  WHEN LOWER(a.requested_level_of_service_code) = LOWER('07')
  THEN 'E04) Next Day BOPUS'
  WHEN LOWER(UPPER(a.delivery_method_code)) = LOWER('PICK')
  THEN 'E03) Same Day BOPUS'
  WHEN LOWER(a.requested_level_of_service_code) = LOWER('11')
  THEN 'E05) Next Day Ship to Store'
  WHEN a.destination_node_num > 0 AND LOWER(UPPER(COALESCE(a.delivery_method_code, 'NONE'))) <> LOWER('PICK') AND LOWER(a
     .requested_level_of_service_code) = LOWER('42')
  THEN 'D02) Free Expedited Ship to Store'
  WHEN LOWER(a.requested_level_of_service_code) = LOWER('42')
  THEN 'D01) Free Expedited Delivery'
  WHEN a.destination_node_num > 0 AND LOWER(UPPER(COALESCE(a.delivery_method_code, 'NONE'))) <> LOWER('PICK')
  THEN 'E02) Ship to Store'
  WHEN LOWER(SUBSTR(UPPER(a.promise_type_code), 1, 3)) IN (LOWER('ONE'), LOWER('TWO'), LOWER('EXP')) OR LOWER(UPPER(a.promise_type_code
      )) LIKE LOWER('%BUSINESSDAY%')
  THEN 'D05) Paid Expedited Delivery'
  ELSE NULL
  END AS fulfillment_service,
  CASE
  WHEN LOWER(UPPER(TRIM(a.curbside_pickup_ind))) = LOWER('Y')
  THEN 'E01) Curbside Pickup'
  ELSE NULL
  END AS curbside_service
FROM t2dl_das_sales_returns.sales_and_returns_fact AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st2 ON a.return_ringing_store_num = st2.store_num
 LEFT JOIN (SELECT DISTINCT remote_sell_swimlane,
   CAST(LPAD(TRIM(upc_num), 32, '0')AS STRING) AS upc_num,
   CAST(floor(cast(global_tran_id as float64)) AS BIGINT) AS global_tran_id
  FROM t2dl_das_remote_selling.remote_sell_transactions
  WHERE LOWER(remote_sell_swimlane) LIKE LOWER('%ATTRIB%')
   OR LOWER(remote_sell_swimlane) IN (LOWER('TRUNK_CLUB'), LOWER('ECF_ATTRIBUTED')) AND upc_num IS NOT NULL AND
    global_tran_id IS NOT NULL) AS boards ON LOWER(TRIM(FORMAT('%20d', a.global_tran_id))) = LOWER(TRIM(FORMAT('%20d',
      boards.global_tran_id))) AND LOWER(SUBSTR(LPAD(TRIM(a.upc_num), 32, '0'), 1, 32)) = LOWER(boards.upc_num)
 LEFT JOIN (SELECT DISTINCT dept_num,
    CASE
    WHEN dept_num IN (113, 188, 571, 698)
    THEN 'G02) Coffee'
    WHEN dept_num IN (568, 692, 715)
    THEN 'G03) Bar'
    ELSE 'G01) Food'
    END AS restaurant_service
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim
  WHERE division_num = 70) AS r ON CAST(COALESCE(a.merch_dept_num, FORMAT('%11d', - 1 * a.intent_store_num)) AS FLOAT64)
  = r.dept_num
 LEFT JOIN (SELECT DISTINCT LPAD(COALESCE(TRIM(rms_sku_num), 'HI_MOM'), 15, '0') AS rms_sku_num,
   channel_country,
    CASE
    WHEN dept_num = 102
    THEN 'F01) Spa'
    WHEN dept_num = 497 AND (class_num IN (10, 11) OR LOWER(style_desc) LIKE LOWER('%NAIL%'))
    THEN 'F02) Nail Bar'
    WHEN dept_num = 497 AND class_num IN (12, 14)
    THEN 'F03) Eyebrows'
    ELSE NULL
    END AS beauty_service
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
  WHERE dept_num = 102
   OR dept_num = 497 AND (class_num IN (10, 11, 12, 14) OR LOWER(style_desc) LIKE LOWER('%NAIL%'))) AS bsc 
   ON LOWER(LPAD(COALESCE(TRIM(a
       .sku_num), 'HI_MOM'), 15, '0')) = LOWER(bsc.rms_sku_num) AND LOWER(CASE
     WHEN LOWER(UPPER(TRIM(a.business_unit_desc))) IN (LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('RACK CANADA'))
     THEN 'CA'
     ELSE 'US'
     END) = LOWER(bsc.channel_country)
 INNER JOIN realigned_fiscal_calendar AS cal ON COALESCE(a.order_date, a.tran_date) = cal.day_date
WHERE LOWER(a.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'))
 AND a.business_day_date >= (SELECT earliest_dt
   FROM date_range)
 AND a.business_day_date <= (SELECT latest_mo_dt
   FROM date_range)
 AND CASE
   WHEN a.shipped_usd_sales > 0
   THEN COALESCE(a.order_date, a.tran_date)
   ELSE a.tran_date
   END >= (SELECT earliest_dt
   FROM date_range)
 AND CASE
   WHEN a.shipped_usd_sales > 0
   THEN COALESCE(a.order_date, a.tran_date)
   ELSE a.tran_date
   END <= (SELECT latest_mo_dt
   FROM date_range)
 AND a.shipped_usd_sales >= 0
 AND COALESCE(a.employee_discount_usd_amt, 0) = 0
 AND a.acp_id IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS sls_olf_combined_returns
CLUSTER BY global_tran_id, line_item_seq_num
AS
SELECT DISTINCT a.global_tran_id,
 a.line_item_seq_num,
 COALESCE(a.return_date, DATE_ADD(a.business_day_date, INTERVAL 20000 DAY)) AS return_date,
 cal2.month_num AS return_mo,
 COALESCE(- 1 * a.return_usd_amt, 0) AS return_usd_amt,
 a.return_ringing_store_num AS return_store,
  CASE
  WHEN LOWER(SUBSTR(UPPER(st2.business_unit_desc), 1, 4)) IN (LOWER('FULL'), LOWER('RACK')) AND a.upc_num IS NOT NULL
  THEN 'I_) STORE RETURNS'
  ELSE NULL
  END AS store_return_service
FROM t2dl_das_sales_returns.sales_and_returns_fact AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st2 ON a.return_ringing_store_num = st2.store_num
 LEFT JOIN realigned_fiscal_calendar AS cal2 ON COALESCE(a.return_date, DATE_ADD(a.business_day_date, INTERVAL 20000 DAY
    )) = cal2.day_date
WHERE COALESCE(a.return_date, DATE_ADD(a.business_day_date, INTERVAL 20000 DAY)) >= (SELECT earliest_dt
   FROM date_range)
 AND COALESCE(a.return_date, DATE_ADD(a.business_day_date, INTERVAL 20000 DAY)) <= (SELECT latest_mo_dt
   FROM date_range)
 AND COALESCE(a.employee_discount_usd_amt, 0) = 0;


CREATE TEMPORARY TABLE IF NOT EXISTS sls_olf_combined
CLUSTER BY global_tran_id, line_item_seq_num
AS
SELECT a.global_tran_id,
 a.line_item_seq_num,
 a.padded_sku,
 a.acp_id,
 a.purch_dt,
 a.purch_mo,
 a.business_day_date,
 a.gross_sales,
 a.reporting_store_num,
 a.reporting_channel,
 a.intent_store_num,
 a.destination_store,
 a.destination_zip_code,
 a.country,
 a.commission_slsprsn_num,
 a.order_num,
 a.bought_giftcard,
 a.style_board_service,
 a.style_link_service,
 a.private_styling_service,
 a.trunk_service,
 a.restaurant_service,
 a.n2u_service,
 a.n2u_closet_service,
 a.fee_code_services,
 a.beauty_service,
 a.selling_service,
 a.instore_styling_service,
 a.fulfillment_service,
 a.curbside_service,
 b.return_date,
 b.return_mo,
 b.return_usd_amt,
 b.return_store,
 b.store_return_service
FROM sls_olf_combined_pre AS a
 LEFT JOIN sls_olf_combined_returns AS b ON a.global_tran_id = b.global_tran_id AND a.line_item_seq_num = b.line_item_seq_num;


CREATE TEMPORARY TABLE IF NOT EXISTS service_line_item_detail
CLUSTER BY global_tran_id, line_item_seq_num
AS
SELECT DISTINCT global_tran_id,
 line_item_seq_num,
 padded_sku,
 acp_id,
 reporting_store_num,
 reporting_channel,
 intent_store_num,
 destination_store,
 purch_dt,
 purch_mo,
 business_day_date,
  CASE
  WHEN bought_giftcard = 0
  THEN gross_sales
  ELSE 0
  END AS gross_usd_amt,
 return_date,
 return_mo,
  CASE
  WHEN bought_giftcard = 0
  THEN COALESCE(return_usd_amt, 0)
  ELSE 0
  END AS return_usd_amt,
 return_store,
                                CASE
                                WHEN private_styling_service IS NOT NULL
                                THEN 1
                                ELSE 0
                                END + CASE
                                WHEN style_board_service IS NOT NULL
                                THEN 1
                                ELSE 0
                                END + CASE
                               WHEN style_link_service IS NOT NULL
                               THEN 1
                               ELSE 0
                               END + CASE
                              WHEN trunk_service IS NOT NULL
                              THEN 1
                              ELSE 0
                              END + CASE
                             WHEN n2u_service IS NOT NULL
                             THEN 1
                             ELSE 0
                             END + CASE
                            WHEN n2u_closet_service IS NOT NULL
                            THEN 1
                            ELSE 0
                            END + CASE
                           WHEN instore_styling_service IS NOT NULL
                           THEN 1
                           ELSE 0
                           END + CASE
                          WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B01')
                          THEN 1
                          ELSE 0
                          END + CASE
                         WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B02')
                         THEN 1
                         ELSE 0
                         END + CASE
                        WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B03')
                        THEN 1
                        ELSE 0
                        END + CASE
                       WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B04')
                       THEN 1
                       ELSE 0
                       END + CASE
                      WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C01')
                      THEN 1
                      ELSE 0
                      END + CASE
                     WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C02')
                     THEN 1
                     ELSE 0
                     END + CASE
                    WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C03')
                    THEN 1
                    ELSE 0
                    END + CASE
                   WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C04')
                   THEN 1
                   ELSE 0
                   END + CASE
                  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D01')
                  THEN 1
                  ELSE 0
                  END + CASE
                 WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
                 THEN 1
                 ELSE 0
                 END + CASE
                WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D04')
                THEN 1
                ELSE 0
                END + CASE
               WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D05')
               THEN 1
               ELSE 0
               END + CASE
              WHEN curbside_service IS NOT NULL
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E02')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E03')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E04')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E05')
          THEN 1
          ELSE 0
          END + CASE
         WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F01')
         THEN 1
         ELSE 0
         END + CASE
        WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F02')
        THEN 1
        ELSE 0
        END + CASE
       WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F03')
       THEN 1
       ELSE 0
       END + CASE
      WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('F04')
      THEN 1
      ELSE 0
      END + CASE
     WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G01')
     THEN 1
     ELSE 0
     END + CASE
    WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G02')
    THEN 1
    ELSE 0
    END + CASE
   WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G03')
   THEN 1
   ELSE 0
   END AS service_count,
  CASE
  WHEN CASE
                                  WHEN private_styling_service IS NOT NULL
                                  THEN 1
                                  ELSE 0
                                  END + CASE
                                  WHEN style_board_service IS NOT NULL
                                  THEN 1
                                  ELSE 0
                                  END + CASE
                                 WHEN style_link_service IS NOT NULL
                                 THEN 1
                                 ELSE 0
                                 END + CASE
                                WHEN trunk_service IS NOT NULL
                                THEN 1
                                ELSE 0
                                END + CASE
                               WHEN n2u_service IS NOT NULL
                               THEN 1
                               ELSE 0
                               END + CASE
                              WHEN n2u_closet_service IS NOT NULL
                              THEN 1
                              ELSE 0
                              END + CASE
                             WHEN instore_styling_service IS NOT NULL
                             THEN 1
                             ELSE 0
                             END + CASE
                            WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B01')
                            THEN 1
                            ELSE 0
                            END + CASE
                           WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B02')
                           THEN 1
                           ELSE 0
                           END + CASE
                          WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B03')
                          THEN 1
                          ELSE 0
                          END + CASE
                         WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B04')
                         THEN 1
                         ELSE 0
                         END + CASE
                        WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C01')
                        THEN 1
                        ELSE 0
                        END + CASE
                       WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C02')
                       THEN 1
                       ELSE 0
                       END + CASE
                      WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C03')
                      THEN 1
                      ELSE 0
                      END + CASE
                     WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C04')
                     THEN 1
                     ELSE 0
                     END + CASE
                    WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D01')
                    THEN 1
                    ELSE 0
                    END + CASE
                   WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
                   THEN 1
                   ELSE 0
                   END + CASE
                  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D04')
                  THEN 1
                  ELSE 0
                  END + CASE
                 WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D05')
                 THEN 1
                 ELSE 0
                 END + CASE
                WHEN curbside_service IS NOT NULL
                THEN 1
                ELSE 0
                END + CASE
               WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E02')
               THEN 1
               ELSE 0
               END + CASE
              WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E03')
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E04')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E05')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F01')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F02')
          THEN 1
          ELSE 0
          END + CASE
         WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F03')
         THEN 1
         ELSE 0
         END + CASE
        WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('F04')
        THEN 1
        ELSE 0
        END + CASE
       WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G01')
       THEN 1
       ELSE 0
       END + CASE
      WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G02')
      THEN 1
      ELSE 0
      END + CASE
     WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G03')
     THEN 1
     ELSE 0
     END > 0
  THEN 1
  ELSE 0
  END AS any_service,
 ROUND(CAST(CASE
    WHEN bought_giftcard = 1
    THEN 0
    WHEN CASE
                                    WHEN private_styling_service IS NOT NULL
                                    THEN 1
                                    ELSE 0
                                    END + CASE
                                    WHEN style_board_service IS NOT NULL
                                    THEN 1
                                    ELSE 0
                                    END + CASE
                                   WHEN style_link_service IS NOT NULL
                                   THEN 1
                                   ELSE 0
                                   END + CASE
                                  WHEN trunk_service IS NOT NULL
                                  THEN 1
                                  ELSE 0
                                  END + CASE
                                 WHEN n2u_service IS NOT NULL
                                 THEN 1
                                 ELSE 0
                                 END + CASE
                                WHEN n2u_closet_service IS NOT NULL
                                THEN 1
                                ELSE 0
                                END + CASE
                               WHEN instore_styling_service IS NOT NULL
                               THEN 1
                               ELSE 0
                               END + CASE
                              WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B01')
                              THEN 1
                              ELSE 0
                              END + CASE
                             WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B02')
                             THEN 1
                             ELSE 0
                             END + CASE
                            WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B03')
                            THEN 1
                            ELSE 0
                            END + CASE
                           WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B04')
                           THEN 1
                           ELSE 0
                           END + CASE
                          WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C01')
                          THEN 1
                          ELSE 0
                          END + CASE
                         WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C02')
                         THEN 1
                         ELSE 0
                         END + CASE
                        WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C03')
                        THEN 1
                        ELSE 0
                        END + CASE
                       WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C04')
                       THEN 1
                       ELSE 0
                       END + CASE
                      WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D01')
                      THEN 1
                      ELSE 0
                      END + CASE
                     WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
                     THEN 1
                     ELSE 0
                     END + CASE
                    WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D04')
                    THEN 1
                    ELSE 0
                    END + CASE
                   WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D05')
                   THEN 1
                   ELSE 0
                   END + CASE
                  WHEN curbside_service IS NOT NULL
                  THEN 1
                  ELSE 0
                  END + CASE
                 WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E02')
                 THEN 1
                 ELSE 0
                 END + CASE
                WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E03')
                THEN 1
                ELSE 0
                END + CASE
               WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E04')
               THEN 1
               ELSE 0
               END + CASE
              WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E05')
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F01')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F02')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F03')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('F04')
          THEN 1
          ELSE 0
          END + CASE
         WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G01')
         THEN 1
         ELSE 0
         END + CASE
        WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G02')
        THEN 1
        ELSE 0
        END + CASE
       WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G03')
       THEN 1
       ELSE 0
       END > 0
    THEN CAST(gross_sales AS FLOAT64) / CAST((CASE
                                      WHEN private_styling_service IS NOT NULL
                                      THEN 1
                                      ELSE 0
                                      END + CASE
                                      WHEN style_board_service IS NOT NULL
                                      THEN 1
                                      ELSE 0
                                      END + CASE
                                     WHEN style_link_service IS NOT NULL
                                     THEN 1
                                     ELSE 0
                                     END + CASE
                                    WHEN trunk_service IS NOT NULL
                                    THEN 1
                                    ELSE 0
                                    END + CASE
                                   WHEN n2u_service IS NOT NULL
                                   THEN 1
                                   ELSE 0
                                   END + CASE
                                  WHEN n2u_closet_service IS NOT NULL
                                  THEN 1
                                  ELSE 0
                                  END + CASE
                                 WHEN instore_styling_service IS NOT NULL
                                 THEN 1
                                 ELSE 0
                                 END + CASE
                                WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B01')
                                THEN 1
                                ELSE 0
                                END + CASE
                               WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B02')
                               THEN 1
                               ELSE 0
                               END + CASE
                              WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B03')
                              THEN 1
                              ELSE 0
                              END + CASE
                             WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B04')
                             THEN 1
                             ELSE 0
                             END + CASE
                            WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C01')
                            THEN 1
                            ELSE 0
                            END + CASE
                           WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C02')
                           THEN 1
                           ELSE 0
                           END + CASE
                          WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C03')
                          THEN 1
                          ELSE 0
                          END + CASE
                         WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C04')
                         THEN 1
                         ELSE 0
                         END + CASE
                        WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D01')
                        THEN 1
                        ELSE 0
                        END + CASE
                       WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
                       THEN 1
                       ELSE 0
                       END + CASE
                      WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D04')
                      THEN 1
                      ELSE 0
                      END + CASE
                     WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D05')
                     THEN 1
                     ELSE 0
                     END + CASE
                    WHEN curbside_service IS NOT NULL
                    THEN 1
                    ELSE 0
                    END + CASE
                   WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E02')
                   THEN 1
                   ELSE 0
                   END + CASE
                  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E03')
                  THEN 1
                  ELSE 0
                  END + CASE
                 WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E04')
                 THEN 1
                 ELSE 0
                 END + CASE
                WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E05')
                THEN 1
                ELSE 0
                END + CASE
               WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F01')
               THEN 1
               ELSE 0
               END + CASE
              WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F02')
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F03')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('F04')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G01')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G02')
          THEN 1
          ELSE 0
          END) + CASE
        WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G03')
        THEN 1
        ELSE 0
        END AS FLOAT64)
    ELSE CAST(gross_sales AS FLOAT64)
    END AS NUMERIC), 2) AS gross_usd_amt_svc_split,
 ROUND(CAST(CASE
    WHEN bought_giftcard = 1
    THEN 0
    WHEN CASE
                                    WHEN private_styling_service IS NOT NULL
                                    THEN 1
                                    ELSE 0
                                    END + CASE
                                    WHEN style_board_service IS NOT NULL
                                    THEN 1
                                    ELSE 0
                                    END + CASE
                                   WHEN style_link_service IS NOT NULL
                                   THEN 1
                                   ELSE 0
                                   END + CASE
                                  WHEN trunk_service IS NOT NULL
                                  THEN 1
                                  ELSE 0
                                  END + CASE
                                 WHEN n2u_service IS NOT NULL
                                 THEN 1
                                 ELSE 0
                                 END + CASE
                                WHEN n2u_closet_service IS NOT NULL
                                THEN 1
                                ELSE 0
                                END + CASE
                               WHEN instore_styling_service IS NOT NULL
                               THEN 1
                               ELSE 0
                               END + CASE
                              WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B01')
                              THEN 1
                              ELSE 0
                              END + CASE
                             WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B02')
                             THEN 1
                             ELSE 0
                             END + CASE
                            WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B03')
                            THEN 1
                            ELSE 0
                            END + CASE
                           WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B04')
                           THEN 1
                           ELSE 0
                           END + CASE
                          WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C01')
                          THEN 1
                          ELSE 0
                          END + CASE
                         WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C02')
                         THEN 1
                         ELSE 0
                         END + CASE
                        WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C03')
                        THEN 1
                        ELSE 0
                        END + CASE
                       WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C04')
                       THEN 1
                       ELSE 0
                       END + CASE
                      WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D01')
                      THEN 1
                      ELSE 0
                      END + CASE
                     WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
                     THEN 1
                     ELSE 0
                     END + CASE
                    WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D04')
                    THEN 1
                    ELSE 0
                    END + CASE
                   WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D05')
                   THEN 1
                   ELSE 0
                   END + CASE
                  WHEN curbside_service IS NOT NULL
                  THEN 1
                  ELSE 0
                  END + CASE
                 WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E02')
                 THEN 1
                 ELSE 0
                 END + CASE
                WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E03')
                THEN 1
                ELSE 0
                END + CASE
               WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E04')
               THEN 1
               ELSE 0
               END + CASE
              WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E05')
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F01')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F02')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F03')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('F04')
          THEN 1
          ELSE 0
          END + CASE
         WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G01')
         THEN 1
         ELSE 0
         END + CASE
        WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G02')
        THEN 1
        ELSE 0
        END + CASE
       WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G03')
       THEN 1
       ELSE 0
       END > 0
    THEN CAST(COALESCE(return_usd_amt, 0) AS FLOAT64) / CAST((CASE
                                      WHEN private_styling_service IS NOT NULL
                                      THEN 1
                                      ELSE 0
                                      END + CASE
                                      WHEN style_board_service IS NOT NULL
                                      THEN 1
                                      ELSE 0
                                      END + CASE
                                     WHEN style_link_service IS NOT NULL
                                     THEN 1
                                     ELSE 0
                                     END + CASE
                                    WHEN trunk_service IS NOT NULL
                                    THEN 1
                                    ELSE 0
                                    END + CASE
                                   WHEN n2u_service IS NOT NULL
                                   THEN 1
                                   ELSE 0
                                   END + CASE
                                  WHEN n2u_closet_service IS NOT NULL
                                  THEN 1
                                  ELSE 0
                                  END + CASE
                                 WHEN instore_styling_service IS NOT NULL
                                 THEN 1
                                 ELSE 0
                                 END + CASE
                                WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B01')
                                THEN 1
                                ELSE 0
                                END + CASE
                               WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B02')
                               THEN 1
                               ELSE 0
                               END + CASE
                              WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B03')
                              THEN 1
                              ELSE 0
                              END + CASE
                             WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B04')
                             THEN 1
                             ELSE 0
                             END + CASE
                            WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C01')
                            THEN 1
                            ELSE 0
                            END + CASE
                           WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C02')
                           THEN 1
                           ELSE 0
                           END + CASE
                          WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C03')
                          THEN 1
                          ELSE 0
                          END + CASE
                         WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C04')
                         THEN 1
                         ELSE 0
                         END + CASE
                        WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D01')
                        THEN 1
                        ELSE 0
                        END + CASE
                       WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
                       THEN 1
                       ELSE 0
                       END + CASE
                      WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D04')
                      THEN 1
                      ELSE 0
                      END + CASE
                     WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D05')
                     THEN 1
                     ELSE 0
                     END + CASE
                    WHEN curbside_service IS NOT NULL
                    THEN 1
                    ELSE 0
                    END + CASE
                   WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E02')
                   THEN 1
                   ELSE 0
                   END + CASE
                  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E03')
                  THEN 1
                  ELSE 0
                  END + CASE
                 WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E04')
                 THEN 1
                 ELSE 0
                 END + CASE
                WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E05')
                THEN 1
                ELSE 0
                END + CASE
               WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F01')
               THEN 1
               ELSE 0
               END + CASE
              WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F02')
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F03')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('F04')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G01')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G02')
          THEN 1
          ELSE 0
          END) + CASE
        WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G03')
        THEN 1
        ELSE 0
        END AS FLOAT64)
    ELSE CAST(COALESCE(return_usd_amt, 0) AS FLOAT64)
    END AS NUMERIC), 2) AS return_usd_amt_svc_split,
        CASE
        WHEN private_styling_service IS NOT NULL OR style_board_service IS NOT NULL OR style_link_service IS NOT NULL OR
            trunk_service IS NOT NULL OR n2u_service IS NOT NULL OR n2u_closet_service IS NOT NULL OR
         instore_styling_service IS NOT NULL
        THEN 1
        ELSE 0
        END + CASE
        WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 1)) = LOWER('B')
        THEN 1
        ELSE 0
        END + CASE
       WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('C')
       THEN 1
       ELSE 0
       END + CASE
      WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('D')
      THEN 1
      ELSE 0
      END + CASE
     WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('E') OR curbside_service IS NOT NULL OR
       LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
     THEN 1
     ELSE 0
     END + CASE
    WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 1)) = LOWER('F') OR LOWER(SUBSTR(COALESCE(fee_code_services,
         'XYZ'), 1, 1)) = LOWER('F')
    THEN 1
    ELSE 0
    END + CASE
   WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 1)) = LOWER('G')
   THEN 1
   ELSE 0
   END AS service_group_count,
 ROUND(CAST(CASE
    WHEN CASE
            WHEN private_styling_service IS NOT NULL OR style_board_service IS NOT NULL OR style_link_service IS NOT NULL OR trunk_service IS NOT NULL OR n2u_service IS NOT NULL OR n2u_closet_service IS NOT NULL OR instore_styling_service IS NOT NULL
            THEN 1
            ELSE 0
            END + CASE
            WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 1)) = LOWER('B')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('C')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('D')
          THEN 1
          ELSE 0
          END + CASE
         WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('E') OR curbside_service IS NOT NULL OR LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
         THEN 1
         ELSE 0
         END + CASE
        WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 1)) = LOWER('F') OR LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('F')
        THEN 1
        ELSE 0
        END + CASE
       WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 1)) = LOWER('G')
       THEN 1
       ELSE 0
       END > 0
    THEN CAST(gross_sales AS FLOAT64) / CAST((CASE
              WHEN private_styling_service IS NOT NULL OR style_board_service IS NOT NULL OR style_link_service IS NOT NULL OR trunk_service IS NOT NULL OR n2u_service IS NOT NULL OR n2u_closet_service IS NOT NULL OR instore_styling_service IS NOT NULL
              THEN 1
              ELSE 0
              END + CASE
              WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 1)) = LOWER('B')
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('C')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('D')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('E') OR curbside_service IS NOT NULL OR LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 1)) = LOWER('F') OR LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('F')
          THEN 1
          ELSE 0
          END) + CASE
        WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 1)) = LOWER('G')
        THEN 1
        ELSE 0
        END AS FLOAT64)
    ELSE CAST(gross_sales AS FLOAT64)
    END AS NUMERIC), 2) AS gross_usd_amt_svcgrp_split,
 ROUND(CAST(CASE
    WHEN CASE
            WHEN private_styling_service IS NOT NULL OR style_board_service IS NOT NULL OR style_link_service IS NOT NULL OR trunk_service IS NOT NULL OR n2u_service IS NOT NULL OR n2u_closet_service IS NOT NULL OR instore_styling_service IS NOT NULL
            THEN 1
            ELSE 0
            END + CASE
            WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 1)) = LOWER('B')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('C')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('D')
          THEN 1
          ELSE 0
          END + CASE
         WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('E') OR curbside_service IS NOT NULL OR LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
         THEN 1
         ELSE 0
         END + CASE
        WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 1)) = LOWER('F') OR LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('F')
        THEN 1
        ELSE 0
        END + CASE
       WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 1)) = LOWER('G')
       THEN 1
       ELSE 0
       END > 0
    THEN CAST(COALESCE(return_usd_amt, 0) AS FLOAT64) / CAST((CASE
              WHEN private_styling_service IS NOT NULL OR style_board_service IS NOT NULL OR style_link_service IS NOT NULL OR trunk_service IS NOT NULL OR n2u_service IS NOT NULL OR n2u_closet_service IS NOT NULL OR instore_styling_service IS NOT NULL
              THEN 1
              ELSE 0
              END + CASE
              WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 1)) = LOWER('B')
              THEN 1
              ELSE 0
              END + CASE
             WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('C')
             THEN 1
             ELSE 0
             END + CASE
            WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('D')
            THEN 1
            ELSE 0
            END + CASE
           WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('E') OR curbside_service IS NOT NULL OR LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
           THEN 1
           ELSE 0
           END + CASE
          WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 1)) = LOWER('F') OR LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('F')
          THEN 1
          ELSE 0
          END) + CASE
        WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 1)) = LOWER('G')
        THEN 1
        ELSE 0
        END AS FLOAT64)
    ELSE CAST(COALESCE(return_usd_amt, 0) AS FLOAT64)
    END AS NUMERIC), 2) AS return_usd_amt_svcgrp_split,
  CASE
  WHEN private_styling_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_private_styling,
  CASE
  WHEN style_board_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_style_board,
  CASE
  WHEN style_link_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_style_link,
  CASE
  WHEN trunk_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_trunk,
  CASE
  WHEN n2u_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_n2u,
  CASE
  WHEN n2u_closet_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_n2u_closet,
  CASE
  WHEN instore_styling_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_instore_styling,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B01')
  THEN 1
  ELSE 0
  END AS svc_independent_stylist,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B02')
  THEN 1
  ELSE 0
  END AS svc_beauty_stylist,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B03')
  THEN 1
  ELSE 0
  END AS svc_emerging_stylist,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 3)) = LOWER('B04')
  THEN 1
  ELSE 0
  END AS svc_personal_stylist,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C01')
  THEN 1
  ELSE 0
  END AS svc_nordstrom_alterations,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C02')
  THEN 1
  ELSE 0
  END AS svc_nonnord_alterations,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C03')
  THEN 1
  ELSE 0
  END AS svc_rushed_alterations,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('C04')
  THEN 1
  ELSE 0
  END AS svc_personalized_alterations,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D01')
  THEN 1
  ELSE 0
  END AS svc_free_exp_delivery,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D02')
  THEN 1
  ELSE 0
  END AS svc_free_exp_ship_to_store,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) IN (LOWER('D01'), LOWER('D02'))
  THEN 1
  ELSE 0
  END AS svc_any_free_exp,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D04')
  THEN 1
  ELSE 0
  END AS svc_same_day_delivery,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('D05')
  THEN 1
  ELSE 0
  END AS svc_paid_exp_delivery,
  CASE
  WHEN curbside_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svc_curbside_pickup,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E02')
  THEN 1
  ELSE 0
  END AS svc_ship_to_store,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E03')
  THEN 1
  ELSE 0
  END AS svc_same_day_bopus,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E04')
  THEN 1
  ELSE 0
  END AS svc_next_day_bopus,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) = LOWER('E05')
  THEN 1
  ELSE 0
  END AS svc_next_day_ship_to_store,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) IN (LOWER('E04'), LOWER('E05'))
  THEN 1
  ELSE 0
  END AS svc_any_next_day_pickup,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) IN (LOWER('D02'), LOWER('E02'), LOWER('E03'), LOWER('E04'
     ), LOWER('E05'))
  THEN 1
  ELSE 0
  END AS svc_any_order_pickup,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F01')
  THEN 1
  ELSE 0
  END AS svc_spa,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F02')
  THEN 1
  ELSE 0
  END AS svc_nail_bar,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 3)) = LOWER('F03')
  THEN 1
  ELSE 0
  END AS svc_eyebrows,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 3)) = LOWER('F04')
  THEN 1
  ELSE 0
  END AS svc_shoe_shine,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G01')
  THEN 1
  ELSE 0
  END AS svc_food,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G02')
  THEN 1
  ELSE 0
  END AS svc_coffee,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 3)) = LOWER('G03')
  THEN 1
  ELSE 0
  END AS svc_bar,
  CASE
  WHEN private_styling_service IS NOT NULL OR style_board_service IS NOT NULL OR style_link_service IS NOT NULL OR
      trunk_service IS NOT NULL OR n2u_service IS NOT NULL OR n2u_closet_service IS NOT NULL OR instore_styling_service
   IS NOT NULL
  THEN 1
  ELSE 0
  END AS svcgroup_styling,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(selling_service, 'XYZ'), 1, 1)) = LOWER('B')
  THEN 1
  ELSE 0
  END AS svcgroup_selling_rel,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fee_code_services, 'XYZ'), 1, 1)) = LOWER('C')
  THEN 1
  ELSE 0
  END AS svcgroup_alterations,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('D')
  THEN 1
  ELSE 0
  END AS svcgroup_exp_delivery,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 1)) = LOWER('E') OR curbside_service IS NOT NULL OR LOWER(SUBSTR(COALESCE(fulfillment_service
       , 'XYZ'), 1, 3)) = LOWER('D02')
  THEN 1
  ELSE 0
  END AS svcgroup_pickup,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(beauty_service, 'XYZ'), 1, 1)) = LOWER('F') OR LOWER(SUBSTR(COALESCE(fee_code_services,
       'XYZ'), 1, 1)) = LOWER('F')
  THEN 1
  ELSE 0
  END AS svcgrp_in_store,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(restaurant_service, 'XYZ'), 1, 1)) = LOWER('G')
  THEN 1
  ELSE 0
  END AS svcgrp_restaurant,
  CASE
  WHEN store_return_service IS NOT NULL
  THEN 1
  ELSE 0
  END AS svcgrp_store_returns,
 destination_zip_code,
 private_styling_service,
 style_board_service,
 style_link_service,
 trunk_service,
 beauty_service,
 restaurant_service,
 fee_code_services,
 n2u_service,
 n2u_closet_service,
 instore_styling_service,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) IN (LOWER('D01'), LOWER('D02'))
  THEN 'D03) Any Free Expedited'
  ELSE NULL
  END AS any_free_exp_service,
 fulfillment_service,
  CASE
  WHEN LOWER(SUBSTR(COALESCE(fulfillment_service, 'XYZ'), 1, 3)) IN (LOWER('E04'), LOWER('E05'))
  THEN 'E06) Any Next-Day Pickup'
  ELSE NULL
  END AS any_next_day_pickup_service,
 store_return_service,
 curbside_service,
 selling_service
FROM sls_olf_combined AS b
WHERE purch_dt >= (SELECT earliest_dt
    FROM date_range) AND purch_dt <= (SELECT latest_mo_dt
    FROM date_range)
 OR return_date >= (SELECT earliest_dt
    FROM date_range) AND return_date <= (SELECT latest_mo_dt
    FROM date_range);


CREATE TEMPORARY TABLE IF NOT EXISTS acp_id_service_month_temp (
acp_id string,
service_mo NUMERIC(6),
service string,
customer_qualifier INTEGER,
nseam_only_ind INTEGER,
gross_usd_amt_whole NUMERIC(20,2),
net_usd_amt_whole NUMERIC(20,2),
gross_usd_amt_split NUMERIC(20,2),
net_usd_amt_split NUMERIC(20,2)
) CLUSTER BY acp_id, service_mo, service;


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'Z_) Nordstrom' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  private_styling_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND private_styling_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  private_styling_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  style_board_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND style_board_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  style_board_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  style_link_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND style_link_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  style_link_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  n2u_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND n2u_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  n2u_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  n2u_closet_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND n2u_closet_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  n2u_closet_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  beauty_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND beauty_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  beauty_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  restaurant_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND restaurant_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  restaurant_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  fee_code_services AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND fee_code_services IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  fee_code_services,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  fulfillment_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND fulfillment_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  fulfillment_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  curbside_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND curbside_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  curbside_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  selling_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND selling_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  selling_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  trunk_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND trunk_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  trunk_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  instore_styling_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  SUM(gross_usd_amt_svc_split) AS gross_usd_amt_split,
  SUM(gross_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND instore_styling_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  instore_styling_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'Z_) Nordstrom' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  private_styling_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND private_styling_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  private_styling_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  style_board_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND style_board_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  style_board_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  style_link_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND style_link_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  style_link_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  n2u_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND n2u_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  n2u_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  n2u_closet_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND n2u_closet_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  n2u_closet_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  beauty_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND beauty_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  beauty_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  restaurant_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND restaurant_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  restaurant_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  fee_code_services AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND fee_code_services IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  fee_code_services,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  fulfillment_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND fulfillment_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  fulfillment_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  curbside_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND curbside_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  curbside_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  selling_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND selling_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  selling_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  trunk_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND trunk_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  trunk_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  instore_styling_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  SUM(return_usd_amt_svc_split) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND instore_styling_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  instore_styling_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  store_return_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND store_return_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  store_return_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'A_) Styling GROUP' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_private_styling + svc_style_board + svc_style_link + svc_trunk + svc_n2u + svc_n2u_closet + svc_instore_styling)) AS NUMERIC)
  AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_private_styling + svc_style_board + svc_style_link + svc_trunk + svc_n2u + svc_n2u_closet + svc_instore_styling)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE svcgroup_styling = 1
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'B_) Selling Relationship GROUP' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_independent_stylist + svc_beauty_stylist + svc_emerging_stylist + svc_personal_stylist)) AS NUMERIC)
  AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_independent_stylist + svc_beauty_stylist + svc_emerging_stylist + svc_personal_stylist)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE svcgroup_selling_rel = 1
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'C_) Alterations GROUP' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_nordstrom_alterations + svc_nonnord_alterations + svc_rushed_alterations + svc_personalized_alterations)) AS NUMERIC)
  AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_nordstrom_alterations + svc_nonnord_alterations + svc_rushed_alterations + svc_personalized_alterations)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE svcgroup_alterations = 1
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'D_) Expedited Delivery GROUP' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_free_exp_delivery + svc_free_exp_ship_to_store + svc_same_day_delivery + svc_paid_exp_delivery)) AS NUMERIC)
  AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_free_exp_delivery + svc_free_exp_ship_to_store + svc_same_day_delivery + svc_paid_exp_delivery)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE svcgroup_exp_delivery = 1
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'E_) Order Pickup GROUP' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_free_exp_ship_to_store + svc_curbside_pickup + svc_ship_to_store + svc_same_day_bopus + svc_next_day_bopus + svc_next_day_ship_to_store)) AS NUMERIC)
  AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_free_exp_ship_to_store + svc_curbside_pickup + svc_ship_to_store + svc_same_day_bopus + svc_next_day_bopus + svc_next_day_ship_to_store)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE svcgroup_pickup = 1
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'F_) In-Store Services GROUP' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_spa + svc_nail_bar + svc_eyebrows + svc_shoe_shine)) AS NUMERIC) AS
  gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_spa + svc_nail_bar + svc_eyebrows + svc_shoe_shine)) AS NUMERIC) AS
  net_usd_amt_split
 FROM service_line_item_detail
 WHERE svcgrp_in_store = 1
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'G_) Restaurant GROUP' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_food + svc_coffee + svc_bar)) AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_food + svc_coffee + svc_bar)) AS NUMERIC) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE svcgrp_restaurant = 1
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  any_free_exp_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_free_exp_delivery + svc_free_exp_ship_to_store)) AS NUMERIC) AS
  gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_free_exp_delivery + svc_free_exp_ship_to_store)) AS NUMERIC) AS
  net_usd_amt_split
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND any_free_exp_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  any_free_exp_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  any_next_day_pickup_service AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt_svc_split * (svc_next_day_bopus + svc_next_day_ship_to_store)) AS NUMERIC) AS
  gross_usd_amt_split,
  CAST(SUM(gross_usd_amt_svc_split * (svc_next_day_bopus + svc_next_day_ship_to_store)) AS NUMERIC) AS net_usd_amt_split
  
 FROM service_line_item_detail
 WHERE purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
  AND any_next_day_pickup_service IS NOT NULL
 GROUP BY acp_id,
  purch_mo,
  any_next_day_pickup_service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(purch_mo AS NUMERIC) AS service_mo,
  'H_) Any Service Engagement' AS service,
  1 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(gross_usd_amt) AS NUMERIC) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE service_count > 0
  AND purch_mo >= (SELECT earliest_act_mo
    FROM get_months)
  AND purch_mo <= (SELECT last_complete_mo
    FROM get_months)
 GROUP BY acp_id,
  purch_mo,
  service,
  customer_qualifier,
  nseam_only_ind);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'A_) Styling GROUP' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_private_styling + svc_style_board + svc_style_link + svc_trunk + svc_n2u + svc_n2u_closet + svc_instore_styling)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND svcgroup_styling = 1
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'B_) Selling Relationship GROUP' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_independent_stylist + svc_beauty_stylist + svc_emerging_stylist + svc_personal_stylist)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND svcgroup_selling_rel = 1
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'C_) Alterations GROUP' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_nordstrom_alterations + svc_nonnord_alterations + svc_rushed_alterations + svc_personalized_alterations)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND svcgroup_alterations = 1
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'D_) Expedited Delivery GROUP' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_free_exp_delivery + svc_free_exp_ship_to_store + svc_same_day_delivery + svc_paid_exp_delivery)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND svcgroup_exp_delivery = 1
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'E_) Order Pickup GROUP' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_free_exp_ship_to_store + svc_curbside_pickup + svc_ship_to_store + svc_same_day_bopus + svc_next_day_bopus + svc_next_day_ship_to_store)) AS NUMERIC)
  AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND svcgroup_pickup = 1
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'F_) In-Store Services GROUP' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_spa + svc_nail_bar + svc_eyebrows + svc_shoe_shine)) AS NUMERIC) AS
  net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND svcgrp_in_store = 1
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'G_) Restaurant GROUP' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_food + svc_coffee + svc_bar)) AS NUMERIC) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND svcgrp_restaurant = 1
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  any_free_exp_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_free_exp_delivery + svc_free_exp_ship_to_store)) AS NUMERIC) AS
  net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND any_free_exp_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  any_free_exp_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  any_next_day_pickup_service AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt_svc_split * (svc_next_day_bopus + svc_next_day_ship_to_store)) AS NUMERIC) AS
  net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND any_next_day_pickup_service IS NOT NULL
 GROUP BY acp_id,
  return_mo,
  any_next_day_pickup_service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  CAST(return_mo AS NUMERIC) AS service_mo,
  'H_) Any Service Engagement' AS service,
  0 AS customer_qualifier,
  0 AS nseam_only_ind,
  CAST(0 AS NUMERIC) AS gross_usd_amt_whole,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_whole,
  CAST(0 AS NUMERIC) AS gross_usd_amt_split,
  CAST(SUM(return_usd_amt) AS NUMERIC) AS net_usd_amt_split
 FROM service_line_item_detail
 WHERE COALESCE(return_mo, 999999) >= (SELECT earliest_act_mo
    FROM get_months)
  AND COALESCE(return_mo, 999999) <= (SELECT last_complete_mo
    FROM get_months)
  AND service_count > 0
 GROUP BY acp_id,
  return_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  gross_usd_amt_whole,
  gross_usd_amt_split);


CREATE TEMPORARY TABLE IF NOT EXISTS nseam_wide
CLUSTER BY ticket_id, month_num
AS
SELECT xr.acp_id,
 a.ticket_id,
 b.month_num,
 MAX(ntg.nord_ind) AS nord_ind,
 MAX(ntg.nonnord_ind) AS nonnord_ind,
 MAX(ntg.rushed_ind) AS rushed_ind,
 MAX(ntg.pers_ind) AS pers_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.alteration_ticket_fact AS a
 INNER JOIN (SELECT ticket_id,
   MAX(CASE
     WHEN LOWER(UPPER(garment_origin)) LIKE LOWER('%COG%') OR LOWER(UPPER(garment_origin)) IN (LOWER('OUTSIDE MERCHANDISE'
         ), LOWER('UNDEFINED'))
     THEN 1
     ELSE 0
     END) AS nonnord_ind,
   MAX(CASE
     WHEN LOWER(UPPER(garment_origin)) NOT LIKE LOWER('%COG%') AND LOWER(UPPER(garment_origin)) NOT IN (LOWER('OUTSIDE MERCHANDISE'
         ), LOWER('UNDEFINED'))
     THEN 1
     ELSE 0
     END) AS nord_ind,
   MAX(CASE
     WHEN COALESCE(expedite_fee, 0) > 0
     THEN 1
     ELSE 0
     END) AS rushed_ind,
   MAX(CASE
     WHEN LOWER(UPPER(SUBSTR(garment_type, 1, 8))) = LOWER('MONOGRAM')
     THEN 1
     ELSE 0
     END) AS pers_ind
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.alteration_ticket_garment_fact
  WHERE LOWER(LOWER(garment_status)) NOT LIKE LOWER('%delete%')
  GROUP BY ticket_id) AS ntg ON a.ticket_id = ntg.ticket_id
 INNER JOIN realigned_fiscal_calendar AS b ON a.ticket_date = b.day_date
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON a.store_origin = st.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.acp_analytical_cust_xref AS xr ON LOWER(a.customer_id) = LOWER(xr.cust_id)
WHERE LOWER(st.store_type_code) IN (LOWER('FL'), LOWER('VS'), LOWER('NL'), LOWER('FC'))
 AND a.ticket_date >= (SELECT earliest_act_dt
   FROM date_range)
 AND a.ticket_date <= (SELECT latest_mo_dt
   FROM date_range)
 AND LOWER(LOWER(a.ticket_status)) NOT LIKE LOWER('%delete%')
 AND a.store_origin IS NOT NULL
 AND xr.acp_id IS NOT NULL
GROUP BY a.ticket_id,
 b.month_num,
 xr.acp_id;


--COLLECT   STATISTICS COLUMN (PERS_IND) ON nseam_wide


--COLLECT   STATISTICS COLUMN (RUSHED_IND) ON nseam_wide


--COLLECT   STATISTICS COLUMN (NONNORD_IND) ON nseam_wide


--COLLECT   STATISTICS COLUMN (ACP_ID ,MONTH_NUM) ON     nseam_wide


--COLLECT   STATISTICS COLUMN (NORD_IND) ON nseam_wide


CREATE TEMPORARY TABLE IF NOT EXISTS nseam_long (
acp_id string,
service_mo NUMERIC(6),
service string
) CLUSTER BY acp_id, service_mo, service;

INSERT INTO nseam_long
(SELECT DISTINCT acp_id,
  CAST(month_num AS NUMERIC) AS service_mo,
  'C01) Nordstrom Alterations' AS service
 FROM nseam_wide AS a
 WHERE nord_ind = 1);


INSERT INTO nseam_long
(SELECT DISTINCT acp_id,
  CAST(month_num AS NUMERIC) AS service_mo,
  'C02) Non-Nordstrom Alterations' AS service
 FROM nseam_wide AS a
 WHERE nonnord_ind = 1);


INSERT INTO nseam_long
(SELECT DISTINCT acp_id,
  CAST(month_num AS NUMERIC) AS service_mo,
  'C03) Rushed Alterations' AS service
 FROM nseam_wide AS a
 WHERE rushed_ind = 1);


INSERT INTO nseam_long
(SELECT DISTINCT acp_id,
  CAST(month_num AS NUMERIC) AS service_mo,
  'C04) Personalized Alterations' AS service
 FROM nseam_wide AS a
 WHERE pers_ind = 1);


INSERT INTO acp_id_service_month_temp
(SELECT acp_id,
  service_mo,
  service,
  customer_qualifier,
  nseam_only_ind,
  CAST(gross_usd_amt_whole AS NUMERIC) AS gross_usd_amt_whole,
  CAST(net_usd_amt_whole AS NUMERIC) AS net_usd_amt_whole,
  CAST(gross_usd_amt_split AS NUMERIC) AS gross_usd_amt_split,
  CAST(net_usd_amt_split AS NUMERIC) AS net_usd_amt_split
 FROM (SELECT DISTINCT acp_id,
     service_mo,
     service,
     1 AS customer_qualifier,
     1 AS nseam_only_ind,
     0 AS gross_usd_amt_whole,
     0 AS net_usd_amt_whole,
     0 AS gross_usd_amt_split,
     0 AS net_usd_amt_split
    FROM nseam_long
    WHERE service_mo >= (SELECT earliest_act_mo
       FROM get_months)
     AND service_mo <= (SELECT last_complete_mo
       FROM get_months)
    UNION ALL
    SELECT DISTINCT acp_id,
     service_mo,
     'C_) Alterations GROUP' AS service,
     1 AS customer_qualifier,
     1 AS nseam_only_ind,
     0 AS gross_usd_amt_whole,
     0 AS net_usd_amt_whole,
     0 AS gross_usd_amt_split,
     0 AS net_usd_amt_split
    FROM nseam_long
    WHERE service_mo >= (SELECT earliest_act_mo
       FROM get_months)
     AND service_mo <= (SELECT last_complete_mo
       FROM get_months)
    UNION ALL
    SELECT DISTINCT acp_id,
     service_mo,
     'H_) Any Service Engagement' AS service,
     1 AS customer_qualifier,
     1 AS nseam_only_ind,
     0 AS gross_usd_amt_whole,
     0 AS net_usd_amt_whole,
     0 AS gross_usd_amt_split,
     0 AS net_usd_amt_split
    FROM nseam_long
    WHERE service_mo >= (SELECT earliest_act_mo
       FROM get_months)
     AND service_mo <= (SELECT last_complete_mo
       FROM get_months)
    UNION ALL
    SELECT DISTINCT acp_id,
     service_mo,
     'Z_) Nordstrom' AS service,
     1 AS customer_qualifier,
     1 AS nseam_only_ind,
     0 AS gross_usd_amt_whole,
     0 AS net_usd_amt_whole,
     0 AS gross_usd_amt_split,
     0 AS net_usd_amt_split
    FROM nseam_long
    WHERE service_mo >= (SELECT earliest_act_mo
       FROM get_months)
     AND service_mo <= (SELECT last_complete_mo
       FROM get_months)) AS t26);


--COLLECT   STATISTICS COLUMN (ACP_ID ,SERVICE_MO ,SERVICE) ON acp_id_service_month_temp


CREATE TEMPORARY TABLE IF NOT EXISTS acp_id_service_month_smash (
acp_id string,
service_mo NUMERIC(6),
service string,
customer_qualifier INTEGER,
gross_usd_amt_whole NUMERIC(20,2),
net_usd_amt_whole NUMERIC(20,2),
gross_usd_amt_split NUMERIC(20,2),
net_usd_amt_split NUMERIC(20,2),
private_style INTEGER
) CLUSTER BY acp_id, service_mo, service;


INSERT INTO acp_id_service_month_smash
(SELECT acp_id,
  service_mo,
  service,
  MAX(customer_qualifier) AS customer_qualifier,
  SUM(gross_usd_amt_whole) AS gross_usd_amt_whole,
  SUM(net_usd_amt_whole) AS net_usd_amt_whole,
  SUM(gross_usd_amt_split) AS gross_usd_amt_split,
  SUM(net_usd_amt_split) AS net_usd_amt_split,
  MAX(CASE
    WHEN LOWER(service) LIKE LOWER('A01)%') OR LOWER(service) LIKE LOWER('A02)%') OR LOWER(service) LIKE LOWER('A03)%')
    THEN 1
    ELSE 0
    END) AS private_style
 FROM acp_id_service_month_temp
 GROUP BY acp_id,
  service_mo,
  service);


--COLLECT   STATISTICS COLUMN (ACP_ID ,SERVICE_MO) ON acp_id_service_month_smash


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.service_eng_t2_schema}}.cust_service_month;


INSERT INTO `{{params.gcp_project_id}}`.{{params.service_eng_t2_schema}}.cust_service_month
(SELECT DISTINCT a.acp_id,
  a.service_mo AS month_num,
  a.service AS service_name,
  a.customer_qualifier,
  a.gross_usd_amt_whole,
  a.net_usd_amt_whole,
  a.gross_usd_amt_split,
  a.net_usd_amt_split,
  a.private_style,
  CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
 FROM acp_id_service_month_smash AS a
  INNER JOIN (SELECT DISTINCT acp_id,
    service_mo
   FROM acp_id_service_month_smash
   WHERE LOWER(service) = LOWER('Z_) Nordstrom')) AS b 
   ON LOWER(a.acp_id) = LOWER(b.acp_id) AND a.service_mo = b.service_mo
    );


--COLLECT   STATISTICS COLUMN(acp_id),                    COLUMN(month_num),                    COLUMN(service_name) ON T2DL_DAS_SERVICE_ENG.cust_service_month