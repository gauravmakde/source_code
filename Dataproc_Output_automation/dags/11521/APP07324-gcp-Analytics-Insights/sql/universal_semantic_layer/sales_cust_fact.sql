CREATE TEMPORARY TABLE IF NOT EXISTS sf_start_date
AS
SELECT CASE
  WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE('PST8PDT')) = {{params.backfill_day_of_week}} 
  THEN 18658
  ELSE (EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT')) - 1900) * 10000 + EXTRACT(MONTH FROM CURRENT_DATE('PST8PDT')) * 100 + EXTRACT(DAY FROM
     CURRENT_DATE('PST8PDT')) - {{params.incremental_look_back}}
  END AS start_date,
  CASE
  WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE('PST8PDT')) = {{params.backfill_day_of_week}} 
  THEN 'backfill'
  ELSE 'incremental'
  END AS delete_range;


-- DATE FILTER
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
CLUSTER BY week_num
AS
SELECT week_num,
 month_num,
 quarter_num,
 year_num,
 MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
WHERE CAST(((EXTRACT(YEAR FROM day_date) - 1900) * 10000 + EXTRACT(MONTH FROM day_date) * 100) + EXTRACT(DAY FROM day_date) AS FLOAT64)
  BETWEEN (SELECT start_date
   FROM sf_start_date) AND (CAST((EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) - 1900) * 10000 + EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) * 100 + EXTRACT(DAY FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS FLOAT64)
   )
GROUP BY week_num,
 month_num,
 quarter_num,
 year_num;

-- CORE DATA REFERENCE TABLE

-- OUR ORIGINAL VERSION
CREATE TEMPORARY TABLE IF NOT EXISTS retail_info
CLUSTER BY acp_id
AS
SELECT sf.sale_date,
 d.week_num,
 d.month_num,
 d.quarter_num,
 d.year_num,
 sf.global_tran_id,
 sf.line_item_seq_num,
 sf.store_number,
 sf.acp_id,
 sf.sku_id,
 sf.upc_num,
 sf.trip_id,
 sf.employee_discount_flag,
 sf.transaction_type_id,
 sf.device_id,
 sf.ship_method_id,
 sf.price_type_id,
 sf.line_net_usd_amt,
 sf.giftcard_flag,
 sf.sales_returned,
 sf.units_returned,
 sf.non_gc_line_net_usd_amt,
 st.business_unit_desc,
 sf.line_item_quantity AS items,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA')) AND LOWER(TRIM(nts.aare_chnl_code
      )) = LOWER('FLS')
  THEN 1
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB')) AND LOWER(TRIM(nts.aare_chnl_code
      )) = LOWER('NCOM')
  THEN 1
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA')) AND LOWER(TRIM(nts.aare_chnl_code)) = LOWER('RACK'
     )
  THEN 1
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE')) AND LOWER(TRIM(nts.aare_chnl_code)) = LOWER('NRHL')
  THEN 1
  ELSE 0
  END AS new_to_jwn,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN '1) Nordstrom Stores'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN '2) Nordstrom.com'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN '3) Rack Stores'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN '4) Rack.com'
  ELSE NULL
  END AS channel,
  CASE
  WHEN LOWER(CASE
     WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
     THEN '1) Nordstrom Stores'
     WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
     THEN '2) Nordstrom.com'
     WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
     THEN '3) Rack Stores'
     WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
     THEN '4) Rack.com'
     ELSE NULL
     END) IN (LOWER('1) Nordstrom Stores'), LOWER('2) Nordstrom.com'))
  THEN '1) Nordstrom Banner'
  WHEN LOWER(CASE
     WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
     THEN '1) Nordstrom Stores'
     WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
     THEN '2) Nordstrom.com'
     WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
     THEN '3) Rack Stores'
     WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
     THEN '4) Rack.com'
     ELSE NULL
     END) IN (LOWER('3) Rack Stores'), LOWER('4) Rack.com'))
  THEN '2) Rack Banner'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_fact AS sf
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON sf.store_number = st.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS d ON sf.sale_date = d.day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS nts ON sf.global_tran_id = nts.aare_global_tran_id
WHERE sf.sale_date >= (SELECT MIN(ty_start_dt)
   FROM date_lookup)
 AND sf.sale_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND sf.acp_id IS NOT NULL;

--  Calculate the core target segment for the last 2 FY for each customer 

CREATE TEMPORARY TABLE IF NOT EXISTS core_target_segment
CLUSTER BY acp_id
AS
SELECT acp_id,
 predicted_ct_segment AS predicted_segment
FROM `{{params.gcp_project_id}}`.t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment AS cas
GROUP BY acp_id,
 predicted_segment;

-- Acquire a single value DMA and Region for each Customer!

CREATE TEMPORARY TABLE IF NOT EXISTS location_info
CLUSTER BY acp_id
AS
SELECT acp_id,
  CASE
  WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
  THEN 'DMA_UNKNOWN'
  ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
  END AS dma,
  CASE
  WHEN LOWER(UPPER(SUBSTR(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, 1, 9))) IN (LOWER('LOS ANGEL'), LOWER('BAKERSFIE'), LOWER('SANTA BAR'), LOWER('SAN DIEGO'), LOWER('PALM SPRI'
     ), LOWER('YUMA AZ-E'))
  THEN 'SCAL'
  WHEN LOWER(UPPER(SUBSTR(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, 1, 7))) IN (LOWER('RICHMON'), LOWER('ROANOKE'), LOWER('NORFOLK'), LOWER('HARRISO'))
  THEN 'NORTHEAST'
  WHEN LOWER(UPPER(SUBSTR(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, 1, 11))) = LOWER('CHARLOTTESV')
  THEN 'NORTHEAST'
  WHEN LOWER(SUBSTR(REPLACE(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, ')', ''), LENGTH(REPLACE(CASE
         WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
         THEN 'DMA_UNKNOWN'
         ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
         END, ')', '')) - 1, 2)) IN (LOWER('AB'), LOWER('BC'), LOWER('MB'), LOWER('NB'), LOWER('NL'), LOWER('NS'), LOWER('NT'
     ), LOWER('NU'), LOWER('ON'), LOWER('PE'), LOWER('QC'), LOWER('SK'), LOWER('YT'))
  THEN 'CANADA'
  WHEN LOWER(SUBSTR(REPLACE(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, ')', ''), LENGTH(REPLACE(CASE
         WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
         THEN 'DMA_UNKNOWN'
         ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
         END, ')', '')) - 1, 2)) IN (LOWER('IA'), LOWER('IL'), LOWER('IN'), LOWER('KS'), LOWER('KY'), LOWER('MI'), LOWER('MN'
     ), LOWER('MO'), LOWER('ND'), LOWER('NE'), LOWER('OH'), LOWER('SD'), LOWER('WI'))
  THEN 'MIDWEST'
  WHEN LOWER(SUBSTR(REPLACE(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, ')', ''), LENGTH(REPLACE(CASE
         WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
         THEN 'DMA_UNKNOWN'
         ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
         END, ')', '')) - 1, 2)) IN (LOWER('CT'), LOWER('DC'), LOWER('DE'), LOWER('MA'), LOWER('MD'), LOWER('ME'), LOWER('NH'
     ), LOWER('NJ'), LOWER('NY'), LOWER('PA'), LOWER('RI'), LOWER('VT'), LOWER('WV'))
  THEN 'NORTHEAST'
  WHEN LOWER(SUBSTR(REPLACE(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, ')', ''), LENGTH(REPLACE(CASE
         WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
         THEN 'DMA_UNKNOWN'
         ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
         END, ')', '')) - 1, 2)) IN (LOWER('AK'), LOWER('CA'), LOWER('ID'), LOWER('MT'), LOWER('NV'), LOWER('OR'), LOWER('WA'
     ), LOWER('WY'))
  THEN 'NORTHWEST'
  WHEN LOWER(SUBSTR(REPLACE(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, ')', ''), LENGTH(REPLACE(CASE
         WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
         THEN 'DMA_UNKNOWN'
         ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
         END, ')', '')) - 1, 2)) = LOWER('HI')
  THEN 'SCAL'
  WHEN LOWER(SUBSTR(REPLACE(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, ')', ''), LENGTH(REPLACE(CASE
         WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
         THEN 'DMA_UNKNOWN'
         ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
         END, ')', '')) - 1, 2)) IN (LOWER('AL'), LOWER('FL'), LOWER('GA'), LOWER('MS'), LOWER('NC'), LOWER('PR'), LOWER('SC'
     ), LOWER('TN'), LOWER('VA'))
  THEN 'SOUTHEAST'
  WHEN LOWER(SUBSTR(REPLACE(CASE
       WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
       THEN 'DMA_UNKNOWN'
       ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
       END, ')', ''), LENGTH(REPLACE(CASE
         WHEN LOWER(SUBSTR(UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN')), 1, 5)) = LOWER('OTHER')
         THEN 'DMA_UNKNOWN'
         ELSE UPPER(COALESCE(ca_dma_desc, us_dma_desc, 'DMA_UNKNOWN'))
         END, ')', '')) - 1, 2)) IN (LOWER('AR'), LOWER('AZ'), LOWER('CO'), LOWER('LA'), LOWER('NM'), LOWER('OK'), LOWER('TX'
     ), LOWER('UT'))
  THEN 'SOUTHWEST'
  ELSE 'UNKNOWN'
  END AS region
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac
WHERE acp_id IN (SELECT DISTINCT acp_id
   FROM retail_info);

-- Logic for calculating the AEC of each customer

CREATE TEMPORARY TABLE IF NOT EXISTS aec
CLUSTER BY acp_id, execution_qtr
AS
SELECT DISTINCT acp_id,
 execution_qtr,
 ROUND(CAST(TRUNC(execution_qtr / 10) AS INT64), 0) AS engagement_year,
 engagement_cohort
FROM `{{params.gcp_project_id}}`.t2dl_das_aec.audience_engagement_cohorts AS aec_t
WHERE execution_qtr IN (SELECT DISTINCT quarter_num
   FROM retail_info);

-- Logic of calculating the loyalty of each of the customers.

CREATE TEMPORARY TABLE IF NOT EXISTS cardmember_fix_table
CLUSTER BY loyalty_id
AS
SELECT x.loyalty_id,
 y.acp_id,
 x.acct_open_dt AS cardmember_enroll_date,
  CASE
  WHEN LOWER(x.acct_close_dt) = LOWER('20500101')
  THEN NULL
  ELSE PARSE_DATE('%Y%m%d', x.acct_close_dt)
  END AS max_close_dt
FROM (SELECT loyalty_id,
   MIN(PARSE_DATE('%Y%m%d', SUBSTR(CAST(acct_open_dt AS STRING), 1, 8))) AS acct_open_dt,
   MAX(CASE
     WHEN acct_close_dt IS NULL
     THEN '20500101'
     ELSE SUBSTR(CAST(acct_close_dt AS STRING), 1, 8)
     END) AS acct_close_dt
  FROM `{{params.gcp_project_id}}`.t3dl_paymnt_loyalty.creditloyaltysync
  WHERE loyalty_id IS NOT NULL
  GROUP BY loyalty_id) AS x
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS y ON LOWER(x.loyalty_id) = LOWER(y.loyalty_id);

--drop table cust_level_loyalty_cardmember;

CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty_cardmember
CLUSTER BY acp_id
AS
SELECT lmd.acp_id,
 1 AS flg_cardmember,
 MAX(CASE
   WHEN LOWER(t1.rewards_level) IN (LOWER('MEMBER'))
   THEN 1
   WHEN LOWER(t1.rewards_level) IN (LOWER('INSIDER'), LOWER('INFLUENCER'))
   THEN 3
   WHEN LOWER(t1.rewards_level) IN (LOWER('AMBASSADOR'))
   THEN 4
   WHEN LOWER(t1.rewards_level) IN (LOWER('ICON'))
   THEN 5
   ELSE 0
   END) AS cardmember_level,
 MIN(lmd.cardmember_enroll_date) AS cardmember_enroll_date
FROM cardmember_fix_table AS lmd
 LEFT JOIN (SELECT *
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
  WHERE start_day_date <= (SELECT MAX(ty_end_dt)
     FROM date_lookup)
   AND end_day_date > (SELECT MAX(ty_end_dt)
     FROM date_lookup)) AS t1 ON LOWER(lmd.loyalty_id) = LOWER(t1.loyalty_id)
WHERE lmd.cardmember_enroll_date < COALESCE(lmd.max_close_dt, DATE '2099-12-31')
 AND lmd.cardmember_enroll_date <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND COALESCE(lmd.max_close_dt, DATE '2099-12-31') > (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND lmd.acp_id IN (SELECT DISTINCT acp_id
   FROM retail_info)
 AND lmd.acp_id IS NOT NULL
GROUP BY lmd.acp_id,
 flg_cardmember;

-- drop table cust_level_loyalty_member;

CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty_member
CLUSTER BY acp_id
AS
SELECT lmd.acp_id,
 1 AS flg_member,
 MAX(CASE
   WHEN LOWER(t1.rewards_level) IN (LOWER('MEMBER'))
   THEN 1
   WHEN LOWER(t1.rewards_level) IN (LOWER('INSIDER'), LOWER('INFLUENCER'))
   THEN 3
   WHEN LOWER(t1.rewards_level) IN (LOWER('AMBASSADOR'))
   THEN 4
   WHEN LOWER(t1.rewards_level) IN (LOWER('ICON'))
   THEN 5
   ELSE 0
   END) AS member_level,
 MIN(lmd.member_enroll_date) AS member_enroll_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd
 LEFT JOIN (SELECT *
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
  WHERE start_day_date <= (SELECT MAX(ty_end_dt)
     FROM date_lookup)
   AND end_day_date > (SELECT MAX(ty_end_dt)
     FROM date_lookup)) AS t1 ON LOWER(lmd.loyalty_id) = LOWER(t1.loyalty_id)
WHERE COALESCE(lmd.member_enroll_date, DATE '2099-12-31') < COALESCE(lmd.member_close_date, DATE '2099-12-31')
 AND COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <= (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND COALESCE(lmd.member_close_date, DATE '2099-12-31') > (SELECT MAX(ty_end_dt)
   FROM date_lookup)
 AND lmd.acp_id IN (SELECT DISTINCT acp_id
   FROM retail_info)
 AND lmd.acp_id IS NOT NULL
GROUP BY lmd.acp_id,
 flg_member;

/************************************************************************************/
/********* II-D: Combine both types of Loyalty customers into 1 lookup table
 *    (prioritize "Cardmember" type & level if present, use "Member" otherwise) *****/
/************************************************************************************/


CREATE TEMPORARY TABLE IF NOT EXISTS loyalty
CLUSTER BY acp_id
AS
SELECT DISTINCT a.acp_id,
  CASE
  WHEN b.acp_id IS NOT NULL
  THEN 'a) Cardmember'
  WHEN c.acp_id IS NOT NULL
  THEN 'b) Member'
  ELSE 'c) Non-Loyalty'
  END AS loyalty_type,
  CASE
  WHEN b.acp_id IS NOT NULL OR c.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS loyalty_ind,
  CASE
  WHEN CAST(a.employee_discount_flag AS FLOAT64) = 1 AND b.acp_id IS NOT NULL AND c.acp_id IS NOT NULL
  THEN '1) MEMBER'
  WHEN b.acp_id IS NOT NULL AND b.cardmember_level <= 3
  THEN '2) INFLUENCER'
  WHEN b.acp_id IS NOT NULL AND b.cardmember_level = 4
  THEN '3) AMBASSADOR'
  WHEN b.acp_id IS NOT NULL AND b.cardmember_level = 5
  THEN '4) ICON'
  WHEN c.member_level <= 1
  THEN '1) MEMBER'
  WHEN c.member_level = 3
  THEN '2) INFLUENCER'
  WHEN c.member_level >= 4
  THEN '3) AMBASSADOR'
  ELSE NULL
  END AS loyalty_level,
 c.member_enroll_date AS loyalty_member_start_dt,
 b.cardmember_enroll_date AS loyalty_cardmember_start_dt
FROM retail_info AS a
 LEFT JOIN cust_level_loyalty_cardmember AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
 LEFT JOIN cust_level_loyalty_member AS c ON LOWER(a.acp_id) = LOWER(c.acp_id);

-- Combining all the different pieces together for one table to output

CREATE TEMPORARY TABLE IF NOT EXISTS usl_sales_cust_fact
CLUSTER BY acp_id, week_num, quarter_num, month_num
AS
SELECT ri.sale_date,
 ri.week_num,
 ri.month_num,
 ri.quarter_num,
 ri.year_num,
 ri.global_tran_id,
 ri.line_item_seq_num,
 ri.store_number AS store_num,
 ri.acp_id,
 ri.sku_id AS sku_num,
 ri.upc_num,
 ri.trip_id,
 ri.employee_discount_flag,
 ri.transaction_type_id,
 ri.device_id,
 ri.ship_method_id,
 ri.price_type_id,
 ri.line_net_usd_amt,
 ri.giftcard_flag,
 ri.items,
 ri.sales_returned AS returned_sales,
 ri.units_returned AS returned_items,
 ri.non_gc_line_net_usd_amt AS non_gc_amt,
 s.region,
 s.dma,
 e.engagement_cohort,
 c.predicted_segment,
 COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
 COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
 ri.new_to_jwn,
 ri.channel,
 ri.banner,
 ri.business_unit_desc
FROM retail_info AS ri
 LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
 LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
 LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
 LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 6 MONTH)
 AND ri.sale_date <= CURRENT_DATE('PST8PDT');


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 12 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 6 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 18 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 12 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 18 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 30 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 36 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 30 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 42 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 36 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 48 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 42 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 54 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 48 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 60 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 54 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 66 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 60 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 72 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 66 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 78 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 72 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 84 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 78 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 90 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 84 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 96 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 90 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 102 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 96 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 108 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 102 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 114 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 108 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 120 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 114 MONTH));


INSERT INTO usl_sales_cust_fact
(SELECT ri.sale_date,
  ri.week_num,
  ri.month_num,
  ri.quarter_num,
  ri.year_num,
  ri.global_tran_id,
  ri.line_item_seq_num,
  ri.store_number AS store_num,
  ri.acp_id,
  ri.sku_id AS sku_num,
  ri.upc_num,
  ri.trip_id,
  ri.employee_discount_flag,
  ri.transaction_type_id,
  ri.device_id,
  ri.ship_method_id,
  ri.price_type_id,
  ri.line_net_usd_amt,
  ri.giftcard_flag,
  ri.items,
  ri.sales_returned AS returned_sales,
  ri.units_returned AS returned_items,
  ri.non_gc_line_net_usd_amt AS non_gc_amt,
  s.region,
  s.dma,
  e.engagement_cohort,
  c.predicted_segment,
  COALESCE(l.loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(l.loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  ri.new_to_jwn,
  ri.channel,
  ri.banner,
  ri.business_unit_desc
 FROM retail_info AS ri
  LEFT JOIN core_target_segment AS c ON LOWER(ri.acp_id) = LOWER(c.acp_id)
  LEFT JOIN location_info AS s ON LOWER(ri.acp_id) = LOWER(s.acp_id)
  LEFT JOIN aec AS e ON LOWER(ri.acp_id) = LOWER(e.acp_id) AND ri.quarter_num = e.execution_qtr
  LEFT JOIN loyalty AS l ON LOWER(l.acp_id) = LOWER(ri.acp_id)
 WHERE ri.sale_date >= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 126 MONTH)
  AND ri.sale_date < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 120 MONTH));

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact
WHERE CAST((EXTRACT(YEAR FROM sale_date) - 1900) * 10000 + EXTRACT(MONTH FROM sale_date) * 100 + EXTRACT(DAY FROM sale_date) AS FLOAT64) >= (SELECT start_date
            FROM sf_start_date) AND EXISTS (SELECT 1
        FROM sf_start_date
        WHERE LOWER(delete_range) = LOWER('incremental'));


DELETE FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact
WHERE EXISTS (SELECT 1
    FROM sf_start_date
    WHERE LOWER(delete_range) = LOWER('backfill'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_cust_fact
(SELECT sale_date,
  week_num,
  month_num,
  quarter_num,
  year_num,
  global_tran_id,
  line_item_seq_num,
  store_num,
  acp_id,
  sku_num,
  upc_num,
  trip_id,
  employee_discount_flag,
  transaction_type_id,
  device_id,
  ship_method_id,
  price_type_id,
  line_net_usd_amt,
  giftcard_flag,
  items,
  returned_sales,
  returned_items,
  non_gc_amt,
  region,
  dma,
  COALESCE(engagement_cohort, 'UNDEFINED') AS engagement_cohort,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn,
  channel,
  banner,
  business_unit_desc,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM usl_sales_cust_fact);