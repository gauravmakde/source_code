/*
Name: Store Customer Counts DDL
APPID-Name: APP09268 Merch Analytics - Store Reporting
Purpose: Populate Weekly Customer Counts Store Reporting T2 Source Data in 
Variable(s):    "environment_schema" - T2DL_DAS_<PROJECT DATALAB> 
                "start_date" - date for previous full week start period for output data 
                        (Default = "(CURRENT_DATE('PST8PDT') - 1)  - INTERVAL '14' DAY")
                "end_date" - date for previous full week end period for output data 
                        (Default = "(CURRENT_DATE('PST8PDT') - 1)")
DAG: merch_main_cust_counts_store
Author(s): Alli Moore
Date Created: 2024-02-27
Date Last Updated: 2024-03-15
*/


/********************************************** START DIMENSIONS **************************************************/

-- begin
-- Realigned Dates Lookup
--DROP TABLE DT_LKUP;



CREATE TEMPORARY TABLE IF NOT EXISTS dt_lkup AS 
WITH ty_lkp AS (
  SELECT DISTINCT day_date AS ty_day_date,
   day_idnt,
   day_date_last_year_realigned,
   fiscal_week_num,
   week_idnt,
   week_desc,
   week_label,
   week_start_day_date,
   week_end_day_date,
   week_num_of_fiscal_month,
   month_idnt,
   fiscal_month_num,
   month_desc,
   month_abrv,
   month_label,
   month_start_day_date,
   month_start_week_idnt,
   month_end_day_date,
   month_end_week_idnt,
   quarter_idnt,
   fiscal_quarter_num,
   fiscal_year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
  WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num)
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE week_end_day_date <= {{params.end_date}})
   AND fiscal_week_num >= (SELECT COALESCE(MAX(fiscal_week_num), 1) AS fiscal_week_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE week_end_day_date <= {{params.start_date}}
      AND fiscal_year_num = (SELECT MAX(fiscal_year_num)
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
        WHERE week_end_day_date <= {{params.end_date}}))
   AND fiscal_week_num <= (SELECT MAX(fiscal_week_num)
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE week_end_day_date <= {{params.end_date}}
      AND fiscal_year_num = (SELECT MAX(fiscal_year_num)
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
        WHERE week_end_day_date <= {{params.end_date}}))) 

SELECT SUBSTR('TY', 1, 3) AS ty_ly_lly_ind,
   ty_day_date AS day_date,
   day_idnt,
   fiscal_week_num,
   week_idnt,
   week_desc,
   week_label,
   week_start_day_date,
   week_end_day_date,
   week_num_of_fiscal_month,
   fiscal_month_num,
   month_idnt,
   month_desc,
   month_abrv,
     TRIM(FORMAT('%11d', fiscal_year_num)) || ' ' || TRIM(month_abrv) AS month_label,
   month_start_day_date,
   month_start_week_idnt,
   month_end_day_date,
   month_end_week_idnt,
   fiscal_quarter_num,
   CAST(TRIM(TRIM(FORMAT('%11d', fiscal_year_num)) || TRIM(FORMAT('%11d', fiscal_quarter_num))) AS INTEGER) AS
   quarter_idnt,
   fiscal_year_num,
   MIN(ty_day_date) OVER (PARTITION BY SUBSTR('TY', 1, 3) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   period_start_day_date,
   MAX(ty_day_date) OVER (PARTITION BY SUBSTR('TY', 1, 3) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   period_end_day_date
  FROM ty_lkp AS a
UNION ALL
SELECT SUBSTR('LY', 1, 3) AS ty_ly_lly_ind,
   a.day_date_last_year_realigned AS day_date,
    a.day_idnt - 1000 AS day_idnt,
   a.fiscal_week_num,
    a.week_idnt - 100 AS week_idnt,
   a.week_desc,
   TRIM(TRIM(SUBSTR(CAST(a.fiscal_year_num - 1 AS STRING), 1, 4)) || ' ' || a.month_abrv || ' ' || a.week_desc) AS
   week_label,
   b.week_start_day_date,
   b.week_end_day_date,
   a.week_num_of_fiscal_month,
   a.fiscal_month_num,
    a.month_idnt - 100 AS month_idnt,
   a.month_desc,
   a.month_abrv,
     TRIM(FORMAT('%11d', a.fiscal_year_num - 1)) || ' ' || TRIM(a.month_abrv) AS month_label,
   MIN(a.day_date_last_year_realigned) OVER (PARTITION BY a.fiscal_month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS month_start_day_date,
   MIN(a.week_idnt - 100) OVER (PARTITION BY a.fiscal_month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS month_start_week_idnt,
   MAX(a.day_date_last_year_realigned) OVER (PARTITION BY a.fiscal_month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS month_end_day_date,
   MAX(a.week_idnt - 100) OVER (PARTITION BY a.fiscal_month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS month_end_week_idnt,
   a.fiscal_quarter_num,
   CAST(TRIM(TRIM(FORMAT('%11d', a.fiscal_year_num - 1)) || TRIM(FORMAT('%11d', a.fiscal_quarter_num))) AS INTEGER) AS
   quarter_idnt,
    a.fiscal_year_num - 1 AS fiscal_year_num,
   MIN(a.day_date_last_year_realigned) OVER (PARTITION BY SUBSTR('LY', 1, 3) RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS period_start_day_date,
   MAX(a.day_date_last_year_realigned) OVER (PARTITION BY SUBSTR('LY', 1, 3) RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS period_end_day_date
  FROM ty_lkp AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.day_date_last_year_realigned = b.day_date;


--COLLECT STATISTICS COLUMN(day_idnt, day_date) ON DT_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS wk_lkup
AS
SELECT DISTINCT ty_ly_lly_ind,
 week_idnt,
 week_desc,
 week_label,
 fiscal_week_num,
 week_start_day_date,
 week_end_day_date,
 week_num_of_fiscal_month,
 month_idnt,
 month_desc,
 month_label,
 month_abrv,
 fiscal_month_num,
 month_start_day_date,
 month_start_week_idnt,
 month_end_day_date,
 month_end_week_idnt,
 quarter_idnt,
 fiscal_year_num
FROM dt_lkup AS dt;


--COLLECT STATISTICS COLUMN(week_idnt) ON WK_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup
AS
SELECT st.store_num,
 st.store_name,
 st.store_type_code,
 st.store_type_desc,
 st.selling_store_ind,
 st.region_num,
 st.region_desc,
 st.business_unit_num,
 st.business_unit_desc,
 st.subgroup_num,
 st.subgroup_desc,
 st.store_dma_code,
 dma.dma_desc,
 dma.dma_shrt_desc,
 st.channel_num,
 st.channel_desc,
 st.comp_status_code,
 st.comp_status_desc,
 MAX(CASE
   WHEN LOWER(cls.peer_group_type_code) = LOWER('OPD')
   THEN cls.peer_group_num
   ELSE NULL
   END) AS rack_district_num,
 MAX(CASE
   WHEN LOWER(cls.peer_group_type_code) = LOWER('OPD')
   THEN cls.peer_group_desc
   ELSE NULL
   END) AS rack_district_desc,
 MAX(CASE
   WHEN st.channel_num = 110 AND LOWER(cls.peer_group_type_code) = LOWER('FPC')
   THEN cls.peer_group_desc
   WHEN st.channel_num = 210 AND LOWER(cls.peer_group_type_code) = LOWER('OPC')
   THEN cls.peer_group_desc
   ELSE NULL
   END) AS cluster_climate,
 MAX(CASE
   WHEN st.channel_num = 110 AND LOWER(cls.peer_group_type_code) = LOWER('FPI')
   THEN cls.peer_group_desc
   WHEN st.channel_num = 210 AND LOWER(cls.peer_group_type_code) = LOWER('OCP')
   THEN cls.peer_group_desc
   ELSE NULL
   END) AS cluster_price,
 MAX(CASE
   WHEN st.channel_num = 110 AND LOWER(cls.peer_group_type_code) = LOWER('FPD')
   THEN cls.peer_group_desc
   ELSE NULL
   END) AS cluster_designer,
 MAX(CASE
   WHEN st.channel_num = 110 AND LOWER(cls.peer_group_type_code) = LOWER('FPP')
   THEN cls.peer_group_desc
   ELSE NULL
   END) AS cluster_presidents_cup
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_peer_group_dim AS cls ON st.store_num = cls.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_dma AS dma ON CAST(st.store_dma_code AS FLOAT64) = dma.dma_code
WHERE LOWER(st.selling_store_ind) = LOWER('S')
 AND st.store_close_date IS NULL
 AND LOWER(st.store_type_code) IN (LOWER('FL'), LOWER('NL'), LOWER('RK'))
 AND st.channel_num IN (110, 210)
GROUP BY st.store_num,
 st.store_name,
 st.store_type_code,
 st.store_type_desc,
 st.selling_store_ind,
 st.region_num,
 st.region_desc,
 st.business_unit_num,
 st.business_unit_desc,
 st.subgroup_num,
 st.subgroup_desc,
 st.store_dma_code,
 dma.dma_desc,
 dma.dma_shrt_desc,
 st.channel_num,
 st.channel_desc,
 st.comp_status_code,
 st.comp_status_desc;


--COLLECT STATISTICS COLUMN(store_num) ON STORE_LKUP


CREATE TEMPORARY TABLE IF NOT EXISTS acp_lkup_ty
AS
SELECT 
day_date,
ty_ly_lly_ind,
loyalty_id,
acp_id,
rewards_level,
rn
from (
SELECT DISTINCT d.day_date,
 d.ty_ly_lly_ind,
 rwd.loyalty_id,
 ac.acp_id,
 rwd.rewards_level,
 ROW_NUMBER() OVER (PARTITION BY rwd.acp_id, d.day_date ORDER BY rwd.start_day_date DESC) AS rn
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
 INNER JOIN dt_lkup AS d ON d.day_date BETWEEN rwd.start_day_date AND (DATE_SUB(rwd.end_day_date, INTERVAL 1 DAY)) AND
   LOWER(d.ty_ly_lly_ind) = LOWER('TY')
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac ON LOWER(ac.acp_loyalty_id) = LOWER(rwd.loyalty_id)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd ON LOWER(ac.acp_loyalty_id) = LOWER(lmd.loyalty_id) AND
    COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <= d.period_end_day_date AND COALESCE(lmd.member_close_date,
    DATE '2099-12-31') >= d.period_start_day_date
WHERE COALESCE(lmd.member_enroll_date, DATE '2099-12-31') < COALESCE(lmd.member_close_date, DATE '2099-12-31')
 AND LOWER(d.ty_ly_lly_ind) = LOWER('TY')
 AND ac.acp_id IS NOT NULL)
 WHERE rn = 1;


--COLLECT STATISTICS COLUMN(day_date, loyalty_id, acp_id) ON acp_lkup_ty


CREATE TEMPORARY TABLE IF NOT EXISTS acp_lkup_ly
AS
SELECT
day_date,
ty_ly_lly_ind,
loyalty_id,
acp_id,
rewards_level,
rn
from (
SELECT DISTINCT d.day_date,
 d.ty_ly_lly_ind,
 rwd.loyalty_id,
 ac.acp_id,
 rwd.rewards_level,
 ROW_NUMBER() OVER (PARTITION BY rwd.acp_id, d.day_date ORDER BY rwd.start_day_date DESC) AS rn
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
 INNER JOIN dt_lkup AS d ON d.day_date BETWEEN rwd.start_day_date AND (DATE_SUB(rwd.end_day_date, INTERVAL 1 DAY)) AND
   LOWER(d.ty_ly_lly_ind) = LOWER('LY')
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac ON LOWER(ac.acp_loyalty_id) = LOWER(rwd.loyalty_id)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd ON LOWER(ac.acp_loyalty_id) = LOWER(lmd.loyalty_id) AND
    COALESCE(lmd.member_enroll_date, DATE '2099-12-31') <= d.period_end_day_date AND COALESCE(lmd.member_close_date,
    DATE '2099-12-31') >= d.period_start_day_date
WHERE COALESCE(lmd.member_enroll_date, DATE '2099-12-31') < COALESCE(lmd.member_close_date, DATE '2099-12-31')
 AND LOWER(d.ty_ly_lly_ind) = LOWER('LY')
 AND ac.acp_id IS NOT NULL )
 where rn = 1;


--COLLECT STATISTICS COLUMN(day_date, loyalty_id, acp_id) ON acp_lkup_ly


CREATE TEMPORARY TABLE IF NOT EXISTS tran_ty_stg
AS
SELECT dtl.order_date,
 dtl.tran_date,
 dtl.acp_id,
 dtl.line_net_usd_amt,
 dtl.line_item_quantity,
 dtl.line_item_order_type,
 dtl.line_item_fulfillment_type,
 dtl.line_item_seq_num,
 dtl.data_source_code,
 dtl.ringing_store_num,
 dtl.intent_store_num,
 dtl.global_tran_id,
 dtl.sku_num,
 b.day_date,
 b.week_idnt,
 b.week_start_day_date,
 b.week_end_day_date,
 b.ty_ly_lly_ind,
 str.store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN store_lkup AS str ON CASE
   WHEN LOWER(dtl.line_item_order_type) LIKE LOWER('CustInit%') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
       ) AND LOWER(dtl.data_source_code) = LOWER('COM')
   THEN dtl.ringing_store_num
   ELSE dtl.intent_store_num
   END = str.store_num
 INNER JOIN dt_lkup AS b ON CASE
    WHEN dtl.line_net_usd_amt > 0
    THEN COALESCE(dtl.order_date, dtl.tran_date)
    ELSE dtl.tran_date
    END = b.day_date AND LOWER(b.ty_ly_lly_ind) = LOWER('TY')
WHERE LOWER(b.ty_ly_lly_ind) = LOWER('TY')
 AND dtl.acp_id IS NOT NULL;


--COLLECT STATISTICS COLUMN(GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON tran_ty_stg


CREATE TEMPORARY TABLE IF NOT EXISTS full_tran_ty_stg
AS
SELECT dtl.order_date,
 dtl.tran_date,
 dtl.acp_id,
 dtl.line_net_usd_amt,
 dtl.line_item_quantity,
 dtl.line_item_order_type,
 dtl.line_item_fulfillment_type,
 dtl.line_item_seq_num,
 dtl.data_source_code,
 dtl.ringing_store_num,
 dtl.intent_store_num,
 dtl.global_tran_id,
 dtl.week_idnt,
 dtl.week_start_day_date,
 dtl.week_end_day_date,
 dtl.ty_ly_lly_ind,
 dtl.store_num,
 acp.rewards_level AS nordy_level,
  CASE
  WHEN ntn.ntn_instance = 1
  THEN 1
  ELSE 0
  END AS ntn_ind
FROM tran_ty_stg AS dtl
 LEFT JOIN acp_lkup_ty AS acp ON LOWER(acp.acp_id) = LOWER(dtl.acp_id) AND dtl.day_date = acp.day_date AND LOWER(acp.ty_ly_lly_ind
    ) = LOWER(dtl.ty_ly_lly_ind)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_fact AS ntn ON LOWER(ntn.acp_id) = LOWER(dtl.acp_id) AND dtl.global_tran_id =
      ntn.ntn_global_tran_id AND LOWER(ntn.sku_num) = LOWER(dtl.sku_num) AND dtl.store_num = ntn.store_num AND COALESCE(ntn
    .order_date, ntn.business_day_date) = dtl.day_date;


--COLLECT STATISTICS COLUMN(GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON full_tran_ty_stg


CREATE TEMPORARY TABLE IF NOT EXISTS tran_ly_stg
AS
SELECT dtl.order_date,
 dtl.tran_date,
 dtl.acp_id,
 dtl.line_net_usd_amt,
 dtl.line_item_quantity,
 dtl.line_item_order_type,
 dtl.line_item_fulfillment_type,
 dtl.line_item_seq_num,
 dtl.data_source_code,
 dtl.ringing_store_num,
 dtl.intent_store_num,
 dtl.global_tran_id,
 dtl.sku_num,
 b.day_date,
 b.week_idnt,
 b.week_start_day_date,
 b.week_end_day_date,
 b.ty_ly_lly_ind,
 str.store_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN store_lkup AS str ON CASE
   WHEN LOWER(dtl.line_item_order_type) LIKE LOWER('CustInit%') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
       ) AND LOWER(dtl.data_source_code) = LOWER('COM')
   THEN dtl.ringing_store_num
   ELSE dtl.intent_store_num
   END = str.store_num
 INNER JOIN dt_lkup AS b ON CASE
    WHEN dtl.line_net_usd_amt > 0
    THEN COALESCE(dtl.order_date, dtl.tran_date)
    ELSE dtl.tran_date
    END = b.day_date AND LOWER(b.ty_ly_lly_ind) = LOWER('LY')
WHERE LOWER(b.ty_ly_lly_ind) = LOWER('LY')
 AND dtl.acp_id IS NOT NULL;


--COLLECT STATISTICS COLUMN(GLOBAL_TRAN_ID, TRAN_DATE, LINE_ITEM_SEQ_NUM, STORE_NUM) ON tran_ly_stg


CREATE TEMPORARY TABLE IF NOT EXISTS full_tran_ly_stg
AS
SELECT dtl.order_date,
 dtl.tran_date,
 dtl.acp_id,
 dtl.line_net_usd_amt,
 dtl.line_item_quantity,
 dtl.line_item_order_type,
 dtl.line_item_fulfillment_type,
 dtl.line_item_seq_num,
 dtl.data_source_code,
 dtl.ringing_store_num,
 dtl.intent_store_num,
 dtl.global_tran_id,
 dtl.week_idnt,
 dtl.week_start_day_date,
 dtl.week_end_day_date,
 dtl.ty_ly_lly_ind,
 dtl.store_num,
 acp.rewards_level AS nordy_level,
  CASE
  WHEN ntn.ntn_instance = 1
  THEN 1
  ELSE 0
  END AS ntn_ind
FROM tran_ly_stg AS dtl
 LEFT JOIN acp_lkup_ly AS acp ON LOWER(acp.acp_id) = LOWER(dtl.acp_id) AND dtl.day_date = acp.day_date AND LOWER(acp.ty_ly_lly_ind
    ) = LOWER(dtl.ty_ly_lly_ind)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_fact AS ntn ON LOWER(ntn.acp_id) = LOWER(dtl.acp_id) AND dtl.global_tran_id =
      ntn.ntn_global_tran_id AND LOWER(ntn.sku_num) = LOWER(dtl.sku_num) AND dtl.store_num = ntn.store_num AND COALESCE(ntn
    .order_date, ntn.business_day_date) = dtl.day_date;


CREATE TEMPORARY TABLE IF NOT EXISTS tran
AS
SELECT week_idnt,
 week_start_day_date,
 week_end_day_date,
 ty_ly_lly_ind,
 store_num,
 nordy_level,
 COUNT(DISTINCT acp_id) AS cust_count,
 COUNT(DISTINCT CASE
   WHEN ntn_ind = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_cust_count,
 SUM(line_net_usd_amt) AS net_spend,
 SUM(line_item_quantity) AS net_units,
 COUNT(DISTINCT global_tran_id) AS net_tran_ct,
  CASE
  WHEN COUNT(DISTINCT global_tran_id) <> 0
  THEN ROUND(CAST(CAST(SUM(line_item_quantity) AS FLOAT64) / CAST(COUNT(DISTINCT global_tran_id) AS FLOAT64) AS NUMERIC)
   , 2)
  ELSE NULL
  END AS net_upt,
 SUM(CASE
   WHEN line_net_usd_amt > 0
   THEN line_item_quantity
   ELSE 0
   END) AS gross_units,
 COUNT(DISTINCT CASE
   WHEN line_net_usd_amt > 0
   THEN global_tran_id
   ELSE NULL
   END) AS gross_tran_ct,
  CASE
  WHEN COUNT(DISTINCT CASE
     WHEN line_net_usd_amt > 0
     THEN global_tran_id
     ELSE NULL
     END) <> 0
  THEN ROUND(CAST(CAST(SUM(CASE
        WHEN line_net_usd_amt > 0
        THEN line_item_quantity
        ELSE 0
        END) AS FLOAT64) / CAST(COUNT(DISTINCT CASE
        WHEN line_net_usd_amt > 0
        THEN global_tran_id
        ELSE NULL
        END) AS FLOAT64) AS NUMERIC), 2)
  ELSE NULL
  END AS gross_upt,
 SUM(CASE
   WHEN line_net_usd_amt > 0
   THEN line_net_usd_amt
   ELSE 0
   END) AS gross_spend,
 COUNT(DISTINCT CASE
   WHEN line_net_usd_amt > 0
   THEN acp_id || FORMAT('%11d', store_num) || CAST(COALESCE(order_date, tran_date) AS STRING)
   ELSE NULL
   END) AS trips
FROM full_tran_ty_stg AS dtl
GROUP BY week_idnt,
 week_start_day_date,
 week_end_day_date,
 ty_ly_lly_ind,
 store_num,
 nordy_level
UNION ALL
SELECT week_idnt,
 week_start_day_date,
 week_end_day_date,
 ty_ly_lly_ind,
 store_num,
 nordy_level,
 COUNT(DISTINCT acp_id) AS cust_count,
 COUNT(DISTINCT CASE
   WHEN ntn_ind = 1
   THEN acp_id
   ELSE NULL
   END) AS ntn_cust_count,
 SUM(line_net_usd_amt) AS net_spend,
 SUM(line_item_quantity) AS net_units,
 COUNT(DISTINCT global_tran_id) AS net_tran_ct,
  CASE
  WHEN COUNT(DISTINCT global_tran_id) <> 0
  THEN ROUND(CAST(CAST(SUM(line_item_quantity) AS FLOAT64) / CAST(COUNT(DISTINCT global_tran_id) AS FLOAT64) AS NUMERIC
    ), 2)
  ELSE NULL
  END AS net_upt,
 SUM(CASE
   WHEN line_net_usd_amt > 0
   THEN line_item_quantity
   ELSE 0
   END) AS gross_units,
 COUNT(DISTINCT CASE
   WHEN line_net_usd_amt > 0
   THEN global_tran_id
   ELSE NULL
   END) AS gross_tran_ct,
  CASE
  WHEN COUNT(DISTINCT CASE
     WHEN line_net_usd_amt > 0
     THEN global_tran_id
     ELSE NULL
     END) <> 0
  THEN ROUND(CAST(CAST(SUM(CASE
        WHEN line_net_usd_amt > 0
        THEN line_item_quantity
        ELSE 0
        END) AS FLOAT64) / CAST(COUNT(DISTINCT CASE
        WHEN line_net_usd_amt > 0
        THEN global_tran_id
        ELSE NULL
        END) AS FLOAT64) AS NUMERIC), 2)
  ELSE NULL
  END AS gross_upt,
 SUM(CASE
   WHEN line_net_usd_amt > 0
   THEN line_net_usd_amt
   ELSE 0
   END) AS gross_spend,
 COUNT(DISTINCT CASE
   WHEN line_net_usd_amt > 0
   THEN acp_id || FORMAT('%11d', store_num) || CAST(COALESCE(order_date, tran_date) AS STRING)
   ELSE NULL
   END) AS trips
FROM full_tran_ly_stg AS dtl
GROUP BY week_idnt,
 week_start_day_date,
 week_end_day_date,
 ty_ly_lly_ind,
 store_num,
 nordy_level;


DROP TABLE IF EXISTS full_tran_ty_stg;


DROP TABLE IF EXISTS full_tran_ly_stg;


CREATE TEMPORARY TABLE IF NOT EXISTS tran_final
AS
SELECT tr.week_idnt,
 tr.store_num,
 st.store_name,
 dt.ty_ly_lly_ind,
 dt.week_desc,
 dt.week_label,
 dt.fiscal_week_num,
 dt.week_start_day_date,
 dt.week_end_day_date,
 dt.week_num_of_fiscal_month,
 dt.month_idnt,
 dt.month_desc,
 dt.month_label,
 dt.month_abrv,
 dt.fiscal_month_num,
 dt.month_start_day_date,
 dt.month_start_week_idnt,
 dt.month_end_day_date,
 dt.month_end_week_idnt,
 dt.quarter_idnt,
 dt.fiscal_year_num,
 st.store_type_code,
 st.store_type_desc,
 st.selling_store_ind,
 st.region_num,
 st.region_desc,
 st.business_unit_num,
 st.business_unit_desc,
 st.subgroup_num,
 st.subgroup_desc,
 st.store_dma_code,
 st.dma_desc,
 st.dma_shrt_desc,
 st.rack_district_num,
 st.rack_district_desc,
 st.channel_num,
 st.channel_desc,
 st.comp_status_code,
 st.comp_status_desc,
 st.cluster_climate,
 st.cluster_price,
 st.cluster_designer,
 st.cluster_presidents_cup,
 tr.nordy_level,
 tr.cust_count,
 tr.ntn_cust_count,
 tr.net_spend,
 tr.net_units,
 tr.net_tran_ct,
 tr.net_upt,
 tr.gross_units,
 tr.gross_tran_ct,
 tr.gross_upt,
 tr.gross_spend,
 tr.trips
FROM tran AS tr
 LEFT JOIN wk_lkup AS dt ON tr.week_idnt = dt.week_idnt
 LEFT JOIN store_lkup AS st ON tr.store_num = st.store_num;



DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.store_cust_count{{params.env_suffix}}
WHERE fiscal_year_num <= (SELECT MAX(fiscal_year_num) - 2
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE week_end_day_date <= {{params.end_date}}) OR fiscal_year_num = (SELECT MAX(fiscal_year_num) - 1
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE week_end_day_date <= {{params.end_date}}) AND LOWER(ty_ly_lly_ind) = LOWER('TY') OR fiscal_week_num BETWEEN (SELECT
    COALESCE(MAX(fiscal_week_num), 1) AS fiscal_week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date <= {{params.start_date}}
    AND fiscal_year_num = (SELECT MAX(fiscal_year_num)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE week_end_day_date <= {{params.end_date}})) AND (SELECT MAX(fiscal_week_num)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date <= {{params.end_date}}
    AND fiscal_year_num = (SELECT MAX(fiscal_year_num)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE week_end_day_date <= {{params.end_date}}));


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.store_cust_count{{params.env_suffix}}
(SELECT week_idnt,
  store_num,
  store_name,
  ty_ly_lly_ind,
  week_desc,
  week_label,
  fiscal_week_num,
  week_start_day_date,
  week_end_day_date,
  week_num_of_fiscal_month,
  month_idnt,
  month_desc,
  month_label,
  month_abrv,
  fiscal_month_num,
  month_start_day_date,
  month_start_week_idnt,
  month_end_day_date,
  month_end_week_idnt,
  quarter_idnt,
  fiscal_year_num,
  store_type_code,
  store_type_desc,
  selling_store_ind,
  region_num,
  region_desc,
  business_unit_num,
  business_unit_desc,
  subgroup_num,
  subgroup_desc,
  store_dma_code,
  dma_desc,
  dma_shrt_desc,
  CAST(TRUNC(CAST(rack_district_num AS FLOAT64)) AS INTEGER) AS rack_district_num,
  rack_district_desc,
  channel_num,
  channel_desc,
  comp_status_code,
  comp_status_desc,
  cluster_climate,
  cluster_price,
  cluster_designer,
  cluster_presidents_cup,
  nordy_level,
  cust_count,
  ntn_cust_count,
  net_spend,
  CAST(TRUNC(net_units) AS INTEGER) AS net_units,
  net_tran_ct,
  CAST(net_upt AS NUMERIC) AS net_upt,
  CAST(TRUNC(gross_units) AS INTEGER) AS gross_units,
  gross_tran_ct,
  CAST(gross_upt AS NUMERIC) AS gross_upt,
  gross_spend,
  trips
 FROM tran_final);

