
CREATE TEMPORARY TABLE IF NOT EXISTS dates AS
SELECT MIN(min_day_date) AS min_date,
 MAX(max_day_date) AS max_date
FROM (SELECT * FROM(SELECT DISTINCT MAX(day_date) OVER (PARTITION BY month_454_num, quarter_454_num, year_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_day_date,
   MIN(day_date) OVER (PARTITION BY month_454_num, quarter_454_num, year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS min_day_date,
       '(' || CAST(MIN(day_date) OVER (PARTITION BY month_454_num, quarter_454_num, year_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS STRING)
        || ' - ' || CAST(MAX(day_date) OVER (PARTITION BY month_454_num, quarter_454_num, year_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS STRING)
      || ')' AS fm_range,
   month_454_num AS fiscal_month,
   quarter_454_num AS fiscal_quarter,
   year_num AS fiscal_year
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE day_date BETWEEN DATE '2022-12-31' AND (DATE_ADD(DATE_SUB(CURRENT_DATE ('PST8PDT'), INTERVAL 1 DAY), INTERVAL 60 DAY)))
   WHERE (max_day_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY) AND fiscal_year >= 2023
    AND fm_range NOT IN (SELECT
       DISTINCT fm_range
       FROM `{{params.gcp_project_id}}`.{{params.cli_t2_schema}}.consented_customer_seller_monthly))) AS t5;

	   
CREATE TEMPORARY TABLE IF NOT EXISTS cal_dates
CLUSTER BY day_date
AS
SELECT a.day_date,
 a.max_day_date,
 a.min_day_date,
 a.fm_range,
 a.fiscal_week,
 a.fiscal_month,
 a.fiscal_quarter,
 a.fiscal_year
FROM (SELECT dc.day_date,
   MAX(dc.day_date) OVER (PARTITION BY dc.month_454_num, dc.quarter_454_num, dc.year_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_day_date,
   MIN(dc.day_date) OVER (PARTITION BY dc.month_454_num, dc.quarter_454_num, dc.year_num RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_day_date,
       '(' || CAST(MIN(dc.day_date) OVER (PARTITION BY dc.month_454_num, dc.quarter_454_num, dc.year_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS STRING)
        || ' - ' || CAST(MAX(dc.day_date) OVER (PARTITION BY dc.month_454_num, dc.quarter_454_num, dc.year_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS STRING)
      || ')' AS fm_range,
   dc.week_of_fyr AS fiscal_week,
   dc.month_454_num AS fiscal_month,
   dc.quarter_454_num AS fiscal_quarter,
   dc.year_num AS fiscal_year
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS dc
   INNER JOIN dates AS d ON dc.day_date BETWEEN DATE_SUB(d.min_date, INTERVAL 30 DAY) AND (DATE_ADD(d.max_date, INTERVAL
      30 DAY))) AS a
 INNER JOIN dates AS b ON a.day_date BETWEEN b.min_date AND b.max_date;


CREATE TEMPORARY TABLE IF NOT EXISTS restaurant_dept_lookup
CLUSTER BY dept_num
AS
SELECT DISTINCT dept_num
FROM prd_nap_usr_vws.department_dim
WHERE division_num = 70;


CREATE TEMPORARY TABLE IF NOT EXISTS hr_range
CLUSTER BY worker_number
AS
SELECT DISTINCT LPAD(a.worker_number, 15, '0') AS worker_number,
 CONCAT(nme.last_name, ', ', nme.first_name) AS employee_name,
 nme.termination_date,
 nme.hire_date AS latest_hire_date,
 LTRIM(a.payroll_store, '0') AS payroll_store,
 stor.store_name AS payroll_store_description,
 dpt.organization_description AS payroll_department_description,
 COALESCE(CAST(a.eff_begin_date AS DATE), nme.hire_date) AS eff_begin_date,
 CAST(DATETIME_SUB(a.eff_end_date, INTERVAL 1 SECOND) AS DATE) AS eff_end_date,
 MIN(COALESCE(CAST(a.eff_begin_date AS DATE), nme.hire_date)) OVER (PARTITION BY a.worker_number RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS original_hire_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_org_details_dim_eff_date_vw AS a
 LEFT JOIN (SELECT worker_number,
   last_name,
   first_name,
   hire_date,
   worker_status,
   termination_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_worker_v1_dim AS a) AS nme ON LOWER(a.worker_number) = LOWER(nme.worker_number) AND CAST(a.eff_begin_date AS DATE)
   BETWEEN nme.hire_date AND (COALESCE(nme.termination_date, DATE '9999-12-31'))
 INNER JOIN (SELECT store_num,
   store_name,
   store_type_code
  FROM prd_nap_usr_vws.store_dim
  WHERE LOWER(store_type_code) IN (LOWER('FL'))) AS stor ON LOWER(SUBSTR(LTRIM(a.payroll_store, '0'), 1, 10)) = LOWER(SUBSTR(CAST(stor.store_num AS STRING)
    , 1, 10))
 INNER JOIN (SELECT organization_code,
   organization_description
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_worker_org_dim
  WHERE LOWER(organization_type) = LOWER('DEPARTMENT')
   AND CAST(CASE
      WHEN is_inactive = ''
      THEN '0'
      ELSE is_inactive
      END AS INTEGER) = 0) AS dpt ON LOWER(a.payroll_department) = LOWER(dpt.organization_code)
WHERE CAST(a.eff_end_date AS DATE) >= (SELECT min_date
   FROM dates);



CREATE TEMPORARY TABLE IF NOT EXISTS tyly_sales AS
WITH dat AS (
  SELECT 
    dtl.acp_id,
    dtl.global_tran_id,
    dtl.line_item_seq_num,
    COALESCE(dtl.order_date, dtl.tran_date) AS sale_date,
    dtl.line_item_quantity AS item_cnt,
    dtl.line_net_usd_amt AS gross_amt,
    CASE 
      WHEN dtl.line_item_order_type = 'CustInitWebOrder' AND dtl.line_item_fulfillment_type = 'StorePickUp'
           AND dtl.line_item_net_amt_currency_code = 'CAD' THEN 867
      WHEN dtl.line_item_order_type = 'CustInitWebOrder' AND dtl.line_item_fulfillment_type = 'StorePickUp'
           AND dtl.line_item_net_amt_currency_code = 'USD' THEN 808
      ELSE dtl.intent_store_num 
    END AS store_num,
    CASE 
      WHEN dtl.commission_slsprsn_num IS NOT NULL 
           AND dtl.commission_slsprsn_num NOT IN ('2079333', '2079334', '0000')
      THEN dtl.commission_slsprsn_num 
      ELSE NULL 
    END AS commission_slsprsn_num,
    dtl.item_source,
    dtl.trip_id
  FROM 
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw dtl
  LEFT JOIN 
    restaurant_dept_lookup r 


  ON COALESCE(CAST(dtl.merch_dept_num AS STRING), CAST(-1 * dtl.intent_store_num AS STRING)) = CAST(r.dept_num AS STRING)


  WHERE 
    TRIM(COALESCE(dtl.upc_num, '-1')) <> '-1'
    AND dtl.acp_id IS NOT NULL
    AND dtl.line_net_usd_amt > 0
    AND COALESCE(dtl.nonmerch_fee_code, 'na') <> '6666'
    AND dtl.business_day_date > DATE('2022-01-01')
    AND COALESCE(dtl.order_date, dtl.tran_date) > DATE('2022-01-01')
    AND EXISTS (
      SELECT 1  FROM dates d WHERE d.max_date BETWEEN d.min_date AND d.max_date
    )
    AND r.dept_num IS NULL
)
SELECT DISTINCT 
  dat.*,
  str.business_unit_desc,
  CASE 
    WHEN dat.commission_slsprsn_num IS NOT NULL THEN 'Y' 
    ELSE 'N' 
  END AS sale_w_commissioned_seller,
  MAX(CASE 
        WHEN dat.commission_slsprsn_num = c.seller_id THEN 'Y' 
        ELSE 'N' 
      END) OVER (PARTITION BY CONCAT(global_tran_id, line_item_seq_num)) AS sale_w_consented_seller,
  CASE 
    WHEN dat.item_source = 'SB_SALESEMPINIT' THEN 'Y' 
    ELSE 'N' 
  END AS digital_styling
FROM 
  dat
JOIN 
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str 
ON 
  dat.store_num = str.store_num
  AND str.business_unit_desc IN ('FULL LINE', 'N.COM')
JOIN 
  T2DL_DAS_CLIENTELING.consented_customer_relationship_hist c 
ON 
  dat.acp_id = c.acp_id
  AND dat.sale_date BETWEEN c.opt_in_date_pst AND c.opt_out_date_pst;





CREATE TEMPORARY TABLE IF NOT EXISTS tyly_sales_hr AS
SELECT 
  ccc.*,
  
  MAX(cal.fm_range) OVER (PARTITION BY ccc.sale_date) AS fm_range,
  MAX(cal.fiscal_month) OVER (PARTITION BY ccc.sale_date) AS fiscal_month,
  MAX(cal.fiscal_quarter) OVER (PARTITION BY ccc.sale_date) AS fiscal_quarter,
  MAX(cal.fiscal_year) OVER (PARTITION BY ccc.sale_date) AS fiscal_year,

  CASE 
    WHEN (
      (ccc.sale_date >= COALESCE(hr.termination_date, DATE('9999-12-31')) 
       AND ccc.sale_date <= COALESCE(hr.latest_hire_date, DATE('9999-12-31')))
      OR
      (ccc.sale_date >= COALESCE(hr.termination_date, DATE('9999-12-31')) 
       AND hr.termination_date > hr.latest_hire_date)
    ) THEN 'TERMINATED'
    WHEN ccc.commission_slsprsn_num IS NULL THEN NULL
    ELSE 'ACTIVE' 
  END AS worker_status,

  CASE 
    WHEN (CASE 
    WHEN (
      (ccc.sale_date >= COALESCE(hr.termination_date, DATE('9999-12-31')) 
       AND ccc.sale_date <= COALESCE(hr.latest_hire_date, DATE('9999-12-31')))
      OR
      (ccc.sale_date >= COALESCE(hr.termination_date, DATE('9999-12-31')) 
       AND hr.termination_date > hr.latest_hire_date)
    ) THEN 'TERMINATED'
    WHEN ccc.commission_slsprsn_num IS NULL THEN NULL
    ELSE 'ACTIVE' 
  END) = 'TERMINATED' THEN 'TERMINATED' 
    ELSE ccc.commission_slsprsn_num 
  END AS seller_id,

  CASE 
    WHEN (CASE 
    WHEN (
      (ccc.sale_date >= COALESCE(hr.termination_date, DATE('9999-12-31')) 
       AND ccc.sale_date <= COALESCE(hr.latest_hire_date, DATE('9999-12-31')))
      OR
      (ccc.sale_date >= COALESCE(hr.termination_date, DATE('9999-12-31')) 
       AND hr.termination_date > hr.latest_hire_date)
    ) THEN 'TERMINATED'
    WHEN ccc.commission_slsprsn_num IS NULL THEN NULL
    ELSE 'ACTIVE' 
  END) = 'TERMINATED' THEN 'TERMINATED' 
    ELSE hr.employee_name 
  END AS seller_name,

  CASE 
    WHEN hr.payroll_store_description LIKE '%CLOSED%' THEN 'CLOSED' 
    ELSE hr.payroll_store 
  END AS payroll_store,

  CASE 
    WHEN hr.payroll_store_description LIKE '%CLOSED%' THEN 'CLOSED' 
    ELSE hr.payroll_store_description 
  END AS payroll_store_description,

  CASE 
    WHEN hr.payroll_store_description LIKE '%CLOSED%' THEN 'CLOSED' 
    ELSE hr.payroll_department_description 
  END AS payroll_department_description

FROM 
  tyly_sales ccc
JOIN 
  cal_dates cal 
ON 
  ccc.sale_date = cal.day_date
LEFT JOIN 
  hr_range hr 
ON 
  LPAD(ccc.commission_slsprsn_num, 15, '0') = hr.worker_number
  AND ccc.sale_date BETWEEN hr.eff_begin_date AND hr.eff_end_date;







CREATE TEMPORARY TABLE IF NOT EXISTS tyly_sales_daily_tripShare AS
SELECT DISTINCT
  a.sale_date,
  a.fm_range,
  a.fiscal_month,
  a.fiscal_quarter,
  a.fiscal_year,
  a.trip_id,
  a.acp_id,
  a.sale_w_commissioned_seller AS TS_sale_w_commissioned_seller,
  a.sale_w_consented_seller AS TS_sale_w_consented_seller,

  CASE 
    WHEN a.sale_w_consented_seller = 'Y' THEN a.commission_slsprsn_num
    WHEN a.commission_slsprsn_num IS NOT NULL THEN 'Non-Consented Seller'
    ELSE NULL
  END AS TS_commission_slsprsn_num,

  CASE 
    WHEN a.sale_w_consented_seller = 'Y' THEN a.seller_name
    WHEN a.commission_slsprsn_num IS NOT NULL THEN 'Non-Consented Seller'
    ELSE NULL
  END AS TS_seller_name,

  CASE 
    WHEN a.sale_w_consented_seller = 'Y' THEN a.worker_status
    WHEN a.commission_slsprsn_num IS NOT NULL THEN 'Non-Consented Seller'
    ELSE NULL
  END AS TS_worker_status,

  a.payroll_store,
  a.payroll_store_description,
  a.payroll_department_description,

  CASE 
    WHEN a.business_unit_desc = 'FULL LINE' THEN roll.trip_gross_sales
    ELSE 0 
  END AS TS_gross_FLS_spend,

  CASE 
    WHEN a.business_unit_desc = 'FULL LINE' THEN 1
    ELSE 0 
  END AS TS_FLS_trips,

  CASE 
    WHEN a.business_unit_desc = 'N.COM' THEN roll.trip_gross_sales
    ELSE 0 
  END AS TS_gross_NCOM_spend,

  CASE 
    WHEN a.business_unit_desc = 'N.COM' THEN 1
    ELSE 0 
  END AS TS_NCOM_trips,

  CASE 
    WHEN a.business_unit_desc = 'N.COM' AND a.digital_styling = 'Y' THEN roll.trip_gross_sales
    ELSE 0 
  END AS TS_sb_gross_NCOM_spend,

  CASE 
    WHEN a.business_unit_desc = 'N.COM' AND a.digital_styling = 'Y' THEN 1
    ELSE 0 
  END AS TS_sb_NCOM_trips

FROM 
  tyly_sales_hr a
JOIN (select * from (
  SELECT rnk.*,
    DENSE_RANK() OVER (
      PARTITION BY trip_id
      ORDER BY 
        sale_w_consented_seller DESC,
        digital_styling DESC,
        sale_w_commissioned_seller DESC,
        trip_gross_sales DESC,
        worker_status,
        seller_id,
        commission_slsprsn_num,
        payroll_department_description
    ) AS ranknbr
  FROM (
    SELECT DISTINCT
      trip_id,
      COALESCE(seller_id, 'na') AS seller_id,
      sale_w_commissioned_seller,
      sale_w_consented_seller,
      digital_styling,
      COALESCE(worker_status, 'na') AS worker_status,
      COALESCE(commission_slsprsn_num, 'na') AS commission_slsprsn_num,
      COALESCE(payroll_department_description, 'na') AS payroll_department_description,
      SUM(gross_amt) OVER (PARTITION BY trip_id) AS trip_gross_sales
    FROM 
      tyly_sales_hr
  ) rnk)
  WHERE ranknbr = 1
) roll
ON 
  a.trip_id = roll.trip_id
  AND COALESCE(a.seller_id, 'na') = roll.seller_id
  AND a.sale_w_commissioned_seller = roll.sale_w_commissioned_seller
  AND a.sale_w_consented_seller = roll.sale_w_consented_seller
  AND a.digital_styling = roll.digital_styling
  AND COALESCE(a.worker_status, 'na') = roll.worker_status
  AND COALESCE(a.commission_slsprsn_num, 'na') = roll.commission_slsprsn_num
  AND COALESCE(a.payroll_department_description, 'na') = roll.payroll_department_description;









CREATE TEMPORARY TABLE IF NOT EXISTS  tyly_sales_daily_sellershare AS
SELECT
  a.sale_date,
  a.fm_range,
  a.fiscal_month,
  a.fiscal_quarter,
  a.fiscal_year,
  a.trip_id,
  a.acp_id,
  a.sale_w_commissioned_seller,
  a.sale_w_consented_seller,
  a.commission_slsprsn_num,

  CASE 
    WHEN a.sale_w_consented_seller = 'Y' THEN a.seller_name
    WHEN a.commission_slsprsn_num IS NOT NULL THEN 'Non-Consented Seller'
    ELSE NULL 
  END AS seller_name,

  CASE 
    WHEN a.sale_w_consented_seller = 'Y' THEN a.worker_status
    WHEN a.commission_slsprsn_num IS NOT NULL THEN 'Non-Consented Seller'
    ELSE NULL 
  END AS worker_status,

  a.payroll_store,
  a.payroll_store_description,
  a.payroll_department_description,

  CASE 
    WHEN a.business_unit_desc = 'FULL LINE' THEN 1 
    ELSE 0 
  END AS FLS_trips,

  CASE 
    WHEN a.business_unit_desc = 'N.COM' THEN 1 
    ELSE 0 
  END AS NCOM_trips,

  CASE 
    WHEN a.business_unit_desc = 'N.COM' AND a.digital_styling = 'Y' THEN 1 
    ELSE 0 
  END AS sb_NCOM_trips,

  SUM(CASE 
        WHEN a.business_unit_desc = 'FULL LINE' THEN COALESCE(a.gross_amt, 0) 
        ELSE 0 
      END) AS gross_FLS_spend,

  SUM(CASE 
        WHEN a.business_unit_desc = 'N.COM' THEN COALESCE(a.gross_amt, 0) 
        ELSE 0 
      END) AS gross_NCOM_spend,

  SUM(CASE 
        WHEN a.business_unit_desc = 'N.COM' AND a.digital_styling = 'Y' THEN COALESCE(a.gross_amt, 0) 
        ELSE 0 
      END) AS sb_gross_NCOM_spend

FROM 
  tyly_sales_hr a
GROUP BY 
  a.sale_date, 
  a.fm_range, 
  a.fiscal_month, 
  a.fiscal_quarter, 
  a.fiscal_year, 
  a.trip_id, 
  a.acp_id, 
  a.sale_w_commissioned_seller, 
  a.sale_w_consented_seller, 
  a.commission_slsprsn_num, 
  a.seller_name, 
  a.worker_status, 
  a.payroll_store, 
  a.payroll_store_description, 
  a.payroll_department_description, 
  a.business_unit_desc, 
  a.digital_styling;





CREATE TEMPORARY TABLE IF NOT EXISTS tyly_sales_weekly AS
WITH dbl AS (
    SELECT DISTINCT
        fm_range,
        acp_id,
        fiscal_month,
        fiscal_quarter,
        fiscal_year,
        CASE 
            WHEN sale_w_consented_seller = 'Y' THEN commission_slsprsn_num
            WHEN commission_slsprsn_num IS NOT NULL THEN 'Non-Consented Seller'
            ELSE NULL 
        END AS seller_id,
        seller_name,
        worker_status,
        payroll_store,
        payroll_store_description,
        payroll_department_description,

        SUM(gross_FLS_spend) OVER (PARTITION BY fm_range, acp_id, worker_status,  payroll_store, payroll_department_description) AS gross_FLS_spend,
        SUM(FLS_trips) OVER (PARTITION BY fm_range, acp_id, worker_status,  payroll_store, payroll_department_description) AS FLS_trips,
        SUM(gross_NCOM_spend) OVER (PARTITION BY fm_range, acp_id, worker_status,  payroll_store, payroll_department_description) AS gross_NCOM_spend,
        SUM(NCOM_trips) OVER (PARTITION BY fm_range, acp_id, worker_status,  payroll_store, payroll_department_description) AS NCOM_trips,
        SUM(sb_gross_NCOM_spend) OVER (PARTITION BY fm_range, acp_id, worker_status, payroll_store, payroll_department_description) AS sb_gross_NCOM_spend,
        SUM(sb_NCOM_trips) OVER (PARTITION BY fm_range, acp_id, worker_status,  payroll_store, payroll_department_description) AS sb_NCOM_trips
    FROM tyly_sales_daily_sellershare
),
ts AS (
    SELECT DISTINCT
        fm_range,
        acp_id,
        fiscal_month,
        fiscal_quarter,
        fiscal_year,
        TS_commission_slsprsn_num,
        TS_seller_name,
        TS_worker_status,
        payroll_store,
        payroll_store_description,
        payroll_department_description,
        
        SUM(TS_gross_FLS_spend) OVER (PARTITION BY fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) AS TS_gross_FLS_spend,
        SUM(TS_FLS_trips) OVER (PARTITION BY fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) AS TS_FLS_trips,
        SUM(TS_gross_NCOM_spend) OVER (PARTITION BY fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) AS TS_gross_NCOM_spend,
        SUM(TS_NCOM_trips) OVER (PARTITION BY fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) AS TS_NCOM_trips,
        SUM(TS_sb_gross_NCOM_spend) OVER (PARTITION BY fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) AS TS_sb_gross_NCOM_spend,
        SUM(TS_sb_NCOM_trips) OVER (PARTITION BY fm_range, acp_id, TS_commission_slsprsn_num, payroll_store, payroll_department_description) AS TS_sb_NCOM_trips,

        SUM(TS_FLS_trips) OVER (PARTITION BY fm_range, acp_id) AS TS_acp_FLS_trips,
        SUM(TS_NCOM_trips) OVER (PARTITION BY fm_range, acp_id) AS TS_acp_NCOM_trips,

        SUM(CASE WHEN TS_sale_w_commissioned_seller = 'Y' THEN TS_FLS_trips END) OVER (PARTITION BY fm_range, acp_id) AS TS_acp_FLS_commissioned_trips,
        SUM(CASE WHEN TS_sale_w_commissioned_seller = 'Y' THEN TS_NCOM_trips END) OVER (PARTITION BY fm_range, acp_id) AS TS_acp_NCOM_commissioned_trips,

        SUM(CASE WHEN TS_sale_w_consented_seller = 'Y' THEN TS_FLS_trips END) OVER (PARTITION BY fm_range, acp_id) AS TS_acp_FLS_consented_trips,
        SUM(CASE WHEN TS_sale_w_consented_seller = 'Y' THEN TS_NCOM_trips END) OVER (PARTITION BY fm_range, acp_id) AS TS_acp_NCOM_consented_trips,

        SUM(TS_sb_NCOM_trips) OVER (PARTITION BY fm_range, acp_id) AS TS_acp_sb_NCOM_trips
    FROM tyly_sales_daily_tripShare
)
SELECT DISTINCT
    dbl.fm_range,
    dbl.acp_id,
    dbl.fiscal_month,
    dbl.fiscal_quarter,
    dbl.fiscal_year,
    dbl.seller_id,
    dbl.seller_name,
    dbl.worker_status,
    dbl.payroll_store,
    dbl.payroll_store_description,
    dbl.payroll_department_description,

    dbl.FLS_trips AS seller_FLS_trips,
    dbl.NCOM_trips AS seller_NCOM_trips,
    dbl.sb_NCOM_trips AS seller_sb_NCOM_trips,

    dbl.gross_FLS_spend AS seller_gross_FLS_spend,
    dbl.gross_NCOM_spend AS seller_gross_NCOM_spend,
    dbl.sb_gross_NCOM_spend AS seller_sb_gross_NCOM_spend,

    COALESCE(ts.TS_FLS_trips, 0) AS TS_FLS_trips,
    COALESCE(ts.TS_NCOM_trips, 0) AS TS_NCOM_trips,

    COALESCE(ts.TS_gross_FLS_spend, 0) AS TS_gross_FLS_spend,
    COALESCE(ts.TS_gross_NCOM_spend, 0) AS TS_gross_NCOM_spend,
    COALESCE(ts.TS_sb_gross_NCOM_spend, 0) AS TS_sb_gross_NCOM_spend,
    COALESCE(ts.TS_sb_NCOM_trips, 0) AS TS_sb_NCOM_trips,

    MAX(COALESCE(ts.TS_acp_FLS_trips, 0)) OVER (PARTITION BY dbl.fm_range, dbl.acp_id) AS TS_acp_FLS_trips,
    MAX(COALESCE(ts.TS_acp_NCOM_trips, 0)) OVER (PARTITION BY dbl.fm_range, dbl.acp_id) AS TS_acp_NCOM_trips,

    MAX(COALESCE(ts.TS_acp_FLS_commissioned_trips, 0)) OVER (PARTITION BY dbl.fm_range, dbl.acp_id) AS TS_acp_FLS_commissioned_trips,
    MAX(COALESCE(ts.TS_acp_NCOM_commissioned_trips, 0)) OVER (PARTITION BY dbl.fm_range, dbl.acp_id) AS TS_acp_NCOM_commissioned_trips,

    MAX(COALESCE(ts.TS_acp_FLS_consented_trips, 0)) OVER (PARTITION BY dbl.fm_range, dbl.acp_id) AS TS_acp_FLS_consented_trips,
    MAX(COALESCE(ts.TS_acp_NCOM_consented_trips, 0)) OVER (PARTITION BY dbl.fm_range, dbl.acp_id) AS TS_acp_NCOM_consented_trips,

    MAX(COALESCE(ts.TS_acp_sb_NCOM_trips, 0)) OVER (PARTITION BY dbl.fm_range, dbl.acp_id) AS TS_acp_sb_NCOM_trips
FROM dbl
LEFT JOIN ts
    ON dbl.fm_range = ts.fm_range
    AND dbl.acp_id = ts.acp_id
    AND COALESCE(dbl.seller_id, 'na') = COALESCE(ts.TS_commission_slsprsn_num, 'na')
    AND COALESCE(dbl.payroll_store, 'na') = COALESCE(ts.payroll_store, 'na')
    AND COALESCE(dbl.payroll_department_description, 'na') = COALESCE(ts.payroll_department_description, 'na');






CREATE TEMPORARY TABLE IF NOT EXISTS consented_daily_pt1
CLUSTER BY acp_id, seller_id
AS
SELECT cc.acp_id,
 a.fm_range,
 a.min_day_date,
 a.max_day_date,
 a.fiscal_month,
 a.fiscal_quarter,
 a.fiscal_year,
 cc.first_seller_consent_dt,
 cc.first_clienteling_consent_dt,
  CASE
  WHEN LOWER(CASE
     WHEN a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND a.day_date <= COALESCE(hr.latest_hire_date
         , DATE '9999-12-31') OR a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND hr.termination_date
        > hr.latest_hire_date
     THEN 'TERMINATED'
     WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
     THEN 'CLOSED'
     WHEN cc.seller_id IS NULL
     THEN NULL
     ELSE 'ACTIVE'
     END) = LOWER('TERMINATED')
  THEN 'TERMINATED'
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE cc.seller_id
  END AS seller_id,
  CASE
  WHEN LOWER(CASE
     WHEN a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND a.day_date <= COALESCE(hr.latest_hire_date
         , DATE '9999-12-31') OR a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND hr.termination_date
        > hr.latest_hire_date
     THEN 'TERMINATED'
     WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
     THEN 'CLOSED'
     WHEN cc.seller_id IS NULL
     THEN NULL
     ELSE 'ACTIVE'
     END) = LOWER('TERMINATED')
  THEN 'TERMINATED'
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.employee_name
  END AS seller_name,
  CASE
  WHEN a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND a.day_date <= COALESCE(hr.latest_hire_date,
      DATE '9999-12-31') OR a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND hr.termination_date > hr
     .latest_hire_date
  THEN 'TERMINATED'
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  WHEN cc.seller_id IS NULL
  THEN NULL
  ELSE 'ACTIVE'
  END AS worker_status,
  CASE
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.payroll_store
  END AS payroll_store,
  CASE
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.payroll_store_description
  END AS payroll_store_description,
  CASE
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.payroll_department_description
  END AS payroll_department_description,
 MAX(cc.seller_history) AS seller_history,
 MAX(cc.clienteling_history) AS clienteling_history,
 MAX(CASE
   WHEN cc.first_clienteling_consent_dt BETWEEN a.min_day_date AND a.max_day_date AND LOWER(cc.clienteling_history) =
     LOWER('NEW TO CLIENTELING')
   THEN 1
   ELSE 0
   END) AS new_to_clienteling,
 MAX(CASE
   WHEN a.day_date = cc.opt_in_date_pst
   THEN 1
   ELSE 0
   END) AS optedintoday,
 MAX(CASE
   WHEN a.day_date = cc.opt_out_date_pst
   THEN 1
   ELSE 0
   END) AS optedouttoday,
 MAX(CASE
   WHEN a.day_date >= cc.first_seller_consent_dt
   THEN 1
   ELSE 0
   END) AS consented_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 30 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1mo_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 90 DAY)
   THEN 1
   ELSE 0
   END) AS consented_3mo_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 180 DAY)
   THEN 1
   ELSE 0
   END) AS consented_6mo_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 365 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1yr_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 730 DAY)
   THEN 1
   ELSE 0
   END) AS consented_2yr_wseller,
 MAX(CASE
   WHEN a.day_date >= cc.first_clienteling_consent_dt
   THEN 1
   ELSE 0
   END) AS consented_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 30 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1mo_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 90 DAY)
   THEN 1
   ELSE 0
   END) AS consented_3mo_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 180 DAY)
   THEN 1
   ELSE 0
   END) AS consented_6mo_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 365 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1yr_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 730 DAY)
   THEN 1
   ELSE 0
   END) AS consented_2yr_wclienteling
FROM cal_dates AS a
 INNER JOIN t2dl_das_clienteling.consented_customer_relationship_hist AS cc ON a.day_date BETWEEN cc.opt_in_date_pst AND
  cc.opt_out_date_pst
 LEFT JOIN hr_range AS hr ON LOWER(LPAD(cc.seller_id, 15, '0')) = LOWER(hr.worker_number) AND a.day_date BETWEEN hr.eff_begin_date
   AND hr.eff_end_date
WHERE (LOWER(a.fm_range) LIKE LOWER('%2023%') OR LOWER(a.fm_range) LIKE LOWER('%2025%'))
 AND cc.acp_id IS NOT NULL
GROUP BY cc.acp_id,
 a.fm_range,
 a.min_day_date,
 a.max_day_date,
 a.fiscal_month,
 a.fiscal_quarter,
 a.fiscal_year,
 cc.first_seller_consent_dt,
 cc.first_clienteling_consent_dt,
 seller_id,
 seller_name,
 worker_status,
 payroll_store,
 payroll_store_description,
 payroll_department_description;

CREATE TEMPORARY TABLE IF NOT EXISTS consented_daily_pt2
CLUSTER BY acp_id, seller_id
AS
SELECT cc.acp_id,
 a.fm_range,
 a.min_day_date,
 a.max_day_date,
 a.fiscal_month,
 a.fiscal_quarter,
 a.fiscal_year,
 cc.first_seller_consent_dt,
 cc.first_clienteling_consent_dt,
  CASE
  WHEN LOWER(CASE
     WHEN a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND a.day_date <= COALESCE(hr.latest_hire_date
         , DATE '9999-12-31') OR a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND hr.termination_date
        > hr.latest_hire_date
     THEN 'TERMINATED'
     WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
     THEN 'CLOSED'
     WHEN cc.seller_id IS NULL
     THEN NULL
     ELSE 'ACTIVE'
     END) = LOWER('TERMINATED')
  THEN 'TERMINATED'
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE cc.seller_id
  END AS seller_id,
  CASE
  WHEN LOWER(CASE
     WHEN a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND a.day_date <= COALESCE(hr.latest_hire_date
         , DATE '9999-12-31') OR a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND hr.termination_date
        > hr.latest_hire_date
     THEN 'TERMINATED'
     WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
     THEN 'CLOSED'
     WHEN cc.seller_id IS NULL
     THEN NULL
     ELSE 'ACTIVE'
     END) = LOWER('TERMINATED')
  THEN 'TERMINATED'
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.employee_name
  END AS seller_name,
  CASE
  WHEN a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND a.day_date <= COALESCE(hr.latest_hire_date,
      DATE '9999-12-31') OR a.day_date >= COALESCE(hr.termination_date, DATE '9999-12-31') AND hr.termination_date > hr
     .latest_hire_date
  THEN 'TERMINATED'
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  WHEN cc.seller_id IS NULL
  THEN NULL
  ELSE 'ACTIVE'
  END AS worker_status,
  CASE
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.payroll_store
  END AS payroll_store,
  CASE
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.payroll_store_description
  END AS payroll_store_description,
  CASE
  WHEN LOWER(hr.payroll_store_description) LIKE LOWER('%CLOSED%')
  THEN 'CLOSED'
  ELSE hr.payroll_department_description
  END AS payroll_department_description,
 MAX(cc.seller_history) AS seller_history,
 MAX(cc.clienteling_history) AS clienteling_history,
 MAX(CASE
   WHEN cc.first_clienteling_consent_dt BETWEEN a.min_day_date AND a.max_day_date AND LOWER(cc.clienteling_history) =
     LOWER('NEW TO CLIENTELING')
   THEN 1
   ELSE 0
   END) AS new_to_clienteling,
 MAX(CASE
   WHEN a.day_date = cc.opt_in_date_pst
   THEN 1
   ELSE 0
   END) AS optedintoday,
 MAX(CASE
   WHEN a.day_date = cc.opt_out_date_pst
   THEN 1
   ELSE 0
   END) AS optedouttoday,
 MAX(CASE
   WHEN a.day_date >= cc.first_seller_consent_dt
   THEN 1
   ELSE 0
   END) AS consented_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 30 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1mo_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 90 DAY)
   THEN 1
   ELSE 0
   END) AS consented_3mo_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 180 DAY)
   THEN 1
   ELSE 0
   END) AS consented_6mo_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 365 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1yr_wseller,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_seller_consent_dt, INTERVAL 730 DAY)
   THEN 1
   ELSE 0
   END) AS consented_2yr_wseller,
 MAX(CASE
   WHEN a.day_date >= cc.first_clienteling_consent_dt
   THEN 1
   ELSE 0
   END) AS consented_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 30 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1mo_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 90 DAY)
   THEN 1
   ELSE 0
   END) AS consented_3mo_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 180 DAY)
   THEN 1
   ELSE 0
   END) AS consented_6mo_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 365 DAY)
   THEN 1
   ELSE 0
   END) AS consented_1yr_wclienteling,
 MAX(CASE
   WHEN a.day_date >= DATE_ADD(cc.first_clienteling_consent_dt, INTERVAL 730 DAY)
   THEN 1
   ELSE 0
   END) AS consented_2yr_wclienteling
FROM cal_dates AS a
 INNER JOIN t2dl_das_clienteling.consented_customer_relationship_hist AS cc ON a.day_date BETWEEN cc.opt_in_date_pst AND
  cc.opt_out_date_pst
 LEFT JOIN hr_range AS hr ON LOWER(LPAD(cc.seller_id, 15, '0')) = LOWER(hr.worker_number) AND a.day_date BETWEEN hr.eff_begin_date
   AND hr.eff_end_date
WHERE LOWER(a.fm_range) NOT LIKE LOWER('%2023%')
 AND LOWER(a.fm_range) NOT LIKE LOWER('%2025%')
 AND cc.acp_id IS NOT NULL
GROUP BY cc.acp_id,
 a.fm_range,
 a.min_day_date,
 a.max_day_date,
 a.fiscal_month,
 a.fiscal_quarter,
 a.fiscal_year,
 cc.first_seller_consent_dt,
 cc.first_clienteling_consent_dt,
 seller_id,
 seller_name,
 worker_status,
 payroll_store,
 payroll_store_description,
 payroll_department_description;

CREATE TEMPORARY TABLE IF NOT EXISTS consented_daily
CLUSTER BY acp_id, seller_id
AS
SELECT *
FROM consented_daily_pt1
UNION DISTINCT
SELECT *
FROM consented_daily_pt2;



CREATE OR REPLACE TEMP TABLE customer_date_list AS
WITH consented_data AS (
    SELECT DISTINCT
        fm_range,
        fiscal_month,
        fiscal_quarter,
        fiscal_year,
        acp_id,
        seller_id,
        seller_name,
        CAST(worker_status AS STRING) AS worker_status,
        payroll_store,
        CAST(payroll_store_description AS STRING) AS payroll_store_description,
        CAST(payroll_department_description AS STRING) AS payroll_department_description
    FROM consented_daily

    UNION ALL

    SELECT DISTINCT
        a.fm_range,
        a.fiscal_month,
        a.fiscal_quarter,
        a.fiscal_year,
        a.acp_id,
        a.seller_id,
        a.seller_name,
        CAST(a.worker_status AS STRING) AS worker_status,
        a.payroll_store,
        CAST(a.payroll_store_description AS STRING) AS payroll_store_description,
        CAST(a.payroll_department_description AS STRING) AS payroll_department_description
    FROM tyly_sales_weekly a
    JOIN (
        SELECT DISTINCT acp_id, first_clienteling_consent_dt
        FROM consented_daily
    ) b ON a.acp_id = b.acp_id
)
SELECT * FROM consented_data;




CREATE TEMPORARY TABLE IF NOT EXISTS consented_trx_acp AS
WITH ccc_data AS (
  SELECT
    fm_range,
    acp_id,
    seller_id,
    worker_status,
    payroll_store,
    payroll_department_description,
    min_day_date,
    max_day_date,
    first_seller_consent_dt,
    first_clienteling_consent_dt,
    seller_history,
    clienteling_history,
    new_to_clienteling,
    optedInToday,
    optedOutToday,
    consented_wSeller,
    consented_1mo_wSeller,
    consented_3mo_wSeller,
    consented_6mo_wSeller,
    consented_1yr_wSeller,
    consented_2yr_wSeller,
    consented_wClienteling,
    consented_1mo_wClienteling,
    consented_3mo_wClienteling,
    consented_6mo_wClienteling,
    consented_1yr_wClienteling,
    consented_2yr_wClienteling
  FROM consented_daily
),
s_data AS (
  SELECT
    fm_range,
    acp_id,
    seller_id,
    worker_status,
    payroll_store,
    payroll_department_description,
    TS_FLS_trips,
    TS_NCOM_trips,
    TS_sb_NCOM_trips,
    TS_gross_FLS_spend,
    TS_gross_NCOM_spend,
    TS_sb_gross_NCOM_spend
  FROM tyly_sales_weekly
),
combined_data AS (
  SELECT
    a.acp_id,
    a.fm_range,
    MAX(ccc.min_day_date) OVER (PARTITION BY a.fm_range) AS min_day_date,
    MAX(ccc.max_day_date) OVER (PARTITION BY a.fm_range) AS max_day_date,
    a.fiscal_month,
    a.fiscal_quarter,
    a.fiscal_year,
    a.seller_id,
    MAX(a.seller_name) OVER (PARTITION BY a.seller_id, a.fm_range) AS seller_name,
    a.worker_status,
    a.payroll_store,
    a.payroll_store_description,
    a.payroll_department_description,
    ccc.first_seller_consent_dt,
    ccc.first_clienteling_consent_dt,
    ccc.seller_history,
    ccc.clienteling_history,
    ccc.new_to_clienteling,
    CASE
      WHEN COALESCE(ccc.worker_status, s.worker_status) <> 'TERMINATED' THEN 1
      ELSE 0
    END AS hasNonTerminatedSeller_ind,
    COALESCE(s.TS_FLS_trips, 0) AS seller_FLS_trips,
    COALESCE(s.TS_NCOM_trips, 0) AS seller_NCOM_trips,
    COALESCE(s.TS_sb_NCOM_trips, 0) AS seller_sb_NCOM_trips,
    COALESCE(s.TS_gross_FLS_spend, 0) AS seller_gross_FLS_spend,
    COALESCE(s.TS_gross_NCOM_spend, 0) AS seller_gross_NCOM_spend,
    COALESCE(s.TS_gross_NCOM_spend, 0) AS seller_sb_gross_NCOM_spend,
    COALESCE(s.TS_FLS_trips, 0) AS TS_FLS_trips,
    COALESCE(s.TS_NCOM_trips, 0) AS TS_NCOM_trips,
    COALESCE(s.TS_sb_NCOM_trips, 0) AS TS_sb_NCOM_trips,
    COALESCE(s.TS_gross_FLS_spend, 0) AS TS_gross_FLS_spend,
    COALESCE(s.TS_gross_NCOM_spend, 0) AS TS_gross_NCOM_spend,
    COALESCE(s.TS_sb_gross_NCOM_spend, 0) AS TS_sb_gross_NCOM_spend,
    SUM(COALESCE(s.TS_FLS_trips, 0)) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_FLS_trips,
    SUM(COALESCE(s.TS_NCOM_trips, 0)) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_NCOM_trips,
    SUM(COALESCE(s.TS_sb_NCOM_trips, 0)) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_sb_NCOM_trips,
    SUM(CASE
      WHEN UPPER(a.seller_id) NOT IN ('NA', 'TERMINATED', 'CLOSED', 'NON-CONSENTED SELLER') AND a.seller_id IS NOT NULL THEN s.TS_FLS_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_FLS_trips_Connection,
    SUM(CASE
      WHEN UPPER(a.seller_id) NOT IN ('NA', 'TERMINATED', 'CLOSED', 'NON-CONSENTED SELLER') AND a.seller_id IS NOT NULL THEN s.TS_NCOM_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_NCOM_trips_Connection,
    SUM(CASE
      WHEN UPPER(a.seller_id) NOT IN ('NA', 'TERMINATED', 'CLOSED', 'NON-CONSENTED SELLER') AND a.seller_id IS NOT NULL THEN s.TS_sb_NCOM_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_sb_NCOM_trips_Connection,
    SUM(CASE
      WHEN UPPER(a.seller_id) = 'NON-CONSENTED SELLER' THEN s.TS_FLS_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_FLS_trips_NonConnection,
    SUM(CASE
      WHEN UPPER(a.seller_id) = 'NON-CONSENTED SELLER' THEN s.TS_NCOM_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_NCOM_trips_NonConnection,
    SUM(CASE
      WHEN UPPER(a.seller_id) = 'NON-CONSENTED SELLER' THEN s.TS_sb_NCOM_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_sb_NCOM_trips_NonConnection,
    SUM(CASE
      WHEN UPPER(a.seller_id) = 'NA' OR a.seller_id IS NULL THEN s.TS_FLS_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_FLS_trips_woSeller,
    SUM(CASE
      WHEN UPPER(a.seller_id) = 'NA' OR a.seller_id IS NULL THEN s.TS_NCOM_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_NCOM_trips_woSeller,
    SUM(CASE
      WHEN UPPER(a.seller_id) = 'NA' OR a.seller_id IS NULL THEN s.TS_sb_NCOM_trips
      END) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_sb_NCOM_trips_woSeller,
    SUM(COALESCE(s.TS_gross_FLS_spend, 0)) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_gross_FLS_spend,
    SUM(COALESCE(s.TS_gross_NCOM_spend, 0)) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_gross_NCOM_spend,
    SUM(COALESCE(s.TS_sb_gross_NCOM_spend, 0)) OVER (PARTITION BY a.fm_range, a.acp_id, COALESCE(ccc.payroll_store, s.payroll_store)) AS store_sb_gross_NCOM_spend,
    SUM(COALESCE(s.TS_FLS_trips, 0)) OVER (PARTITION BY a.fm_range, a.acp_id) AS mo_FLS_trips,
    SUM(COALESCE(s.TS_NCOM_trips, 0)) OVER (PARTITION BY a.fm_range, a.acp_id) AS mo_NCOM_trips,
    SUM(COALESCE(s.TS_sb_NCOM_trips, 0)) OVER (PARTITION BY a.fm_range, a.acp_id) AS mo_sb_NCOM_trips,
    SUM(COALESCE(s.TS_gross_FLS_spend, 0)) OVER (PARTITION BY a.fm_range, a.acp_id) AS mo_gross_FLS_spend,
    SUM(COALESCE(s.TS_gross_NCOM_spend, 0)) OVER (PARTITION BY a.fm_range, a.acp_id) AS mo_gross_NCOM_spend,
    SUM(COALESCE(s.TS_sb_gross_NCOM_spend, 0)) OVER (PARTITION BY a.fm_range, a.acp_id) AS mo_sb_gross_NCOM_spend,
    ccc.consented_1yr_wSeller,
    ccc.consented_1yr_wClienteling,
    ccc.optedInToday ,
    ccc.OptedOutToday,
       ccc.consented_wSeller,
    ccc.consented_1mo_wSeller,
    ccc.consented_3mo_wSeller,
    ccc.consented_6mo_wSeller,
   ccc.consented_1mo_wClienteling,
    ccc.consented_2yr_wSeller,
    ccc.consented_3mo_wClienteling,
    ccc.consented_6mo_wClienteling,
    ccc.consented_2yr_wClienteling
    
  FROM customer_date_list a 
  LEFT JOIN ccc_data ccc ON a.fm_range = ccc.fm_range
    AND a.acp_id = ccc.acp_id
    AND COALESCE(a.seller_id, 'na') = COALESCE(ccc.seller_id, 'na')
    AND COALESCE(a.worker_status, 'na') = COALESCE(ccc.worker_status, 'na')
    AND COALESCE(a.payroll_store, 'na') = COALESCE(ccc.payroll_store, 'na')
    AND COALESCE(a.payroll_department_description, 'na') = COALESCE(ccc.payroll_department_description, 'na')
  LEFT JOIN s_data s ON a.fm_range = s.fm_range
    AND a.acp_id = s.acp_id
    AND COALESCE(a.seller_id, 'na') = COALESCE(s.seller_id, 'na')
    AND COALESCE(a.worker_status, 'na') = COALESCE(s.worker_status, 'na')
    AND COALESCE(a.payroll_store, 'na') = COALESCE(s.payroll_store, 'na')
    AND COALESCE(a.payroll_department_description, 'na') = COALESCE(s.payroll_department_description, 'na')
)
SELECT * FROM combined_data;




CREATE TEMPORARY TABLE IF NOT EXISTS rtdf
CLUSTER BY acp_id, sale_date
AS
SELECT DISTINCT dtl.acp_id,
 COALESCE(dtl.order_date, dtl.tran_date) AS sale_date,
  CASE
  WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  ELSE dtl.intent_store_num
  END AS store_num,
  CASE
  WHEN dtl.commission_slsprsn_num IS NOT NULL AND LOWER(dtl.commission_slsprsn_num) NOT IN (LOWER('2079333'), LOWER('2079334'
      ), LOWER('0000'))
  THEN dtl.commission_slsprsn_num
  ELSE NULL
  END AS commission_slsprsn_num,
 dtl.item_source
FROM prd_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 LEFT JOIN restaurant_dept_lookup AS r ON CAST(COALESCE(dtl.merch_dept_num, FORMAT('%11d', - 1 * dtl.intent_store_num)) AS FLOAT64)
  = r.dept_num
WHERE LOWER(TRIM(COALESCE(dtl.upc_num, '-1'))) <> LOWER('-1')
 AND dtl.line_net_usd_amt > 0
 AND LOWER(COALESCE(dtl.nonmerch_fee_code, 'na')) <> LOWER('6666')
 AND COALESCE(dtl.order_date, dtl.tran_date) BETWEEN (SELECT DATE_SUB(min_date, INTERVAL 365 DAY)
   FROM dates) AND (SELECT DATE_ADD(max_date, INTERVAL 30 DAY)
   FROM dates)
 AND dtl.business_day_date BETWEEN (SELECT DATE_SUB(min_date, INTERVAL 365 DAY)
   FROM dates) AND (SELECT DATE_ADD(max_date, INTERVAL 30 DAY)
   FROM dates)
 AND r.dept_num IS NULL
 AND dtl.acp_id IS NOT NULL;



CREATE TEMPORARY TABLE IF NOT EXISTS tyly_sales_365hist1 AS (
  SELECT
    c.acp_id,
    c.fm_range,
    c.min_day_date,
    c.max_day_date,
    
    MAX(CASE 
        WHEN worker_status = 'ACTIVE' THEN 1
        WHEN worker_status = 'TERMINATED' THEN 0 
        ELSE NULL 
    END) AS hasNonTerminatedSeller_ind,
    
    MAX(CASE 
        WHEN worker_status = 'ACTIVE' AND c.consented_1yr_wSeller = 1 THEN 1 
        ELSE 0 
    END) AS has1yrNonTerminatedSeller_ind,
    
    MAX(c.consented_1yr_wClienteling) AS consented_1yr_wClienteling,
    
    MAX(c.new_to_clienteling) AS new_to_clienteling,
    
    MAX(CASE 
        WHEN dat365.acp_id IS NOT NULL THEN 1 
        ELSE 0 
    END) AS fp_sale,
    
    MAX(CASE 
        WHEN str.business_unit_desc = 'FULL LINE' THEN 1 
        ELSE 0 
    END) AS fls_sale,
    
    MAX(CASE 
        WHEN str.business_unit_desc = 'N.COM' THEN 1 
        ELSE 0 
    END) AS ncom_sale

  FROM consented_trx_acp c
  
  LEFT JOIN rtdf dat365 
    ON dat365.acp_id = c.acp_id
    AND dat365.sale_date BETWEEN DATE_SUB(c.max_day_date, INTERVAL 365 DAY) AND c.max_day_date
  
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str 
    ON dat365.store_num = str.store_num
    AND str.business_unit_desc IN ('FULL LINE', 'N.COM')
  
  GROUP BY c.acp_id, c.fm_range, c.min_day_date, c.max_day_date
);







CREATE TEMPORARY TABLE IF NOT EXISTS tyly_sales_365hist2 AS (
  SELECT
    c.acp_id,
    c.fm_range,
    c.min_day_date,
    c.max_day_date,
    
    MAX(CASE 
        WHEN datSel.commission_slsprsn_num IS NOT NULL THEN 'Y' 
        ELSE 'N' 
    END) AS sale_w_consented_seller

  FROM consented_trx_acp c

  LEFT JOIN rtdf datSel 
    ON datSel.acp_id = c.acp_id
    AND datSel.sale_date BETWEEN DATE_SUB(c.max_day_date, INTERVAL 365 DAY) AND c.max_day_date
    AND COALESCE(c.seller_id, 'na') = datSel.commission_slsprsn_num

  GROUP BY c.acp_id, c.fm_range, c.min_day_date, c.max_day_date
);





CREATE TEMPORARY TABLE IF NOT EXISTS tyly_sales_365hist AS (
  SELECT DISTINCT
    c.acp_id,
    c.fm_range,
    c.min_day_date,
    c.max_day_date,
    
    c.hasNonTerminatedSeller_ind,
    c.has1yrNonTerminatedSeller_ind,
    c.consented_1yr_wClienteling,
    c.new_to_clienteling,
    b.sale_w_consented_seller,
    c.fp_sale,
    c.fls_sale,
    c.ncom_sale

  FROM tyly_sales_365hist1 c
  
  LEFT JOIN tyly_sales_365hist2 b
    ON c.acp_id = b.acp_id
    AND c.fm_range = b.fm_range
);






CREATE TEMPORARY TABLE IF NOT EXISTS pre_attributes AS (
  SELECT
    a.*, 
    MAX(pmt.ph_preference_value) AS phone_marketability,
    MAX(pmt.em_preference_value) AS email_marketability
  FROM tyly_sales_365hist a
  LEFT JOIN (
    SELECT DISTINCT 
      acp_id, 
      email_pref_cur AS em_preference_value, 
      phone_pref_cur AS ph_preference_value
    FROM T2DL_DAS_CLIENTELING.consented_customer_relationship_hist
  ) pmt 
  ON a.acp_id = pmt.acp_id
  GROUP BY
    a.acp_id, a.fm_range, a.min_day_date, a.max_day_date, a.hasNonTerminatedSeller_ind,
    a.has1yrNonTerminatedSeller_ind, a.consented_1yr_wClienteling, a.new_to_clienteling,
   a.sale_w_consented_seller,
     a.fp_sale, a.fls_sale, a.ncom_sale
);




CREATE TEMPORARY TABLE IF NOT EXISTS customer_attrb AS (
  SELECT DISTINCT
    acp_id,
    fm_range,
    min_day_date,
    max_day_date,
    consented_1yr_wClienteling,
    
    CASE 
      WHEN hasNonTerminatedSeller_ind = 0 THEN 1
      WHEN hasNonTerminatedSeller_ind = 1 THEN 0
    END AS hasOnlyTerminatedSeller_ind,

    has1yrNonTerminatedSeller_ind,
    new_to_clienteling,
    
    CASE 
      WHEN b.sale_w_consented_seller = 'N' AND has1yrNonTerminatedSeller_ind = 1 THEN 1
      ELSE 0
    END AS allConnections_churned_ind,

    CASE 
      WHEN fp_sale = 0 AND has1yrNonTerminatedSeller_ind = 1 THEN 1
      ELSE 0
    END AS FP_churned_ind,

    CASE 
      WHEN fls_sale = 0 AND has1yrNonTerminatedSeller_ind = 1 THEN 1
      ELSE 0
    END AS FLS_churned_ind,

    CASE 
      WHEN ncom_sale = 0 AND has1yrNonTerminatedSeller_ind = 1 THEN 1
      ELSE 0
    END AS NCOM_churned_ind,

    phone_marketability,
    email_marketability

  FROM pre_attributes b
);




CREATE TEMPORARY TABLE IF NOT EXISTS store_attrb AS (
  SELECT 
    b.fm_range,
    b.payroll_store,

    -- Store Totals
    b.cnt_OptedInToday AS store_tot_cnt_OptedInToday,
    b.cnt_OptedOutToday AS store_tot_cnt_OptedOutToday,
    b.cnt_Consented_wSeller AS store_tot_cnt_Consented_wSeller,
    b.cnt_Consented_1mo_wSeller AS store_tot_cnt_Consented_1mo_wSeller,
    b.cnt_Consented_3mo_wSeller AS store_tot_cnt_Consented_3mo_wSeller,
    b.cnt_Consented_6mo_wSeller AS store_tot_cnt_Consented_6mo_wSeller,
    b.cnt_Consented_1yr_wSeller AS store_tot_cnt_Consented_1yr_wSeller,
    b.cnt_Consented_2yr_wSeller AS store_tot_cnt_Consented_2yr_wSeller,
    b.cnt_Consented_1mo_wClienteling AS store_tot_cnt_Consented_1mo_wClienteling,
    b.cnt_Consented_3mo_wClienteling AS store_tot_cnt_Consented_3mo_wClienteling,
    b.cnt_Consented_6mo_wClienteling AS store_tot_cnt_Consented_6mo_wClienteling,
    b.cnt_Consented_1yr_wClienteling AS store_tot_cnt_Consented_1yr_wClienteling,
    b.cnt_Consented_2yr_wClienteling AS store_tot_cnt_Consented_2yr_wClienteling,

    -- Summing Attributes
    SUM(a.hasOnlyTerminatedSeller_ind) AS store_tot_hasOnlyTerminatedSeller_ind,
    SUM(a.new_to_clienteling) AS store_tot_new_to_clienteling,
    SUM(a.has1yrNonTerminatedSeller_ind) AS store_tot_has1yrNonTerminatedSeller_ind,
    SUM(a.allConnections_churned_ind) AS store_tot_allConnections_churned_ind,
    SUM(a.FP_churned_ind) AS store_tot_FP_churned_ind,

    -- Marketability
    SUM(CASE WHEN a.phone_marketability = 'Y' THEN 1 ELSE 0 END) AS store_tot_phoneMarketable_Y,
    SUM(CASE WHEN a.phone_marketability = 'N' THEN 1 ELSE 0 END) AS store_tot_phoneMarketable_N,
    SUM(CASE WHEN a.email_marketability = 'Y' THEN 1 ELSE 0 END) AS store_tot_emailMarketable_Y,
    SUM(CASE WHEN a.email_marketability = 'N' THEN 1 ELSE 0 END) AS store_tot_emailMarketable_N,

    -- Store Trips
    SUM(a.store_fls_trips) AS store_tot_fls_trips,
    SUM(a.store_ncom_trips) AS store_tot_ncom_trips,
    SUM(a.store_sb_ncom_trips) AS store_tot_sb_ncom_trips,

    -- Store Trips by Connection
    SUM(a.store_fls_trips_Connection) AS store_tot_fls_trips_Connection,
    SUM(a.store_ncom_trips_Connection) AS store_tot_ncom_trips_Connection,
    SUM(a.store_sb_ncom_trips_Connection) AS store_tot_sb_ncom_trips_Connection,

    -- Store Trips by NonConnection
    SUM(a.store_fls_trips_NonConnection) AS store_tot_fls_trips_NonConnection,
    SUM(a.store_ncom_trips_NonConnection) AS store_tot_ncom_trips_NonConnection,
    SUM(a.store_sb_ncom_trips_NonConnection) AS store_tot_sb_ncom_trips_NonConnection,

    -- Store Trips without Seller
    SUM(a.store_fls_trips_woSeller) AS store_tot_fls_trips_woSeller,
    SUM(a.store_ncom_trips_woSeller) AS store_tot_ncom_trips_woSeller,
    SUM(a.store_sb_ncom_trips_woSeller) AS store_tot_sb_ncom_trips_woSeller,

    -- Store Spend
    SUM(a.store_gross_fls_spend) AS store_tot_gross_fls_spend,
    SUM(a.store_gross_ncom_spend) AS store_tot_gross_ncom_spend,
    SUM(a.store_sb_gross_ncom_spend) AS store_tot_sb_gross_ncom_spend

  FROM (
    SELECT 
      a.payroll_store,
      a.fm_range,
      a.acp_id,
      b.consented_1yr_wClienteling,
      b.has1yrNonTerminatedSeller_ind,
      b.hasOnlyTerminatedSeller_ind,
      b.new_to_clienteling,
      b.allConnections_churned_ind,
      b.FP_churned_ind,
      b.phone_marketability,
      b.email_marketability,

      -- Averaging Store Trips
      AVG(a.store_fls_trips) AS store_fls_trips,
      AVG(a.store_ncom_trips) AS store_ncom_trips,
      AVG(a.store_sb_ncom_trips) AS store_sb_ncom_trips,

      AVG(a.store_fls_trips_Connection) AS store_fls_trips_Connection,
      AVG(a.store_ncom_trips_Connection) AS store_ncom_trips_Connection,
      AVG(a.store_sb_ncom_trips_Connection) AS store_sb_ncom_trips_Connection,

      AVG(a.store_fls_trips_NonConnection) AS store_fls_trips_NonConnection,
      AVG(a.store_ncom_trips_NonConnection) AS store_ncom_trips_NonConnection,
      AVG(a.store_sb_ncom_trips_NonConnection) AS store_sb_ncom_trips_NonConnection,

      AVG(a.store_fls_trips_woSeller) AS store_fls_trips_woSeller,
      AVG(a.store_ncom_trips_woSeller) AS store_ncom_trips_woSeller,
      AVG(a.store_sb_ncom_trips_woSeller) AS store_sb_ncom_trips_woSeller,

      AVG(a.store_gross_fls_spend) AS store_gross_fls_spend,
      AVG(a.store_gross_ncom_spend) AS store_gross_ncom_spend,
      AVG(a.store_sb_gross_ncom_spend) AS store_sb_gross_ncom_spend

    FROM consented_trx_acp a
    LEFT JOIN customer_attrb b ON a.fm_range = b.fm_range AND a.acp_id = b.acp_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
  ) a

  LEFT JOIN (
    SELECT 
      fm_range, 
      payroll_store,
      COUNT(DISTINCT CASE WHEN optedInToday = 1 THEN acp_id END) AS cnt_OptedInToday,
      COUNT(DISTINCT CASE WHEN OptedOutToday = 1 THEN acp_id END) AS cnt_OptedOutToday,
      COUNT(DISTINCT CASE WHEN consented_wSeller = 1 THEN acp_id END) AS cnt_Consented_wSeller,
      COUNT(DISTINCT CASE WHEN consented_1mo_wSeller = 1 THEN acp_id END) AS cnt_Consented_1mo_wSeller,
      COUNT(DISTINCT CASE WHEN consented_3mo_wSeller = 1 THEN acp_id END) AS cnt_Consented_3mo_wSeller,
      COUNT(DISTINCT CASE WHEN consented_6mo_wSeller = 1 THEN acp_id END) AS cnt_Consented_6mo_wSeller,
      COUNT(DISTINCT CASE WHEN consented_1yr_wSeller = 1 THEN acp_id END) AS cnt_Consented_1yr_wSeller,
      COUNT(DISTINCT CASE WHEN consented_2yr_wSeller = 1 THEN acp_id END) AS cnt_Consented_2yr_wSeller,
      COUNT(DISTINCT CASE WHEN consented_1mo_wClienteling = 1 THEN acp_id END) AS cnt_Consented_1mo_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_3mo_wClienteling = 1 THEN acp_id END) AS cnt_Consented_3mo_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_6mo_wClienteling = 1 THEN acp_id END) AS cnt_Consented_6mo_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_1yr_wClienteling = 1 THEN acp_id END) AS cnt_Consented_1yr_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_2yr_wClienteling = 1 THEN acp_id END) AS cnt_Consented_2yr_wClienteling

    FROM consented_trx_acp 
    GROUP BY 1,2
  ) b ON a.fm_range = b.fm_range AND COALESCE(a.payroll_store, 'unknown') = COALESCE(b.payroll_store, 'unknown')

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
);




CREATE TEMPORARY TABLE IF NOT EXISTS month_attrb AS (
  SELECT 
    b.fm_range,

    -- Monthly Totals from Aggregated Counts
    b.cnt_OptedInToday AS month_tot_cnt_OptedInToday,
    b.cnt_OptedOutToday AS month_tot_cnt_OptedOutToday,
    b.cnt_Consented_wSeller AS month_tot_cnt_Consented_wSeller,
    b.cnt_Consented_1mo_wSeller AS month_tot_cnt_Consented_1mo_wSeller,
    b.cnt_Consented_3mo_wSeller AS month_tot_cnt_Consented_3mo_wSeller,
    b.cnt_Consented_6mo_wSeller AS month_tot_cnt_Consented_6mo_wSeller,
    b.cnt_Consented_1yr_wSeller AS month_tot_cnt_Consented_1yr_wSeller,
    b.cnt_Consented_2yr_wSeller AS month_tot_cnt_Consented_2yr_wSeller,
    b.cnt_Consented_1mo_wClienteling AS month_tot_cnt_Consented_1mo_wClienteling,
    b.cnt_Consented_3mo_wClienteling AS month_tot_cnt_Consented_3mo_wClienteling,
    b.cnt_Consented_6mo_wClienteling AS month_tot_cnt_Consented_6mo_wClienteling,
    b.cnt_Consented_1yr_wClienteling AS month_tot_cnt_Consented_1yr_wClienteling,
    b.cnt_Consented_2yr_wClienteling AS month_tot_cnt_Consented_2yr_wClienteling,

    -- Aggregated Sums
    SUM(a.hasOnlyTerminatedSeller_ind) AS month_tot_hasOnlyTerminatedSeller_ind,
    SUM(a.new_to_clienteling) AS month_tot_new_to_clienteling,
    SUM(a.has1yrNonTerminatedSeller_ind) AS month_tot_has1yrNonTerminatedSeller_ind,
    SUM(a.allConnections_churned_ind) AS month_tot_allConnections_churned_ind,
    SUM(a.FP_churned_ind) AS month_tot_FP_churned_ind,

    -- Marketability
    SUM(CASE WHEN a.phone_marketability = 'Y' THEN 1 ELSE 0 END) AS month_tot_phoneMarketable_Y,
    SUM(CASE WHEN a.phone_marketability = 'N' THEN 1 ELSE 0 END) AS month_tot_phoneMarketable_N,
    SUM(CASE WHEN a.email_marketability = 'Y' THEN 1 ELSE 0 END) AS month_tot_emailMarketable_Y,
    SUM(CASE WHEN a.email_marketability = 'N' THEN 1 ELSE 0 END) AS month_tot_emailMarketable_N,

    -- Monthly Trips
    SUM(a.mo_fls_trips) AS month_tot_fls_trips,
    SUM(a.mo_ncom_trips) AS month_tot_ncom_trips,
    SUM(a.mo_sb_ncom_trips) AS month_tot_sb_ncom_trips,

    -- Monthly Spend
    SUM(a.mo_gross_fls_spend) AS month_tot_gross_fls_spend,
    SUM(a.mo_gross_ncom_spend) AS month_tot_gross_ncom_spend,
    SUM(a.mo_sb_gross_ncom_spend) AS month_tot_sb_gross_ncom_spend

  FROM (
    SELECT 
      a.fm_range,
      a.acp_id,
      b.consented_1yr_wClienteling,
      b.has1yrNonTerminatedSeller_ind,
      b.hasOnlyTerminatedSeller_ind,
      b.new_to_clienteling,
      b.allConnections_churned_ind,
      b.FP_churned_ind,
      b.phone_marketability,
      b.email_marketability,

      -- Averaged Monthly Metrics
      AVG(a.mo_fls_trips) AS mo_fls_trips,
      AVG(a.mo_ncom_trips) AS mo_ncom_trips,
      AVG(a.mo_sb_ncom_trips) AS mo_sb_ncom_trips,

      AVG(a.mo_gross_fls_spend) AS mo_gross_fls_spend,
      AVG(a.mo_gross_ncom_spend) AS mo_gross_ncom_spend,
      AVG(a.mo_sb_gross_ncom_spend) AS mo_sb_gross_ncom_spend

    FROM consented_trx_acp a
    LEFT JOIN customer_attrb b 
      ON a.fm_range = b.fm_range
      AND a.acp_id = b.acp_id
    GROUP BY a.fm_range,
    a.optedInToday ,
     a.acp_id, b.consented_1yr_wClienteling, b.has1yrNonTerminatedSeller_ind,
             b.hasOnlyTerminatedSeller_ind, b.new_to_clienteling, b.allConnections_churned_ind, 
             b.FP_churned_ind, b.phone_marketability, b.email_marketability
  ) a

  LEFT JOIN (
    SELECT 
      fm_range,
      COUNT(DISTINCT CASE WHEN OptedInToday = 1 THEN acp_id END) AS cnt_OptedInToday,
      COUNT(DISTINCT CASE WHEN OptedOutToday = 1 THEN acp_id END) AS cnt_OptedOutToday,
      COUNT(DISTINCT CASE WHEN consented_wSeller = 1 THEN acp_id END) AS cnt_Consented_wSeller,
      COUNT(DISTINCT CASE WHEN consented_1mo_wSeller = 1 THEN acp_id END) AS cnt_Consented_1mo_wSeller,
      COUNT(DISTINCT CASE WHEN consented_3mo_wSeller = 1 THEN acp_id END) AS cnt_Consented_3mo_wSeller,
      COUNT(DISTINCT CASE WHEN consented_6mo_wSeller = 1 THEN acp_id END) AS cnt_Consented_6mo_wSeller,
      COUNT(DISTINCT CASE WHEN consented_1yr_wSeller = 1 THEN acp_id END) AS cnt_Consented_1yr_wSeller,
      COUNT(DISTINCT CASE WHEN consented_2yr_wSeller = 1 THEN acp_id END) AS cnt_Consented_2yr_wSeller,
      COUNT(DISTINCT CASE WHEN consented_1mo_wClienteling = 1 THEN acp_id END) AS cnt_Consented_1mo_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_3mo_wClienteling = 1 THEN acp_id END) AS cnt_Consented_3mo_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_6mo_wClienteling = 1 THEN acp_id END) AS cnt_Consented_6mo_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_1yr_wClienteling = 1 THEN acp_id END) AS cnt_Consented_1yr_wClienteling,
      COUNT(DISTINCT CASE WHEN consented_2yr_wClienteling = 1 THEN acp_id END) AS cnt_Consented_2yr_wClienteling

    FROM consented_trx_acp
    GROUP BY fm_range
  ) b ON a.fm_range = b.fm_range

  GROUP BY b.fm_range,  b.cnt_OptedInToday ,
    b.cnt_OptedOutToday ,
    b.cnt_Consented_wSeller ,
    b.cnt_Consented_1mo_wSeller ,
    b.cnt_Consented_3mo_wSeller ,
    b.cnt_Consented_6mo_wSeller ,
    b.cnt_Consented_1yr_wSeller ,
    b.cnt_Consented_2yr_wSeller,
    b.cnt_Consented_1mo_wClienteling ,
    b.cnt_Consented_3mo_wClienteling ,
    b.cnt_Consented_6mo_wClienteling ,
    b.cnt_Consented_1yr_wClienteling ,
    b.cnt_Consented_2yr_wClienteling ,

    -- Aggregated Sums
    a.hasOnlyTerminatedSeller_ind,
    a.new_to_clienteling,
    a.has1yrNonTerminatedSeller_ind,
  a.allConnections_churned_ind,
   a.FP_churned_ind,

    -- Marketability
   a.phone_marketability ,
     a.phone_marketability,
     a.email_marketability,
     a.email_marketability ,

    -- Monthly Trips
  a.mo_fls_trips,
   a.mo_ncom_trips,
    a.mo_sb_ncom_trips,

    -- Monthly Spend
   a.mo_gross_fls_spend,
    a.mo_gross_ncom_spend,
    a.mo_sb_gross_ncom_spend
);




CREATE TEMPORARY TABLE IF NOT EXISTS outp_pt1 AS (
  SELECT 
    a.fm_range,
    a.fiscal_month, 
    a.fiscal_quarter, 
    a.fiscal_year,
  
    COALESCE(a.seller_id, 'NA') AS seller_id,
    COALESCE(a.worker_status, 'NA') AS worker_status,
    COALESCE(a.payroll_store, 'NA') AS payroll_store,
    COALESCE(a.payroll_store_description, 'NA') AS payroll_store_description,
    COALESCE(a.payroll_department_description, 'NA') AS payroll_department_description,

    d.month_tot_cnt_OptedInToday,
    d.month_tot_cnt_OptedOutToday,
    d.month_tot_cnt_Consented_wSeller,
    d.month_tot_hasOnlyTerminatedSeller_ind,
    d.month_tot_new_to_clienteling,
    d.month_tot_has1yrNonTerminatedSeller_ind,
    d.month_tot_allConnections_churned_ind,
    d.month_tot_FP_churned_ind,
    d.month_tot_phoneMarketable_Y,
    d.month_tot_phoneMarketable_N,
    d.month_tot_emailMarketable_Y,
    d.month_tot_emailMarketable_N,
    cast(d.month_tot_fls_trips as int64) as month_tot_fls_trips,
    d.month_tot_ncom_trips,
    d.month_tot_sb_ncom_trips,
    d.month_tot_gross_fls_spend,
    d.month_tot_gross_ncom_spend,
    d.month_tot_sb_gross_ncom_spend,

    c.store_tot_cnt_OptedInToday,
    c.store_tot_cnt_OptedOutToday,
    c.store_tot_cnt_Consented_wSeller,
    c.store_tot_hasOnlyTerminatedSeller_ind,
    c.store_tot_new_to_clienteling,
    c.store_tot_has1yrNonTerminatedSeller_ind,
    c.store_tot_allConnections_churned_ind,
    c.store_tot_FP_churned_ind,
    c.store_tot_phoneMarketable_Y,
    c.store_tot_phoneMarketable_N,
    c.store_tot_emailMarketable_Y,
    c.store_tot_emailMarketable_N,
    c.store_tot_fls_trips,
    c.store_tot_ncom_trips,
    c.store_tot_sb_ncom_trips,
    c.store_tot_fls_trips_Connection,
    c.store_tot_ncom_trips_Connection,
    c.store_tot_sb_ncom_trips_Connection,
    c.store_tot_fls_trips_NonConnection,
    c.store_tot_ncom_trips_NonConnection,
    c.store_tot_sb_ncom_trips_NonConnection,
    c.store_tot_fls_trips_woSeller,
    c.store_tot_ncom_trips_woSeller,
    c.store_tot_sb_ncom_trips_woSeller,
    c.store_tot_gross_fls_spend,
    c.store_tot_gross_ncom_spend,
    c.store_tot_sb_gross_ncom_spend,

    SUM(a.optedInToday) AS cnt_optedInToday,
    SUM(a.optedOutToday) AS cnt_optedOutToday,
    SUM(a.consented_wSeller) AS cnt_Consented_wSeller,
    SUM(b.has1yrNonTerminatedSeller_ind) AS cnt_has1yrNonTerminatedSeller,
    SUM(b.hasOnlyTerminatedSeller_ind) AS cnt_hasOnlyTerminatedSeller,
    SUM(b.allConnections_churned_ind) AS cnt_allConnections_churned,
    SUM(b.fp_churned_ind) AS cnt_fp_churned,
    SUM(b.new_to_clienteling) AS cnt_new_to_clienteling,
    SUM(CASE WHEN b.phone_marketability = 'Y' THEN 1 ELSE 0 END) AS cnt_phoneMarketable_Y,
    SUM(CASE WHEN b.phone_marketability = 'N' THEN 1 ELSE 0 END) AS cnt_phoneMarketable_N,
    SUM(CASE WHEN b.email_marketability = 'Y' THEN 1 ELSE 0 END) AS cnt_emailMarketable_Y,
    SUM(CASE WHEN b.email_marketability = 'N' THEN 1 ELSE 0 END) AS cnt_emailMarketable_N,
  
    SUM(a.consented_1mo_wSeller) AS cnt_1mo_wSeller,
    SUM(a.consented_3mo_wSeller) AS cnt_3mo_wSeller,
    SUM(a.consented_6mo_wSeller) AS cnt_6mo_wSeller,
    SUM(a.consented_1yr_wSeller) AS cnt_1yr_wSeller,
    SUM(a.consented_2yr_wSeller) AS cnt_2yr_wSeller,

    SUM(a.seller_fls_trips) AS toConsented_fls_trips,
    SUM(a.seller_ncom_trips) AS toConsented_ncom_trips,
    SUM(a.seller_sb_ncom_trips) AS toConsented_sb_ncom_trips,

    SUM(a.seller_gross_fls_spend) AS toConsented_gross_fls_spend,
    SUM(a.seller_gross_ncom_spend) AS toConsented_gross_ncom_spend,
    SUM(a.seller_sb_gross_ncom_spend) AS toConsented_sb_gross_ncom_spend

  FROM consented_trx_acp a
  JOIN customer_attrb b 
    ON a.acp_id = b.acp_id
    AND a.fm_range = b.fm_range
  JOIN store_attrb c 
    ON COALESCE(a.payroll_store, 'na') = COALESCE(c.payroll_store, 'na')
    AND a.fm_range = c.fm_range
  JOIN month_attrb d 
    ON a.fm_range = d.fm_range

  WHERE (a.fm_range LIKE '%2023%' OR a.fm_range LIKE '%2025%')

  GROUP BY a.fm_range, a.fiscal_month, a.fiscal_quarter, a.fiscal_year, a.seller_id, a.worker_status, 
           a.payroll_store, a.payroll_store_description, a.payroll_department_description,
           d.month_tot_cnt_OptedInToday, d.month_tot_cnt_OptedOutToday, d.month_tot_cnt_Consented_wSeller,
           d.month_tot_hasOnlyTerminatedSeller_ind, d.month_tot_new_to_clienteling, 
           d.month_tot_has1yrNonTerminatedSeller_ind, d.month_tot_allConnections_churned_ind, 
           d.month_tot_FP_churned_ind, d.month_tot_phoneMarketable_Y, d.month_tot_phoneMarketable_N, 
           d.month_tot_emailMarketable_Y, d.month_tot_emailMarketable_N, d.month_tot_fls_trips,
           d.month_tot_ncom_trips, d.month_tot_sb_ncom_trips, d.month_tot_gross_fls_spend,
           d.month_tot_gross_ncom_spend, d.month_tot_sb_gross_ncom_spend,
           c.store_tot_cnt_OptedInToday, c.store_tot_cnt_OptedOutToday, c.store_tot_cnt_Consented_wSeller,
           c.store_tot_hasOnlyTerminatedSeller_ind, c.store_tot_new_to_clienteling, 
           c.store_tot_has1yrNonTerminatedSeller_ind, c.store_tot_allConnections_churned_ind, 
           c.store_tot_FP_churned_ind, c.store_tot_phoneMarketable_Y, c.store_tot_phoneMarketable_N, 
           c.store_tot_emailMarketable_Y, c.store_tot_emailMarketable_N, c.store_tot_fls_trips, 
           c.store_tot_ncom_trips, c.store_tot_sb_ncom_trips, c.store_tot_fls_trips_Connection,
           c.store_tot_ncom_trips_Connection, c.store_tot_sb_ncom_trips_Connection,
           c.store_tot_fls_trips_NonConnection, c.store_tot_ncom_trips_NonConnection, 
           c.store_tot_sb_ncom_trips_NonConnection, c.store_tot_fls_trips_woSeller,
           c.store_tot_ncom_trips_woSeller, c.store_tot_sb_ncom_trips_woSeller, 
           c.store_tot_gross_fls_spend, c.store_tot_gross_ncom_spend, c.store_tot_sb_gross_ncom_spend
);



CREATE TEMPORARY TABLE IF NOT EXISTS outp_pt2 AS (
  SELECT 
    a.fm_range,
    a.fiscal_month, 
    a.fiscal_quarter, 
    a.fiscal_year,
  
    COALESCE(a.seller_id, 'NA') AS seller_id,
    COALESCE(a.worker_status, 'NA') AS worker_status,
    COALESCE(a.payroll_store, 'NA') AS payroll_store,
    COALESCE(a.payroll_store_description, 'NA') AS payroll_store_description,
    COALESCE(a.payroll_department_description, 'NA') AS payroll_department_description,

    d.month_tot_cnt_OptedInToday,
    d.month_tot_cnt_OptedOutToday,
    d.month_tot_cnt_Consented_wSeller,
    d.month_tot_hasOnlyTerminatedSeller_ind,
    d.month_tot_new_to_clienteling,
    d.month_tot_has1yrNonTerminatedSeller_ind,
    d.month_tot_allConnections_churned_ind,
    d.month_tot_FP_churned_ind,
    d.month_tot_phoneMarketable_Y,
    d.month_tot_phoneMarketable_N,
    d.month_tot_emailMarketable_Y,
    d.month_tot_emailMarketable_N,
    cast(d.month_tot_fls_trips as int64) as  month_tot_fls_trips,
    d.month_tot_ncom_trips,
    d.month_tot_sb_ncom_trips,
    d.month_tot_gross_fls_spend,
    d.month_tot_gross_ncom_spend,
    d.month_tot_sb_gross_ncom_spend,

    c.store_tot_cnt_OptedInToday,
    c.store_tot_cnt_OptedOutToday,
    c.store_tot_cnt_Consented_wSeller,
    c.store_tot_hasOnlyTerminatedSeller_ind,
    c.store_tot_new_to_clienteling,
    c.store_tot_has1yrNonTerminatedSeller_ind,
    c.store_tot_allConnections_churned_ind,
    c.store_tot_FP_churned_ind,
    c.store_tot_phoneMarketable_Y,
    c.store_tot_phoneMarketable_N,
    c.store_tot_emailMarketable_Y,
    c.store_tot_emailMarketable_N,
    c.store_tot_fls_trips,
    c.store_tot_ncom_trips,
    c.store_tot_sb_ncom_trips,
    c.store_tot_fls_trips_Connection,
    c.store_tot_ncom_trips_Connection,
    c.store_tot_sb_ncom_trips_Connection,
    c.store_tot_fls_trips_NonConnection,
    c.store_tot_ncom_trips_NonConnection,
    c.store_tot_sb_ncom_trips_NonConnection,
    c.store_tot_fls_trips_woSeller,
    c.store_tot_ncom_trips_woSeller,
    c.store_tot_sb_ncom_trips_woSeller,
    c.store_tot_gross_fls_spend,
    c.store_tot_gross_ncom_spend,
    c.store_tot_sb_gross_ncom_spend,

    SUM(a.optedInToday) AS cnt_optedInToday,
    SUM(a.optedOutToday) AS cnt_optedOutToday,
    SUM(a.consented_wSeller) AS cnt_Consented_wSeller,
    SUM(b.has1yrNonTerminatedSeller_ind) AS cnt_has1yrNonTerminatedSeller,
    SUM(b.hasOnlyTerminatedSeller_ind) AS cnt_hasOnlyTerminatedSeller,
    SUM(b.allConnections_churned_ind) AS cnt_allConnections_churned,
    SUM(b.fp_churned_ind) AS cnt_fp_churned,
    SUM(b.new_to_clienteling) AS cnt_new_to_clienteling,
    SUM(CASE WHEN b.phone_marketability = 'Y' THEN 1 ELSE 0 END) AS cnt_phoneMarketable_Y,
    SUM(CASE WHEN b.phone_marketability = 'N' THEN 1 ELSE 0 END) AS cnt_phoneMarketable_N,
    SUM(CASE WHEN b.email_marketability = 'Y' THEN 1 ELSE 0 END) AS cnt_emailMarketable_Y,
    SUM(CASE WHEN b.email_marketability = 'N' THEN 1 ELSE 0 END) AS cnt_emailMarketable_N,
  
    SUM(a.consented_1mo_wSeller) AS cnt_1mo_wSeller,
    SUM(a.consented_3mo_wSeller) AS cnt_3mo_wSeller,
    SUM(a.consented_6mo_wSeller) AS cnt_6mo_wSeller,
    SUM(a.consented_1yr_wSeller) AS cnt_1yr_wSeller,
    SUM(a.consented_2yr_wSeller) AS cnt_2yr_wSeller,

    SUM(a.seller_fls_trips) AS toConsented_fls_trips,
    SUM(a.seller_ncom_trips) AS toConsented_ncom_trips,
    SUM(a.seller_sb_ncom_trips) AS toConsented_sb_ncom_trips,

    SUM(a.seller_gross_fls_spend) AS toConsented_gross_fls_spend,
    SUM(a.seller_gross_ncom_spend) AS toConsented_gross_ncom_spend,
    SUM(a.seller_sb_gross_ncom_spend) AS toConsented_sb_gross_ncom_spend

  FROM consented_trx_acp a
  JOIN customer_attrb b 
    ON a.acp_id = b.acp_id
    AND a.fm_range = b.fm_range
  JOIN store_attrb c 
    ON COALESCE(a.payroll_store, 'na') = COALESCE(c.payroll_store, 'na')
    AND a.fm_range = c.fm_range
  JOIN month_attrb d 
    ON a.fm_range = d.fm_range

  WHERE NOT (a.fm_range LIKE '%2023%' OR a.fm_range LIKE '%2025%')

  GROUP BY a.fm_range, a.fiscal_month, a.fiscal_quarter, a.fiscal_year, a.seller_id, a.worker_status, 
           a.payroll_store, a.payroll_store_description, a.payroll_department_description,
           d.month_tot_cnt_OptedInToday, d.month_tot_cnt_OptedOutToday, d.month_tot_cnt_Consented_wSeller,
           d.month_tot_hasOnlyTerminatedSeller_ind, d.month_tot_new_to_clienteling, 
           d.month_tot_has1yrNonTerminatedSeller_ind, d.month_tot_allConnections_churned_ind, 
           d.month_tot_FP_churned_ind, d.month_tot_phoneMarketable_Y, d.month_tot_phoneMarketable_N, 
           d.month_tot_emailMarketable_Y, d.month_tot_emailMarketable_N, d.month_tot_fls_trips,
           d.month_tot_ncom_trips, d.month_tot_sb_ncom_trips, d.month_tot_gross_fls_spend,
           d.month_tot_gross_ncom_spend, d.month_tot_sb_gross_ncom_spend,
           c.store_tot_cnt_OptedInToday, c.store_tot_cnt_OptedOutToday, c.store_tot_cnt_Consented_wSeller,
           c.store_tot_hasOnlyTerminatedSeller_ind, c.store_tot_new_to_clienteling, 
           c.store_tot_has1yrNonTerminatedSeller_ind, c.store_tot_allConnections_churned_ind, 
           c.store_tot_FP_churned_ind, c.store_tot_phoneMarketable_Y, c.store_tot_phoneMarketable_N, 
           c.store_tot_emailMarketable_Y, c.store_tot_emailMarketable_N, c.store_tot_fls_trips, 
           c.store_tot_ncom_trips, c.store_tot_sb_ncom_trips, c.store_tot_fls_trips_Connection,
           c.store_tot_ncom_trips_Connection, c.store_tot_sb_ncom_trips_Connection,
           c.store_tot_fls_trips_NonConnection, c.store_tot_ncom_trips_NonConnection, 
           c.store_tot_sb_ncom_trips_NonConnection, c.store_tot_fls_trips_woSeller,
           c.store_tot_ncom_trips_woSeller, c.store_tot_sb_ncom_trips_woSeller, 
           c.store_tot_gross_fls_spend, c.store_tot_gross_ncom_spend, c.store_tot_sb_gross_ncom_spend
);





DELETE FROM `{{params.gcp_project_id}}`.{{params.cli_t2_schema}}.consented_customer_seller_monthly
WHERE fm_range IN (
  SELECT DISTINCT fm_range FROM outp_pt1
  UNION DISTINCT
  SELECT DISTINCT fm_range FROM outp_pt2
);




INSERT INTO `{{params.gcp_project_id}}`.{{params.cli_t2_schema}}.consented_customer_seller_monthly
SELECT DISTINCT
  fm_range			
,fiscal_month			
,fiscal_quarter			
,fiscal_year			
,seller_id			
,worker_status			
,payroll_store			
,payroll_store_description			
,payroll_department_description			
,month_tot_cnt_optedintoday			
,month_tot_cnt_optedouttoday			
,month_tot_cnt_consented_wseller			
,month_tot_hasonlyterminatedseller_ind			
,month_tot_new_to_clienteling			
,month_tot_has1yrnonterminatedseller_ind			
,month_tot_allconnections_churned_ind			
,month_tot_fp_churned_ind			
,month_tot_phonemarketable_y			
,month_tot_phonemarketable_n			
,month_tot_emailmarketable_y			
,month_tot_emailmarketable_n			
,cast(month_tot_fls_trips	 as int64)		
,cast(month_tot_ncom_trips	 as int64)		
,cast(month_tot_sb_ncom_trips		as int64)	
,month_tot_gross_fls_spend			
,month_tot_gross_ncom_spend			
,month_tot_sb_gross_ncom_spend			
,store_tot_cnt_optedintoday			
,store_tot_cnt_optedouttoday			
,store_tot_cnt_consented_wseller			
,store_tot_hasonlyterminatedseller_ind			
,store_tot_new_to_clienteling			
,store_tot_has1yrnonterminatedseller_ind			
,store_tot_allconnections_churned_ind			
,store_tot_fp_churned_ind			
,store_tot_phonemarketable_y			
,store_tot_phonemarketable_n			
,store_tot_emailmarketable_y			
,store_tot_emailmarketable_n			
,cast(store_tot_fls_trips	as int64)		
,cast(store_tot_ncom_trips	as int64)		
,cast(store_tot_sb_ncom_trips		as int64)	
,cast(store_tot_fls_trips_connection	as int64)		
,cast(store_tot_ncom_trips_connection	as int64)		
,cast(store_tot_sb_ncom_trips_connection	as int64)		
,cast(store_tot_fls_trips_nonconnection		as int64)	
,cast(store_tot_ncom_trips_nonconnection	as int64)		
,cast(store_tot_sb_ncom_trips_nonconnection	as int64)		
,cast(store_tot_fls_trips_woseller	as int64)		
,cast(store_tot_ncom_trips_woseller	as int64)		
,cast(store_tot_sb_ncom_trips_woseller		as int64)	
,cast(store_tot_gross_fls_spend		as int64)	
,cast(store_tot_gross_ncom_spend		as int64)	
,cast(store_tot_sb_gross_ncom_spend		as int64)	
,cnt_optedintoday			
,cnt_optedouttoday			
,cnt_consented_wseller			
,cnt_has1yrnonterminatedseller			
,cnt_hasonlyterminatedseller			
,cnt_allconnections_churned			
,cnt_fp_churned			
,cnt_new_to_clienteling			
,cnt_phonemarketable_y			
,cnt_phonemarketable_n			
,cnt_emailmarketable_y			
,cnt_emailmarketable_n			
,cnt_1mo_wseller			
,cnt_3mo_wseller			
,cnt_6mo_wseller			
,cnt_1yr_wseller			
,cnt_2yr_wseller			
,toconsented_fls_trips			
,toconsented_ncom_trips			
,toconsented_sb_ncom_trips			
,toconsented_gross_fls_spend			
,toconsented_gross_ncom_spend			
,toconsented_sb_gross_ncom_spend		,	

  CURRENT_datetime('PST8PDT') AS dw_sys_load_tmstp
FROM (
  SELECT * FROM outp_pt1
  UNION ALL
  SELECT * FROM outp_pt2
) a;



