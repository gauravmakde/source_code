BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08240;
DAG_ID=cco_tables_11521_ACE_ENG;
---     Task_Name=run_cco_job_4_cco_customer_level_attributes;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS curr_mo_lkp
AS
SELECT DISTINCT month_num AS curr_mo,
  CASE
  WHEN MOD(month_num, 100) = 1
  THEN month_num - 89
  ELSE month_num - 1
  END AS prior_mo,
 year_num AS curr_year
FROM t2dl_das_usl.usl_rolling_52wk_calendar
WHERE day_date = CURRENT_DATE('PST8PDT');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_parameter_lookup
AS
SELECT MIN(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS fy18_start_dt,
 MAX(CASE
   WHEN year_num = 2018
   THEN day_date
   ELSE NULL
   END) AS fy18_end_dt,
 MIN(CASE
   WHEN year_num = 2019
   THEN day_date
   ELSE NULL
   END) AS fy19_start_dt,
 MAX(CASE
   WHEN month_num < (SELECT curr_mo
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS latest_mo_dt,
 MIN(CASE
   WHEN month_num = (SELECT curr_mo - 400
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr1_start_dt,
 MIN(CASE
   WHEN month_num = (SELECT curr_mo - 500
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr0_start_dt,
 MAX(CASE
   WHEN month_num = (SELECT prior_mo - 400
     FROM curr_mo_lkp)
   THEN day_date
   ELSE NULL
   END) AS r4yr0_end_dt
FROM t2dl_das_usl.usl_rolling_52wk_calendar;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yrs
AS
SELECT year_num,
 RANK() OVER (ORDER BY year_num DESC) AS recency_rank
FROM (SELECT DISTINCT year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
  WHERE week_of_fyr = 53
   AND day_date BETWEEN '2009-01-01' AND (CURRENT_DATE('PST8PDT'))) AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS week_53_yr_count
AS
SELECT COUNT(DISTINCT year_num) AS year_count
FROM week_53_yrs AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS realigned_calendar (
day_date DATE,
day_num INTEGER,
day_desc STRING,
week_num INTEGER,
week_desc STRING,
month_num INTEGER,
month_short_desc STRING,
quarter_num INTEGER,
halfyear_num INTEGER,
year_num INTEGER,
month_454_num INTEGER,
year_454_num INTEGER
);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO realigned_calendar
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
 WHERE day_date BETWEEN '2009-01-01' AND (CURRENT_DATE('PST8PDT'))
  AND week_of_fyr <> 53);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS month_lookup
AS
SELECT DISTINCT month_num AS current_mo,
  CASE
  WHEN MOD(month_num, 100) = 1
  THEN month_num - 89
  ELSE month_num - 1
  END AS yr4_end_mo,
  month_num - 100 AS yr4_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 100 AS yr3_end_mo,
  month_num - 200 AS yr3_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 200 AS yr2_end_mo,
  month_num - 300 AS yr2_start_mo,
   CASE
   WHEN MOD(month_num, 100) = 1
   THEN month_num - 89
   ELSE month_num - 1
   END - 300 AS yr1_end_mo,
  month_num - 400 AS yr1_start_mo
FROM realigned_calendar
WHERE day_date = DATE_ADD((SELECT latest_mo_dt
    FROM date_parameter_lookup), INTERVAL 1 DAY);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS month_names
AS
SELECT MAX(CASE
   WHEN month_num = (SELECT yr1_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr1_name,
 MAX(CASE
   WHEN month_num = (SELECT yr2_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr2_name,
 MAX(CASE
   WHEN month_num = (SELECT yr3_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr3_name,
 MAX(CASE
   WHEN month_num = (SELECT yr4_end_mo
     FROM month_lookup)
   THEN SUBSTR(CASE
     WHEN LOWER(month_short_desc) = LOWER('JAN')
     THEN 'FY-' || SUBSTR(CAST(MOD(year_num, 2000) AS STRING), 1, 2)
     ELSE 'YE-' || SUBSTR(month_short_desc, 1, 1) || LOWER(SUBSTR(month_short_desc, 2, 2)) || SUBSTR(CAST(MOD(year_num, 2000) AS STRING)
       , 1, 2)
     END, 1, 8)
   ELSE NULL
   END) AS yr4_name
FROM realigned_calendar
WHERE month_num IN ((SELECT yr1_end_mo
    FROM month_lookup), (SELECT yr2_end_mo
    FROM month_lookup), (SELECT yr3_end_mo
    FROM month_lookup), (SELECT yr4_end_mo
    FROM month_lookup));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT CASE
  WHEN month_num BETWEEN (SELECT yr1_start_mo
    FROM month_lookup) AND (SELECT yr1_end_mo
    FROM month_lookup)
  THEN (SELECT yr1_name
   FROM month_names)
  WHEN month_num BETWEEN (SELECT yr2_start_mo
    FROM month_lookup) AND (SELECT yr2_end_mo
    FROM month_lookup)
  THEN (SELECT yr2_name
   FROM month_names)
  WHEN month_num BETWEEN (SELECT yr3_start_mo
    FROM month_lookup) AND (SELECT yr3_end_mo
    FROM month_lookup)
  THEN (SELECT yr3_name
   FROM month_names)
  WHEN month_num BETWEEN (SELECT yr4_start_mo
    FROM month_lookup) AND (SELECT yr4_end_mo
    FROM month_lookup)
  THEN (SELECT yr4_name
   FROM month_names)
  ELSE NULL
  END AS reporting_year,
 MIN(day_date) AS start_dt,
 MAX(day_date) AS end_dt,
 MIN(month_num) AS start_mo,
 MAX(month_num) AS end_mo
FROM realigned_calendar AS a
WHERE month_num BETWEEN (SELECT yr1_start_mo
   FROM month_lookup) AND (SELECT yr4_end_mo
   FROM month_lookup)
GROUP BY reporting_year;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_year_driver
AS
SELECT a.acp_id,
 a.reporting_year_shopped,
 b.start_dt,
 b.end_dt,
 b.start_mo,
 b.end_mo,
 MAX(a.employee_flag) AS employee_flag,
 SUM(a.net_sales) AS net_sales
FROM `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items AS a
 LEFT JOIN date_lookup AS b ON LOWER(a.reporting_year_shopped) = LOWER(b.reporting_year)
WHERE a.reporting_year_shopped IS NOT NULL
GROUP BY a.acp_id,
 a.reporting_year_shopped,
 b.start_dt,
 b.end_dt,
 b.start_mo,
 b.end_mo;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CREATE TEMPORARY TABLE IF NOT EXISTS consolidated_nms_nodes AS(
select distinct market
  ,cast(case when LOWER(upper(trim(market)))=LOWER('LOS_ANGELES') then date'2019-06-20'
    when LOWER(upper(trim(market)))=LOWER('NEW_YORK') then '2019-10-23'
    when LOWER(upper(trim(market)))=LOWER('SAN_FRANCISCO') then '2019-10-29'
    when LOWER(upper(trim(market)))=LOWER('CHICAGO') then '2019-11-04'
    when LOWER(upper(trim(market)))=LOWER('DALLAS') then '2019-11-04'
    when LOWER(upper(trim(market)))=LOWER('SEATTLE') then '2020-09-28'
    when LOWER(upper(trim(market)))=LOWER('BOSTON') then '2020-10-05'
    when LOWER(upper(trim(market)))=LOWER('PHILADELPHIA') then '2020-10-05'
    when LOWER(upper(trim(market)))=LOWER('WASHINGTON') then '2020-10-05'
    when LOWER(upper(trim(market)))=LOWER('TORONTO') then '2020-10-28'
    when LOWER(upper(trim(market)))=LOWER('DENVER') then '2021-02-15'
    when LOWER(upper(trim(market)))=LOWER('SAN_DIEGO') then '2021-02-15'
    when LOWER(upper(trim(market)))=LOWER('PORTLAND') then '2021-02-17'
    when LOWER(upper(trim(market)))=LOWER('AUSTIN') then '2021-02-22'
    when LOWER(upper(trim(market)))=LOWER('HOUSTON') then '2021-02-22'
    when LOWER(upper(trim(market)))=LOWER('ATLANTA') then '2021-03-01'
    when LOWER(upper(trim(market)))=LOWER('DETROIT') then '2021-03-01'
    when LOWER(upper(trim(market)))=LOWER('MIAMI') then '2021-03-01'
    when LOWER(upper(trim(market)))=LOWER('MINNEAPOLIS') then '2021-03-01'
    else null end as date) nms_launch_date
  ,node_num
from
  (
  select local_market as market, cast(trunc(cast(node as float64)) as integer) as node_num
  from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.local_market_node_dim
  where CURRENT_DATETIME('PST8PDT') between eff_begin_date and eff_end_date
  union DISTINCT
  select market,node_num from t2dl_das_strategy.cco_nms_nodes
  ) x
);

CREATE TEMPORARY TABLE IF NOT EXISTS customer_market AS(
select distinct a.acp_id
  ,a.market
  ,case when b.market is not null then 1 else 0 end NMS_Market
  ,b.nms_launch_date
from
  (
  select distinct acp_id
    ,COALESCE(bill_zip_market,fls_market,rack_market,ca_dma_desc,us_dma_desc) market
  from
    (
      select distinct acp_id,
        case when b.market is not null then b.market end as bill_zip_market,
        case when c.market is not null then c.market end  as fls_market,
        case when d.market is not null then d.market end as rack_market,
        ca_dma_desc,
        us_dma_desc
      from
        (
         select x.acp_id, billing_postal_code, ca_dma_desc, us_dma_desc, fls_loyalty_store_num, rack_loyalty_store_num,
            case when ca_dma_desc is not null then left(billing_postal_code,3) else billing_postal_code end as join_zip
         from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer x
            join customer_year_driver y on LOWER(x.acp_id)=LOWER(y.acp_id)
         where x.acp_id is not null
        ) a
        left join ( select distinct local_market as market
                    ,case when LOWER(local_market) = LOWER('TORONTO') then left(coarse_postal_code,3) else coarse_postal_code end as join_zip
                  from  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.local_market_postal_dim
                  where CURRENT_DATETIME('PST8PDT') between eff_begin_date and eff_end_date
                  ) b  
                  on LOWER(a.join_zip)= LOWER(b.join_zip)
        left join consolidated_nms_nodes c on a.fls_loyalty_store_num= c.node_num
        left join consolidated_nms_nodes d on a.rack_loyalty_store_num= d.node_num
      where a.acp_id IS NOT NULL)z
  where COALESCE(bill_zip_market,fls_market,rack_market,ca_dma_desc,us_dma_desc) is not null or LOWER(COALESCE(bill_zip_market,fls_market,rack_market,ca_dma_desc,us_dma_desc)) not in (LOWER('Not Defined'), LOWER('Other'))
   ) a
left join
    (select distinct market, nms_launch_date
    from consolidated_nms_nodes) b
  on LOWER(a.market)=LOWER(b.market)
);


CREATE TEMPORARY TABLE IF NOT EXISTS customer_nms_market AS(
select distinct acp_id
,market
from customer_market
where NMS_Market=1
);


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_dma_prep
AS
SELECT a.acp_id,
  CASE
  WHEN b.ca_dma_desc IS NOT NULL
  THEN 'CA'
  WHEN b.us_dma_desc IS NOT NULL
  THEN 'US'
  ELSE NULL
  END AS cust_country,
 COALESCE(b.ca_dma_desc, b.us_dma_desc) AS cust_dma,
 a.reporting_year_shopped,
 a.net_sales
FROM customer_year_driver AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS b ON LOWER(a.acp_id) = LOWER(b.acp_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS dma_ranking
AS
SELECT cust_dma,
 reporting_year_shopped,
 SUM(net_sales) AS dma_net_sales,
 RANK() OVER (PARTITION BY reporting_year_shopped ORDER BY SUM(net_sales) DESC) AS dma_rank
FROM customer_dma_prep
WHERE LOWER(COALESCE(cust_dma, 'Other')) <> LOWER('Other')
GROUP BY cust_dma,
 reporting_year_shopped,
 net_sales;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_dma
AS
SELECT DISTINCT a.acp_id,
 a.cust_dma,
 a.cust_country,
 a.reporting_year_shopped,
  CASE
  WHEN b.dma_rank IS NULL
  THEN 'DMA missing'
  WHEN b.dma_rank BETWEEN 1 AND 5
  THEN '1) Top 5'
  WHEN b.dma_rank BETWEEN 6 AND 10
  THEN '2) 6-10'
  WHEN b.dma_rank BETWEEN 11 AND 20
  THEN '3) 11-20'
  WHEN b.dma_rank BETWEEN 21 AND 30
  THEN '4) 21-30'
  WHEN b.dma_rank BETWEEN 31 AND 50
  THEN '5) 31-50'
  WHEN b.dma_rank BETWEEN 51 AND 100
  THEN '6) 51-100'
  ELSE '7) > 100'
  END AS dma_rank
FROM customer_dma_prep AS a
 LEFT JOIN dma_ranking AS b ON LOWER(a.cust_dma) = LOWER(b.cust_dma) AND LOWER(a.reporting_year_shopped) = LOWER(b.reporting_year_shopped
    );
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS anchor_brands
AS
SELECT dtl.global_tran_id,
 dtl.line_item_seq_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS ps ON LOWER(dtl.sku_num) = LOWER(ps.rms_sku_num) AND LOWER(ps.channel_country
    ) = LOWER('US')
 INNER JOIN (SELECT DISTINCT supplier_idnt
  FROM t2dl_das_in_season_management_reporting.anchor_brands
  WHERE LOWER(anchor_brand_ind) = LOWER('Y')) AS s ON CAST(ps.prmy_supp_num AS FLOAT64) = s.supplier_idnt
WHERE dtl.business_day_date >= (SELECT fy19_start_dt
   FROM date_parameter_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS strategic_brands
AS
SELECT dtl.global_tran_id,
 dtl.line_item_seq_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS ps ON LOWER(dtl.sku_num) = LOWER(ps.rms_sku_num) AND LOWER(ps.channel_country
    ) = LOWER('US')
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS sup ON LOWER(ps.prmy_supp_num) = LOWER(sup.vendor_num)
 INNER JOIN (SELECT DISTINCT supplier_name
  FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
  WHERE LOWER(rack_strategic_brand_ind) = LOWER('Y')) AS rsb ON LOWER(rsb.supplier_name) = LOWER(sup.vendor_name)
WHERE dtl.business_day_date >= (SELECT fy19_start_dt
   FROM date_parameter_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (global_tran_id, line_item_seq_num) ON strategic_brands;
--COLLECT STATISTICS COLUMN (global_tran_id, line_item_seq_num) ON anchor_brands;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS derived_cust_attributes
AS
SELECT a.acp_id,
 a.reporting_year_shopped,
 MAX(a.ntn_tran) AS ntn_this_year,
 COUNT(DISTINCT a.channel) AS channels_shopped,
  CASE
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 0
  THEN '01) NordStore-only'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 0
  THEN '02) N.com-only'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 0
  THEN '03) RackStore-only'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '04) Rack.com-only'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 0
  THEN '05) NordStore+N.com'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 0
  THEN '06) NordStore+RackStore'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '07) NordStore+Rack.com'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 0
  THEN '08) N.com+RackStore'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '09) N.com+Rack.com'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '10) RackStore+Rack.com'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 0
  THEN '11) NordStore+N.com+RackStore'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '12) NordStore+N.com+Rack.com'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '13) NordStore+RackStore+Rack.com'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 0 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '14) N.com+RackStore+Rack.com'
  WHEN MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('1')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
        WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('2')
        THEN 1
        ELSE 0
        END) = 1 AND MAX(CASE
       WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('3')
       THEN 1
       ELSE 0
       END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) = LOWER('4')
      THEN 1
      ELSE 0
      END) = 1
  THEN '15) 4-Box'
  ELSE '99) Error'
  END AS chan_combo,
 COUNT(DISTINCT a.banner) AS banners_shopped,
  CASE
  WHEN MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
      THEN 1
      ELSE 0
      END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
      THEN 1
      ELSE 0
      END) = 0
  THEN '1) Nordstrom-only'
  WHEN MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
      THEN 1
      ELSE 0
      END) = 0 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
      THEN 1
      ELSE 0
      END) = 1
  THEN '2) Rack-only'
  WHEN MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
      THEN 1
      ELSE 0
      END) = 1 AND MAX(CASE
      WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
      THEN 1
      ELSE 0
      END) = 1
  THEN '3) Dual-Banner'
  ELSE '99) Error'
  END AS banner_combo,
 MAX(a.employee_flag) AS employee_flag,
  CASE
  WHEN COUNT(DISTINCT CASE
     WHEN a.gross_incl_gc > 0
     THEN a.acp_id || FORMAT('%11d', a.store_num) || CAST(a.date_shopped AS STRING)
     ELSE NULL
     END) < 10
  THEN '0' || SUBSTR(CAST(COUNT(DISTINCT CASE
        WHEN a.gross_incl_gc > 0
        THEN a.acp_id || FORMAT('%11d', a.store_num) || CAST(a.date_shopped AS STRING)
        ELSE NULL
        END) AS STRING), 1, 1) || ' trips'
  ELSE '10+ trips'
  END AS jwn_trip_bucket,
  CASE
  WHEN SUM(a.net_sales) = 0
  THEN '0) $0'
  WHEN SUM(a.net_sales) > 0 AND SUM(a.net_sales) <= 50
  THEN '1) $0-50'
  WHEN SUM(a.net_sales) > 50 AND SUM(a.net_sales) <= 100
  THEN '2) $50-100'
  WHEN SUM(a.net_sales) > 100 AND SUM(a.net_sales) <= 250
  THEN '3) $100-250'
  WHEN SUM(a.net_sales) > 250 AND SUM(a.net_sales) <= 500
  THEN '4) $250-500'
  WHEN SUM(a.net_sales) > 500 AND SUM(a.net_sales) <= 1000
  THEN '5) $500-1K'
  WHEN SUM(a.net_sales) > 1000 AND SUM(a.net_sales) <= 2000
  THEN '6) 1-2K'
  WHEN SUM(a.net_sales) > 2000 AND SUM(a.net_sales) <= 5000
  THEN '7) 2-5K'
  WHEN SUM(a.net_sales) > 5000 AND SUM(a.net_sales) <= 10000
  THEN '8) 5-10K'
  WHEN SUM(a.net_sales) > 10000
  THEN '9) 10K+'
  ELSE NULL
  END AS jwn_net_spend_bucket,
  CASE
  WHEN MAX(a.ntn_tran) = 1 AND COALESCE(COUNT(DISTINCT CASE
      WHEN a.gross_incl_gc > 0 AND a.div_num <> 70
      THEN a.acp_id || FORMAT('%11d', a.store_num) || CAST(a.date_shopped AS STRING)
      ELSE NULL
      END),0) <= 5
  THEN 'Acquire & Activate'
  WHEN COALESCE(COUNT(DISTINCT CASE
     WHEN a.gross_incl_gc > 0 AND a.div_num <> 70
     THEN a.acp_id || FORMAT('%11d', a.store_num) || CAST(a.date_shopped AS STRING)
     ELSE NULL
     END),0) <= 5
  THEN 'Lightly-Engaged'
  WHEN COALESCE(COUNT(DISTINCT CASE
     WHEN a.gross_incl_gc > 0 AND a.div_num <> 70
     THEN a.acp_id || FORMAT('%11d', a.store_num) || CAST(a.date_shopped AS STRING)
     ELSE NULL
     END),0) <= 13
  THEN 'Moderately-Engaged'
  WHEN COALESCE(COUNT(DISTINCT CASE
     WHEN a.gross_incl_gc > 0 AND a.div_num <> 70
     THEN a.acp_id || FORMAT('%11d', a.store_num) || CAST(a.date_shopped AS STRING)
     ELSE NULL
     END),0) >= 14
  THEN 'Highly-Engaged'
  ELSE NULL
  END AS engagement_cohort,
 SUM(a.gross_sales) AS gross_sales,
 SUM(a.return_amt) AS return_amt,
 SUM(a.net_sales) AS net_sales,
 SUM(CASE
   WHEN a.div_num = 351
   THEN a.net_sales
   ELSE 0
   END) AS net_sales_apparel,
 COUNT(DISTINCT CASE
   WHEN a.gross_incl_gc > 0
   THEN a.acp_id || FORMAT('%11d', a.store_num) || CAST(a.date_shopped AS STRING)
   ELSE NULL
   END) AS trips,
 SUM(a.gross_items) AS gross_items,
 SUM(a.return_items) AS return_items,
 SUM(a.gross_items - a.return_items) AS net_items,
 MAX(a.tender_nordstrom) AS cust_tender_nordstrom,
 MAX(a.tender_nordstrom_note) AS cust_tender_nordstrom_note,
 MAX(a.tender_3rd_party_credit) AS cust_tender_3rd_party_credit,
 MAX(a.tender_debit_card) AS cust_tender_debit_card,
 MAX(a.tender_gift_card) AS cust_tender_gift_card,
 MAX(a.tender_cash) AS cust_tender_cash,
 MAX(a.tender_paypal) AS cust_tender_paypal,
 MAX(a.tender_check) AS cust_tender_check,
 MAX(a.event_holiday) AS cust_event_holiday,
 MAX(a.event_anniversary) AS cust_event_anniversary,
 MAX(a.svc_group_exp_delivery) AS cust_svc_group_exp_delivery,
 MAX(a.svc_group_order_pickup) AS cust_svc_group_order_pickup,
 MAX(a.svc_group_selling_relation) AS cust_svc_group_selling_relation,
 MAX(a.svc_group_remote_selling) AS cust_svc_group_remote_selling,
 MAX(a.svc_group_alterations) AS cust_svc_group_alterations,
 MAX(a.svc_group_in_store) AS cust_svc_group_in_store,
 MAX(a.svc_group_restaurant) AS cust_svc_group_restaurant,
 MAX(a.service_free_exp_delivery) AS cust_service_free_exp_delivery,
 MAX(a.service_next_day_pickup) AS cust_service_next_day_pickup,
 MAX(a.service_same_day_bopus) AS cust_service_same_day_bopus,
 MAX(a.service_curbside_pickup) AS cust_service_curbside_pickup,
 MAX(a.service_style_boards) AS cust_service_style_boards,
 MAX(a.service_gift_wrapping) AS cust_service_gift_wrapping,
 MAX(a.marketplace_flag) AS cust_marketplace_flag,
 MAX(a.service_pop_in) AS cust_service_pop_in,
 MAX(CASE
   WHEN LOWER(TRIM(a.platform)) = LOWER('WEB')
   THEN 1
   ELSE 0
   END) AS cust_platform_desktop,
 MAX(CASE
   WHEN LOWER(TRIM(a.platform)) = LOWER('MOW')
   THEN 1
   ELSE 0
   END) AS cust_platform_mow,
 MAX(CASE
   WHEN LOWER(TRIM(a.platform)) = LOWER('IOS')
   THEN 1
   ELSE 0
   END) AS cust_platform_ios,
 MAX(CASE
   WHEN LOWER(TRIM(a.platform)) = LOWER('ANDROID')
   THEN 1
   ELSE 0
   END) AS cust_platform_android,
 MAX(CASE
   WHEN LOWER(TRIM(a.platform)) = LOWER('POS')
   THEN 1
   ELSE 0
   END) AS cust_platform_pos,
 MAX(CASE
   WHEN ab.global_tran_id IS NOT NULL AND LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('2'))
   THEN 1
   ELSE 0
   END) AS cust_anchor_brand,
 MAX(CASE
   WHEN sb.global_tran_id IS NOT NULL AND LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('3'), LOWER('4'))
   THEN 1
   ELSE 0
   END) AS cust_strategic_brand,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('1'), LOWER('3'))
   THEN 1
   ELSE 0
   END) AS cust_store_customer,
 MAX(CASE
   WHEN LOWER(SUBSTR(a.channel, 1, 1)) IN (LOWER('2'), LOWER('4'))
   THEN 1
   ELSE 0
   END) AS cust_digital_customer
FROM `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_line_items AS a
 LEFT JOIN anchor_brands AS ab ON a.global_tran_id = ab.global_tran_id AND a.line_item_seq_num = ab.line_item_seq_num
 LEFT JOIN strategic_brands AS sb ON a.global_tran_id = sb.global_tran_id AND a.line_item_seq_num = sb.line_item_seq_num
   
GROUP BY a.acp_id,
 a.reporting_year_shopped;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS modeled_ages
AS
SELECT t1.acp_id,
 t1.reporting_year_shopped,
 t1.model_age_adjusted
FROM (SELECT DISTINCT dr.acp_id,
   dr.reporting_year_shopped,
   CAST(TRUNC(CAST(ROUND(CAST(a.model_age AS NUMERIC), 2) - DATE_DIFF(CAST(a.update_timestamp AS DATE), dr.end_dt, DAY) / 365.25 AS FLOAT64)) AS INTEGER)
   AS model_age_adjusted,
   a.update_timestamp,
   a.acp_id AS acp_id0
  FROM t2dl_das_age_model.new_age_model_scoring_all AS a
   INNER JOIN customer_year_driver AS dr ON LOWER(a.acp_id) = LOWER(dr.acp_id)
  QUALIFY (RANK() OVER (PARTITION BY acp_id0 ORDER BY a.update_timestamp DESC)) = 1) AS t1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS experian_demos
AS
SELECT DISTINCT dr.acp_id,
 dr.reporting_year_shopped,
 a.gender,
 CAST(TRUNC(CAST(CASE
   WHEN LOWER(a.age_type) = LOWER('Exact Age') AND a.age_value IS NOT NULL
   THEN CASE
    WHEN LENGTH(TRIM(a.birth_year_and_month)) = 6
    THEN DATE_DIFF(dr.end_dt, CAST(PARSE_DATE('%Y/%m/%d', SUBSTR(a.birth_year_and_month, 1, 4) || '/' || SUBSTR(a.birth_year_and_month, 5, 2) || '/' || '15') AS DATE), DAY) / 365.25
    ELSE CAST(a.age_value AS NUMERIC) - DATE_DIFF(CAST(a.object_system_time AS DATE), dr.end_dt, DAY) / 365.25
    END
   ELSE NULL
   END AS FLOAT64)) AS INTEGER) AS experian_age_adjusted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_cust_usr_vws.customer_experian_demographic_prediction_dim AS a
 INNER JOIN customer_year_driver AS dr ON LOWER(a.acp_id) = LOWER(dr.acp_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS both_ages
AS
SELECT acp_id,
 reporting_year_shopped,
 gender,
 age,
  CASE
  WHEN age BETWEEN 14 AND 22
  THEN '01) Young Adult'
  WHEN age BETWEEN 23 AND 29
  THEN '02) Early Career'
  WHEN age BETWEEN 30 AND 44
  THEN '03) Mid Career'
  WHEN age BETWEEN 45 AND 64
  THEN '04) Late Career'
  WHEN age >= 65
  THEN '05) Retired'
  ELSE 'Unknown'
  END AS lifestage,
  CASE
  WHEN age < 18
  THEN '0) <18 yrs'
  WHEN age >= 18 AND age <= 24
  THEN '1) 18-24 yrs'
  WHEN age > 24 AND age <= 34
  THEN '2) 25-34 yrs'
  WHEN age > 34 AND age <= 44
  THEN '3) 35-44 yrs'
  WHEN age > 44 AND age <= 54
  THEN '4) 45-54 yrs'
  WHEN age > 54 AND age <= 64
  THEN '5) 55-64 yrs'
  WHEN age > 64
  THEN '6) 65+ yrs'
  ELSE 'Unknown'
  END AS age_group
FROM (SELECT DISTINCT a.acp_id,
   a.reporting_year_shopped,
   b.gender,
   COALESCE(b.experian_age_adjusted, c.model_age_adjusted) AS age
  FROM customer_year_driver AS a
   LEFT JOIN experian_demos AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(b.reporting_year_shopped
      )
   LEFT JOIN modeled_ages AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.reporting_year_shopped) = LOWER(c.reporting_year_shopped
      )) AS x;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty_cardmember
AS
SELECT lmd.acp_id,
 dr.reporting_year_shopped,
 1 AS flg_cardmember,
 MAX(CASE
   WHEN LOWER(rwd.rewards_level) IN (LOWER('MEMBER'))
   THEN 1
   WHEN LOWER(rwd.rewards_level) IN (LOWER('INSIDER'), LOWER('INFLUENCER'))
   THEN 3
   WHEN LOWER(rwd.rewards_level) IN (LOWER('AMBASSADOR'))
   THEN 4
   WHEN LOWER(rwd.rewards_level) IN (LOWER('ICON'))
   THEN 5
   ELSE 0
   END) AS cardmember_level,
 MIN(lmd.cardmember_enroll_date) AS cardmember_enroll_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd
 INNER JOIN customer_year_driver AS dr ON LOWER(lmd.acp_id) = LOWER(dr.acp_id)
 LEFT JOIN (SELECT acp_id,
   PARSE_DATE('%Y%m%d', SUBSTR(CAST(max_close_dt AS STRING), 1, 8)) AS max_close_dt
  FROM t2dl_das_strategy.cco_credit_close_dts) AS ccd ON LOWER(lmd.acp_id) = LOWER(ccd.acp_id)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd ON LOWER(lmd.loyalty_id) = LOWER(rwd.loyalty_id) AND
    dr.end_dt >= rwd.start_day_date AND dr.end_dt < rwd.end_day_date
WHERE COALESCE(lmd.cardmember_enroll_date,'2099-12-31') < COALESCE(ccd.max_close_dt, lmd.cardmember_close_date,
  '2099-12-31')
 AND COALESCE(lmd.cardmember_enroll_date,'2099-12-31') <= dr.end_dt
 AND COALESCE(ccd.max_close_dt, lmd.cardmember_close_date,'2099-12-31') >= dr.end_dt
 AND lmd.acp_id IS NOT NULL
GROUP BY lmd.acp_id,
 dr.reporting_year_shopped,
 flg_cardmember;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty_member
AS
SELECT lmd.acp_id,
 dr.reporting_year_shopped,
 1 AS flg_member,
 MAX(CASE
   WHEN LOWER(rwd.rewards_level) IN (LOWER('MEMBER'))
   THEN 1
   WHEN LOWER(rwd.rewards_level) IN (LOWER('INSIDER'), LOWER('INFLUENCER'))
   THEN 3
   WHEN LOWER(rwd.rewards_level) IN (LOWER('AMBASSADOR'))
   THEN 4
   WHEN LOWER(rwd.rewards_level) IN (LOWER('ICON'))
   THEN 5
   ELSE 0
   END) AS member_level,
 MIN(lmd.member_enroll_date) AS member_enroll_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd
 INNER JOIN customer_year_driver AS dr ON LOWER(lmd.acp_id) = LOWER(dr.acp_id)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd ON LOWER(lmd.loyalty_id) = LOWER(rwd.loyalty_id) AND
    dr.end_dt >= rwd.start_day_date AND dr.end_dt < rwd.end_day_date
WHERE COALESCE(lmd.member_enroll_date,'2099-12-31') < COALESCE(lmd.member_close_date,'2099-12-31')
 AND COALESCE(lmd.member_enroll_date,'2099-12-31') <= dr.end_dt
 AND COALESCE(lmd.member_close_date,'2099-12-31') >= dr.end_dt
 AND lmd.acp_id IS NOT NULL
GROUP BY lmd.acp_id,
 dr.reporting_year_shopped,
 flg_member;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cust_level_loyalty
AS
SELECT a.acp_id,
 a.reporting_year_shopped,
  CASE
  WHEN b.acp_id IS NOT NULL
  THEN 'a) Cardmember'
  WHEN c.acp_id IS NOT NULL
  THEN 'b) Member'
  ELSE 'c) Non-Loyalty'
  END AS loyalty_type,
  CASE
  WHEN a.employee_flag = 1 AND b.acp_id IS NOT NULL AND c.acp_id IS NOT NULL
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
FROM customer_year_driver AS a
 LEFT JOIN cust_level_loyalty_cardmember AS b ON LOWER(a.acp_id) = LOWER(b.acp_id) AND LOWER(a.reporting_year_shopped) =
   LOWER(b.reporting_year_shopped)
 LEFT JOIN cust_level_loyalty_member AS c ON LOWER(a.acp_id) = LOWER(c.acp_id) AND LOWER(a.reporting_year_shopped) =
   LOWER(c.reporting_year_shopped);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition_tenure_prep
AS
SELECT a.acp_id,
 b.reporting_year_shopped,
 a.aare_status_date AS acquisition_date,
  CASE
  WHEN LOWER(a.aare_chnl_code) = LOWER('FLS')
  THEN '1) Nordstrom Stores'
  WHEN LOWER(a.aare_chnl_code) = LOWER('NCOM')
  THEN '2) Nordstrom.com'
  WHEN LOWER(a.aare_chnl_code) = LOWER('RACK')
  THEN '3) Rack Stores'
  WHEN LOWER(a.aare_chnl_code) = LOWER('NRHL')
  THEN '4) Rack.com'
  ELSE NULL
  END AS acquisition_channel,
  CASE
  WHEN LOWER(a.aare_chnl_code) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'NORDSTROM'
  WHEN LOWER(a.aare_chnl_code) IN (LOWER('RACK'), LOWER('NRHL'))
  THEN 'RACK'
  ELSE NULL
  END AS acquisition_banner,
 a.aare_brand_name AS acquisition_brand,
 CAST(TRUNC(CAST(FLOOR(DATE_DIFF(b.end_dt, a.aare_status_date, DAY) / 365.25) AS FLOAT64)) AS INTEGER) AS tenure_years,
 CAST(TRUNC(CAST(FLOOR(12 * DATE_DIFF(b.end_dt, a.aare_status_date, DAY) / 365.25) AS FLOAT64)) AS INTEGER) AS tenure_months
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS a
 INNER JOIN customer_year_driver AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
WHERE a.aare_status_date <= b.end_dt
QUALIFY (RANK() OVER (PARTITION BY a.acp_id, b.reporting_year_shopped ORDER BY a.aare_status_date DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_acquisition_tenure
AS
SELECT DISTINCT a.acp_id,
 a.reporting_year_shopped,
 a.acquisition_date,
 d.year_num AS acquisition_fiscal_year,
 a.acquisition_channel,
 a.acquisition_banner,
 a.acquisition_brand,
 a.tenure_years,
  CASE
  WHEN a.tenure_months < 4
  THEN '1) 0-3 months'
  WHEN a.tenure_months BETWEEN 4 AND 6
  THEN '2) 4-6 months'
  WHEN a.tenure_months BETWEEN 7 AND 12
  THEN '3) 7-12 months'
  WHEN a.tenure_months BETWEEN 13 AND 24
  THEN '4) 13-24 months'
  WHEN a.tenure_months BETWEEN 25 AND 36
  THEN '5) 25-36 months'
  WHEN a.tenure_months BETWEEN 37 AND 48
  THEN '6) 37-48 months'
  WHEN a.tenure_months BETWEEN 49 AND 60
  THEN '7) 49-60 months'
  WHEN a.tenure_months > 60
  THEN '8) 61+ months'
  ELSE 'unknown'
  END AS tenure_bucket_months,
  CASE
  WHEN a.tenure_months <= 12
  THEN '1) <= 1 year'
  WHEN a.tenure_months BETWEEN 13 AND 24
  THEN '2) 1-2 years'
  WHEN a.tenure_months BETWEEN 25 AND 60
  THEN '3) 2-5 years'
  WHEN a.tenure_months BETWEEN 61 AND 120
  THEN '4) 5-10 years'
  WHEN a.tenure_months > 120
  THEN '5) 10+ years'
  ELSE 'unknown'
  END AS tenure_bucket_years
FROM customer_acquisition_tenure_prep AS a
 INNER JOIN realigned_calendar AS d ON a.acquisition_date = d.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_activation
AS
SELECT a.acp_id,
 b.reporting_year_shopped,
 a.activated_date AS activation_date,
  CASE
  WHEN LOWER(a.activated_chnl_code) = LOWER('FLS')
  THEN '1) Nordstrom Stores'
  WHEN LOWER(a.activated_chnl_code) = LOWER('NCOM')
  THEN '2) Nordstrom.com'
  WHEN LOWER(a.activated_chnl_code) = LOWER('RACK')
  THEN '3) Rack Stores'
  WHEN LOWER(a.activated_chnl_code) = LOWER('NRHL')
  THEN '4) Rack.com'
  ELSE NULL
  END AS activation_channel,
  CASE
  WHEN LOWER(a.activated_chnl_code) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'NORDSTROM'
  WHEN LOWER(a.activated_chnl_code) IN (LOWER('RACK'), LOWER('NRHL'))
  THEN 'RACK'
  ELSE NULL
  END AS activation_banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_activated_fact AS a
 INNER JOIN customer_year_driver AS b ON LOWER(a.acp_id) = LOWER(b.acp_id)
WHERE a.activated_date <= b.end_dt
QUALIFY (RANK() OVER (PARTITION BY a.acp_id, b.reporting_year_shopped ORDER BY a.activated_date DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS clv_date
AS
SELECT dl.reporting_year AS reporting_year_shopped,
 MAX(a.scored_date) AS scored_date
FROM date_lookup AS dl
 LEFT JOIN (SELECT DISTINCT scored_date
  FROM t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist) AS a ON dl.start_dt <= a.scored_date
    AND dl.end_dt > a.scored_date
GROUP BY reporting_year_shopped;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_customer_level_attributes;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

INSERT INTO `{{params.gcp_project_id}}`.{{params.cco_t2_schema}}.cco_customer_level_attributes
select distinct a.acp_id
,a.reporting_year_shopped
,coalesce(b.gender,'Unknown') cust_gender
,b.age cust_age
,b.lifestage cust_lifestage
,b.age_group cust_age_group
,coalesce(trim(c.market),'Z - NON-NMS') cust_NMS_market
,d.cust_dma
,d.cust_country
,coalesce(d.dma_rank,'DMA missing') cust_dma_rank
,e.loyalty_type cust_loyalty_type
,e.loyalty_level cust_loyalty_level
,e.loyalty_member_start_dt cust_loy_member_enroll_dt
,e.loyalty_cardmember_start_dt cust_loy_cardmember_enroll_dt
,g.acquisition_date cust_acquisition_date
,g.acquisition_fiscal_year cust_acquisition_fiscal_year
,g.acquisition_channel cust_acquisition_channel
,g.acquisition_banner cust_acquisition_banner
,g.acquisition_brand cust_acquisition_brand
,g.tenure_bucket_months cust_tenure_bucket_months
,g.tenure_bucket_years cust_tenure_bucket_years
,h.activation_date cust_activation_date
,h.activation_channel cust_activation_channel
,h.activation_banner cust_activation_banner
,i.engagement_cohort cust_engagement_cohort
,i.channels_shopped cust_channel_count
,i.chan_combo cust_channel_combo
,i.banners_shopped cust_banner_count
,i.banner_combo cust_banner_combo
,i.employee_flag cust_employee_flag
,i.jwn_trip_bucket cust_jwn_trip_bucket
,i.jwn_net_spend_bucket cust_jwn_net_spend_bucket
,i.gross_sales cust_jwn_gross_sales
,i.return_amt cust_jwn_return_amt
,i.net_sales cust_jwn_net_sales
,i.net_sales_apparel cust_jwn_net_sales_apparel
,i.trips cust_jwn_trips
,i.gross_items cust_jwn_gross_items
,i.return_items cust_jwn_return_items
,i.net_items cust_jwn_net_items
,i.cust_tender_nordstrom
,i.cust_tender_nordstrom_note
,i.cust_tender_3rd_party_credit
,i.cust_tender_debit_card
,i.cust_tender_gift_card
,i.cust_tender_cash
,i.cust_tender_paypal
,i.cust_tender_check

,i.cust_event_holiday
,i.cust_event_anniversary
,i.cust_svc_group_exp_delivery
,i.cust_svc_group_order_pickup
,i.cust_svc_group_selling_relation
,i.cust_svc_group_remote_selling
,i.cust_svc_group_alterations
,i.cust_svc_group_in_store
,i.cust_svc_group_restaurant
,i.cust_service_free_exp_delivery
,i.cust_service_next_day_pickup
,i.cust_service_same_day_bopus
,i.cust_service_curbside_pickup
,i.cust_service_style_boards
,i.cust_service_gift_wrapping
,i.cust_service_pop_in
,i.cust_marketplace_flag
,i.cust_platform_desktop
,i.cust_platform_MOW
,i.cust_platform_IOS
,i.cust_platform_Android
,i.cust_platform_POS
,i.cust_anchor_brand
,i.cust_strategic_brand
,i.cust_store_customer
,i.cust_digital_customer
,clv.clv_jwn as cust_clv_jwn
,clv.clv_fp as cust_clv_fp
,clv.clv_op as cust_clv_op
,CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as dw_sys_load_tmstp
,CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as dw_sys_updt_tmstp
from customer_year_driver a
left join both_ages b on LOWER(a.acp_id)=LOWER(b.acp_id) and LOWER(a.reporting_year_shopped)= LOWER(b.reporting_year_shopped)
left join customer_nms_market c on LOWER(a.acp_id)=LOWER(c.acp_id)
left join customer_dma d on LOWER(a.acp_id)=LOWER(d.acp_id) and LOWER(a.reporting_year_shopped)=LOWER(d.reporting_year_shopped)
left join cust_level_loyalty e on LOWER(a.acp_id)= LOWER(e.acp_id) and LOWER(a.reporting_year_shopped) = LOWER(e.reporting_year_shopped)

left join customer_acquisition_tenure g on LOWER(a.acp_id) = LOWER(g.acp_id) and LOWER(a.reporting_year_shopped)=LOWER(g.reporting_year_shopped)
left join customer_activation h on LOWER(a.acp_id) = LOWER(h.acp_id) and LOWER(a.reporting_year_shopped)=LOWER(h.reporting_year_shopped)
left join derived_cust_attributes i on LOWER(a.acp_id) = LOWER(i.acp_id) and LOWER(a.reporting_year_shopped)=LOWER(i.reporting_year_shopped)
left join clv_date cld on LOWER(a.reporting_year_shopped) = LOWER(cld.reporting_year_shopped)
left join t2dl_das_customber_model_attribute_productionalization.customer_prediction_clv_hist clv
on lower(a.acp_id) = lower(clv.acp_id) and cld.scored_date = clv.scored_date;


--collect statistics column  (acp_id), column  (reporting_year_shopped), column  (cust_gender), column  (cust_age), column  (cust_lifestage), column  (cust_age_group), column  (cust_NMS_market), column  (cust_dma), column  (cust_country), column  (cust_dma_rank), column  (cust_loyalty_type), column  (cust_loyalty_level), column  (cust_employee_flag), column  (acp_id, reporting_year_shopped) on {cco_t2_schema}.cco_customer_level_attributes;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
