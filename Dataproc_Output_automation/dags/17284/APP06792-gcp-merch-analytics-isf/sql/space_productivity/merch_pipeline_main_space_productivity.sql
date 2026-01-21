CREATE TEMPORARY TABLE IF NOT EXISTS custom_hierarchy
AS
SELECT DISTINCT dept_num,
 dept_name,
 subdivision_num,
 subdivision_name,
 division_num,
 division_name,
  CASE
  WHEN subdivision_num = 700
  THEN 'Wmns Non-Dsnr Shoes'
  WHEN dept_num = 835
  THEN 'Wmns Dsnr Shoes'
  WHEN subdivision_num = 705 OR dept_num IN (836, 892)
  THEN 'Mens Dsnr/Non-Dsnr Shoes'
  WHEN subdivision_num = 710 OR dept_num = 837
  THEN 'Kids Dsnr/Non-Dsnr Shoes'
  WHEN dept_num = 829
  THEN 'Wmns Dsnr Apparel'
  WHEN dept_num = 846
  THEN 'Wmns Plus Apparel'
  WHEN dept_num = 855
  THEN 'Wmns Dresses'
  WHEN dept_num = 858
  THEN 'Wmns YA Apparel'
  WHEN subdivision_num = 775
  THEN 'Wmns Btr, Best, Denim, Outrwr'
  WHEN dept_num IN (861, 142, 856, 3)
  THEN 'Wmns Lingerie, Sleepwear & Legwear'
  WHEN dept_num IN (854, 857)
  THEN 'Wmns Active & Swim'
  WHEN dept_num IN (886)
  THEN 'Mens Dresswear'
  WHEN dept_num IN (863, 864, 824, 952)
  THEN 'Mens Sportswear'
  WHEN dept_num = 866
  THEN 'Mens YA Apparel'
  WHEN dept_num = 885
  THEN 'Mens Denim'
  WHEN dept_num IN (871, 868)
  THEN 'Mens Spec & Sleepwear'
  WHEN dept_num IN (867, 869, 870)
  THEN 'Mens Active, Swim & Outrwr'
  WHEN dept_num IN (827, 828)
  THEN 'Mens Dsnr App & Acess'
  WHEN dept_num IN (834)
  THEN 'Kids Dsnr Apparel'
  WHEN dept_num IN (872, 873)
  THEN 'Baby Apparel'
  WHEN dept_num IN (874, 893)
  THEN 'Baby Gear & Toys'
  WHEN dept_num IN (875, 888)
  THEN 'Girls Apparel'
  WHEN dept_num IN (889, 887)
  THEN 'Boys Apparel'
  WHEN dept_num = 818
  THEN 'Mens Beauty'
  WHEN division_num = 340
  THEN 'Wmns Beauty'
  WHEN dept_num IN (876, 294, 951)
  THEN 'Accessories & Trend'
  WHEN dept_num = 877
  THEN 'Eyewear'
  WHEN dept_num IN (878, 879)
  THEN 'Fashion Jewelry & Watches'
  WHEN dept_num IN (881, 905)
  THEN 'Fine Jewelry'
  WHEN dept_num = 832
  THEN 'Wmns Dsnr Accessories'
  WHEN dept_num = 833
  THEN 'Dsnr Handbags'
  WHEN dept_num = 882
  THEN 'Non-Dsnr Handbags'
  WHEN division_num = 365
  THEN 'Home'
  WHEN dept_num = 694
  THEN 'Mens Dsnr App & Acess'
  WHEN dept_num IN (645, 705)
  THEN 'Nordstrom x Nike'
  WHEN dept_num = 608
  THEN 'Wmns Emerging/Space'
  WHEN dept_num = 587 OR division_num = 700
  THEN 'Pop In Shop'
  WHEN division_num = 800
  THEN 'Leased Boutique'
  ELSE 'No Match'
  END AS subdepartment,
 '110' AS chnl_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim
WHERE division_num <> 800
UNION ALL
SELECT DISTINCT dept_num,
 dept_name,
 subdivision_num,
 subdivision_name,
 division_num,
 division_name,
  CASE
  WHEN subdivision_num = 700
  THEN 'Wmns Shoes'
  WHEN subdivision_num = 705
  THEN 'Mens Shoes'
  WHEN subdivision_num = 710
  THEN 'Kids Shoes'
  WHEN dept_num IN (835, 836, 837)
  THEN 'Dsnr Shoes'
  WHEN subdivision_num IN (775, 785) OR dept_num = 829
  THEN 'Wmns App/Spec/Dsnr'
  WHEN subdivision_num IN (780, 790) OR dept_num = 827
  THEN 'Mens App/Spec/Dsnr'
  WHEN subdivision_num IN (770) OR dept_num = 834
  THEN 'Kids App w/Dsnr'
  WHEN division_num IN (360)
  THEN 'Accessories'
  WHEN division_num IN (365)
  THEN 'Home'
  WHEN division_num IN (340)
  THEN 'Beauty'
  WHEN LOWER(dept_name) NOT LIKE LOWER('INACT%')
  THEN 'Other'
  ELSE 'No Match'
  END AS subdepartment,
 '210' AS chnl_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim
WHERE division_num <> 800;


CREATE TEMPORARY TABLE IF NOT EXISTS dates
AS
SELECT ty_ly_lly_ind,
 fiscal_month_num,
 month_idnt,
 month_label,
 month_end_day_date,
 month_start_day_date,
 fiscal_year_num,
 week_idnt,
 day_date,
 week_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
WHERE month_end_day_date <= current_date('PST8PDT')
GROUP BY ty_ly_lly_ind,
 fiscal_month_num,
 month_idnt,
 month_label,
 month_end_day_date,
 month_start_day_date,
 fiscal_year_num,
 week_idnt,
 day_date,
 week_end_day_date;


CREATE TEMPORARY TABLE IF NOT EXISTS kpis
AS
SELECT store_num,
 subdepartment,
 department,
 selling_type,
 channel_num,
 month_num,
 year_num,
 SUM(net_sales_cost) AS net_sales_cost,
 SUM(net_sales_retail) AS net_sales_retail,
 AVG(eoh_r) AS eoh_r,
 AVG(eoh_c) AS eoh_c
FROM (SELECT a.store_num,
   c.subdepartment,
   d.department,
   d.selling_type,
   a.channel_num,
   a.month_num,
   a.year_num,
   SUM(a.net_sales_cost) AS net_sales_cost,
   SUM(a.net_sales_retail) AS net_sales_retail,
   SUM(a.closing_stock_retail) AS eoh_r,
   SUM(a.closing_stock_cost) AS eoh_c
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_eow_sl_olap_vw AS a
   INNER JOIN (SELECT DISTINCT week_idnt
    FROM dates) AS b ON a.week_num = b.week_idnt
   LEFT JOIN custom_hierarchy AS c ON a.department_num = c.dept_num AND a.channel_num = CAST(c.chnl_idnt AS FLOAT64)
   LEFT JOIN (SELECT DISTINCT chnl_idnt,
     department,
     subdepartment,
     selling_type
    FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.space_productivity_area_mapping) AS d ON LOWER(c.subdepartment) = LOWER(d
      .subdepartment) AND CAST(c.chnl_idnt AS FLOAT64) = d.chnl_idnt
  WHERE a.channel_num IN (110, 210)
  GROUP BY a.store_num,
   c.subdepartment,
   d.department,
   d.selling_type,
   a.week_num,
   a.month_num,
   a.year_num,
   a.channel_num) AS a
GROUP BY store_num,
 subdepartment,
 department,
 selling_type,
 channel_num,
 month_num,
 year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS store_base
AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
  CASE
  WHEN LOWER(region_detail) = LOWER('US FL - MANHATTAN')
  THEN 'US FL - NY'
  WHEN LOWER(region_detail) IN (LOWER('US FL - NCAL'), LOWER('US FL - OR'), LOWER('US FL - WA/AK'))
  THEN 'US FL - NW'
  WHEN LOWER(region_detail) = LOWER('US FL - TX')
  THEN 'US FL - SW'
  WHEN LOWER(region_detail) IN (LOWER('US NO - SC'), LOWER('US SO - SC'))
  THEN 'US FL - SCAL'
  WHEN LOWER(region_detail) = LOWER('US RK - SCAL')
  THEN 'US RK - SCAL'
  ELSE region_detail
  END AS region,
 month_idnt,
 fiscal_month_num,
 fiscal_year_num,
 SUM(area_sq_ft) AS area_sq_ft
FROM (SELECT DISTINCT a.chnl_idnt,
   a.department,
   a.subdepartment,
   a.selling_type,
   a.dept_key,
   b.store_num,
   b.store_name,
   b.comp_status_desc,
   b.region_detail,
   d.month_idnt,
   d.fiscal_month_num,
   d.fiscal_year_num,
   COALESCE(c.area_sq_ft, 0) AS area_sq_ft
  FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.space_productivity_area_mapping AS a
   INNER JOIN (SELECT DISTINCT store_num,
     store_name,
     channel_num,
        'US ' || store_type_code || ' - ' || REGEXP_EXTRACT_ALL(region_medium_desc, '[^ ]+')[SAFE_OFFSET(0)] AS
     region_detail,
     comp_status_desc
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
    WHERE channel_num IN (110, 210)
     AND LOWER(store_type_code) IN (LOWER('FL'), LOWER('RK'))
     AND LOWER(store_name) NOT LIKE LOWER('%CLOSED%')
     AND LOWER(store_name) NOT LIKE LOWER('VS%')
     AND LOWER(store_name) NOT LIKE LOWER('FINANCIAL STORE%')
     AND LOWER(store_name) NOT LIKE LOWER('%TEST %')
     AND LOWER(store_name) NOT LIKE LOWER('%CLSD%')
     AND LOWER(store_name) NOT LIKE LOWER('%EMPLOYEE%')
     AND LOWER(store_name) NOT LIKE LOWER('%BULK%')
     AND LOWER(store_name) NOT LIKE LOWER('%UNASSIGNED%')) AS b ON a.chnl_idnt = b.channel_num
   LEFT JOIN (SELECT DISTINCT *
    FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.store_square_footage) AS c ON LOWER(a.dept_key) = LOWER(c.dept_key) AND
     b.store_num = c.store_num
   LEFT JOIN dates AS d ON d.day_date BETWEEN COALESCE(c.eff_start_date, DATE '2022-01-30') AND (COALESCE(c.eff_end_date
      , DATE '9999-12-31'))
  WHERE d.day_date = d.month_start_day_date) AS e
GROUP BY chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_idnt,
 fiscal_month_num,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS month_base AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'SUBDEPARTMENT / STORE' AS level,
 'Month' AS timeframe
FROM (SELECT a.chnl_idnt,
   a.department,
   a.subdepartment,
   a.selling_type,
   a.store_num,
   a.store_name,
   a.comp_status_desc,
   a.region_detail,
   a.region,
   a.month_num,
   a.fiscal_year_num,
   a.area_sq_ft,
   b.net_sales_retail,
   b.net_sales_cost,
   b.eoh_r,
   b.eoh_c,
   SUM(a.area_sq_ft) OVER (PARTITION BY a.store_num, a.month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_sq_ft,
    CASE
    WHEN (SUM(a.area_sq_ft) OVER (PARTITION BY a.store_num, a.month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(a.area_sq_ft AS NUMERIC) / (SUM(a.area_sq_ft) OVER (PARTITION BY a.store_num, a.month_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN a.area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(b.net_sales_retail) OVER (PARTITION BY a.store_num, a.month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_volume,
    CASE
    WHEN (SUM(b.net_sales_retail) OVER (PARTITION BY a.store_num, a.month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(b.net_sales_retail AS BIGNUMERIC) / (SUM(b.net_sales_retail) OVER (PARTITION BY a.store_num, a.month_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.month_num ORDER BY a.area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.month_num ORDER BY b.net_sales_retail DESC) AS
   rank_sales_fleet,
    CASE
    WHEN a.area_sq_ft = 0
    THEN NULL
    ELSE b.net_sales_retail / a.area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.month_num ORDER BY CASE
       WHEN a.area_sq_ft = 0
       THEN NULL
       ELSE b.net_sales_retail / a.area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN a.area_sq_ft = 0
    THEN NULL
    ELSE b.eoh_r / a.area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN a.area_sq_ft = 0
    THEN NULL
    ELSE b.eoh_c / a.area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN b.net_sales_retail = 0
    THEN 0
    ELSE (b.net_sales_retail - b.net_sales_cost) / b.net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.month_num ORDER BY CASE
       WHEN b.net_sales_retail = 0
       THEN 0
       ELSE (b.net_sales_retail - b.net_sales_cost) / b.net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_idnt AS month_num,
     fiscal_year_num,
     area_sq_ft
    FROM store_base AS a) AS a
   LEFT JOIN kpis AS b ON a.chnl_idnt = b.channel_num AND a.store_num = CAST(b.store_num AS FLOAT64) AND LOWER(a.subdepartment
       ) = LOWER(b.subdepartment) AND a.month_num = b.month_num) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS sub_region_month AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'SUBDEPARTMENT / REGION' AS level,
 'Month' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       )) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY region, month_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY net_sales_retail DESC) AS rank_sales_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, month_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     0 AS store_num,
     'N/A' AS store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     'N/A' AS region_detail,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM month_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
    GROUP BY chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     region_detail) AS t1) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS dep_region_month AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'DEPARTMENT / REGION' AS level,
 'Month' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       )) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY region, month_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY net_sales_retail DESC) AS rank_sales_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     'N/A' AS subdepartment,
     selling_type,
     0 AS store_num,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     'N/A' AS store_name,
     'N/A' AS region_detail,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM month_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
    GROUP BY chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     store_name,
     region_detail) AS t1) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS dep_store_month AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'DEPARTMENT / STORE' AS level,
 'Month' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY store_num, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY store_num, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY store_num, month_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY store_num, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY store_num, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY store_num, month_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY net_sales_retail DESC) AS rank_sales_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, department, month_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     'N/A' AS subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM month_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
    GROUP BY chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num) AS t1) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS tot_store_month AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, month_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, month_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'TOTAL / STORE' AS level,
 'Month' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       )) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY region, month_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY region, month_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY region, month_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, month_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, month_num ORDER BY net_sales_retail DESC) AS rank_sales_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, month_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, month_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     'N/A' AS department,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     'N/A' AS subdepartment,
     'N/A' AS region_detail,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM month_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
    GROUP BY chnl_idnt,
     department,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     subdepartment,
     region_detail) AS t1) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS year_base AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY pct_store_sq_ft DESC) AS
 rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY pct_store_volume DESC) AS
 rank_volume_fleet,
 'SUBDEPARTMENT / STORE' AS level,
 'Year' AS timeframe
FROM (SELECT a.chnl_idnt,
   a.department,
   a.subdepartment,
   a.selling_type,
   a.store_num,
   a.store_name,
   a.comp_status_desc,
   a.region_detail,
   a.region,
   a.month_num,
   a.fiscal_year_num,
   a.area_sq_ft,
   b.net_sales_retail,
   b.net_sales_cost,
   b.eoh_r,
   b.eoh_c,
   SUM(a.area_sq_ft) OVER (PARTITION BY a.store_num, a.fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_sq_ft,
    CASE
    WHEN (SUM(a.area_sq_ft) OVER (PARTITION BY a.store_num, a.fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(a.area_sq_ft AS NUMERIC) / (SUM(a.area_sq_ft) OVER (PARTITION BY a.store_num, a.fiscal_year_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN a.area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(b.net_sales_retail) OVER (PARTITION BY a.store_num, a.fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_volume,
    CASE
    WHEN (SUM(b.net_sales_retail) OVER (PARTITION BY a.store_num, a.fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING
       AND UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(b.net_sales_retail AS BIGNUMERIC) / (SUM(b.net_sales_retail) OVER (PARTITION BY a.store_num, a.fiscal_year_num
         RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.fiscal_year_num ORDER BY a.area_sq_ft DESC) AS
   rank_area_fleet,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.fiscal_year_num ORDER BY b.net_sales_retail DESC) AS
   rank_sales_fleet,
    CASE
    WHEN a.area_sq_ft = 0
    THEN NULL
    ELSE b.net_sales_retail / a.area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.fiscal_year_num ORDER BY CASE
       WHEN a.area_sq_ft = 0
       THEN NULL
       ELSE b.net_sales_retail / a.area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN a.area_sq_ft = 0
    THEN NULL
    ELSE b.eoh_r / a.area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN a.area_sq_ft = 0
    THEN NULL
    ELSE b.eoh_c / a.area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN b.net_sales_retail = 0
    THEN 0
    ELSE (b.net_sales_retail - b.net_sales_cost) / b.net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY a.chnl_idnt, a.subdepartment, a.month_num ORDER BY CASE
       WHEN b.net_sales_retail = 0
       THEN 0
       ELSE (b.net_sales_retail - b.net_sales_cost) / b.net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     NULL AS month_num,
     fiscal_year_num,
     area_sq_ft
    FROM store_base AS a
    WHERE fiscal_month_num = 12
     AND fiscal_year_num IN (SELECT DISTINCT fiscal_year_num
       FROM dates
       WHERE LOWER(ty_ly_lly_ind) IN (LOWER('LLY'), LOWER('LY')))) AS a
   LEFT JOIN (SELECT store_num,
     subdepartment,
     department,
     selling_type,
     channel_num,
     year_num,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(net_sales_retail) AS net_sales_retail,
     AVG(eoh_r) AS eoh_r,
     AVG(eoh_c) AS eoh_c
    FROM kpis
    GROUP BY store_num,
     subdepartment,
     department,
     selling_type,
     channel_num,
     year_num) AS b ON a.chnl_idnt = b.channel_num AND a.store_num = CAST(b.store_num AS FLOAT64) AND LOWER(a.subdepartment
       ) = LOWER(b.subdepartment) AND a.fiscal_year_num = b.year_num) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS sub_region_year AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY pct_store_sq_ft DESC) AS
 rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY pct_store_volume DESC) AS
 rank_volume_fleet,
 'SUBDEPARTMENT / REGION' AS level,
 'Year' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY net_sales_retail DESC) AS
   rank_sales_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, subdepartment, fiscal_year_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     0 AS store_num,
     'N/A' AS store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     'N/A' AS region_detail,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM year_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
     AND fiscal_year_num IN (SELECT DISTINCT fiscal_year_num
       FROM dates
       WHERE LOWER(ty_ly_lly_ind) IN (LOWER('LLY'), LOWER('LY')))
    GROUP BY chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     region_detail) AS t5) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS dep_region_year AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet
 ,
 RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'DEPARTMENT / REGION' AS level,
 'Year' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY net_sales_retail DESC) AS rank_sales_fleet
   ,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     'N/A' AS subdepartment,
     selling_type,
     0 AS store_num,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     'N/A' AS store_name,
     'N/A' AS region_detail,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM year_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
     AND fiscal_year_num IN (SELECT DISTINCT fiscal_year_num
       FROM dates
       WHERE LOWER(ty_ly_lly_ind) IN (LOWER('LLY'), LOWER('LY')))
    GROUP BY chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     store_name,
     region_detail) AS t5) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS dep_store_year AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet
 ,
 RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'DEPARTMENT / STORE' AS level,
 'Year' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY store_num, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY store_num, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY store_num, fiscal_year_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY store_num, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY store_num, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY store_num, fiscal_year_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY net_sales_retail DESC) AS rank_sales_fleet
   ,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, department, fiscal_year_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     department,
     'N/A' AS subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM year_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
     AND fiscal_year_num IN (SELECT DISTINCT fiscal_year_num
       FROM dates
       WHERE LOWER(ty_ly_lly_ind) IN (LOWER('LLY'), LOWER('LY')))
    GROUP BY chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num) AS t5) AS c;


CREATE TEMPORARY TABLE IF NOT EXISTS tot_store_year AS
SELECT chnl_idnt,
 department,
 subdepartment,
 selling_type,
 store_num,
 store_name,
 comp_status_desc,
 region_detail,
 region,
 month_num,
 fiscal_year_num,
 area_sq_ft,
 net_sales_retail,
 net_sales_cost,
 eoh_r,
 eoh_c,
 store_sq_ft,
 pct_store_sq_ft,
 no_space_ind,
 store_volume,
 pct_store_volume,
 rank_area_fleet,
 rank_sales_fleet,
 sales_psf,
 rank_sales_psf_fleet,
 eoh_r_psf,
 eoh_c_psf,
 gm_pct,
 rank_gm_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, fiscal_year_num ORDER BY pct_store_sq_ft DESC) AS rank_pct_store_fleet,
 RANK() OVER (PARTITION BY chnl_idnt, fiscal_year_num ORDER BY pct_store_volume DESC) AS rank_volume_fleet,
 'TOTAL / STORE' AS level,
 'Year' AS timeframe
FROM (SELECT chnl_idnt,
   department,
   subdepartment,
   selling_type,
   store_num,
   store_name,
   comp_status_desc,
   region_detail,
   region,
   month_num,
   fiscal_year_num,
   area_sq_ft,
   net_sales_retail,
   net_sales_cost,
   eoh_r,
   eoh_c,
   SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS store_sq_ft,
    CASE
    WHEN (SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(area_sq_ft AS NUMERIC) / (SUM(area_sq_ft) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_sq_ft,
    CASE
    WHEN area_sq_ft = 0
    THEN 1
    ELSE 0
    END AS no_space_ind,
   SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS store_volume,
    CASE
    WHEN (SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num RANGE BETWEEN UNBOUNDED PRECEDING AND
       UNBOUNDED FOLLOWING)) = 0
    THEN 0
    ELSE CAST(net_sales_retail AS BIGNUMERIC) / (SUM(net_sales_retail) OVER (PARTITION BY region, fiscal_year_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
    END AS pct_store_volume,
   RANK() OVER (PARTITION BY chnl_idnt, fiscal_year_num ORDER BY area_sq_ft DESC) AS rank_area_fleet,
   RANK() OVER (PARTITION BY chnl_idnt, fiscal_year_num ORDER BY net_sales_retail DESC) AS rank_sales_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE net_sales_retail / area_sq_ft
    END AS sales_psf,
   RANK() OVER (PARTITION BY chnl_idnt, fiscal_year_num ORDER BY CASE
       WHEN area_sq_ft = 0
       THEN NULL
       ELSE net_sales_retail / area_sq_ft
       END DESC) AS rank_sales_psf_fleet,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_r / area_sq_ft
    END AS eoh_r_psf,
    CASE
    WHEN area_sq_ft = 0
    THEN NULL
    ELSE eoh_c / area_sq_ft
    END AS eoh_c_psf,
    CASE
    WHEN net_sales_retail = 0
    THEN 0
    ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
    END AS gm_pct,
   RANK() OVER (PARTITION BY chnl_idnt, fiscal_year_num ORDER BY CASE
       WHEN net_sales_retail = 0
       THEN 0
       ELSE (net_sales_retail - net_sales_cost) / net_sales_retail
       END DESC) AS rank_gm_fleet
  FROM (SELECT chnl_idnt,
     'N/A' AS department,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     'N/A' AS subdepartment,
     'N/A' AS region_detail,
     SUM(area_sq_ft) AS area_sq_ft,
     SUM(net_sales_retail) AS net_sales_retail,
     SUM(net_sales_cost) AS net_sales_cost,
     SUM(eoh_r) AS eoh_r,
     SUM(eoh_c) AS eoh_c
    FROM year_base
    WHERE LOWER(comp_status_desc) = LOWER('COMP')
     AND fiscal_year_num IN (SELECT DISTINCT fiscal_year_num
       FROM dates
       WHERE LOWER(ty_ly_lly_ind) IN (LOWER('LLY'), LOWER('LY')))
    GROUP BY chnl_idnt,
     department,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region,
     month_num,
     fiscal_year_num,
     subdepartment,
     region_detail) AS t5) AS c;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.space_productivity{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.space_productivity{{params.env_suffix}}
(SELECT a.chnl_idnt,
  a.department,
  a.subdepartment,
  a.selling_type,
  a.store_num,
  a.store_name,
  a.comp_status_desc,
  a.region_detail,
  a.region,
  a.month_num,
  a.fiscal_year_num,
  a.area_sq_ft,
  a.net_sales_retail,
  a.net_sales_cost,
  CAST(a.eoh_r AS FLOAT64) AS eoh_r,
  CAST(a.eoh_c AS FLOAT64) AS eoh_c,
  a.store_sq_ft,
  a.pct_store_sq_ft,
  a.no_space_ind,
  a.store_volume,
  a.pct_store_volume,
  a.rank_area_fleet,
  a.rank_sales_fleet,
  a.sales_psf,
  a.rank_sales_psf_fleet,
  CAST(a.eoh_r_psf AS FLOAT64) AS eoh_r_psf,
  CAST(a.eoh_c_psf AS FLOAT64) AS eoh_c_psf,
  a.gm_pct,
  a.rank_gm_fleet,
  a.rank_pct_store_fleet,
  a.rank_volume_fleet,
  a.level,
  a.timeframe,
  UPPER(b.store_address_city) AS store_city,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS updated_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as updated_timestamp_tz
 FROM (SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM month_base
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM sub_region_month
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM dep_store_month
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM dep_region_month
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM tot_store_month
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     CAST(month_num AS INTEGER) AS month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM year_base
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     CAST(month_num AS INTEGER) AS month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM sub_region_year
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     CAST(month_num AS INTEGER) AS month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM dep_store_year
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     CAST(month_num AS INTEGER) AS month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM dep_region_year
    UNION ALL
    SELECT chnl_idnt,
     department,
     subdepartment,
     selling_type,
     store_num,
     store_name,
     comp_status_desc,
     region_detail,
     region,
     CAST(month_num AS INTEGER) AS month_num,
     fiscal_year_num,
     area_sq_ft,
     net_sales_retail,
     net_sales_cost,
     eoh_r,
     eoh_c,
     store_sq_ft,
     pct_store_sq_ft,
     no_space_ind,
     store_volume,
     pct_store_volume,
     rank_area_fleet,
     rank_sales_fleet,
     sales_psf,
     rank_sales_psf_fleet,
     eoh_r_psf,
     eoh_c_psf,
     gm_pct,
     rank_gm_fleet,
     rank_pct_store_fleet,
     rank_volume_fleet,
     level,
     timeframe
    FROM tot_store_year) AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b ON a.store_num = b.store_num);