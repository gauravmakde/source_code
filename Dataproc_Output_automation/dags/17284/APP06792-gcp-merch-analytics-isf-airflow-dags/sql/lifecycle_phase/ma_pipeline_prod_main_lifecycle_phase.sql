CREATE TEMPORARY TABLE IF NOT EXISTS channel_lkup
AS
SELECT DISTINCT channel_num,
 channel_brand AS banner
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE channel_num IN (110, 120, 210, 220, 250, 260, 310);


--collect statistics    column(channel_num)    ,column(channel_num, banner) on channel_lkup 


CREATE TEMPORARY TABLE IF NOT EXISTS price_sequencing
AS
SELECT cc,
 channel_brand AS banner,
  CASE
  WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
  THEN 'R'
  WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  ELSE NULL
  END AS ownership_price_type_code,
 last_md_version AS last_markdown_version,
 regular_price_amt AS regular_price,
 ownership_retail_price_amt AS ownership_price,
 LAG(regular_price_amt, 1) OVER (PARTITION BY cc, channel_brand ORDER BY pst_eff_begin_date) AS previous_regular_price,
  CASE
  WHEN regular_price_amt = (LAG(regular_price_amt, 1) OVER (PARTITION BY cc, channel_brand ORDER BY pst_eff_begin_date)
    )
  THEN 0
  ELSE 1
  END AS regular_price_change,
 LAG(ownership_retail_price_amt, 1) OVER (PARTITION BY cc, channel_brand ORDER BY pst_eff_begin_date) AS
 previous_ownership_price,
  CASE
  WHEN ownership_retail_price_amt = (LAG(ownership_retail_price_amt, 1) OVER (PARTITION BY cc, channel_brand ORDER BY
       pst_eff_begin_date))
  THEN 0
  ELSE 1
  END AS ownership_price_change,
 eff_begin_tmstp,
 eff_end_tmstp,
 pst_eff_begin_date,
 pst_eff_end_date,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN CASE
   WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
   THEN 8
   WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
   THEN CASE
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
    THEN 1
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
    THEN CASE
     WHEN racked_flag = 1
     THEN 4
     WHEN last_md_version = 1
     THEN 2
     WHEN last_md_version > 1
     THEN 3
     ELSE NULL
     END
    ELSE NULL
    END
   ELSE NULL
   END
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN CASE
   WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
   THEN 8
   WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
   THEN CASE
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
    THEN 5
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
    THEN CASE
     WHEN last_md_version = 1
     THEN 6
     WHEN last_md_version > 1
     THEN 7
     ELSE NULL
     END
    ELSE NULL
    END
   ELSE NULL
   END
  ELSE NULL
  END AS lifecycle_phase_number,
   TRIM(FORMAT('%11d', CASE
      WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
      THEN CASE
       WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
       THEN 8
       WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
       THEN CASE
        WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
        THEN 1
        WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
        THEN CASE
         WHEN racked_flag = 1
         THEN 4
         WHEN last_md_version = 1
         THEN 2
         WHEN last_md_version > 1
         THEN 3
         ELSE NULL
         END
        ELSE NULL
        END
       ELSE NULL
       END
      WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
      THEN CASE
       WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
       THEN 8
       WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
       THEN CASE
        WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
        THEN 5
        WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
        THEN CASE
         WHEN last_md_version = 1
         THEN 6
         WHEN last_md_version > 1
         THEN 7
         ELSE NULL
         END
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END)) || '. ' || CASE
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 1
   THEN 'NORD REGULAR PRICE'
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 2
   THEN 'NORD FIRST MARK'
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 3
   THEN 'NORD REMARK'
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 4
   THEN 'RACKING'
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 5
   THEN 'RACK REGULAR PRICE'
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 6
   THEN 'RACK FIRST MARK'
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 7
   THEN 'RACK REMARK'
   WHEN CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END = 8
   THEN 'LAST CHANCE'
   ELSE NULL
   END AS lifecycle_phase_name,
 FIRST_VALUE(pst_eff_begin_date) OVER (PARTITION BY cc, channel_brand, CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END, cycle_pst_eff_begin_dt ORDER BY pst_eff_begin_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS
 lifecycle_phase_pst_eff_begin_date,
 LAST_VALUE(pst_eff_end_date) OVER (PARTITION BY cc, channel_brand, CASE
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 1
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN racked_flag = 1
        THEN 4
        WHEN last_md_version = 1
        THEN 2
        WHEN last_md_version > 1
        THEN 3
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN CASE
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 1
      THEN 8
      WHEN CAST(TRUNC(CAST(lc_flag AS FLOAT64)) AS INTEGER) = 0
      THEN CASE
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
       THEN 5
       WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
       THEN CASE
        WHEN last_md_version = 1
        THEN 6
        WHEN last_md_version > 1
        THEN 7
        ELSE NULL
        END
       ELSE NULL
       END
      ELSE NULL
      END
     ELSE NULL
     END, cycle_pst_eff_begin_dt ORDER BY pst_eff_end_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS
 lifecycle_phase_pst_eff_end_date,
 DENSE_RANK() OVER (PARTITION BY cc, channel_brand ORDER BY cc, channel_brand, cycle_pst_eff_begin_dt) AS
 banner_lifecycle_number,
 cycle_pst_eff_begin_dt AS banner_lifecycle_pst_eff_begin_date,
 cycle_pst_eff_end_dt AS banner_lifecycle_pst_eff_end_date
FROM {{params.gcp_project_id}}.t2dl_das_markdown.cc_price_hist AS prc
WHERE LOWER(channel_country) = LOWER('US');


--collect statistics    -- high confidence    column(cc, banner)    -- medium confidence    ,column(cc, banner, last_markdown_version, regular_price, ownership_price, pst_eff_begin_date) on price_sequencing 


CREATE TEMPORARY TABLE IF NOT EXISTS sequence_grouping
AS
SELECT cc,
 banner,
 ownership_price_type_code,
 last_markdown_version,
 regular_price,
 ownership_price,
 previous_regular_price,
 regular_price_change,
 previous_ownership_price,
 ownership_price_change,
 eff_begin_tmstp,
 eff_end_tmstp,
 pst_eff_begin_date,
 pst_eff_end_date,
 lifecycle_phase_number,
 lifecycle_phase_name,
 lifecycle_phase_pst_eff_begin_date,
 lifecycle_phase_pst_eff_end_date,
 banner_lifecycle_number,
 banner_lifecycle_pst_eff_begin_date,
 banner_lifecycle_pst_eff_end_date,
 SUM(regular_price_change) OVER (PARTITION BY cc, banner ORDER BY pst_eff_begin_date ROWS BETWEEN UNBOUNDED PRECEDING
  AND CURRENT ROW) AS regular_price_counter,
 SUM(ownership_price_change) OVER (PARTITION BY cc, banner ORDER BY pst_eff_begin_date ROWS BETWEEN UNBOUNDED PRECEDING
  AND CURRENT ROW) AS ownership_price_counter,
   (SUM(regular_price_change) OVER (PARTITION BY cc, banner ORDER BY pst_eff_begin_date ROWS BETWEEN UNBOUNDED PRECEDING
    AND CURRENT ROW)) + (SUM(ownership_price_change) OVER (PARTITION BY cc, banner ORDER BY pst_eff_begin_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS sequence_group
FROM price_sequencing;


--collect statistics    -- high confidence    column(       cc, banner, ownership_price_type_code, last_markdown_version, regular_price, ownership_price       ,lifecycle_phase_number, lifecycle_phase_name, banner_lifecycle_number       ,sequence_group     )    -- medium confidence    ,column(banner_lifecycle_number)    ,column(lifecycle_phase_number)    ,column(last_markdown_version)    ,column(regular_price)    ,column(ownership_price)    ,column(sequence_group) on sequence_grouping 


CREATE TEMPORARY TABLE IF NOT EXISTS final_ranges
AS
SELECT cc,
 banner,
 ownership_price_type_code,
 last_markdown_version,
 regular_price,
 ownership_price,
 lifecycle_phase_number,
 lifecycle_phase_name,
 banner_lifecycle_number,
 MIN(pst_eff_begin_date) AS pst_eff_begin_date,
 MAX(pst_eff_end_date) AS pst_eff_end_date,
 MIN(lifecycle_phase_pst_eff_begin_date) AS lifecycle_phase_pst_eff_begin_date,
 MAX(lifecycle_phase_pst_eff_end_date) AS lifecycle_phase_pst_eff_end_date,
 MIN(banner_lifecycle_pst_eff_begin_date) AS banner_lifecycle_pst_eff_begin_date,
 MAX(banner_lifecycle_pst_eff_end_date) AS banner_lifecycle_pst_eff_end_date
FROM sequence_grouping AS sg
GROUP BY sequence_group,
 cc,
 banner,
 ownership_price_type_code,
 last_markdown_version,
 regular_price,
 ownership_price,
 lifecycle_phase_number,
 lifecycle_phase_name,
 banner_lifecycle_number;


--collect statistics    -- high confidence    column(cc)    ,column(cc, banner, banner_lifecycle_number) on final_ranges 


CREATE TEMPORARY TABLE IF NOT EXISTS final_data_cc_banner 
AS 
WITH cc_list AS (SELECT DISTINCT CONCAT(TRIM(rms_style_num), '-', TRIM(FORMAT('%6d', CAST(TRUNC(CAST(CASE
        WHEN COALESCE(color_num, FORMAT('%4d', 0)) = ''
        THEN '0'
        ELSE COALESCE(color_num, FORMAT('%4d', 0))
        END AS FLOAT64)) AS SMALLINT)))) AS cc,
   rms_style_num,
    CASE
    WHEN LENGTH(color_num) = 1
    THEN '00' || color_num
    WHEN LENGTH(color_num) = 2
    THEN '0' || color_num
    WHEN LENGTH(color_num) = 3
    THEN color_num
    ELSE NULL
    END AS color_num
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
  WHERE LOWER(channel_country) = LOWER('US')) 
  
  (SELECT f.cc,
   c.rms_style_num,
   c.color_num,
   f.banner,
   f.ownership_price_type_code,
   f.last_markdown_version,
   f.regular_price,
   f.ownership_price,
   f.pst_eff_begin_date,
   f.pst_eff_end_date,
   f.lifecycle_phase_number,
   f.lifecycle_phase_name,
   f.lifecycle_phase_pst_eff_begin_date,
   f.lifecycle_phase_pst_eff_end_date,
   f.banner_lifecycle_number,
   f.banner_lifecycle_pst_eff_begin_date,
   f.banner_lifecycle_pst_eff_end_date,
   MAX(CASE
     WHEN f.lifecycle_phase_number = 1 OR f.lifecycle_phase_number = 5
     THEN 1
     ELSE 0
     END) OVER (PARTITION BY f.cc, f.banner, f.banner_lifecycle_number RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS ind_reg_price_this_banner_cycle,
   MAX(CASE
     WHEN f.lifecycle_phase_number = 2 OR f.lifecycle_phase_number = 6
     THEN 1
     ELSE 0
     END) OVER (PARTITION BY f.cc, f.banner, f.banner_lifecycle_number RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS ind_first_mark_this_banner_cycle,
   MAX(CASE
     WHEN f.lifecycle_phase_number = 3 OR f.lifecycle_phase_number = 7
     THEN 1
     ELSE 0
     END) OVER (PARTITION BY f.cc, f.banner, f.banner_lifecycle_number RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS ind_remark_this_banner_cycle,
   MAX(CASE
     WHEN f.lifecycle_phase_number = 4
     THEN 1
     ELSE 0
     END) OVER (PARTITION BY f.cc, f.banner, f.banner_lifecycle_number RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS ind_racking_this_banner_cycle,
   MAX(CASE
     WHEN f.lifecycle_phase_number = 8
     THEN 1
     ELSE 0
     END) OVER (PARTITION BY f.cc, f.banner, f.banner_lifecycle_number RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS ind_last_chance_this_banner_cycle
  FROM final_ranges AS f
   INNER JOIN cc_list AS c ON LOWER(c.cc) = LOWER(f.cc));


CREATE TEMPORARY TABLE IF NOT EXISTS final_data_sku_channel
AS
SELECT sku.rms_sku_num,
 f.cc,
 f.rms_style_num,
 f.color_num,
 f.banner,
 ch.channel_num,
 f.ownership_price_type_code,
 f.last_markdown_version,
 f.regular_price,
 f.ownership_price,
 f.pst_eff_begin_date,
 f.pst_eff_end_date,
 f.lifecycle_phase_number,
 f.lifecycle_phase_name,
 f.lifecycle_phase_pst_eff_begin_date,
 f.lifecycle_phase_pst_eff_end_date,
 f.banner_lifecycle_number,
 f.banner_lifecycle_pst_eff_begin_date,
 f.banner_lifecycle_pst_eff_end_date,
 f.ind_reg_price_this_banner_cycle,
 f.ind_first_mark_this_banner_cycle,
 f.ind_remark_this_banner_cycle,
 f.ind_racking_this_banner_cycle,
 f.ind_last_chance_this_banner_cycle
FROM final_data_cc_banner AS f
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku ON LOWER(CONCAT(TRIM(sku.rms_style_num), '-', TRIM(FORMAT('%6d', CAST(TRUNC(CAST(CASE
         WHEN COALESCE(sku.color_num, FORMAT('%4d', 0)) = ''
         THEN '0'
         ELSE COALESCE(sku.color_num, FORMAT('%4d', 0))
         END AS FLOAT64)) AS SMALLINT))))) = LOWER(f.cc) AND LOWER(sku.channel_country) = LOWER('US')
 INNER JOIN channel_lkup AS ch ON LOWER(ch.banner) = LOWER(f.banner);


TRUNCATE TABLE {{params.gcp_project_id}}.{{params.environment_schema}}.lifecycle_phase_cc_banner{{params.env_suffix}};


INSERT INTO {{params.gcp_project_id}}.{{params.environment_schema}}.lifecycle_phase_cc_banner{{params.env_suffix}}
(SELECT cc,
  rms_style_num,
  color_num,
  banner,
  ownership_price_type_code,
  last_markdown_version,
  regular_price,
  ownership_price,
  pst_eff_begin_date,
  pst_eff_end_date,
  lifecycle_phase_number,
  lifecycle_phase_name,
  lifecycle_phase_pst_eff_begin_date,
  lifecycle_phase_pst_eff_end_date,
  banner_lifecycle_number,
  banner_lifecycle_pst_eff_begin_date,
  banner_lifecycle_pst_eff_end_date,
  ind_reg_price_this_banner_cycle,
  ind_first_mark_this_banner_cycle,
  ind_remark_this_banner_cycle,
  ind_racking_this_banner_cycle,
  ind_last_chance_this_banner_cycle,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
 FROM final_data_cc_banner AS f);


--collect statistics    column(cc, banner, pst_eff_begin_date)    ,column(cc, banner)    ,column(cc) on t2dl_das_in_season_management_reporting.lifecycle_phase_cc_banner 


TRUNCATE TABLE {{params.gcp_project_id}}.{{params.environment_schema}}.lifecycle_phase_sku_channel{{params.env_suffix}};


INSERT INTO {{params.gcp_project_id}}.{{params.environment_schema}}.lifecycle_phase_sku_channel{{params.env_suffix}}
(SELECT rms_sku_num,
  cc,
  rms_style_num,
  color_num,
  banner,
  channel_num,
  ownership_price_type_code,
  last_markdown_version,
  regular_price,
  ownership_price,
  pst_eff_begin_date,
  pst_eff_end_date,
  lifecycle_phase_number,
  lifecycle_phase_name,
  lifecycle_phase_pst_eff_begin_date,
  lifecycle_phase_pst_eff_end_date,
  banner_lifecycle_number,
  banner_lifecycle_pst_eff_begin_date,
  banner_lifecycle_pst_eff_end_date,
  ind_reg_price_this_banner_cycle,
  ind_first_mark_this_banner_cycle,
  ind_remark_this_banner_cycle,
  ind_racking_this_banner_cycle,
  ind_last_chance_this_banner_cycle,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
 FROM final_data_sku_channel AS f);


--collect statistics    column(rms_sku_num, channel_num, pst_eff_begin_date)    ,column(rms_sku_num, channel_num)    ,column(rms_sku_num, pst_eff_begin_date)    ,column(rms_sku_num, banner, pst_eff_begin_date)    ,column(rms_sku_num, banner)    ,column(cc, banner, pst_eff_begin_date)    ,column(cc, banner)    ,column(cc, pst_eff_begin_date) on t2dl_das_in_season_management_reporting.lifecycle_phase_sku_channel 