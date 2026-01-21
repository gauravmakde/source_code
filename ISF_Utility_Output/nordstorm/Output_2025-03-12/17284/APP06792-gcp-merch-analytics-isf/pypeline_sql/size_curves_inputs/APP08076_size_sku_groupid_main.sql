/*
Name: Size Curves SKU Groupid
APPID-Name: APP08076 Data Driven Size Curves
Purpose: Creates a table with product hierarchy, size and style-color information at the sku level.
    - size_sku_groupid
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_monthly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:6/1/2023
*/

-- begin

-- DROP TABLE suppliers;
CREATE MULTISET VOLATILE TABLE suppliers AS (
SELECT DISTINCT 
     supplier_idnt
    ,supplier_name
    ,can_supp
FROM (
        SELECT DISTINCT
             supplier_idnt AS supplier_idnt
            ,vendor_name
            ,CASE WHEN RIGHT(vendor_name, 3) IN ('-CA', '-VI', 'OP', '-NI') THEN LEFT(vendor_name, char_length(vendor_name)-3) 
                  ELSE vendor_name END AS supp_name
            ,CASE WHEN RIGHT(supp_name, 3) IN ('-CA', '-VI', 'OP', '-NI') THEN LEFT(supp_name, char_length(supp_name)-3) 
                  ELSE supp_name END AS supplier_name
            ,CASE WHEN vendor_name LIKE '%-CA%' 
                    OR channel_country = 'CA'THEN 1 
                  ELSE 0 END AS can_supp
        FROM t2dl_das_assortment_dim.assortment_hierarchy
    ) sup
) WITH DATA
PRIMARY INDEX(supplier_name)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX(supplier_name)
    ,COLUMN(supplier_name)
    ,COLUMN(can_supp)
    ON suppliers;

-- DROP TABLE suppliers_clean;
CREATE MULTISET VOLATILE TABLE suppliers_clean AS (
SELECT DISTINCT
     supplier_idnt AS original_supplier_idnt
    ,s.supplier_name
    ,COALESCE(us_supplier, supplier_idnt) AS supplier_idnt
FROM suppliers s
JOIN (
        SELECT
             supplier_idnt AS us_supplier
            ,supplier_name
            ,ROW_NUMBER() OVER (PARTITION BY supplier_name ORDER BY can_supp) supp_rank
        FROM suppliers
    ) us
  ON s.supplier_name = us.supplier_name
WHERE supp_rank = 1
) WITH DATA
PRIMARY INDEX(original_supplier_idnt)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX(original_supplier_idnt)
    ON suppliers_clean;

-- DROP TABLE units_sold;
CREATE MULTISET VOLATILE TABLE units_sold AS (
SELECT 
     sku_idnt
    ,CASE WHEN business_unit_desc IN ('FULL LINE', 'N.COM','FULL LINE CANADA', 'N.CA') THEN 'NORDSTROM'
          ELSE 'NORDSTROM_RACK' 
          END AS banner
    ,SUM(sales_units) AS sales_units
    ,MAX(cal.month_idnt) AS latest_month
    ,MAX(cal.fiscal_halfyear_num) AS latest_half
    ,MAX(cal.fiscal_year_num) AS latest_year
FROM {environment_schema}.size_sku_loc_weekly sls
JOIN prd_nap_usr_vws.price_store_dim_vw loc
  ON sls.loc_idnt = loc.store_num
JOIN (
        SELECT DISTINCT
             week_idnt
            ,month_idnt
            ,fiscal_halfyear_num
            ,fiscal_year_num
        FROM prd_nap_usr_vws.day_cal_454_dim
    ) cal
  ON sls.week_idnt = cal.week_idnt
WHERE sales_units > 0 
GROUP BY 1,2
) WITH DATA
PRIMARY INDEX (sku_idnt, banner) 
ON COMMIT PRESERVE ROWS 
;

COLLECT STATS 
     PRIMARY INDEX(sku_idnt, banner)
    ON units_sold;

COLLECT STATS   
     COLUMN(size_1_num) 
    ,COLUMN(sku_idnt)
    ,COLUMN(supplier_idnt) 
    ON t2dl_das_assortment_dim.assortment_hierarchy;

-- DROP TABLE supp_size;
CREATE MULTISET VOLATILE TABLE supp_size AS (
SELECT DISTINCT 
     rms_sku_id AS sku_idnt
    ,supplier_size_1 AS size_1_idnt
    ,supplier_size_1_desc AS size_1_desc
    ,supplier_size_2 AS size_2_idnt
    ,supplier_size_2_desc AS size_2_desc
FROM prd_nap_usr_vws.item_supplier_size_dim h
) WITH DATA
PRIMARY INDEX (sku_idnt) 
ON COMMIT PRESERVE ROWS 
;

-- DROP TABLE products;
CREATE MULTISET VOLATILE TABLE products AS (
SELECT
     us.sku_idnt
    ,us.banner
    ,h.supplier_idnt AS supplier_idnt
    ,h.supplier_name AS supplier_name
    ,h.supp_part_num AS supp_part_num
    ,h.vpn AS vpn
    ,h.rms_style_num AS style_id
    ,h.div_idnt AS div_idnt
    ,h.div_desc AS div_desc
    ,h.dept_idnt AS dept_idnt
    ,h.dept_desc AS dept_desc
    ,h.class_idnt AS class_idnt
    ,h.class_desc AS class_desc
    ,h.sbclass_idnt AS sbclass_idnt
    ,h.sbclass_desc AS sbclass_desc
    ,h.supp_size AS supp_size
    ,CASE WHEN COALESCE(ms.size_1_idnt, h.size_1_num) IN ('L/XL', 'M/L', 'P/S', 'S/M', 'XL/2X', 'XS/S', 'XP/P', 'PM/PL') THEN OREPLACE(COALESCE(ms.size_1_idnt, h.size_1_num), '/', '-') 
          ELSE COALESCE(ms.size_1_idnt, h.size_1_num) 
          END size_1_num
    ,COALESCE(ms.size_1_desc, h.size_1_desc) AS size_1_desc
    ,COALESCE(ms.size_2_idnt, h.size_2_num) AS size_2_num
    ,COALESCE(ms.size_2_desc, h.size_2_desc) AS size_2_desc
    ,h.color_num AS color_num
    ,h.color_desc AS color_desc
    ,SUM(sales_units) AS sales_units
    ,MAX(latest_month) AS latest_month
    ,MAX(latest_half) AS latest_half
    ,MAX(latest_year) AS latest_year
FROM units_sold us
LEFT JOIN supp_size ms 
  ON us.sku_idnt = ms.sku_idnt 
LEFT JOIN (
        SELECT
             h.sku_idnt
            ,supp.supplier_idnt
            ,supp.supplier_name
            ,h.supp_part_num
            ,h.supp_part_num || ', ' || h.style_desc AS vpn
            ,h.rms_style_num
            ,h.div_idnt
            ,h.div_desc
            ,h.dept_idnt
            ,h.dept_desc
            ,h.class_idnt
            ,h.class_desc
            ,h.sbclass_idnt
            ,h.sbclass_desc
            ,h.supp_size
            ,h.size_1_num
            ,h.size_1_desc
            ,h.size_2_num
            ,h.size_2_desc
            ,h.color_num
            ,h.color_desc
        FROM t2dl_das_assortment_dim.assortment_hierarchy h
        JOIN suppliers_clean supp
          ON supp.original_supplier_idnt = h.supplier_idnt
        WHERE channel_country = 'US'
          AND  h.size_1_num not in ('NONE', 'O/S', 'AVG', ' ')
    ) h
  ON us.sku_idnt = h.sku_idnt 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
) WITH DATA
PRIMARY INDEX (sku_idnt, banner)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
     PRIMARY INDEX (sku_idnt, banner)
    ,COLUMN(div_idnt)
    ,COLUMN(banner)
    ,COLUMN(banner, supplier_idnt, style_id, size_1_num)
    ,COLUMN(supplier_idnt, style_id, banner)
    ,COLUMN(banner, supplier_idnt, supp_part_num, size_1_num)
    ,COLUMN(supplier_idnt, supp_part_num, banner)
    ON products;

-- DROP TABLE skus;
CREATE MULTISET VOLATILE TABLE skus AS (
SELECT
     p.sku_idnt
    ,p.banner
    ,p.supplier_idnt
    ,p.supplier_name
    ,p.supp_part_num
    ,p.vpn
    ,p.style_id
    ,p.div_idnt
    ,p.div_desc
    ,p.dept_idnt
    ,p.dept_desc
    ,p.class_idnt
    ,p.class_desc
    ,p.sbclass_idnt
    ,p.sbclass_desc
    ,p.supp_size
    ,p.size_1_num
    ,p.size_1_desc
    ,p.size_2_num
    ,p.size_2_desc
    ,p.color_num
    ,p.color_desc
    ,p.sales_units
    ,p.latest_month
    ,p.latest_half
    ,p.latest_year
FROM products p
WHERE (supplier_idnt || '_' || supp_part_num || '_' || banner IN (
                                SELECT DISTINCT 
                                     supplier_idnt || '_' || supp_part_num || '_' || banner
                                FROM products
                                GROUP BY 1
                                HAVING COUNT(DISTINCT size_1_num) > 1
                            )
  OR supplier_idnt || '_' || style_id || '_' || banner IN (
                                SELECT DISTINCT 
                                     supplier_idnt || '_' || style_id || '_' || banner 
                                FROM products
                                GROUP BY 1
                                HAVING COUNT(DISTINCT size_1_num) > 1
                            ) 
       )                     
  AND sku_idnt || '_' || banner NOT IN (
                                SELECT DISTINCT 
                                     sku_idnt || '_' || banner
                                FROM products
                                WHERE div_idnt = 700 
                                  AND banner = 'NORDSTROM_RACK'
                            ) 
) WITH DATA
PRIMARY INDEX (sku_idnt, banner) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
     COLUMN(size_1_num)
    ,column(size_2_num)
     ON skus;

CREATE MULTISET VOLATILE TABLE size_1_rank AS (
WITH sizes AS (
    SELECT DISTINCT
         size_1_num
    FROM skus
),
splits AS (
    SELECT
         size_1_num
        ,CASE WHEN split_position > 0 THEN substr(size_1_num, 1, split_position-1)
              ELSE size_1_num
              END AS split_1
        ,CASE WHEN split_position > 0 THEN substr(size_1_num, split_position + 1, length(size_1_num) - split_position) 
              ELSE NULL 
              END AS split_2
        ,split_position
    FROM (
            SELECT 
                 size_1_num
                ,regexp_instr(CAST(size_1_num as CHAR(10)), '[/-]') AS split_position
            FROM sizes
        ) pstn
),
num_let as (
    SELECT
         size_1_num
        ,split_1
        ,split_2
        ,split_position
        ,CASE WHEN CAST(regexp_substr(split_1, '(\d+)\.*(\d+)*') AS DECIMAL(10,2)) > 1000 THEN substr(regexp_substr(split_1, '(\d+)\.*(\d+)*'),1,2)
              WHEN CAST(regexp_substr(split_1, '(\d+)\.*(\d+)*') AS DECIMAL(10,2)) > 100 THEN substr(regexp_substr(split_1, '(\d+)\.*(\d+)*'),1,1)
              ELSE regexp_substr(split_1, '(\d+)\.*(\d+)*')
              END AS numbers_1
        ,CASE WHEN CAST(regexp_substr(split_1, '(\d+)\.*(\d+)*') AS DECIMAL(10,2)) > 1000 THEN substr(regexp_substr(split_1, '(\d+)\.*(\d+)*'),3,2)
              WHEN CAST(regexp_substr(split_1, '(\d+)\.*(\d+)*') AS DECIMAL(10,2)) > 100 THEN substr(regexp_substr(split_1, '(\d+)\.*(\d+)*'),2,2)
              WHEN CAST(regexp_substr(split_1, '(\d+)\.*(\d+)*') AS DECIMAL(10,2)) - CAST(regexp_substr(split_2, '(\d+)\.*(\d+)*') AS DECIMAL(10,2))>10 THEN NULL
              ELSE regexp_substr(split_2, '(\d+)\.*(\d+)*') 
              END AS numbers_2
        ,regexp_substr(split_1, '[A-Za-z]+') AS letters_1
        ,CASE WHEN regexp_substr(split_1, '^[1|2|3|4|5|6]*[X|x]*[S|M|L|s|m|l]$') IS NOT NULL OR regexp_substr(split_1, '^[0|1|2|3|4|5|6]*[X|x]$') IS NOT NULL THEN split_1 END AS sml_1
        ,CASE WHEN regexp_substr(split_2, '^[1|2|3|4|5|6]*[X|x]*[S|M|L|s|m|l]$') IS NOT NULL OR regexp_substr(split_2, '^[0|1|2|3|4|5|6]*[X|x]$') IS NOT NULL THEN split_2 END AS sml_2
    FROM splits
),
frames AS (
    SELECT
         size_1_num
        ,numbers_1
        ,numbers_2
        ,letters_1
        ,split_1
        ,split_2
        ,split_position
        ,sml_1
        ,sml_2
        ,CASE WHEN split_1 IN ( 'TS','L1','L2') OR split_1 = sml_1 THEN 'Letters'
              WHEN split_1 IN ('P2','PL2')
                OR split_1 LIKE '%T' OR split_1 LIKE 'T%' 
                OR split_1 LIKE '%D' OR split_1 LIKE 'D%'
                OR split_1 LIKE '%XB' OR split_1 LIKE 'XB%' then 'Letters'
              WHEN numbers_1 IS NOT NULL THEN 'Numbers'
              WHEN letters_1 IS NOT NULL THEN 'Letters'
              END AS size_1_frame
        ,CASE WHEN split_1 IN ( 'TS','L1','L2') THEN COALESCE(sml_2, numbers_2)
              WHEN split_1 = sml_1 THEN sml_1
              WHEN split_1 IN ('P2','PL2')
                OR split_1 LIKE '%T' OR split_1 LIKE 'T%'
                OR split_1 LIKE '%D' OR split_1 LIKE 'D%'
                OR split_1 LIKE '%XB' OR split_1 LIKE 'XB%' THEN split_1
              ELSE coalesce(numbers_1, letters_1)
              END AS final_1
    FROM num_let
),
size_rank AS (
    SELECT
         size_1_num
        ,CASE WHEN sml_rank IS NOT NULL AND x_num = 0 THEN (sml_rank * 1) - 100 
              WHEN sml_rank IS NOT NULL THEN (sml_rank * (x_num * 10)) - 100
              WHEN number_rank IS NOT NULL THEN number_rank
              WHEN letter_rank IS NOT NULL THEN -201 + letter_rank
              END AS size_1_rank
    FROM (
            SELECT
                 size_1_num
                ,CASE WHEN numbers_1='000' THEN -2.0
                      WHEN numbers_1='00' THEN -1.0
                      WHEN numbers_2 IS NOT NULL AND numbers_1 IS NOT NULL THEN (CAST(numbers_1 AS DECIMAL(10,2)) + CAST(numbers_2 AS DECIMAL(10,2)))/2
                      WHEN numbers_2 IS NOT NULL THEN CAST(numbers_2 as decimal(10,2))
                      ELSE CAST(numbers_1 AS DECIMAL(10,2))
                      END AS number_rank
                ,CASE WHEN regexp_substr(final_1, '[A|a]') IS NOT NULL THEN 1
                      WHEN regexp_substr(final_1, '[B|b]') IS NOT NULL THEN 2
                      WHEN regexp_substr(final_1, '[C|c]') IS NOT NULL THEN 3
                      WHEN regexp_substr(final_1, '[D|d]') IS NOT NULL THEN 4
                      WHEN regexp_substr(final_1, '[E|e]') IS NOT NULL THEN 5
                      WHEN regexp_substr(final_1, '[F|f]') IS NOT NULL THEN 6
                      WHEN regexp_substr(final_1, '[G|g]') IS NOT NULL THEN 7
                      WHEN regexp_substr(final_1, '[H|h]') IS NOT NULL THEN 8
                      WHEN regexp_substr(final_1, '[Q|q]') IS NOT NULL THEN 9
                      END AS letter_rank
                ,CASE WHEN regexp_substr(final_1, '[T|t]') IS NOT NULL THEN -3
                      WHEN regexp_substr(final_1, '[P|p]') IS NOT NULL THEN -2
                      WHEN regexp_substr(final_1, '[S|s]') IS NOT NULL THEN -1
                      WHEN regexp_substr(final_1, '[M|m]') IS NOT NULL THEN 0
                      WHEN regexp_substr(final_1, '[L|l]') IS NOT NULL THEN 1
                      WHEN regexp_substr(final_1, '[X|x]') IS NOT NULL THEN 2
                      END AS sml_rank
                ,CASE WHEN regexp_instr(final_1, 'X') > 0 THEN coalesce(CAST(regexp_substr(final_1, '\d+') AS INT), 1) 
                      ELSE 0 END AS x_num
            FROM frames
            GROUP BY 1,2,3,4,5
        ) rnk_process 
)
SELECT
     f.size_1_num
    ,size_1_frame
    ,size_1_rank
from frames f
left join size_rank r
on f.size_1_num = r.size_1_num
) WITH DATA
PRIMARY INDEX(size_1_num)
ON COMMIT PRESERVE ROWS
;
     
COLLECT STATS 
     COLUMN(size_1_num)
     ON size_1_rank;

CREATE MULTISET VOLATILE TABLE size_2_rank AS (
SELECT 
     size_2_num
    ,COALESCE(nmw_rank, 100*(letter_rank_pre + (0.1 * number_rank)), number_rank) AS size_2_rank
FROM (
        SELECT DISTINCT 
             size_2_num
            ,CASE WHEN size_2_num IN ('P', 'PP', 'PETITE') THEN -4
                  WHEN size_2_num IN ('SS', '2S', 'XS') THEN -3
                  WHEN size_2_num IN ('S') THEN -2
                  WHEN size_2_num IN ('N') THEN -1
                  WHEN size_2_num IN ('M', 'R', 'MO', 'RR', 'MW', 'OZ', 'W PET') THEN 0
                  WHEN size_2_num IN ('W', 'Plus') THEN 1
                  WHEN size_2_num IN ('WW', '2W', 'X', 'XW') THEN 2
                  WHEN size_2_num IN ('T', 'TLL', 'Y', 'L', 'LONG', 'LT', 'RT', 'UNH', 'NAH', 'BIG') THEN 3
                  WHEN size_2_num IN ('XLR', 'XT', 'XL', 'XLXS', 'XBIG') THEN 4
                  WHEN size_2_num IN ('XLT', 'XLL') THEN 5
                  END AS nmw_rank
            ,CASE WHEN regexp_substr(size_2_num, '[A|a]') IS NOT NULL THEN 1
                  WHEN regexp_substr(size_2_num, '[B|b]') IS NOT NULL THEN 2
                  WHEN regexp_substr(size_2_num, '[C|c]') IS NOT NULL THEN 3
                  WHEN regexp_substr(size_2_num, '[D|d]') IS NOT NULL THEN 4
                  WHEN regexp_substr(size_2_num, '[E|e]') IS NOT NULL THEN 5
                  WHEN regexp_substr(size_2_num, '[F|f]') IS NOT NULL THEN 6
                  WHEN regexp_substr(size_2_num, '[G|g]') IS NOT NULL THEN 7
                  WHEN regexp_substr(size_2_num, '[H|h]') IS NOT NULL THEN 8
                  WHEN regexp_substr(size_2_num, '[I|i]') IS NOT NULL THEN 9
                  WHEN regexp_substr(size_2_num, '[J|j]') IS NOT NULL THEN 10
                  WHEN regexp_substr(size_2_num, '[K|k]') IS NOT NULL THEN 11
                  END As letter_rank_pre
            ,COALESCE(CAST(regexp_substr(size_2_num, '(\d+)') AS INT), 0) AS number_rank
        FROM skus
    ) rnk2_process
)
WITH DATA
UNIQUE PRIMARY INDEX (size_2_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
     COLUMN(size_2_num)
     ON size_2_rank;

-- DROP TABLE sku_sizes;
CREATE MULTISET VOLATILE TABLE sku_sizes AS (
SELECT
     p.sku_idnt
    ,p.style_id || '~~' || color_num AS choice_id
    ,p.banner
    ,p.supplier_idnt
    ,p.supplier_name
    ,p.supp_part_num
    ,p.vpn
    ,p.style_id
    ,p.div_idnt
    ,p.div_desc
    ,p.dept_idnt
    ,p.dept_desc
    ,p.class_idnt
    ,p.class_desc
    ,p.sbclass_idnt
    ,p.sbclass_desc
    ,p.supp_size
    ,p.size_1_num
    ,p.size_1_desc
    ,s1.size_1_rank
    ,s1.size_1_frame
    ,p.size_2_num
    ,p.size_2_desc
    ,s2.size_2_rank
    ,p.color_num
    ,p.color_desc
    ,p.sales_units
    ,p.latest_month
    ,p.latest_half
    ,p.latest_year
FROM skus p
JOIN size_1_rank s1
  ON p.size_1_num = s1.size_1_num
LEFT JOIN size_2_rank s2
  ON p.size_2_num = s2.size_2_num
) WITH DATA
PRIMARY INDEX(sku_idnt, banner)
ON COMMIT PRESERVE ROWS
;

-- DROP TABLE sku_sizes_clean
CREATE MULTISET VOLATILE TABLE sku_sizes_clean AS (
SELECT
     p.sku_idnt
    ,p.choice_id
    ,p.banner
    ,p.supplier_idnt
    ,p.supplier_name
    ,p.supp_part_num
    ,p.vpn
    ,p.style_id
    ,p.div_idnt
    ,p.div_desc
    ,p.dept_idnt
    ,p.dept_desc
    ,p.class_idnt
    ,p.class_desc
    ,p.sbclass_idnt
    ,p.sbclass_desc
    ,p.supp_size
    ,p.size_1_num
    ,p.size_1_desc
    ,p.size_1_rank
    ,CASE WHEN f.frames = 2 OR v.frames = 2 THEN 'Letters' ELSE p.size_1_frame END AS size_1_frame
    ,p.size_2_num
    ,p.size_2_desc
    ,p.size_2_rank
    ,p.color_num
    ,p.color_desc
    ,p.sales_units
    ,p.latest_month
    ,p.latest_half
    ,p.latest_year
FROM sku_sizes p
JOIN (
        SELECT
              choice_id
             ,COUNT(DISTINCT size_1_frame) frames
        FROM sku_sizes
        GROUP BY 1
    ) f
  ON p.choice_id = f.choice_id
JOIN (
        SELECT
              vpn
             ,supplier_idnt
             ,COUNT(DISTINCT size_1_frame) frames
        FROM sku_sizes
        GROUP BY 1,2
    ) v
  ON p.vpn = v.vpn
 AND p.supplier_idnt = v.supplier_idnt
) WITH DATA
PRIMARY INDEX(sku_idnt, banner)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE jda_subclass AS (
WITH supp_class AS (
    SELECT
         banner
        ,dept_idnt
        ,class_idnt
        ,supplier_idnt
        ,sbclass_idnt AS jda_subclass_sc
        ,vpn
    FROM sku_sizes_clean
    GROUP BY 1,2,3,4,5,6
    QUALIFY ROW_NUMBER() OVER (PARTITION BY banner, dept_idnt, supplier_idnt, class_idnt ORDER BY SUM(sales_units) DESC) = 1
),
clss AS (
    SELECT
         banner
        ,dept_idnt
        ,class_idnt
        ,sbclass_idnt AS jda_subclass_c
        ,vpn
    FROM sku_sizes_clean
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY banner, dept_idnt, class_idnt ORDER BY SUM(sales_units) DESC) = 1
)
SELECT
     sc.banner
    ,sc.dept_idnt
    ,sc.supplier_idnt
    ,sc.class_idnt
    ,sc.jda_subclass_sc
    ,c.jda_subclass_c
FROM supp_class sc
JOIN clss c
  ON sc.banner = c.banner
 AND sc.dept_idnt = c.dept_idnt
 AND sc.class_idnt = c.class_idnt
) WITH DATA
PRIMARY INDEX (banner, dept_idnt, supplier_idnt, class_idnt)
ON COMMIT PRESERVE ROWS;


-- DROP TABLE dept_class_filter;
CREATE MULTISET VOLATILE TABLE dept_class_filter AS (
SELECT
     banner
    ,dept_idnt
    ,class_idnt
    ,MAX(latest_half) AS latest_hlaf
FROM skus
GROUP BY 1,2,3
HAVING MAX(latest_half) >= (SELECT DISTINCT fiscal_halfyear_num - 20 FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
) 
with data primary index (banner, dept_idnt, class_idnt)
on commit preserve rows;

COLLECT STATS
    PRIMARY INDEX (banner, dept_idnt, class_idnt)
    ON dept_class_filter;

CREATE MULTISET VOLATILE TABLE supp_class_units AS (
SELECT DISTINCT
     banner
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,SUM(sales_units) OVER (PARTITION BY banner, dept_idnt, class_idnt, supplier_idnt) supplier_class_units_sold
    ,SUM(sales_units) OVER (PARTITION BY banner, dept_idnt) dept_units_sold
FROM sku_sizes_clean
) WITH DATA 
PRIMARY INDEX (banner, dept_idnt, class_idnt, supplier_idnt)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX(banner, dept_idnt, class_idnt, supplier_idnt)
     ON supp_class_units;

-- DROP TABLE skus_groupid;
CREATE MULTISET VOLATILE TABLE skus_groupid AS (
SELECT
     p.sku_idnt AS sku_idnt
    ,p.choice_id
    ,p.banner
    ,CASE WHEN p.banner = 'NORDSTROM' THEN 'fp' ELSE 'op' END AS price_lvl
    ,p.supplier_idnt
    ,p.supplier_name
    ,p.supp_part_num
    ,p.vpn
    ,p.style_id
    ,p.div_idnt
    ,p.div_desc
    ,p.dept_idnt
    ,p.dept_desc
    ,p.class_idnt
    ,p.class_desc
    ,p.sbclass_idnt
    ,p.sbclass_desc
    ,p.supp_size
    ,p.size_1_num
    ,p.size_1_desc
    ,p.size_1_rank
    ,p.size_1_frame
    ,p.size_2_num
    ,p.size_2_desc
    ,p.size_2_rank
    ,p.color_num
    ,p.color_desc
    ,p.sales_units
    ,p.latest_month
    ,p.latest_half
    ,p.latest_year
    ,TRIM(p.dept_idnt) || '~~' || TRIM(p.supplier_idnt) || '~~' || TRIM(p.class_idnt) AS groupid
    ,TRIM(p.dept_idnt) || '~~' || TRIM(p.supplier_idnt) || '~~' || TRIM(p.class_idnt) || '~~' || TRIM(size_1_num) AS groupid_size
    ,sc.supplier_class_units_sold
    ,sc.dept_units_sold
    ,COALESCE(jda.jda_subclass_sc, 0) AS jda_subclass_sc
    ,COALESCE(jda.jda_subclass_c, 0) AS jda_subclass_c
FROM sku_sizes_clean p
JOIN supp_class_units sc
  ON sc.banner = p.banner
 AND sc.dept_idnt = p.dept_idnt 
 AND sc.class_idnt = p.class_idnt
 AND sc.supplier_idnt = p.supplier_idnt 
JOIN dept_class_filter dc
  ON p.dept_idnt = dc.dept_idnt
 AND p.class_idnt = dc.class_idnt
 AND p.banner = dc.banner
LEFT JOIN jda_subclass jda
  ON p.banner = jda.banner
 AND p.dept_idnt = jda.dept_idnt
 AND p.supplier_idnt = jda.supplier_idnt
 AND p.class_idnt = jda.class_idnt
) WITH DATA
PRIMARY INDEX(sku_idnt, banner)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE designer_base AS (
SELECT 
      price_lvl
     ,s.dept_idnt
     ,s.dept_desc
     ,s.class_idnt
     ,s.class_desc
     ,s.supplier_idnt
     ,s.supplier_name
     ,s.size_1_frame
     ,s.size_1_num
     ,CASE WHEN s.size_1_desc LIKE '%/%' THEN (
            CASE WHEN s.size_1_desc LIKE '%IT%' THEN 'IT'
                 WHEN s.size_1_desc LIKE '%FR%' THEN 'FR'
                 WHEN s.size_1_desc LIKE '%UK%' THEN 'UK'
                 WHEN s.size_1_desc LIKE '%AU%' THEN 'AU'
                 WHEN s.size_1_desc LIKE '%EU%' THEN 'EU'
                 ELSE 'US'
            END)
          ELSE 'US' END AS size_country
    ,CASE WHEN s.size_1_desc LIKE '%/%' THEN (
            CASE WHEN s.size_1_desc LIKE '%IT%' THEN 1
                 WHEN s.size_1_desc LIKE '%FR%' THEN 2
                 WHEN s.size_1_desc LIKE '%UK%' THEN 3
                 WHEN s.size_1_desc LIKE '%AU%' THEN 4
                 WHEN s.size_1_desc LIKE '%EU%' THEN 5
                 ELSE 6
            END)
          ELSE 6 END AS size_country_code
    ,TRIM(s.dept_idnt) || '0' || TRIM(s.class_idnt) || '0' || TRIM(size_country_code) || COALESCE(TRIM(buy_number), '0') AS designer_idnt
    ,s.dept_desc || '-' || s.class_desc || '-' || size_country || '-' || COALESCE(buy_planner, 'NONE') AS designer_desc
FROM skus_groupid s
LEFT JOIN (
          SELECT  
             s.dept_num
            ,banner
            ,supplier_num
            ,buy_planner
            ,dense_rank() OVER (PARTITION BY s.dept_num ORDER BY buy_planner) AS buy_number
        FROM prd_nap_usr_vws.supp_dept_map_dim s
        JOIN prd_nap_usr_vws.department_dim d
          ON s.dept_num = d.dept_num
        WHERE d.division_num = 345
          AND buy_planner IS NOT NULL
  ) b
  ON s.price_lvl = b.banner
 AND s.dept_idnt = b.dept_num
 AND s.supplier_idnt = b.supplier_num
WHERE s.latest_year >= YEAR(current_date) - 1
  AND s.supplier_class_units_sold < 200
  AND s.div_idnt = 345
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_idnt, dept_idnt, class_idnt, size_1_frame ORDER BY SUM(sales_units) DESC) = 1 
) WITH DATA 
UNIQUE PRIMARY INDEX (price_lvl, supplier_idnt, dept_idnt, class_idnt, size_1_frame, designer_idnt)
ON COMMIT PRESERVE ROWS;

DELETE FROM {environment_schema}.designer_map{env_suffix} ALL;
INSERT INTO {environment_schema}.designer_map{env_suffix}
SELECT
     price_lvl
    ,d.dept_idnt
    ,d.dept_desc
    ,d.class_idnt
    ,d.class_desc
    ,d.supplier_idnt
    ,d.supplier_name
    ,d.size_1_frame
    ,d.size_country
    ,d.size_country_code
    ,d.designer_idnt
    ,d.designer_desc
    ,jda.designer_jda_subclass_sc
    ,TRIM(d.dept_idnt) || '~~' || TRIM(d.supplier_idnt) || '~~' || TRIM(d.class_idnt) AS groupid
    ,TRIM(d.dept_idnt) || '~~' || TRIM(d.designer_idnt) || '~~' || TRIM(d.class_idnt) AS designer_groupid
    ,CURRENT_TIMESTAMP AS rcd_update_timestamp
FROM designer_base d
JOIN (
        SELECT  
             designer_idnt
            ,jda_subclass_sc AS designer_jda_subclass_sc
        FROM skus_groupid c
        JOIN designer_base d
          ON c.dept_idnt = d.dept_idnt
         AND c.class_idnt = d.class_idnt
         AND c.size_1_frame = d.size_1_frame
         AND c.supplier_name = d.supplier_name
        GROUP BY 1,2
        QUALIFY ROW_NUMBER() OVER (PARTITION BY designer_idnt ORDER BY SUM(sales_units) DESC) = 1 
    ) jda
  ON d.designer_idnt = jda.designer_idnt
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;

DELETE FROM {environment_schema}.size_sku_groupid{env_suffix} ALL;
INSERT INTO {environment_schema}.size_sku_groupid{env_suffix}
SELECT
     s.price_lvl
    ,s.dept_desc AS department_description
    ,s.dept_idnt AS department_id
    ,CASE WHEN div_idnt = 345 THEN 1 ELSE 0 END AS designer_flag
    ,CASE WHEN designer_groupid IS NOT NULL THEN 1 ELSE 0 END AS designer_mapped
    ,COALESCE(designer_desc, s.supplier_name) AS supplier_name
    ,COALESCE(designer_idnt, s.supplier_idnt) AS supplier_id
    ,s.class_desc AS class_description
    ,s.class_idnt AS class_id
    ,sbclass_desc AS subclass_description
    ,sbclass_idnt AS subclass_id
    ,style_id
    ,vpn
    ,choice_id
    ,sku_idnt AS sku_id
    ,supp_size
    ,size_1_num AS size_1_id
    ,size_1_rank
    ,s.size_1_frame
    ,size_1_desc AS size_1_description
    ,size_2_num AS size_2_id
    ,size_2_rank
    ,size_2_desc AS size_2_description
    ,sales_units AS units_sold
    ,latest_year
    ,COALESCE(designer_groupid, s.groupid) AS groupid
    ,COALESCE(designer_groupid, s.groupid) || '~~' || size_1_num AS groupid_size
    ,supplier_class_units_sold
    ,dept_units_sold
    ,COALESCE(designer_jda_subclass_sc, jda_subclass_sc) AS jda_subclass_sc
    ,jda_subclass_c
    ,CURRENT_TIMESTAMP AS rcd_update_timestamp
FROM skus_groupid s
LEFT JOIN {environment_schema}.designer_map{env_suffix} d
  ON s.groupid = d.groupid
 AND s.size_1_frame = d.size_1_frame
 AND s.price_lvl = d.price_lvl
;

-- end
