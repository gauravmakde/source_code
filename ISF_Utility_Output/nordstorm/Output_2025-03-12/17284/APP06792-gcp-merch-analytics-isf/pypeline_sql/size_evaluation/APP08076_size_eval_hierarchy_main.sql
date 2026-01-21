/*
Name: Size Curves Evaluation Actuals
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    {environment_schema} T2DL_DAS_SIZE
                {env_suffix} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 6/12/24

Creates: 
    - supp_size_hierarchy
*/

CREATE MULTISET VOLATILE TABLE sku_list AS (
SELECT
     sku_idnt
FROM t2dl_das_assortment_dim.assortment_hierarchy
WHERE div_idnt IN (310, 351, 345)
GROUP BY 1
) WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS
;
 
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
        FROM t2dl_das_assortment_dim.assortment_hierarchy h
        JOIN sku_list s
          ON h.sku_idnt = s.sku_idnt
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
 
-- -- DROP TABLE suppliers_clean;
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
 
COLLECT STATS  
     COLUMN(size_1_num)
    ,COLUMN(sku_idnt)
    ,COLUMN(supplier_idnt)
    ON t2dl_das_assortment_dim.assortment_hierarchy;
 
-- -- DROP TABLE supp_size;
CREATE MULTISET VOLATILE TABLE supp_size AS (
SELECT DISTINCT
     rms_sku_id AS sku_idnt
    ,supplier_size_1 AS size_1_idnt
    ,supplier_size_1_desc AS size_1_desc
    ,supplier_size_2 AS size_2_idnt
    ,supplier_size_2_desc AS size_2_desc
FROM prd_nap_usr_vws.item_supplier_size_dim h
JOIN sku_list s
  ON h.rms_sku_id = s.sku_idnt
) WITH DATA
PRIMARY INDEX (sku_idnt)
ON COMMIT PRESERVE ROWS
;
 
-- -- DROP TABLE products;
CREATE MULTISET VOLATILE TABLE products AS (
SELECT
     h.sku_idnt
    ,h.supplier_idnt AS supplier_idnt
    ,h.supplier_name AS supplier_name
    ,h.supp_part_num AS supp_part_num
    ,h.vpn AS vpn
    ,h.rms_style_num AS style_id
    ,h.div_idnt AS div_idnt
    ,h.div_desc AS div_desc
    ,h.grp_idnt AS grp_idnt
    ,h.grp_desc AS grp_desc
    ,h.dept_idnt AS dept_idnt
    ,h.dept_desc AS dept_desc
    ,h.class_idnt AS class_idnt
    ,h.class_desc AS class_desc
    ,h.sbclass_idnt AS sbclass_idnt
    ,h.sbclass_desc AS sbclass_desc
    ,h.npg_ind
    ,h.supp_size AS supp_size
    ,CASE WHEN COALESCE(ms.size_1_idnt, h.size_1_num) IN ('L/XL', 'M/L', 'P/S', 'S/M', 'XL/2X', 'XS/S', 'XP/P', 'PM/PL') THEN OREPLACE(COALESCE(ms.size_1_idnt, h.size_1_num), '/', '-')
          ELSE COALESCE(ms.size_1_idnt, h.size_1_num)
          END size_1_num
    ,COALESCE(ms.size_1_desc, h.size_1_desc) AS size_1_desc
    ,COALESCE(ms.size_2_idnt, h.size_2_num) AS size_2_num
    ,COALESCE(ms.size_2_desc, h.size_2_desc) AS size_2_desc
    ,h.color_num AS color_num
    ,h.color_desc AS color_desc
FROM supp_size ms
LEFT JOIN (
        SELECT
             h.sku_idnt
            ,supp.supplier_idnt
            ,supp.supplier_name
            ,h.npg_ind
            ,h.supp_part_num
            ,h.supp_part_num || ', ' || h.style_desc AS vpn
            ,h.rms_style_num
            ,h.div_idnt
            ,h.div_desc
            ,h.grp_idnt
            ,h.grp_desc
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
        JOIN sku_list s
          ON h.sku_idnt = s.sku_idnt
        JOIN suppliers_clean supp
          ON supp.original_supplier_idnt = h.supplier_idnt
        WHERE channel_country = 'US'
          AND  h.size_1_num not in ('NONE', 'O/S', 'AVG', ' ')
    ) h
  ON ms.sku_idnt = h.sku_idnt
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) WITH DATA
PRIMARY INDEX (sku_idnt)
ON COMMIT PRESERVE ROWS
;
 
COLLECT STATS
     PRIMARY INDEX (sku_idnt)
    ,COLUMN(div_idnt)
    ,COLUMN(supplier_idnt, style_id, size_1_num)
    ,COLUMN(supplier_idnt, style_id)
    ,COLUMN(supplier_idnt, supp_part_num, size_1_num)
    ,COLUMN(supplier_idnt, supp_part_num)
    ON products;
 
-- -- DROP TABLE skus;
CREATE MULTISET VOLATILE TABLE skus AS (
SELECT
     p.sku_idnt
    ,p.supplier_idnt
    ,p.supplier_name
    ,p.supp_part_num
    ,p.vpn
    ,p.style_id
    ,p.div_idnt
    ,p.div_desc
    ,p.grp_idnt
    ,p.grp_desc
    ,p.dept_idnt
    ,p.dept_desc
    ,p.class_idnt
    ,p.class_desc
    ,p.sbclass_idnt
    ,p.sbclass_desc
    ,p.npg_ind
    ,p.supp_size
    ,p.size_1_num
    ,p.size_1_desc
    ,p.size_2_num
    ,p.size_2_desc
    ,p.color_num
    ,p.color_desc
FROM products p
WHERE (supplier_idnt || '_' || supp_part_num IN (
                                SELECT DISTINCT
                                     supplier_idnt || '_' || supp_part_num
                                FROM products
                                GROUP BY 1
                                HAVING COUNT(DISTINCT size_1_num) > 1
                            )
  OR supplier_idnt || '_' || style_id IN (
                                SELECT DISTINCT
                                     supplier_idnt || '_' || style_id
                                FROM products
                                GROUP BY 1
                                HAVING COUNT(DISTINCT size_1_num) > 1
                            )
       )                    
) WITH DATA
PRIMARY INDEX (sku_idnt)
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
    ,TRIM(p.dept_idnt) || '~~' || TRIM(p.supplier_idnt) || '~~' || TRIM(p.class_idnt) AS groupid
    ,p.supplier_idnt
    ,p.supplier_name
    ,p.supp_part_num
    ,p.vpn
    ,p.style_id
    ,p.div_idnt
    ,p.div_desc
    ,p.grp_idnt
    ,p.grp_desc
    ,p.dept_idnt
    ,p.dept_desc
    ,p.class_idnt
    ,p.class_desc
    ,p.sbclass_idnt
    ,p.sbclass_desc
    ,p.npg_ind
    ,p.supp_size
    ,p.size_1_num AS size_1_id
    ,p.size_1_desc
    ,s1.size_1_rank
    ,s1.size_1_frame
    ,COALESCE(p.size_2_num, 'NA') AS size_2_id
    ,p.size_2_desc
    ,s2.size_2_rank
    ,p.color_num
    ,p.color_desc
FROM skus p
JOIN size_1_rank s1
  ON p.size_1_num = s1.size_1_num
LEFT JOIN size_2_rank s2
  ON p.size_2_num = s2.size_2_num
) WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS
;
 
COLLECT STATS 
     COLUMN(sku_idnt)
     ON sku_sizes;


CREATE MULTISET VOLATILE TABLE na_size_2_map AS (
    SELECT
         a.*
        ,'NA' AS size_2_id_old
        ,CASE WHEN groupid_r <= 0.1 AND groupid_m <= 0.1 THEN 'NA'
              WHEN groupid_r >= 0.2 THEN 'R'
              WHEN groupid_m >= 0.2 THEN 'M'
              END AS size_2_id_new
    FROM (
        SELECT
             dept_idnt
            ,COUNT(DISTINCT groupid) AS groupid_total
            ,1.0*COUNT(DISTINCT CASE WHEN size_2_id is null THEN groupid end)/COUNT(DISTINCT groupid) AS groupid_na
            ,1.0*COUNT(DISTINCT CASE WHEN size_2_id = 'R' THEN groupid end)/COUNT(DISTINCT groupid) AS groupid_r
            ,1.0*COUNT(DISTINCT CASE WHEN size_2_id = 'M' THEN groupid end)/COUNT(DISTINCT groupid) AS groupid_m
        FROM sku_sizes
        GROUP BY 1
    ) a
) WITH DATA
PRIMARY INDEX (dept_idnt, size_2_id_old)
ON COMMIT PRESERVE ROWS;
 
COLLECT STATS
     PRIMARY INDEX (dept_idnt,size_2_id_old)
    ,COLUMN(dept_idnt)
    ,COLUMN(size_2_id_old)
     ON na_size_2_map;
 
DELETE FROM {environment_schema}.supp_size_hierarchy ALL;
INSERT INTO {environment_schema}.supp_size_hierarchy
SELECT
     p.sku_idnt
    ,p.choice_id
    ,p.supplier_idnt
    ,p.supplier_name
    ,p.supp_part_num
    ,p.vpn
    ,p.style_id
    ,p.div_idnt
    ,p.div_desc
    ,p.grp_idnt
    ,p.grp_desc
    ,p.dept_idnt
    ,p.dept_desc
    ,p.class_idnt
    ,p.class_desc
    ,p.sbclass_idnt
    ,p.sbclass_desc
    ,p.npg_ind
    ,p.supp_size
    ,p.size_1_id
    ,p.size_1_desc
    ,p.size_1_rank
    ,CASE WHEN f.frames = 2 OR v.frames = 2 THEN 'Letters' ELSE p.size_1_frame END AS size_1_frame
    ,COALESCE(na.size_2_id_new, p.size_2_id) AS size_2_id
    ,p.size_2_desc
    ,p.size_2_rank
    ,CURRENT_TIMESTAMP AS update_timestamp
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
LEFT JOIN na_size_2_map na
  ON p.dept_idnt = na.dept_idnt
 AND p.size_2_id = na.size_2_id_old
;
 
COLLECT STATISTICS
     COLUMN(dept_idnt)     
    ,COLUMN(supplier_idnt, dept_idnt, class_idnt, size_1_id, size_2_id)
    ,COLUMN(sku_idnt)
    ,COLUMN(supplier_idnt)
    ,COLUMN(npg_ind)
    ,COLUMN(sku_idnt)
    ,COLUMN(supplier_idnt, dept_idnt, class_idnt, size_1_id, size_1_rank, size_1_frame, size_2_id) 
    ,COLUMN(supplier_idnt ,dept_idnt, class_idnt, npg_ind ,size_1_id ,size_1_frame ,size_2_id) 
    ,COLUMN(supplier_idnt ,dept_idnt, class_idnt, size_1_id, size_1_frame, size_2_id) 
    ON {environment_schema}.supp_size_hierarchy;
