/*
Name: Size Curves Evaluation Actuals
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    t2dl_das_size T2DL_DAS_SIZE
                {env_suffix} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 6/12/24

Creates: 
    - supp_size_hierarchy
*/




CREATE TEMPORARY TABLE IF NOT EXISTS sku_list
AS
SELECT sku_idnt
FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy
WHERE div_idnt IN (310, 351, 345)
GROUP BY sku_idnt;





CREATE TEMPORARY TABLE IF NOT EXISTS suppliers
AS
SELECT DISTINCT supplier_idnt,
 supplier_name,
 can_supp
FROM (SELECT DISTINCT h.supplier_idnt,
   h.vendor_name,
    CASE
    WHEN LOWER(SUBSTR(h.vendor_name, 3 * -1)) IN (LOWER('-CA'), LOWER('-VI'), LOWER('OP'), LOWER('-NI'))
    THEN SUBSTR(h.vendor_name, 0, LENGTH(h.vendor_name) - 3)
    ELSE h.vendor_name
    END AS supp_name,
    CASE
    WHEN LOWER(SUBSTR(CASE
        WHEN LOWER(SUBSTR(h.vendor_name, 3 * -1)) IN (LOWER('-CA'), LOWER('-VI'), LOWER('OP'), LOWER('-NI'))
        THEN SUBSTR(h.vendor_name, 0, LENGTH(h.vendor_name) - 3)
        ELSE h.vendor_name
        END, 3 * -1)) IN (LOWER('-CA'), LOWER('-VI'), LOWER('OP'), LOWER('-NI'))
    THEN SUBSTR(CASE
      WHEN LOWER(SUBSTR(h.vendor_name, 3 * -1)) IN (LOWER('-CA'), LOWER('-VI'), LOWER('OP'), LOWER('-NI'))
      THEN SUBSTR(h.vendor_name, 0, LENGTH(h.vendor_name) - 3)
      ELSE h.vendor_name
      END, 0, LENGTH(CASE
        WHEN LOWER(SUBSTR(h.vendor_name, 3 * -1)) IN (LOWER('-CA'), LOWER('-VI'), LOWER('OP'), LOWER('-NI'))
        THEN SUBSTR(h.vendor_name, 0, LENGTH(h.vendor_name) - 3)
        ELSE h.vendor_name
        END) - 3)
    ELSE CASE
     WHEN LOWER(SUBSTR(h.vendor_name, 3 * -1)) IN (LOWER('-CA'), LOWER('-VI'), LOWER('OP'), LOWER('-NI'))
     THEN SUBSTR(h.vendor_name, 0, LENGTH(h.vendor_name) - 3)
     ELSE h.vendor_name
     END
    END AS supplier_name,
    CASE
    WHEN LOWER(h.vendor_name) LIKE LOWER('%-CA%') OR LOWER(h.channel_country) = LOWER('CA')
    THEN 1
    ELSE 0
    END AS can_supp
  FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h
   INNER JOIN sku_list AS s ON LOWER(h.sku_idnt) = LOWER(s.sku_idnt)) AS sup;


--COLLECT STATS      PRIMARY INDEX(supplier_name)     ,COLUMN(supplier_name)     ,COLUMN(can_supp)     ON suppliers


-- -- DROP TABLE suppliers_clean;


CREATE TEMPORARY TABLE IF NOT EXISTS suppliers_clean
AS
SELECT DISTINCT s.supplier_idnt AS original_supplier_idnt,
 s.supplier_name,
 COALESCE(us.us_supplier, s.supplier_idnt) AS supplier_idnt
FROM suppliers AS s
 INNER JOIN (SELECT supplier_idnt AS us_supplier,
   supplier_name,
   ROW_NUMBER() OVER (PARTITION BY supplier_name ORDER BY can_supp) AS supp_rank
  FROM suppliers) AS us ON LOWER(s.supplier_name) = LOWER(us.supplier_name)
WHERE us.supp_rank = 1;


--COLLECT STATS      PRIMARY INDEX(original_supplier_idnt)     ON suppliers_clean


--COLLECT STATS        COLUMN(size_1_num)     ,COLUMN(sku_idnt)     ,COLUMN(supplier_idnt)     ON t2dl_das_assortment_dim.assortment_hierarchy


-- -- DROP TABLE supp_size;


CREATE TEMPORARY TABLE IF NOT EXISTS supp_size
AS
SELECT DISTINCT h.rms_sku_id AS sku_idnt,
 h.supplier_size_1 AS size_1_idnt,
 h.supplier_size_1_desc AS size_1_desc,
 h.supplier_size_2 AS size_2_idnt,
 h.supplier_size_2_desc AS size_2_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.item_supplier_size_dim AS h
 INNER JOIN sku_list AS s ON LOWER(h.rms_sku_id) = LOWER(s.sku_idnt);


-- -- DROP TABLE products;


CREATE TEMPORARY TABLE IF NOT EXISTS products
AS
SELECT h.sku_idnt,
 h.supplier_idnt,
 h.supplier_name,
 h.supp_part_num,
 h.vpn,
 h.rms_style_num AS style_id,
 h.div_idnt,
 h.div_desc,
 h.grp_idnt,
 h.grp_desc,
 h.dept_idnt,
 h.dept_desc,
 h.class_idnt,
 h.class_desc,
 h.sbclass_idnt,
 h.sbclass_desc,
 h.npg_ind,
 h.supp_size,
  CASE
  WHEN LOWER(COALESCE(ms.size_1_idnt, h.size_1_num)) IN (LOWER('L/XL'), LOWER('M/L'), LOWER('P/S'), LOWER('S/M'), LOWER('XL/2X'
     ), LOWER('XS/S'), LOWER('XP/P'), LOWER('PM/PL'))
  THEN REPLACE(COALESCE(ms.size_1_idnt, h.size_1_num), '/', '-')
  ELSE COALESCE(ms.size_1_idnt, h.size_1_num)
  END AS size_1_num,
 COALESCE(ms.size_1_desc, h.size_1_desc) AS size_1_desc,
 COALESCE(ms.size_2_idnt, h.size_2_num) AS size_2_num,
 COALESCE(ms.size_2_desc, h.size_2_desc) AS size_2_desc,
 h.color_num,
 h.color_desc
FROM supp_size AS ms
 LEFT JOIN (SELECT h.sku_idnt,
   supp.supplier_idnt,
   supp.supplier_name,
   h.npg_ind,
   h.supp_part_num,
     h.supp_part_num || ', ' || h.style_desc AS vpn,
   h.rms_style_num,
   h.div_idnt,
   h.div_desc,
   h.grp_idnt,
   h.grp_desc,
   h.dept_idnt,
   h.dept_desc,
   h.class_idnt,
   h.class_desc,
   h.sbclass_idnt,
   h.sbclass_desc,
   h.supp_size,
   h.size_1_num,
   h.size_1_desc,
   h.size_2_num,
   h.size_2_desc,
   h.color_num,
   h.color_desc
  FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h
   INNER JOIN sku_list AS s ON LOWER(h.sku_idnt) = LOWER(s.sku_idnt)
   INNER JOIN suppliers_clean AS supp 
   ON LOWER(supp.original_supplier_idnt) = LOWER(h.supplier_idnt)
  WHERE LOWER(h.channel_country) = LOWER('US')
   AND LOWER(h.size_1_num) NOT IN (LOWER('NONE'), LOWER('O/S'), LOWER('AVG'), LOWER(' '))) AS h ON LOWER(ms.sku_idnt) = LOWER(h.sku_idnt)
GROUP BY h.sku_idnt,
 h.supplier_idnt,
 h.supplier_name,
 h.supp_part_num,
 h.vpn,
 style_id,
 h.div_idnt,
 h.div_desc,
 h.grp_idnt,
 h.grp_desc,
 h.dept_idnt,
 h.dept_desc,
 h.class_idnt,
 h.class_desc,
 h.sbclass_idnt,
 h.sbclass_desc,
 h.npg_ind,
 h.supp_size,
 size_1_num,
 size_1_desc,
 size_2_num,
 size_2_desc,
 h.color_num,
 h.color_desc;


--COLLECT STATS      PRIMARY INDEX (sku_idnt)     ,COLUMN(div_idnt)     ,COLUMN(supplier_idnt, style_id, size_1_num)     ,COLUMN(supplier_idnt, style_id)     ,COLUMN(supplier_idnt, supp_part_num, size_1_num)     ,COLUMN(supplier_idnt, supp_part_num)     ON products


-- -- DROP TABLE skus;


CREATE TEMPORARY TABLE IF NOT EXISTS skus
AS
SELECT *
FROM products AS p
WHERE supplier_idnt || '_' || supp_part_num IN (SELECT DISTINCT supplier_idnt || '_' || supp_part_num AS A779240828
   FROM products
   GROUP BY A779240828
   HAVING COUNT(DISTINCT size_1_num) > 1)
 OR supplier_idnt || '_' || style_id IN (SELECT DISTINCT supplier_idnt || '_' || style_id AS A466704068
   FROM products
   GROUP BY A466704068
   HAVING COUNT(DISTINCT size_1_num) > 1);


--COLLECT STATS      COLUMN(size_1_num)     ,column(size_2_num)      ON skus


CREATE TEMPORARY TABLE IF NOT EXISTS size_1_rank AS WITH sizes AS (SELECT DISTINCT size_1_num
  FROM skus), splits AS (SELECT size_1_num,
    CASE
    WHEN REGEXP_INSTR(RPAD(size_1_num, 10, ' '), '[/-]') > 0
    THEN SUBSTR(size_1_num, 1, REGEXP_INSTR(RPAD(size_1_num, 10, ' '), '[/-]') - 1)
    ELSE size_1_num
    END AS split_1,
    CASE
    WHEN REGEXP_INSTR(RPAD(size_1_num, 10, ' '), '[/-]') > 0
    THEN SUBSTR(size_1_num,(REGEXP_INSTR(RPAD(size_1_num, 10, ' '), '[/-]') + 1), LENGTH(size_1_num) - REGEXP_INSTR(RPAD(size_1_num, 10, ' '), '[/-]') )
    ELSE NULL
    END AS split_2,
   REGEXP_INSTR(RPAD(size_1_num, 10, ' '), '[/-]') AS split_position
  FROM sizes), 
  
  num_let AS (SELECT size_1_num,
   split_1,
   split_2,
   split_position,
    CASE
    WHEN CAST(regexp_substr(split_1, '(\\d+)\\.*(\\d+)*') AS NUMERIC) > 1000
    THEN SUBSTR(REGEXP_SUBSTR(split_1, '(\\d+)\\.*(\\d+)*'), 1, 2)
    WHEN CAST(regexp_substr(split_1, '(\\d+)\\.*(\\d+)*') AS NUMERIC) > 100
    THEN SUBSTR(REGEXP_SUBSTR(split_1, '(\\d+)\\.*(\\d+)*'), 1, 1)
    ELSE REGEXP_SUBSTR(split_1, '(\\d+)\\.*(\\d+)*')
    END AS numbers_1,
    CASE
    WHEN CAST(regexp_substr(split_1, '(\\d+)\\.*(\\d+)*') AS NUMERIC) > 1000
    THEN SUBSTR(REGEXP_SUBSTR(split_1, '(\\d+)\\.*(\\d+)*'), 3, 2)
    WHEN CAST(regexp_substr(split_1, '(\\d+)\\.*(\\d+)*') AS NUMERIC) > 100
    THEN SUBSTR(REGEXP_SUBSTR(split_1, '(\\d+)\\.*(\\d+)*'), 2, 2)
    WHEN  CAST(regexp_substr(split_1, '(\\d+)\\.*(\\d+)*') AS NUMERIC) - CAST(regexp_substr(split_2, '(\\d+)\\.*(\\d+)*') AS NUMERIC) > 10
    THEN NULL
    ELSE REGEXP_SUBSTR(split_2, '(\\d+)\\.*(\\d+)*')
    END AS numbers_2,
   REGEXP_SUBSTR(split_1, '[A-Za-z]+') AS letters_1,
    CASE
    WHEN REGEXP_SUBSTR(split_1, '^[1|2|3|4|5|6]*[X|x]*[S|M|L|s|m|l]$') IS NOT NULL OR REGEXP_SUBSTR(split_1,
      '^[0|1|2|3|4|5|6]*[X|x]$') IS NOT NULL
    THEN split_1
    ELSE NULL
    END AS sml_1,
    CASE
    WHEN REGEXP_SUBSTR(split_2, '^[1|2|3|4|5|6]*[X|x]*[S|M|L|s|m|l]$') IS NOT NULL OR REGEXP_SUBSTR(split_2,
      '^[0|1|2|3|4|5|6]*[X|x]$') IS NOT NULL
    THEN split_2
    ELSE NULL
    END AS sml_2
  FROM splits AS t4), frames AS (SELECT size_1_num,
   numbers_1,
   numbers_2,
   letters_1,
   split_1,
   split_2,
   split_position,
   sml_1,
   sml_2,
    CASE
    WHEN LOWER(split_1) IN (LOWER('TS'), LOWER('L1'), LOWER('L2')) OR LOWER(split_1) = LOWER(sml_1)
    THEN 'Letters'
    WHEN LOWER(split_1) IN (LOWER('P2'), LOWER('PL2')) OR LOWER(split_1) LIKE LOWER('%T') OR LOWER(split_1) LIKE LOWER('T%'
           ) OR LOWER(split_1) LIKE LOWER('%D') OR LOWER(split_1) LIKE LOWER('D%') OR LOWER(split_1) LIKE LOWER('%XB')
     OR LOWER(split_1) LIKE LOWER('XB%')
    THEN 'Letters'
    WHEN numbers_1 IS NOT NULL
    THEN 'Numbers'
    WHEN letters_1 IS NOT NULL
    THEN 'Letters'
    ELSE NULL
    END AS size_1_frame,
    CASE
    WHEN LOWER(split_1) IN (LOWER('TS'), LOWER('L1'), LOWER('L2'))
    THEN COALESCE(sml_2, numbers_2)
    WHEN LOWER(split_1) = LOWER(sml_1)
    THEN sml_1
    WHEN LOWER(split_1) IN (LOWER('P2'), LOWER('PL2')) OR LOWER(split_1) LIKE LOWER('%T') OR LOWER(split_1) LIKE LOWER('T%'
           ) OR LOWER(split_1) LIKE LOWER('%D') OR LOWER(split_1) LIKE LOWER('D%') OR LOWER(split_1) LIKE LOWER('%XB')
     OR LOWER(split_1) LIKE LOWER('XB%')
    THEN split_1
    ELSE COALESCE(numbers_1, letters_1)
    END AS final_1
  FROM num_let AS t6),
  
   size_rank AS (SELECT 
   size_1_num,CASE WHEN sml_rank IS NOT NULL AND x_num = 0 THEN (sml_rank * 1) - 100
              WHEN sml_rank IS NOT NULL THEN (sml_rank * (x_num * 10)) - 100
              WHEN number_rank IS NOT NULL THEN number_rank
              WHEN letter_rank IS NOT NULL THEN -201 + letter_rank
              END AS size_1_rank
   FROM(
  SELECT
  size_1_num,
    CASE
     WHEN LOWER(numbers_1) = LOWER('000')
     THEN - 2.0
     WHEN LOWER(numbers_1) = LOWER('00')
     THEN - 1.0
     WHEN numbers_2 IS NOT NULL AND numbers_1 IS NOT NULL
     THEN CAST(numbers_1 AS NUMERIC) + CAST(numbers_2 AS NUMERIC) / 2
     WHEN CAST(numbers_2 AS NUMERIC) IS NOT NULL
     THEN CAST(numbers_2 AS NUMERIC)
     ELSE CAST(numbers_1 AS NUMERIC)
     END AS number_rank,
    CASE WHEN regexp_substr(final_1, '[A|a]') IS NOT NULL THEN 1
                      WHEN regexp_substr(final_1, '[B|b]') IS NOT NULL THEN 2
                      WHEN regexp_substr(final_1, '[C|c]') IS NOT NULL THEN 3
                      WHEN regexp_substr(final_1, '[D|d]') IS NOT NULL THEN 4
                      WHEN regexp_substr(final_1, '[E|e]') IS NOT NULL THEN 5
                      WHEN regexp_substr(final_1, '[F|f]') IS NOT NULL THEN 6
                      WHEN regexp_substr(final_1, '[G|g]') IS NOT NULL THEN 7
                      WHEN regexp_substr(final_1, '[H|h]') IS NOT NULL THEN 8
                      WHEN regexp_substr(final_1, '[Q|q]') IS NOT NULL THEN 9
                      END  AS letter_rank,
    CASE
        WHEN REGEXP_SUBSTR(final_1, '[T|t]') IS NOT NULL
        THEN - 3
        WHEN REGEXP_SUBSTR(final_1, '[P|p]') IS NOT NULL
        THEN - 2
        WHEN REGEXP_SUBSTR(final_1, '[S|s]') IS NOT NULL
        THEN - 1
        WHEN REGEXP_SUBSTR(final_1, '[M|m]') IS NOT NULL
        THEN 0
        WHEN REGEXP_SUBSTR(final_1, '[L|l]') IS NOT NULL
        THEN 1
        WHEN REGEXP_SUBSTR(final_1, '[X|x]') IS NOT NULL
        THEN 2
        ELSE NULL
        END as sml_rank,
    CASE
       WHEN REGEXP_INSTR(final_1, 'X') > 0
        THEN COALESCE(cast(trunc(CAST(REGEXP_SUBSTR(final_1, '\\d+')as float64)) AS INTEGER), 1)
       ELSE 0
       END AS x_num,
  FROM frames
   GROUP BY  1,2,3,4,5
  ) )
  (SELECT f.size_1_num,
   f.size_1_frame,
   r.size_1_rank
  FROM frames AS f
   LEFT JOIN size_rank AS r ON LOWER(f.size_1_num) = LOWER(r.size_1_num));


--COLLECT STATS      COLUMN(size_1_num)      ON size_1_rank


CREATE TEMPORARY TABLE IF NOT EXISTS size_2_rank
AS
SELECT size_2_num,
 COALESCE(nmw_rank, 100 * (letter_rank_pre + 0.1 * number_rank), number_rank) AS size_2_rank
FROM (SELECT DISTINCT size_2_num,
    CASE
    WHEN LOWER(size_2_num) IN (LOWER('P'), LOWER('PP'), LOWER('PETITE'))
    THEN - 4
    WHEN LOWER(size_2_num) IN (LOWER('SS'), LOWER('2S'), LOWER('XS'))
    THEN - 3
    WHEN LOWER(size_2_num) IN (LOWER('S'))
    THEN - 2
    WHEN LOWER(size_2_num) IN (LOWER('N'))
    THEN - 1
    WHEN LOWER(size_2_num) IN (LOWER('M'), LOWER('R'), LOWER('MO'), LOWER('RR'), LOWER('MW'), LOWER('OZ'), LOWER('W PET'
       ))
    THEN 0
    WHEN LOWER(size_2_num) IN (LOWER('W'), LOWER('Plus'))
    THEN 1
    WHEN LOWER(size_2_num) IN (LOWER('WW'), LOWER('2W'), LOWER('X'), LOWER('XW'))
    THEN 2
    WHEN LOWER(size_2_num) IN (LOWER('T'), LOWER('TLL'), LOWER('Y'), LOWER('L'), LOWER('LONG'), LOWER('LT'), LOWER('RT'
       ), LOWER('UNH'), LOWER('NAH'), LOWER('BIG'))
    THEN 3
    WHEN LOWER(size_2_num) IN (LOWER('XLR'), LOWER('XT'), LOWER('XL'), LOWER('XLXS'), LOWER('XBIG'))
    THEN 4
    WHEN LOWER(size_2_num) IN (LOWER('XLT'), LOWER('XLL'))
    THEN 5
    ELSE NULL
    END AS nmw_rank,
    CASE
    WHEN REGEXP_SUBSTR(size_2_num, '[A|a]') IS NOT NULL
    THEN 1
    WHEN REGEXP_SUBSTR(size_2_num, '[B|b]') IS NOT NULL
    THEN 2
    WHEN REGEXP_SUBSTR(size_2_num, '[C|c]') IS NOT NULL
    THEN 3
    WHEN REGEXP_SUBSTR(size_2_num, '[D|d]') IS NOT NULL
    THEN 4
    WHEN REGEXP_SUBSTR(size_2_num, '[E|e]') IS NOT NULL
    THEN 5
    WHEN REGEXP_SUBSTR(size_2_num, '[F|f]') IS NOT NULL
    THEN 6
    WHEN REGEXP_SUBSTR(size_2_num, '[G|g]') IS NOT NULL
    THEN 7
    WHEN REGEXP_SUBSTR(size_2_num, '[H|h]') IS NOT NULL
    THEN 8
    WHEN REGEXP_SUBSTR(size_2_num, '[I|i]') IS NOT NULL
    THEN 9
    WHEN REGEXP_SUBSTR(size_2_num, '[J|j]') IS NOT NULL
    THEN 10
    WHEN REGEXP_SUBSTR(size_2_num, '[K|k]') IS NOT NULL
    THEN 11
    ELSE NULL
    END AS letter_rank_pre,
   COALESCE(CAST(trunc(cast(REGEXP_SUBSTR(size_2_num, '(\\d+)' )as float64)) AS INT64), 0) AS number_rank
  FROM skus) AS rnk2_process;


--COLLECT STATS      COLUMN(size_2_num)      ON size_2_rank


-- DROP TABLE sku_sizes;


CREATE TEMPORARY TABLE IF NOT EXISTS sku_sizes
AS
SELECT p.sku_idnt,
   p.style_id || '~~' || p.color_num AS choice_id,
     TRIM(FORMAT('%11d', p.dept_idnt)) || '~~' || TRIM(p.supplier_idnt) || '~~' || TRIM(FORMAT('%11d', p.class_idnt)) AS
 groupid,
 p.supplier_idnt,
 p.supplier_name,
 p.supp_part_num,
 p.vpn,
 p.style_id,
 p.div_idnt,
 p.div_desc,
 p.grp_idnt,
 p.grp_desc,
 p.dept_idnt,
 p.dept_desc,
 p.class_idnt,
 p.class_desc,
 p.sbclass_idnt,
 p.sbclass_desc,
 p.npg_ind,
 p.supp_size,
 p.size_1_num AS size_1_id,
 p.size_1_desc,
 s1.size_1_rank,
 s1.size_1_frame,
 COALESCE(p.size_2_num, 'NA') AS size_2_id,
 p.size_2_desc,
 s2.size_2_rank,
 p.color_num,
 p.color_desc
FROM skus AS p
 INNER JOIN size_1_rank AS s1 ON LOWER(p.size_1_num) = LOWER(s1.size_1_num)
 LEFT JOIN size_2_rank AS s2 ON LOWER(p.size_2_num) = LOWER(s2.size_2_num);


--COLLECT STATS       COLUMN(sku_idnt)      ON sku_sizes


CREATE TEMPORARY TABLE IF NOT EXISTS na_size_2_map
AS
SELECT dept_idnt,
 COUNT(DISTINCT groupid) AS groupid_total,
  CAST(COUNT(DISTINCT CASE
     WHEN size_2_id IS NULL
     THEN groupid
     ELSE NULL
     END) AS NUMERIC) / COUNT(DISTINCT groupid) AS groupid_na,
  CAST(COUNT(DISTINCT CASE
     WHEN LOWER(size_2_id) = LOWER('R')
     THEN groupid
     ELSE NULL
     END) AS NUMERIC) / COUNT(DISTINCT groupid) AS groupid_r,
  CAST(COUNT(DISTINCT CASE
     WHEN LOWER(size_2_id) = LOWER('M')
     THEN groupid
     ELSE NULL
     END) AS NUMERIC) / COUNT(DISTINCT groupid) AS groupid_m,
 'NA' AS size_2_id_old,
  CASE
  WHEN CAST(COUNT(DISTINCT CASE
        WHEN LOWER(size_2_id) = LOWER('R')
        THEN groupid
        ELSE NULL
        END) AS NUMERIC) / COUNT(DISTINCT groupid) <= 0.1 
        AND CAST(COUNT(DISTINCT CASE
        WHEN LOWER(size_2_id) = LOWER('M')
        THEN groupid
        ELSE NULL
        END) AS NUMERIC) / COUNT(DISTINCT groupid) <= 0.1
  THEN 'NA'
  WHEN CAST(COUNT(DISTINCT CASE
       WHEN LOWER(size_2_id) = LOWER('R')
       THEN groupid
       ELSE NULL
       END) AS NUMERIC) / COUNT(DISTINCT groupid) >= 0.2
  THEN 'R'
  WHEN CAST(COUNT(DISTINCT CASE
       WHEN LOWER(size_2_id) = LOWER('M')
       THEN groupid
       ELSE NULL
       END) AS NUMERIC) / COUNT(DISTINCT groupid) >= 0.2
  THEN 'M'
  ELSE NULL
  END AS size_2_id_new
FROM sku_sizes
GROUP BY dept_idnt;


--COLLECT STATS      PRIMARY INDEX (dept_idnt,size_2_id_old)     ,COLUMN(dept_idnt)     ,COLUMN(size_2_id_old)      ON na_size_2_map


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy
(SELECT DISTINCT sku_idnt,
  choice_id,
  supplier_idnt,
  supplier_name,
  supp_part_num,
  vpn,
  style_id,
  div_idnt,
  div_desc,
  grp_idnt,
  grp_desc,
  dept_idnt,
  dept_desc,
  class_idnt,
  class_desc,
  sbclass_idnt,
  sbclass_desc,
  npg_ind,
  supp_size,
  size_1_id,
  size_1_desc,
  size_1_rank,
  size_1_frame,
  size_2_id,
  size_2_desc,
  CAST(size_2_rank AS NUMERIC) AS size_2_rank,
  update_timestamp,
  update_timestamp_tz
 FROM (SELECT p.sku_idnt,
    p.choice_id,
    p.supplier_idnt,
    p.supplier_name,
    p.supp_part_num,
    p.vpn,
    p.style_id,
    p.div_idnt,
    p.div_desc,
    p.grp_idnt,
    p.grp_desc,
    p.dept_idnt,
    p.dept_desc,
    p.class_idnt,
    p.class_desc,
    p.sbclass_idnt,
    p.sbclass_desc,
    p.npg_ind,
    p.supp_size,
    p.size_1_id,
    p.size_1_desc,
    CAST(p.size_1_rank AS NUMERIC) AS size_1_rank,
     CASE
     WHEN f.frames = 2 OR t1.frames = 2
     THEN 'Letters'
     ELSE p.size_1_frame
     END AS size_1_frame,
    COALESCE(na.size_2_id_new, p.size_2_id) AS size_2_id,
    p.size_2_desc,
    p.size_2_rank,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
    update_timestamp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz
   FROM sku_sizes AS p
    INNER JOIN (SELECT choice_id,
      COUNT(DISTINCT size_1_frame) AS frames
     FROM sku_sizes
     GROUP BY choice_id) AS f ON LOWER(p.choice_id) = LOWER(f.choice_id)
    INNER JOIN (SELECT vpn,
      supplier_idnt,
      COUNT(DISTINCT size_1_frame) AS frames
     FROM sku_sizes
     GROUP BY supplier_idnt,
      vpn) AS t1 ON LOWER(p.vpn) = LOWER(t1.vpn) AND LOWER(p.supplier_idnt) = LOWER(t1.supplier_idnt)
    LEFT JOIN na_size_2_map AS na ON p.dept_idnt = na.dept_idnt AND LOWER(p.size_2_id) = LOWER(na.size_2_id_old)) AS t2 );


--COLLECT STATISTICS      COLUMN(dept_idnt)          ,COLUMN(supplier_idnt, dept_idnt, class_idnt, size_1_id, size_2_id)     ,COLUMN(sku_idnt)     ,COLUMN(supplier_idnt)     ,COLUMN(npg_ind)     ,COLUMN(sku_idnt)     ,COLUMN(supplier_idnt, dept_idnt, class_idnt, size_1_id, size_1_rank, size_1_frame, size_2_id)      ,COLUMN(supplier_idnt ,dept_idnt, class_idnt, npg_ind ,size_1_id ,size_1_frame ,size_2_id)      ,COLUMN(supplier_idnt ,dept_idnt, class_idnt, size_1_id, size_1_frame, size_2_id)      ON t2dl_das_size.supp_size_hierarchy