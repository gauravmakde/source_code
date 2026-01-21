/*
Name: Size Curves Sales Tables
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - channel_size_sales_data: table with sales related data at the fiscal year half-channel-cc-size1-size2 level. It is used to create bulk level curves. 
    - location_size_sales_data: table with sales related data at the fiscal year half-location-cc-size1-size2 level. it is used to create location level curves
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_monthly_main
Author(s): Zisis Daffas & Sara Riker
Date Created: 2/03/2023
Date Last Updated:2/21/2023
*/

-- begin 
-- DROP TABLE product_lkup;
CREATE MULTISET VOLATILE TABLE product_lkup AS (
SELECT DISTINCT
     p.price_lvl
    ,p.sku_id
    ,p.choice_id
    ,p.department_id
    ,p.supplier_id
    ,p.class_id
    ,p.groupid
    ,p.class_frame
    ,p.groupid_frame
    ,p.size_1_id
    ,CASE WHEN p.size_1_id IN ('1', '2', '3', '1X', '2X', '3X') 
           AND COALESCE(size2.size_2_rank_new, p.size_2_rank) = 2.00 
           AND wmn.dept_idnt IS NOT NULL 
           THEN p.size_1_rank + 24 
           ELSE p.size_1_rank END AS size_1_rank 
    ,p.size_1_frame
    ,COALESCE(size2.size_2_id_new, p.size_2_id) AS size_2_id
    ,COALESCE(size2.size_2_rank_new, p.size_2_rank) AS size_2_rank
    ,p.supplier_name
    ,p.jda_subclass_sc
    ,p.jda_subclass_c
    ,p.designer_flag
FROM (
        SELECT 
             p.price_lvl
            ,p.choice_id
            ,p.sku_id
            ,p.department_id
            ,p.supplier_id
            ,p.class_id
            ,p.groupid
            ,TRIM(department_id) || '~~' || TRIM(class_id) || '~~' || size_1_frame AS class_frame
            ,TRIM(groupid) || '~~' || size_1_frame AS groupid_frame
            ,p.size_1_id
            ,p.size_1_rank
            ,p.size_1_frame
            ,COALESCE(NULLIF(size_2_id, ''), 'NA') AS size_2_id
            ,p.size_2_rank
            ,p.supplier_name
            ,p.jda_subclass_sc
            ,p.jda_subclass_c
            ,p.designer_flag
        FROM {environment_schema}.size_sku_groupid{env_suffix} p
    ) p
LEFT JOIN (
            SELECT
                 department_id
                ,'NA' AS size_2_id_old
                ,CASE WHEN groupid_r < 0.15 AND groupid_m < 0.15 THEN 'NA'
                      WHEN groupid_r >= 0.15 AND groupid_r >= groupid_m THEN 'R'
                      ELSE 'M' --else is equivalent to " when groupid_m >= 0.2 and groupid_m >= groupid_r then 'M' "
                      END AS size_2_id_new
                ,CASE WHEN size_2_id_new IN ('R', 'M') THEN 0.0 ELSE NULL END AS size_2_rank_new
            FROM (
                    SELECT
                         department_id
                        ,1.00 * COUNT(DISTINCT CASE WHEN UPPER(size_2_id) = 'R' THEN groupid END) / COUNT(DISTINCT groupid) AS groupid_r
                        ,1.00 * COUNT(DISTINCT CASE WHEN UPPER(size_2_id) = 'M' THEN groupid END) / COUNT(DISTINCT groupid) AS groupid_m
                    FROM {environment_schema}.size_sku_groupid{env_suffix}
                    GROUP BY 1
               ) pcts
        ) size2
    ON p.department_id = size2.department_id
   AND p.size_2_id = size2.size_2_id_old
LEFT JOIN (
            SELECT DISTINCT
                dept_idnt
            FROM t2dl_das_assortment_dim.assortment_hierarchy
            WHERE grp_idnt IN (775, 785)
        ) wmn
  ON p.department_id = wmn.dept_idnt
) WITH DATA
PRIMARY INDEX(sku_id, price_lvl)
ON COMMIT PRESERVE ROWS 
;

COLLECT STATS
     PRIMARY INDEX(sku_id, price_lvl)
    ,COLUMN(choice_id, class_frame, groupid_frame, size_1_id, size_2_id)
    ,COLUMN(price_lvl)
    ,COLUMN(price_lvl, sku_id)
    ,COLUMN(sku_id)
    ON product_lkup;


CREATE MULTISET VOLATILE TABLE week_lkup AS (
SELECT DISTINCT
     week_idnt
    ,fiscal_halfyear_num AS half_idnt 
    ,'H' || RIGHT(CAST(fiscal_halfyear_num AS VARCHAR(5)),1) AS half
FROM prd_nap_usr_vws.day_cal_454_dim
WHERE fiscal_halfyear_num >= (SELECT DISTINCT fiscal_halfyear_num FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE()) - 20
  AND fiscal_halfyear_num < (SELECT DISTINCT fiscal_halfyear_num FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE())
) WITH DATA
PRIMARY INDEX(half_idnt)
ON COMMIT PRESERVE ROWS
;    

COLLECT STATS 
     PRIMARY INDEX(half_idnt)
    ,COLUMN(half_idnt, half)
    ,COLUMN(week_idnt)
     ON week_lkup;


-- DROP TABLE loc_lkup;
CREATE MULTISET VOLATILE TABLE loc_lkup AS (
SELECT DISTINCT 
    CASE WHEN store_num IN (209, 210, 212) THEN 209 
         ELSE store_num 
         END AS loc_idnt
    ,CASE WHEN business_unit_num IN ('1000', '6000') THEN 'fp'
          WHEN business_unit_num IN ('2000', '5000') THEN 'op'
          END AS price_lvl
    ,CASE WHEN business_unit_num IN ('1000') THEN 'FLS'
          WHEN business_unit_num IN ('6000') THEN 'N.COM'
          WHEN business_unit_num in ('2000') THEN 'RACK'
          ELSE 'NRHL'
          END AS chnl_idnt
FROM prd_nap_usr_vws.price_store_dim_vw loc
WHERE business_unit_num in ('1000', '6000', '2000', '5000')
  AND store_name NOT like 'CLSD%' -- can be found as CLSD or CLOSED
  AND store_name NOT like 'CLOSED%'
) WITH DATA
PRIMARY INDEX(loc_idnt)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    PRIMARY INDEX(loc_idnt)
   ,COLUMN(loc_idnt)
   ,COLUMN(price_lvl)
    ON loc_lkup;

-- DROP TABLE sku_loc_week;
CREATE MULTISET VOLATILE TABLE sku_loc_week AS (
SELECT 
     cal.week_idnt
    ,cal.half_idnt
    ,cal.half
    ,loc.price_lvl
    ,loc.loc_idnt
    ,loc.chnl_idnt
    ,p.sku_id
    ,p.choice_id
    ,p.groupid
    ,p.class_frame
    ,p.groupid_frame
    ,p.size_1_id
    ,p.size_2_id
    ,p.designer_flag
    ,mstr.sales_units
    ,mstr.eoh_units
FROM {environment_schema}.size_sku_loc_weekly mstr
JOIN loc_lkup loc
  ON mstr.loc_idnt = loc.loc_idnt 
JOIN product_lkup p
  ON mstr.sku_idnt = p.sku_id
 AND loc.price_lvl = p.price_lvl
JOIN week_lkup cal 
  ON mstr.week_idnt = cal.week_idnt
) WITH DATA 
PRIMARY INDEX(week_idnt, loc_idnt, choice_id, size_1_id, size_2_id) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX(week_idnt, loc_idnt, choice_id, size_1_id, size_2_id) 
    ,COLUMN(loc_idnt)
    ,COLUMN(week_idnt)
    ,COLUMN(half_idnt)
    ,COLUMN(price_lvl)
    ,COLUMN(week_idnt, half_idnt, half, price_lvl, loc_idnt, choice_id, class_frame, groupid_frame, size_1_id, size_2_id)
    ,COLUMN(week_idnt, half_idnt, half, price_lvl, chnl_idnt, choice_id, class_frame, groupid_frame, size_1_id, size_2_id)
    ON sku_loc_week;


-- DROP TABLE cc_chnl_week;
CREATE MULTISET VOLATILE TABLE cc_chnl_week AS (
SELECT
     price_lvl
    ,chnl_idnt
    ,week_idnt
    ,half_idnt
    ,half
    ,choice_id
    ,size_1_id
    ,size_2_id
    ,groupid_frame
    ,class_frame
    ,designer_flag
    ,SUM(sales_units) AS sales_units
    ,SUM(eoh_units) AS eoh_units
FROM sku_loc_week
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA 
PRIMARY INDEX(week_idnt, chnl_idnt, choice_id, size_1_id, size_2_id) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX(week_idnt, chnl_idnt, choice_id, size_1_id, size_2_id) 
    ,COLUMN(chnl_idnt, half_idnt, choice_id, size_1_id)
    ,COLUMN(chnl_idnt, half_idnt, choice_id)
    ,COLUMN(chnl_idnt, half_idnt, choice_id, size_1_id, size_2_id)
    ,COLUMN(half_idnt)
    ON cc_chnl_week;

-- DROP TABLE cc_chnl_wk_processed;
CREATE MULTISET VOLATILE TABLE cc_chnl_wk_processed AS (
WITH weeks_on_sale AS (
    SELECT
         half_idnt
        ,chnl_idnt
        ,choice_id
        ,size_1_id
        ,size_2_id
        ,COUNT(week_idnt) AS cc_weeks
    FROM cc_chnl_week
    WHERE eoh_units > 0
       OR sales_units > 0
    GROUP BY 1,2,3,4,5
),
weeks_to_break AS (
    SELECT
         half_idnt
        ,chnl_idnt
        ,choice_id
        ,MIN(cc_weeks) AS cc_weeks_to_break
    FROM weeks_on_sale
    GROUP BY 1,2,3
)
SELECT
     mstr.half
    ,mstr.price_lvl
    ,mstr.chnl_idnt
    ,mstr.week_idnt
    ,mstr.half_idnt
    ,mstr.choice_id
    ,mstr.size_1_id
    ,mstr.size_2_id
    ,mstr.groupid_frame
    ,mstr.class_frame
    ,mstr.designer_flag
    ,mstr.sales_units
    ,wsl.cc_weeks
    ,wbrk.cc_weeks_to_break
FROM cc_chnl_week mstr
JOIN weeks_on_sale wsl
  ON mstr.choice_id = wsl.choice_id
 AND mstr.size_1_id = wsl.size_1_id
 AND mstr.size_2_id = wsl.size_2_id
 AND mstr.chnl_idnt = wsl.chnl_idnt
 AND mstr.half_idnt = wsl.half_idnt
JOIN weeks_to_break wbrk
  ON mstr.choice_id = wbrk.choice_id
 AND mstr.chnl_idnt = wbrk.chnl_idnt
 AND mstr.half_idnt = wbrk.half_idnt
) WITH DATA 
PRIMARY INDEX (half_idnt, week_idnt, price_lvl, chnl_idnt, choice_id, size_1_id, size_2_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX (half_idnt, week_idnt, price_lvl, chnl_idnt, choice_id, size_1_id, size_2_id)
    ,COLUMN(chnl_idnt, half_idnt, choice_id)
    ,COLUMN(cc_weeks)
    ,COLUMN(half_idnt)
    ,COLUMN(choice_id)
    ON cc_chnl_wk_processed;


-- DROP TABLE units_sold_totals;
CREATE MULTISET VOLATILE TABLE units_sold_totals AS (
SELECT
     price_lvl
    ,half
    ,half_idnt
    ,chnl_idnt
    ,groupid_frame
    ,class_frame
    ,choice_id
    ,size_1_id
    ,size_2_id
    ,designer_flag
    ,cc_weeks
    ,cc_weeks_to_break
    ,units_sold
    ,units_sold_avg_weekly
    ,SUM(units_sold) OVER (PARTITION BY half_idnt, chnl_idnt, choice_id) AS units_sold_total
    ,SUM(units_sold_avg_weekly) OVER (PARTITION BY half_idnt, chnl_idnt, choice_id) AS units_sold_avg_weekly_total
FROM (
        SELECT
             price_lvl
            ,half
            ,half_idnt
            ,chnl_idnt
            ,groupid_frame
            ,class_frame
            ,choice_id
            ,size_1_id
            ,size_2_id
            ,designer_flag
            ,cc_weeks
            ,cc_weeks_to_break
            ,CASE WHEN SUM(sales_units) < 0 THEN 0 ELSE SUM(sales_units) END AS units_sold
            ,(CASE WHEN SUM(sales_units) < 0 THEN 0 ELSE SUM(sales_units) END) / AVG(cc_weeks) AS units_sold_avg_weekly
        FROM cc_chnl_wk_processed
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    ) units_sold
) WITH DATA
PRIMARY INDEX (half_idnt, chnl_idnt, choice_id, size_1_id, size_2_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX (half_idnt, chnl_idnt, choice_id, size_1_id, size_2_id)
    ,COLUMN(half, half_idnt, chnl_idnt, groupid_frame, class_frame, choice_id)
    ,COLUMN(half_idnt)
    ,COLUMN(half, chnl_idnt, groupid_frame, class_frame, choice_id)
    ON units_sold_totals;

-- DROP TABLE include_flag;
CREATE MULTISET VOLATILE TABLE include_flag AS (
SELECT
     half
    ,half_idnt
    ,chnl_idnt
    ,groupid_frame
    ,class_frame
    ,choice_id
    ,units_sold_total
    ,cc_weeks_to_break
    ,sparseness
    ,sku_ct
    ,CASE WHEN designer_flag = 1 AND units_sold_total < 25 THEN 0
          WHEN designer_flag = 0 AND units_sold_total < 50 THEN 0 
          WHEN sparseness > .50 THEN 0
          WHEN sku_ct <= 1 THEN 0
          WHEN cc_weeks_to_break <= 3 THEN 0
          ELSE 1 
          END AS include_ind
FROM (
        SELECT
             half
            ,half_idnt
            ,chnl_idnt
            ,groupid_frame
            ,class_frame
            ,choice_id
            ,designer_flag
            ,MAX(units_sold_total) AS units_sold_total
            ,MAX(cc_weeks_to_break) AS cc_weeks_to_break
            ,1.0 * COUNT(CASE WHEN units_sold = 0 THEN 1 END) / COUNT(1) AS sparseness
            ,COUNT(size_1_id || '~' || size_2_id) AS sku_ct
        FROM units_sold_totals
        GROUP BY 1,2,3,4,5,6,7
    ) criteria 
) WITH DATA
PRIMARY INDEX (half_idnt, chnl_idnt, groupid_frame, choice_id)
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
     PRIMARY INDEX (half_idnt, chnl_idnt, groupid_frame, choice_id)
    ,COLUMN(half, chnl_idnt, groupid_frame, class_frame, choice_id)
    ON include_flag;

-- DROP TABLE cc_chnl_sku_units_sold;
CREATE MULTISET VOLATILE TABLE cc_chnl_sku_units_sold AS (
SELECT
     mstr.price_lvl
    ,mstr.half
    ,mstr.half_idnt
    ,mstr.chnl_idnt
    ,mstr.groupid_frame
    ,mstr.class_frame
    ,mstr.choice_id
    ,mstr.designer_flag
    ,mstr.cc_weeks
    ,mstr.cc_weeks_to_break
    ,mstr.units_sold
    ,mstr.units_sold_avg_weekly
    ,mstr.units_sold_total
    ,mstr.units_sold_avg_weekly_total
    ,CASE WHEN mstr.units_sold_avg_weekly_total = 0 THEN 0
          ELSE 1.00 * mstr.units_sold_avg_weekly / mstr.units_sold_avg_weekly_total
          END AS units_sold_avg_weekly_ratio
    ,pr.department_id
    ,pr.class_id
    ,mstr.size_1_id
    ,mstr.size_2_id
    ,pr.size_1_rank
    ,pr.size_2_rank
    ,pr.jda_subclass_sc
    ,pr.jda_subclass_c
    ,pr.supplier_name
    ,top50.include_ind
    ,top50.sparseness
    ,top50.sku_ct
    ,rank_cc.cc_rank_groupid
    ,rank_cc.cc_rank_class
FROM units_sold_totals mstr
JOIN (
        SELECT DISTINCT 
             choice_id
            ,price_lvl
            ,size_1_id
            ,size_2_id
            ,department_id
            ,class_id
            ,size_1_rank
            ,size_2_rank
            ,jda_subclass_sc
            ,jda_subclass_c
            ,supplier_name
            ,groupid_frame
            ,class_frame
        FROM product_lkup pr
    ) pr
  ON mstr.choice_id = pr.choice_id
 AND mstr.price_lvl = pr.price_lvl
 AND mstr.size_1_id = pr.size_1_id
 AND mstr.size_2_id = pr.size_2_id
 AND mstr.groupid_frame = pr.groupid_frame
 AND mstr.class_frame = pr.class_frame
JOIN include_flag top50
  ON mstr.groupid_frame = top50.groupid_frame
 AND mstr.class_frame = top50.class_frame
 AND mstr.choice_id = top50.choice_id
 AND mstr.half = top50.half
 AND mstr.half_idnt = top50.half_idnt
 AND mstr.chnl_idnt = top50.chnl_idnt
LEFT JOIN  (
            SELECT
                 half
                ,half_idnt
                ,chnl_idnt
                ,groupid_frame
                ,class_frame
                ,choice_id
                ,ROW_NUMBER() OVER (PARTITION BY half, chnl_idnt, groupid_frame ORDER BY units_sold_total DESC) AS cc_rank_groupid
                ,ROW_NUMBER() OVER (PARTITION BY half, chnl_idnt, class_frame ORDER BY units_sold_total DESC) AS cc_rank_class
            FROM include_flag
            WHERE include_ind = 1
        ) rank_cc
  ON mstr.groupid_frame = rank_cc.groupid_frame
 AND mstr.class_frame = rank_cc.class_frame
 AND mstr.choice_id = rank_cc.choice_id
 AND mstr.half = rank_cc.half
 AND mstr.half_idnt = rank_cc.half_idnt
 AND mstr.chnl_idnt = rank_cc.chnl_idnt
    -- filter on sku_ct
WHERE top50.sku_ct > 1 
)WITH DATA 
PRIMARY INDEX (half_idnt, chnl_idnt, choice_id, size_1_id, size_2_id, groupid_frame, class_frame)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (half_idnt, chnl_idnt, choice_id, size_1_id, size_2_id, groupid_frame, class_frame)
    ,COLUMN(half, chnl_idnt)
    ,COLUMN(half, chnl_idnt, class_frame)
    ,COLUMN(half, chnl_idnt, class_frame, choice_id)
    ,COLUMN(choice_id)
    ,COLUMN(include_ind)
    ON cc_chnl_sku_units_sold;


-- DROP TABLE channel_data_final;
CREATE MULTISET VOLATILE TABLE channel_data_final AS (
SELECT DISTINCT
     mstr.price_lvl
    ,mstr.half
    ,mstr.half_idnt
    ,mstr.chnl_idnt
    ,mstr.class_frame
    ,mstr.groupid_frame
    ,mstr.choice_id 
    ,mstr.designer_flag
    ,mstr.include_ind
    ,mstr.department_id
    ,mstr.class_id
    ,mstr.size_1_id
    ,mstr.size_2_id
    ,mstr.size_1_rank
    ,mstr.size_2_rank
    ,mstr.jda_subclass_sc
    ,mstr.jda_subclass_c
    ,mstr.supplier_name
    ,groupidinfo.included_cc_per_groupid
    ,classinfo.included_cc_per_class
    ,mstr.cc_rank_groupid
    ,mstr.cc_rank_class
    ,mstr.units_sold
    ,mstr.units_sold_avg_weekly
    ,mstr.units_sold_avg_weekly_ratio
    ,mstr.cc_weeks_to_break
    ,(SELECT COUNT(DISTINCT choice_id) AS choice_id FROM cc_chnl_wk_processed) AS original_cc_count
    ,(SELECT COUNT(DISTINCT choice_id) AS cc FROM cc_chnl_sku_units_sold WHERE include_ind = 1) AS used_in_curve_gen_cc_count
FROM cc_chnl_sku_units_sold mstr
JOIN (
        SELECT
             half
            ,chnl_idnt
            ,groupid_frame
            ,COUNT(DISTINCT CASE WHEN include_ind = 1 THEN choice_id ELSE NULL END) AS included_cc_per_groupid
        FROM cc_chnl_sku_units_sold
        GROUP BY 1,2,3
    ) groupidinfo
  ON mstr.groupid_frame = groupidinfo.groupid_frame
 AND mstr.half = groupidinfo.half
 AND mstr.chnl_idnt = groupidinfo.chnl_idnt
JOIN (
        SELECT
             half
            ,chnl_idnt
            ,class_frame
            ,COUNT(DISTINCT CASE WHEN include_ind = 1 THEN choice_id ELSE NULL END) AS included_cc_per_class
        FROM cc_chnl_sku_units_sold
        GROUP BY 1,2,3
    ) classinfo
  ON mstr.class_frame = classinfo.class_frame
 AND mstr.half = classinfo.half
 AND mstr.chnl_idnt = classinfo.chnl_idnt
) WITH DATA 
UNIQUE PRIMARY INDEX (half_idnt, chnl_idnt, groupid_frame, choice_id, size_1_id, size_2_id)
ON COMMIT PRESERVE ROWS;


DELETE FROM {environment_schema}.channel_size_sales_data{env_suffix} ALL;
INSERT INTO {environment_schema}.channel_size_sales_data{env_suffix}
SELECT
     price_lvl
    ,half
    ,half_idnt
    ,chnl_idnt
    ,class_frame
    ,groupid_frame
    ,designer_flag
    ,choice_id AS cc
    ,include_ind
    ,department_id
    ,class_id
    ,size_1_id
    ,size_2_id
    ,size_1_rank
    ,size_2_rank
    ,jda_subclass_sc
    ,jda_subclass_c
    ,supplier_name
    ,included_cc_per_groupid
    ,included_cc_per_class
    ,cc_rank_groupid
    ,cc_rank_class
    ,units_sold
    ,units_sold_avg_weekly
    ,units_sold_avg_weekly_ratio
    ,cc_weeks_to_break
    ,original_cc_count
    ,used_in_curve_gen_cc_count
    ,CURRENT_TIMESTAMP AS rcd_update_timestamp
FROM channel_data_final;

COLLECT STATS 
     COLUMN(half_idnt, chnl_idnt, groupid_frame, cc, size_1_id, size_2_id) 
    ON  {environment_schema}.channel_size_sales_data{env_suffix};

DROP TABLE channel_data_final;

-- location based

CREATE MULTISET VOLATILE TABLE cc_loc_week_fp AS (
SELECT
     price_lvl
    ,loc_idnt
    ,week_idnt
    ,half_idnt
    ,half
    ,choice_id
    ,size_1_id
    ,size_2_id
    ,groupid_frame
    ,class_frame
    ,SUM(sales_units) AS sales_units
    ,SUM(eoh_units) AS eoh_units
FROM sku_loc_week
WHERE price_lvl = 'fp'
  AND chnl_idnt = 'FLS'
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA 
PRIMARY INDEX(week_idnt, loc_idnt, choice_id, size_1_id, size_2_id) 
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE cc_loc_week_op AS (
SELECT
     price_lvl
    ,loc_idnt
    ,week_idnt
    ,half_idnt
    ,half
    ,choice_id
    ,size_1_id
    ,size_2_id
    ,groupid_frame
    ,class_frame
    ,SUM(sales_units) AS sales_units
    ,SUM(eoh_units) AS eoh_units
FROM sku_loc_week
WHERE price_lvl = 'op'
  AND chnl_idnt = 'RACK'
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA 
PRIMARY INDEX(week_idnt, loc_idnt, choice_id, size_1_id, size_2_id) 
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE cc_loc_week AS (
SELECT
     fp.price_lvl
    ,fp.loc_idnt
    ,fp.week_idnt
    ,fp.half_idnt
    ,fp.half
    ,fp.choice_id
    ,fp.size_1_id
    ,fp.size_2_id
    ,fp.groupid_frame
    ,fp.class_frame
    ,fp.sales_units
    ,fp.eoh_units
FROM cc_loc_week_fp fp
JOIN (
        SELECT DISTINCT 
             price_lvl
            ,half_idnt
            ,class_frame
            ,groupid_frame
        FROM {environment_schema}.channel_size_sales_data
        WHERE price_lvl = 'fp'
          AND chnl_idnt = 'FLS'
    ) bulk
  ON fp.price_lvl = bulk.price_lvl
 AND fp.half_idnt = bulk.half_idnt
 AND fp.class_frame = bulk.class_frame 
 AND fp.groupid_frame = bulk.groupid_frame

UNION 

SELECT
     op.price_lvl
    ,op.loc_idnt
    ,op.week_idnt
    ,op.half_idnt
    ,op.half
    ,op.choice_id
    ,op.size_1_id
    ,op.size_2_id
    ,op.groupid_frame
    ,op.class_frame
    ,op.sales_units
    ,op.eoh_units
FROM cc_loc_week_op op
JOIN (
        SELECT DISTINCT 
             price_lvl
            ,half_idnt
            ,class_frame
            ,groupid_frame
        FROM {environment_schema}.channel_size_sales_data
        WHERE price_lvl = 'op'
          AND chnl_idnt = 'RACK'
    ) bulk
  ON op.price_lvl = bulk.price_lvl
 AND op.half_idnt = bulk.half_idnt
 AND op.class_frame = bulk.class_frame 
 AND op.groupid_frame = bulk.groupid_frame
) WITH DATA 
PRIMARY INDEX(half_idnt, loc_idnt, choice_id, size_1_id, size_2_id) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX(half_idnt, loc_idnt, choice_id, size_1_id, size_2_id) 
    ,COLUMN(half_idnt, loc_idnt, choice_id, size_1_id)
    ,COLUMN(loc_idnt, half_idnt, choice_id)
    ,COLUMN(half_idnt)
    ,COLUMN(loc_idnt)
    ON cc_loc_week;

CREATE MULTISET VOLATILE TABLE cc_loc_wk_processed AS (
WITH weeks_on_sale AS (
    SELECT
         half_idnt
        ,loc_idnt
        ,choice_id
        ,size_1_id
        ,size_2_id
        ,COUNT(week_idnt) AS cc_weeks
    FROM cc_loc_week
    WHERE eoh_units > 0
       OR sales_units > 0
    GROUP BY 1,2,3,4,5
),
weeks_to_break AS (
    SELECT
         half_idnt
        ,loc_idnt
        ,choice_id
        ,MIN(cc_weeks) AS cc_weeks_to_break
    FROM weeks_on_sale
    GROUP BY 1,2,3
)
SELECT
     mstr.half
    ,mstr.price_lvl
    ,mstr.loc_idnt
    ,mstr.week_idnt
    ,mstr.half_idnt
    ,mstr.choice_id
    ,mstr.size_1_id
    ,mstr.size_2_id
    ,mstr.groupid_frame
    ,mstr.class_frame
    ,mstr.sales_units
    ,wsl.cc_weeks
    ,wbrk.cc_weeks_to_break
FROM cc_loc_week mstr
JOIN weeks_on_sale wsl
  ON mstr.choice_id = wsl.choice_id
 AND mstr.size_1_id = wsl.size_1_id
 AND mstr.size_2_id = wsl.size_2_id
 AND mstr.loc_idnt = wsl.loc_idnt
 AND mstr.half_idnt = wsl.half_idnt
JOIN weeks_to_break wbrk
  ON mstr.choice_id = wbrk.choice_id
 AND mstr.loc_idnt = wbrk.loc_idnt
 AND mstr.half_idnt = wbrk.half_idnt
) WITH DATA 
PRIMARY INDEX (half_idnt, week_idnt, price_lvl, loc_idnt, choice_id, size_1_id, size_2_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX (half_idnt, week_idnt, price_lvl, loc_idnt, choice_id, size_1_id, size_2_id)
    ,COLUMN(loc_idnt, half_idnt, choice_id)
    ,COLUMN(half_idnt)
    ON cc_loc_wk_processed;


DROP TABLE units_sold_totals;
CREATE MULTISET VOLATILE TABLE units_sold_totals AS (
SELECT
     price_lvl
    ,half
    ,half_idnt
    ,loc_idnt
    ,groupid_frame
    ,class_frame
    ,choice_id
    ,size_1_id
    ,size_2_id
    ,cc_weeks
    ,cc_weeks_to_break
    ,units_sold
    ,units_sold_avg_weekly
    ,SUM(units_sold) OVER (PARTITION BY half_idnt, loc_idnt, choice_id) AS units_sold_total
    ,SUM(units_sold_avg_weekly) OVER (PARTITION BY half_idnt, loc_idnt, choice_id) AS units_sold_avg_weekly_total
FROM (
        SELECT
             price_lvl
            ,half
            ,half_idnt
            ,loc_idnt
            ,groupid_frame
            ,class_frame
            ,choice_id
            ,size_1_id
            ,size_2_id
            ,cc_weeks
            ,cc_weeks_to_break
            ,CASE WHEN SUM(sales_units) < 0 THEN 0 ELSE SUM(sales_units) END AS units_sold
            ,(CASE WHEN SUM(sales_units) < 0 THEN 0 ELSE SUM(sales_units) END) / AVG(cc_weeks) AS units_sold_avg_weekly
        FROM cc_loc_wk_processed
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    ) units_sold
) WITH DATA
PRIMARY INDEX (half_idnt, loc_idnt, choice_id, size_1_id, size_2_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX (half_idnt, loc_idnt, choice_id, size_1_id, size_2_id)
    ,COLUMN(half, half_idnt, loc_idnt, groupid_frame, class_frame, choice_id)
    ,COLUMN(half_idnt)
    ,COLUMN(half, loc_idnt, groupid_frame, class_frame, choice_id)
    ON units_sold_totals;

DROP TABLE include_flag;
CREATE MULTISET VOLATILE TABLE include_flag AS (
SELECT
     half
    ,half_idnt
    ,loc_idnt
    ,groupid_frame
    ,class_frame
    ,choice_id
    ,units_sold_total
    ,cc_weeks_to_break
    ,sku_ct
    ,CASE WHEN sku_ct <= 1 THEN 0
          ELSE 1 
          END AS include_ind
FROM (
        SELECT
             half
            ,half_idnt
            ,loc_idnt
            ,groupid_frame
            ,class_frame
            ,choice_id
            ,MAX(units_sold_total) AS units_sold_total
            ,MAX(cc_weeks_to_break) AS cc_weeks_to_break
            ,COUNT(size_1_id || '~' || size_2_id) AS sku_ct
        FROM units_sold_totals
        GROUP BY 1,2,3,4,5,6
    ) criteria 
) WITH DATA
PRIMARY INDEX (half_idnt, loc_idnt, groupid_frame, choice_id)
ON COMMIT PRESERVE ROWS;

COLLECT STATS 
     PRIMARY INDEX (half_idnt, loc_idnt, groupid_frame, choice_id)
    ,COLUMN(half, loc_idnt, groupid_frame, class_frame, choice_id)
    ON include_flag;

CREATE MULTISET VOLATILE TABLE cc_loc_sku_units_sold AS (
SELECT
     mstr.price_lvl
    ,mstr.half
    ,mstr.half_idnt
    ,mstr.loc_idnt
    ,mstr.groupid_frame
    ,mstr.class_frame
    ,mstr.choice_id
    ,mstr.cc_weeks
    ,mstr.cc_weeks_to_break
    ,mstr.units_sold
    ,mstr.units_sold_avg_weekly
    ,mstr.units_sold_total
    ,mstr.units_sold_avg_weekly_total
    ,CASE WHEN mstr.units_sold_avg_weekly_total = 0 THEN 0
          ELSE 1.00 * mstr.units_sold_avg_weekly / mstr.units_sold_avg_weekly_total
          END AS units_sold_avg_weekly_ratio
    ,pr.department_id
    ,pr.class_id
    ,mstr.size_1_id
    ,mstr.size_2_id
    ,pr.size_1_rank
    ,pr.size_2_rank
    ,pr.jda_subclass_sc
    ,pr.jda_subclass_c
    ,pr.supplier_name
    ,top50.include_ind
    ,top50.sku_ct
    ,rank_cc.cc_rank_groupid
    ,rank_cc.cc_rank_class
FROM units_sold_totals mstr
JOIN (
        SELECT DISTINCT 
             choice_id
            ,price_lvl
            ,size_1_id
            ,size_2_id
            ,department_id
            ,class_id
            ,size_1_rank
            ,size_2_rank
            ,jda_subclass_sc
            ,jda_subclass_c
            ,supplier_name
        FROM product_lkup pr
    ) pr
  ON mstr.choice_id = pr.choice_id
 AND mstr.price_lvl = pr.price_lvl
 AND mstr.size_1_id = pr.size_1_id
 AND mstr.size_2_id = pr.size_2_id
JOIN include_flag top50
  ON mstr.groupid_frame = top50.groupid_frame
 AND mstr.class_frame = top50.class_frame
 AND mstr.choice_id = top50.choice_id
 AND mstr.half = top50.half
 AND mstr.half_idnt = top50.half_idnt
 AND mstr.loc_idnt = top50.loc_idnt
LEFT JOIN  (
            SELECT
                 half
                ,half_idnt
                ,loc_idnt
                ,groupid_frame
                ,class_frame
                ,choice_id
                ,ROW_NUMBER() OVER (PARTITION BY half, loc_idnt, groupid_frame ORDER BY units_sold_total DESC) AS cc_rank_groupid
                ,ROW_NUMBER() OVER (PARTITION BY half, loc_idnt, class_frame ORDER BY units_sold_total DESC) AS cc_rank_class
            FROM include_flag
            WHERE include_ind = 1
        ) rank_cc
  ON mstr.groupid_frame = rank_cc.groupid_frame
 AND mstr.class_frame = rank_cc.class_frame
 AND mstr.choice_id = rank_cc.choice_id
 AND mstr.half = rank_cc.half
 AND mstr.half_idnt = rank_cc.half_idnt
 AND mstr.loc_idnt = rank_cc.loc_idnt
    -- filter on sku_ct
WHERE top50.sku_ct > 1 
)WITH DATA 
PRIMARY INDEX (half_idnt, loc_idnt, choice_id, size_1_id, size_2_id, groupid_frame, class_frame)
ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE location_data_final AS (
SELECT DISTINCT
     mstr.price_lvl
    ,mstr.half
    ,mstr.half_idnt
    ,mstr.loc_idnt
    ,mstr.class_frame
    ,mstr.groupid_frame
    ,mstr.choice_id
    ,mstr.include_ind
    ,mstr.department_id
    ,mstr.class_id
    ,mstr.size_1_id
    ,mstr.size_2_id
    ,mstr.size_1_rank
    ,mstr.size_2_rank
    ,mstr.jda_subclass_sc
    ,mstr.jda_subclass_c
    ,mstr.supplier_name
    ,groupidinfo.included_cc_per_groupid
    ,classinfo.included_cc_per_class
    ,mstr.cc_rank_groupid
    ,mstr.cc_rank_class
    ,mstr.units_sold
    ,mstr.units_sold_avg_weekly
    ,mstr.units_sold_avg_weekly_ratio
    ,mstr.cc_weeks_to_break
    ,(SELECT COUNT(DISTINCT choice_id) AS choice_id FROM cc_loc_wk_processed) AS original_cc_count
    ,(SELECT COUNT(DISTINCT choice_id) choice_id FROM cc_loc_sku_units_sold WHERE include_ind = 1) AS used_in_curve_gen_cc_count
FROM cc_loc_sku_units_sold mstr
JOIN (
        SELECT
             half
            ,loc_idnt
            ,groupid_frame
            ,COUNT(DISTINCT CASE WHEN include_ind = 1 THEN choice_id ELSE NULL END) AS included_cc_per_groupid
        FROM cc_loc_sku_units_sold
        GROUP BY 1,2,3
    ) groupidinfo
  ON mstr.groupid_frame = groupidinfo.groupid_frame
 AND mstr.half = groupidinfo.half
 AND mstr.loc_idnt = groupidinfo.loc_idnt
JOIN (
        SELECT
             half
            ,loc_idnt
            ,class_frame
            ,COUNT(DISTINCT CASE WHEN include_ind = 1 THEN choice_id ELSE NULL END) AS included_cc_per_class
        FROM cc_loc_sku_units_sold
        GROUP BY 1,2,3
    ) classinfo
  ON mstr.class_frame = classinfo.class_frame
 AND mstr.half = classinfo.half
 AND mstr.loc_idnt = classinfo.loc_idnt
) WITH DATA 
UNIQUE PRIMARY INDEX (half_idnt, loc_idnt, groupid_frame, choice_id, size_1_id, size_2_id)
ON COMMIT PRESERVE ROWS;


/*
----------------------------------------------------------------------------------
--Final step: Insert data into baseline_data table
----------------------------------------------------------------------------------
*/

DELETE FROM {environment_schema}.location_size_sales_data{env_suffix} ALL;
INSERT INTO {environment_schema}.location_size_sales_data{env_suffix}
SELECT
     price_lvl
    ,half
    ,half_idnt
    ,loc_idnt
    ,class_frame
    ,groupid_frame
    ,choice_id AS cc
    ,include_ind
    ,department_id
    ,class_id
    ,size_1_id
    ,size_2_id
    ,size_1_rank
    ,size_2_rank
    ,included_cc_per_groupid
    ,included_cc_per_class
    ,cc_rank_groupid
    ,cc_rank_class
    ,units_sold
    ,units_sold_avg_weekly
    ,units_sold_avg_weekly_ratio
    ,CURRENT_TIMESTAMP AS rcd_update_timestamp
from location_data_final;

COLLECT STATS 
     COLUMN(half_idnt, loc_idnt, groupid_frame, cc, size_1_id, size_2_id) 
    ON {environment_schema}.location_size_sales_data{env_suffix};

-- end