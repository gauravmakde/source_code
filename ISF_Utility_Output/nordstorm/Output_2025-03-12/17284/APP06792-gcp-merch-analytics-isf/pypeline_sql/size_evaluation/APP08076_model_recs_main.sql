/*
Name: Size Curves Evaluation Model Recs
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    {environment_schema} T2DL_DAS_SIZE
                {env_suffix} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 6/12/24
Date Updated: 7/15/24

Creates: 
    - size_curves_model_rec

Dependancies: 
    - size_curve_location_baseline_class_hist
    - size_curve_baseline_hist
    - size_curve_location_cluster_class_hist
    - po_types
    - supp_size_hierarchy
    - location_model_actuals_vw
*/

-- Collect stats for views
COLLECT STATS
      COLUMN(half, department_id, class_frame,groupid_frame, size_1_id, size_2_id, supplier_id, class_id) 
     ,COLUMN(insert_date, generate_month) 
     ,COLUMN(department_id)
     ,COLUMN(department_id, insert_date,generate_month)
     ,COLUMN(department_id, class_frame,groupid_frame, size_1_id, size_2_id, supplier_id, class_id)
     ,COLUMN(price_lvl, cluster_id, class_frame,groupid_frame)
     ,COLUMN(price_lvl, cluster_id, department_id, class_frame, groupid_frame, size_1_rank, size_1_id, size_2_id, supplier_id, class_id)
     ,COLUMN(price_lvl, cluster_id, class_frame)
     ,COLUMN(insert_date)
     ,COLUMN(price_lvl, cluster_id, department_id, class_frame, insert_date)
     ,COLUMN(department_id, class_frame, groupid_frame, size_1_id, size_2_id, supplier_id, class_id)
     ,COLUMN(half, insert_date)
     ,COLUMN(half, department_id, insert_date)
    ON {environment_schema}.size_curve_location_baseline_class_hist;
    
COLLECT STATS
     COLUMN(half, chnl_idnt, department_id, class_frame, groupid_frame, size_1_id, size_2_id, supplier_id,class_id) 
     ,COLUMN(generate_month, insert_date) 
     ,COLUMN(department_id)
     ,COLUMN(generate_month)
     ,COLUMN(department_id, generate_month, insert_date)
     ,COLUMN(insert_date)
     ,COLUMN(chnl_idnt, class_frame, groupid_frame)
     ,COLUMN(chnl_idnt, department_id, class_frame, groupid_frame, size_1_rank, size_1_id, size_2_id, supplier_id, class_id)
     ,COLUMN(chnl_idnt, department_id, class_frame, groupid_frame, size_1_id, size_2_id, supplier_id, class_id)
     ,COLUMN(half, insert_date)
     ,COLUMN(half, department_id, insert_date)
    ON {environment_schema}.size_curve_baseline_hist;
    
COLLECT STATS
      COLUMN(cluster_id, class_frame, half)
     ,COLUMN(half, cluster_id, class_frame, insert_date)
     ,COLUMN(insert_date) 
     ,COLUMN(loc_idnt)
     ,COLUMN(cluster_id, class_frame, price_lvl)
     ,COLUMN(loc_idnt, class_frame, price_lvl)
     ,COLUMN(loc_idnt, cluster_id, class_frame, price_lvl)
     ,COLUMN(half, insert_date)
    ON {environment_schema}.size_curve_location_cluster_class_hist;

COLLECT STATISTICS 
     COLUMN (po_type_desc) 
    ,COLUMN (po_type_code, po_type_desc)
    ,COLUMN (po_type_code)
     ON {environment_schema}.po_types;


CREATE MULTISET VOLATILE TABLE curve_dates AS (
SELECT
     insert_date
     ,half
     ,CASE WHEN a.half = 'H1' THEN CAST(CAST(year(a.insert_date) + 1 AS varchar(4)) || substr(a.half,2,1) AS INTEGER)
          WHEN a.half = 'H2' THEN CAST(CAST(year(a.insert_date + 180) AS varchar(4)) || substr(a.half,2,1) AS INTEGER)
          END AS half_idnt
FROM {environment_schema}.size_curve_baseline_hist a
WHERE generate_month >= 20221
  AND insert_date <> '2024-05-02'
GROUP BY 1,2
) WITH DATA 
PRIMARY INDEX(insert_date, half)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
      COLUMN(insert_date, half)
     ,COLUMN(insert_date)
     ,COLUMN(half)
     ON curve_dates;

-- DROP TABLE cluster_loc;
CREATE MULTISET VOLATILE TABLE cluster_loc AS (
WITH depts AS (
SELECT DISTINCT
    dept_idnt
FROM {environment_schema}.supp_size_hierarchy
)
SELECT
     h.half_idnt
    ,CASE WHEN price_lvl = 'op' THEN 210 ELSE 110 END AS channel_num
    ,loc_idnt AS store_num
    ,class_frame
    ,cluster_id
FROM {environment_schema}.size_curve_location_cluster_class_hist loc
JOIN curve_dates h
  ON loc.insert_date = h.insert_date
 AND loc.half = h.half
JOIN depts d
  ON d.dept_idnt = loc.department_id
GROUP BY 1,2,3,4,5
QUALIFY ROW_NUMBER() OVER (PARTITION BY half_idnt, channel_num, store_num, class_frame ORDER BY COUNT(*) DESC) = 1
 
UNION ALL
 
SELECT
     h.half_idnt
    ,CASE WHEN price_lvl = 'op' THEN 250 ELSE 120 END AS channel_num
    ,CASE WHEN channel_num = 120 THEN 808
          WHEN channel_num = 250 THEN 828
          END AS store_num
    ,class_frame
    ,9 AS cluster_id
FROM {environment_schema}.size_curve_location_cluster_class_hist loc
JOIN curve_dates h
  ON loc.insert_date = h.insert_date
 AND loc.half = h.half
JOIN depts d
  ON d.dept_idnt = loc.department_id
GROUP BY 1,2,3,4,5
) WITH DATA
PRIMARY INDEX(half_idnt, channel_num, class_frame, cluster_id)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE models_combined AS (
WITH depts AS (
SELECT DISTINCT
    dept_idnt
FROM {environment_schema}.supp_size_hierarchy
),
bulk AS (
    SELECT
         d.half_idnt
        ,a.chnl_idnt
        ,CASE WHEN chnl_idnt = 'N.COM' THEN 120
              WHEN chnl_idnt = 'NRHL' THEN 250
              WHEN chnl_idnt = 'RACK' THEN 210
              WHEN chnl_idnt = 'FLS' THEN 110
              END AS channel_num
        ,9 AS cluster_id
        ,a.department_id AS dept_idnt
        ,a.class_id AS class_idnt
        ,a.supplier_id AS supplier_idnt
        ,a.class_frame
        ,a.groupid_frame
        ,a.size_1_rank
        ,a.size_1_id
        ,a.size_2_id
        ,MEDIAN(a.ratio) AS med_bulk_ratio
    FROM {environment_schema}.size_curve_baseline_hist a 
    JOIN curve_dates d
      ON a.insert_date = d.insert_date
     AND a.half = d.half
    JOIN depts dt
      ON dt.dept_idnt = a.department_id
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
mod_cluster AS (
    SELECT
         d.half_idnt
        ,a.price_lvl
        ,CASE WHEN a.price_lvl = 'op' THEN 'RACK' ELSE 'FLS' END AS chnl_idnt
        ,CASE WHEN chnl_idnt = 'N.COM' THEN 120
              WHEN chnl_idnt = 'NRHL' THEN 250
              WHEN chnl_idnt = 'RACK' THEN 210
              WHEN chnl_idnt = 'FLS' THEN 110
              END AS channel_num
        ,a.cluster_id
        ,a.department_id AS dept_idnt
        ,a.class_id AS class_idnt
        ,a.supplier_id AS supplier_idnt
        ,a.class_frame
        ,a.groupid_frame
        ,a.size_1_rank
        ,a.size_1_id
        ,a.size_2_id
        ,MEDIAN(a.ratio) AS med_cluster_ratio
    FROM {environment_schema}.size_curve_location_baseline_class_hist a 
    JOIN curve_dates d
      ON a.insert_date = d.insert_date
     AND a.half = d.half
    JOIN depts dt
      ON dt.dept_idnt = a.department_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
SELECT
     COALESCE(a.half_idnt, m.half_idnt) AS half_idnt
    ,COALESCE(a.chnl_idnt, m.chnl_idnt) AS chnl_idnt
    ,COALESCE(a.channel_num, m.channel_num) AS channel_num
    ,COALESCE(m.cluster_id, 9) AS cluster_id
    ,CASE WHEN m.cluster_id = 1 THEN 'Hard Left'
          WHEN m.cluster_id = 2 THEN 'Left'
          WHEN m.cluster_id = 3 THEN 'Left Mid'
          WHEN m.cluster_id = 4 THEN 'Mid'
          WHEN m.cluster_id = 5 THEN 'Right Mid'
          WHEN m.cluster_id = 6 THEN 'Right'
          WHEN m.cluster_id = 7 THEN 'Hard Right'
          WHEN m.cluster_id = 8 THEN 'Normal'
          ELSE 'Bulk'
          END AS cluster_name
    ,COALESCE(a.dept_idnt, m.dept_idnt) AS dept_idnt
    ,CAST(COALESCE(a.class_idnt, m.class_idnt) AS INT) AS class_idnt
    ,COALESCE(a.supplier_idnt, m.supplier_idnt) AS supplier_idnt
    ,COALESCE(a.class_frame, m.class_frame) class_frame
    ,COALESCE(a.groupid_frame, m.groupid_frame) AS groupid_frame
    ,COALESCE(a.size_1_rank, m.size_1_rank) AS size_1_rank
    ,COALESCE(a.size_1_id, m.size_1_id) AS size_1_id
    ,COALESCE(a.size_2_id, m.size_2_id) AS size_2_id
    ,COALESCE(a.med_bulk_ratio, 0) AS med_bulk_ratio
    ,COALESCE(m.med_cluster_ratio, 0) AS med_cluster_ratio
FROM bulk a
FULL OUTER JOIN mod_cluster m
  ON a.half_idnt = m.half_idnt
 AND a.channel_num = m.channel_num
 AND a.class_frame = m.class_frame
 AND a.groupid_frame = m.groupid_frame
 AND a.chnl_idnt = m.chnl_idnt
 AND a.dept_idnt = m.dept_idnt
 AND a.class_idnt = m.class_idnt
 AND a.supplier_idnt = m.supplier_idnt
 AND a.size_1_id = m.size_1_id
 AND a.size_2_id = m.size_2_id
) WITH DATA
PRIMARY INDEX(
     half_idnt
    ,channel_num
    ,cluster_id
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,class_frame
    ,groupid_frame
    ,size_1_rank
    ,size_1_id
    ,size_2_id)
ON COMMIT PRESERVE ROWS 
;

COLLECT STATISTICS 
     COLUMN(half_idnt, channel_num, cluster_id,dept_idnt, class_idnt,supplier_idnt, class_frame, groupid_frame, size_1_rank, size_1_id, size_2_id)
    ,COLUMN(half_idnt, chnl_idnt, channel_num, cluster_id, groupid_frame)
    ON models_combined;
    

-- DROP TABLE models_size_1;
CREATE MULTISET VOLATILE TABLE models_size_1 AS (
    SELECT 
         a.half_idnt
        ,a.channel_num
        ,a.cluster_id
        ,a.cluster_name 
        ,a.dept_idnt
        ,a.class_idnt
        ,a.supplier_idnt
        ,CASE WHEN a.supplier_idnt = 'NA' THEN 'AllSuppliers'
              ELSE a.supplier_idnt END AS supplier_frame
        ,a.class_frame
        ,a.groupid_frame
        ,STRTOK(a.class_frame, '~~', 3) AS frame
        ,a.size_1_rank
        ,a.size_1_id
        ,SUM(med_bulk_ratio) AS bulk_size
        ,SUM(bulk_size) OVER (PARTITION BY a.half_idnt, a.channel_num, a.cluster_id, a.dept_idnt, a.class_idnt, a.supplier_idnt, a.groupid_frame, frame) bulk_frame
        ,bulk_size / NULLIFZERO(bulk_frame) AS bulk_ratio
        ,SUM(med_cluster_ratio) AS cluster_size
        ,SUM(cluster_size) OVER (PARTITION BY a.half_idnt, a.channel_num,a.cluster_id, a.dept_idnt, a.class_idnt, a.supplier_idnt, a.groupid_frame, frame) cluster_frame
        ,cluster_size / NULLIFZERO(cluster_frame) AS cluster_ratio
    FROM models_combined a
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
PRIMARY INDEX(
     half_idnt
    ,channel_num
    ,cluster_id
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,class_frame
    ,groupid_frame
    ,frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS 
;

COLLECT STATISTICS 
     COLUMN(half_idnt, channel_num, cluster_id,dept_idnt, class_idnt, supplier_idnt, class_frame, groupid_frame, frame, size_1_rank, size_1_id)
    ,COLUMN(half_idnt, channel_num, cluster_id, groupid_frame)
    ON models_size_1;
    

-- DROP TABLE models_size_1_dept;
CREATE MULTISET VOLATILE TABLE models_size_1_dept AS (
SELECT
      half_idnt
     ,channel_num
     ,cluster_id
     ,cluster_name
     ,dept_idnt
     ,NULL AS class_idnt
     ,CAST('NA' AS VARCHAR(20)) AS supplier_idnt
     ,'AllSuppliers' AS supplier_frame
     ,'NA' AS class_frame
     ,'NA' AS groupid_frame
     ,frame
     ,size_1_rank
     ,size_1_id
     ,SUM(bulk_ratio) AS size_bulk
     ,SUM(size_bulk) OVER (PARTITION BY a.half_idnt, a.channel_num, cluster_id, a.dept_idnt, a.frame) AS frame_bulk
     ,size_bulk / frame_bulk AS dept_bulk_ratio
     ,SUM(cluster_ratio) AS size_cluster
     ,SUM(size_cluster) OVER (PARTITION BY a.half_idnt, a.channel_num, cluster_id, a.dept_idnt, a.frame) AS frame_cluster
     ,size_cluster / frame_cluster AS dept_cluster_ratio
FROM models_size_1 a
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)  WITH DATA
PRIMARY INDEX(
     half_idnt
    ,channel_num
    ,cluster_id
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,class_frame
    ,groupid_frame
    ,frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS 
;


COLLECT STATISTICS 
     COLUMN(half_idnt, channel_num, cluster_id,dept_idnt, class_idnt, supplier_idnt, class_frame, groupid_frame, frame, size_1_rank, size_1_id)
    ,COLUMN(half_idnt, channel_num, cluster_id, groupid_frame)
    ON models_size_1_dept;
    
-- -- DROP TABLE cluster_dept_loc;
CREATE MULTISET VOLATILE TABLE cluster_dept_loc AS (
WITH depts AS (
SELECT DISTINCT
    dept_idnt
FROM {environment_schema}.supp_size_hierarchy
)
SELECT
     h.half_idnt
    ,CASE WHEN price_lvl = 'op' THEN 210 ELSE 110 END AS channel_num
    ,loc_idnt AS store_num
    ,department_id 
    ,STRTOK(class_frame, '~~', 3) AS frame
    ,cluster_id
FROM {environment_schema}.size_curve_location_cluster_class_hist loc
JOIN curve_dates h
  ON loc.insert_date = h.insert_date
 AND loc.half = h.half
JOIN depts d
  ON d.dept_idnt = loc.department_id
GROUP BY 1,2,3,4,5,6
QUALIFY ROW_NUMBER() OVER (PARTITION BY half_idnt, channel_num, store_num, department_id, frame ORDER BY COUNT(*) DESC) = 1
 
UNION ALL
 
SELECT
     h.half_idnt
    ,CASE WHEN price_lvl = 'op' THEN 250 ELSE 120 END AS channel_num
    ,CASE WHEN channel_num = 120 THEN 808
          WHEN channel_num = 250 THEN 828
          END AS store_num
    ,department_id
    ,STRTOK(class_frame, '~~', 3) AS frame
    ,9 AS cluster_id
FROM {environment_schema}.size_curve_location_cluster_class_hist loc
JOIN curve_dates h
  ON loc.insert_date = h.insert_date
 AND loc.half = h.half
JOIN depts d
  ON d.dept_idnt = loc.department_id
GROUP BY 1,2,3,4,5,6
) WITH DATA
PRIMARY INDEX(half_idnt, channel_num, department_id, frame, cluster_id)
ON COMMIT PRESERVE ROWS
;
    

DELETE FROM {environment_schema}.size_curves_model_rec;
INSERT INTO {environment_schema}.size_curves_model_rec
SELECT 
     a.half_idnt
    ,a.channel_num
    ,c.store_num 
    ,a.cluster_id
    ,a.cluster_name 
    ,a.dept_idnt
    ,a.supplier_frame
    ,a.class_frame
    ,a.groupid_frame
    ,a.frame
    ,a.size_1_rank
    ,a.size_1_id
    ,a.bulk_ratio
    ,a.cluster_ratio
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM models_size_1 a
LEFT JOIN cluster_loc c
  ON a.half_idnt = c.half_idnt
 AND a.channel_num = c.channel_num
 AND a.class_frame = c.class_frame
 AND a.cluster_id = c.cluster_id

UNION ALL 

SELECT 
     a.half_idnt
    ,a.channel_num
    ,c.store_num 
    ,CAST(a.cluster_id AS INTEGER) AS cluster_id
    ,a.cluster_name 
    ,CAST(a.dept_idnt AS INTEGER) AS dept_idnt
    ,'AllSuppliers' AS supplier_frame
    ,'AllClasses' AS class_frame
    ,CAST(a.groupid_frame AS VARCHAR(50)) AS groupid_frame
    ,a.frame
    ,a.size_1_rank
    ,a.size_1_id
    ,a.dept_bulk_ratio AS bulk_ratio
    ,a.dept_cluster_ratio AS cluster_ratio
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM models_size_1_dept a
LEFT JOIN cluster_dept_loc c
  ON a.half_idnt = c.half_idnt
 AND a.channel_num = c.channel_num
 AND a.dept_idnt = c.department_id
 AND a.frame = c.frame
 AND a.cluster_id = c.cluster_id
 ;

COLLECT STATS 
     COLUMN(half_idnt, channel_num, cluster_id, dept_idnt, supplier_frame, class_frame, groupid_frame, size_1_rank, size_1_id)
    ,COLUMN(store_num, dept_idnt, frame, size_1_id)
    ,COLUMN(channel_num, store_num, dept_idnt, supplier_frame, class_frame, frame, size_1_id) 
    ,COLUMN(half_idnt, channel_num ,store_num,dept_idnt ,frame) 
    ,COLUMN(store_num, dept_idnt, class_frame)
    ,COLUMN(store_num, cluster_id, cluster_name, dept_idnt, class_frame) 
    ,COLUMN(half_idnt, channel_num, store_num, dept_idnt, frame, size_1_rank, size_1_id) 
    ,COLUMN(half_idnt) 
    ,COLUMN(store_num ,dept_idnt, supplier_frame, class_frame)
    ,COLUMN(store_num ,dept_idnt,supplier_frame, class_frame ,frame)
    ,COLUMN(cluster_id)
    ,COLUMN(channel_num, store_num, class_frame, size_1_id) 
    ,COLUMN(store_num, class_frame, supplier_frame)
    ON {environment_schema}.size_curves_model_rec;