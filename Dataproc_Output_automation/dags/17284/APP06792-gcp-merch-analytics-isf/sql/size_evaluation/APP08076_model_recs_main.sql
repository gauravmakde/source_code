/*
Name: Size Curves Evaluation Model Recs
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    t2dl_das_size T2DL_DAS_SIZE
                env_suffix '' or '_dev' tablesuffix for prod testing

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



CREATE TEMPORARY TABLE IF NOT EXISTS curve_dates
AS
SELECT insert_date,
 half,
  CASE
  WHEN LOWER(half) = LOWER('H1')
  THEN CAST(TRUNC(CAST(CASE
    WHEN SUBSTR(CAST(EXTRACT(YEAR FROM insert_date) + 1 AS STRING), 1, 4) || SUBSTR(half, 2, 1) = ''
    THEN '0'
    ELSE SUBSTR(CAST(EXTRACT(YEAR FROM insert_date) + 1 AS STRING), 1, 4) || SUBSTR(half, 2, 1)
    END AS FLOAT64)) AS INTEGER)
  WHEN LOWER(half) = LOWER('H2')
  THEN CAST(TRUNC(CAST(CASE
    WHEN SUBSTR(CAST(EXTRACT(YEAR FROM DATE_ADD(insert_date, INTERVAL 180 DAY)) AS STRING), 1, 4) || SUBSTR(half, 2, 1)
     = ''
    THEN '0'
    ELSE SUBSTR(CAST(EXTRACT(YEAR FROM DATE_ADD(insert_date, INTERVAL 180 DAY)) AS STRING), 1, 4) || SUBSTR(half, 2, 1)
    END AS FLOAT64)) AS INTEGER)
  ELSE NULL
  END AS half_idnt
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_baseline_hist AS a
WHERE generate_month >= 20221
 AND insert_date <> DATE '2024-05-02'
GROUP BY half,
 insert_date;


--COLLECT STATS        COLUMN(insert_date, half)      ,COLUMN(insert_date)      ,COLUMN(half)      ON curve_dates


-- DROP TABLE cluster_loc;


CREATE TEMPORARY TABLE IF NOT EXISTS cluster_loc AS 
WITH depts AS (SELECT dept_idnt
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy
  GROUP BY dept_idnt) 
  (SELECT *
  FROM (SELECT h.half_idnt,
       CASE
       WHEN LOWER(loc.price_lvl) = LOWER('op')
       THEN 210
       ELSE 110
       END AS channel_num,
      loc.loc_idnt AS store_num,
      loc.class_frame,
      loc.cluster_id
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_location_cluster_class_hist AS loc
      INNER JOIN curve_dates AS h ON loc.insert_date = h.insert_date AND LOWER(loc.half) = LOWER(h.half)
      INNER JOIN (SELECT DISTINCT *
       FROM depts AS d) AS t1 ON loc.department_id = t1.dept_idnt
     GROUP BY h.half_idnt,
      channel_num,
      store_num,
      loc.class_frame,
      loc.cluster_id
     QUALIFY (ROW_NUMBER() OVER (PARTITION BY h.half_idnt, channel_num, store_num, loc.class_frame ORDER BY count(*)
           DESC)) = 1
     UNION ALL
     SELECT h0.half_idnt,
       CASE
       WHEN LOWER(loc0.price_lvl) = LOWER('op')
       THEN 250
       ELSE 120
       END AS channel_num,
       CASE
       WHEN CASE
         WHEN LOWER(loc0.price_lvl) = LOWER('op')
         THEN 250
         ELSE 120
         END = 120
       THEN 808
       WHEN CASE
         WHEN LOWER(loc0.price_lvl) = LOWER('op')
         THEN 250
         ELSE 120
         END = 250
       THEN 828
       ELSE NULL
       END AS store_num,
      loc0.class_frame,
      9 AS cluster_id
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_location_cluster_class_hist AS loc0
      INNER JOIN curve_dates AS h0 ON loc0.insert_date = h0.insert_date AND LOWER(loc0.half) = LOWER(h0.half)
      INNER JOIN (SELECT DISTINCT *
       FROM depts AS d) AS t9 ON loc0.department_id = t9.dept_idnt
     GROUP BY h0.half_idnt,
      channel_num,
      store_num,
      loc0.class_frame,
      cluster_id));


CREATE TEMPORARY TABLE IF NOT EXISTS models_combined AS WITH depts AS (SELECT dept_idnt
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy
  GROUP BY dept_idnt) (SELECT COALESCE(t3.half_idnt, t8.half_idnt) AS half_idnt,
   COALESCE(t3.chnl_idnt, TRIM(t8.chnl_idnt)) AS chnl_idnt,
   COALESCE(t3.channel_num, t8.channel_num) AS channel_num,
   COALESCE(t8.cluster_id, 9) AS cluster_id,
    CASE
    WHEN t8.cluster_id = 1
    THEN 'Hard Left'
    WHEN t8.cluster_id = 2
    THEN 'Left'
    WHEN t8.cluster_id = 3
    THEN 'Left Mid'
    WHEN t8.cluster_id = 4
    THEN 'Mid'
    WHEN t8.cluster_id = 5
    THEN 'Right Mid'
    WHEN t8.cluster_id = 6
    THEN 'Right'
    WHEN t8.cluster_id = 7
    THEN 'Hard Right'
    WHEN t8.cluster_id = 8
    THEN 'Normal'
    ELSE 'Bulk'
    END AS cluster_name,
   COALESCE(t3.dept_idnt, t8.dept_idnt) AS dept_idnt,
   CAST(TRUNC(CAST(CASE
     WHEN COALESCE(t3.class_idnt, t8.class_idnt) = ''
     THEN '0'
     ELSE COALESCE(t3.class_idnt, t8.class_idnt)
     END AS FLOAT64)) AS INTEGER) AS class_idnt,
   COALESCE(t3.supplier_idnt, t8.supplier_idnt) AS supplier_idnt,
   COALESCE(t3.class_frame, t8.class_frame) AS class_frame,
   COALESCE(t3.groupid_frame, t8.groupid_frame) AS groupid_frame,
   COALESCE(t3.size_1_rank, t8.size_1_rank) AS size_1_rank,
   COALESCE(t3.size_1_id, t8.size_1_id) AS size_1_id,
   COALESCE(t3.size_2_id, t8.size_2_id) AS size_2_id,
   COALESCE(t3.med_bulk_ratio, 0) AS med_bulk_ratio,
   COALESCE(t8.med_cluster_ratio, 0) AS med_cluster_ratio
  FROM (SELECT DISTINCT d.half_idnt,
     a.chnl_idnt,
      CASE
      WHEN LOWER(a.chnl_idnt) = LOWER('N.COM')
      THEN 120
      WHEN LOWER(a.chnl_idnt) = LOWER('NRHL')
      THEN 250
      WHEN LOWER(a.chnl_idnt) = LOWER('RACK')
      THEN 210
      WHEN LOWER(a.chnl_idnt) = LOWER('FLS')
      THEN 110
      ELSE NULL
      END AS channel_num,
     9 AS cluster_id,
     a.department_id AS dept_idnt,
     a.class_id AS class_idnt,
     a.supplier_id AS supplier_idnt,
     a.class_frame,
     a.groupid_frame,
     a.size_1_rank,
     a.size_1_id,
     a.size_2_id,
     PERCENTILE_CONT(a.ratio, 0.5) OVER (PARTITION BY d.half_idnt, a.chnl_idnt, CASE
         WHEN LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN 120
         WHEN LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN 250
         WHEN LOWER(a.chnl_idnt) = LOWER('RACK')
         THEN 210
         WHEN LOWER(a.chnl_idnt) = LOWER('FLS')
         THEN 110
         ELSE NULL
         END, 9, a.department_id, a.class_id, a.supplier_id, a.class_frame, a.groupid_frame, CAST(a.size_1_rank AS BIGNUMERIC)
        , a.size_1_id, a.size_2_id) AS med_bulk_ratio
    FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_baseline_hist AS a
     INNER JOIN curve_dates AS d ON a.insert_date = d.insert_date AND LOWER(a.half) = LOWER(d.half)
     INNER JOIN (SELECT DISTINCT *
      FROM depts AS dt) AS t1 ON a.department_id = t1.dept_idnt) AS t3
   FULL JOIN (SELECT DISTINCT d0.half_idnt,
     a0.price_lvl,
      CASE
      WHEN LOWER(a0.price_lvl) = LOWER('op')
      THEN 'RACK'
      ELSE 'FLS'
      END AS chnl_idnt,
      CASE
      WHEN LOWER(CASE
         WHEN LOWER(a0.price_lvl) = LOWER('op')
         THEN 'RACK'
         ELSE 'FLS'
         END) = LOWER('N.COM')
      THEN 120
      WHEN LOWER(CASE
         WHEN LOWER(a0.price_lvl) = LOWER('op')
         THEN 'RACK'
         ELSE 'FLS'
         END) = LOWER('NRHL')
      THEN 250
      WHEN LOWER(CASE
         WHEN LOWER(a0.price_lvl) = LOWER('op')
         THEN 'RACK'
         ELSE 'FLS'
         END) = LOWER('RACK')
      THEN 210
      WHEN LOWER(CASE
         WHEN LOWER(a0.price_lvl) = LOWER('op')
         THEN 'RACK'
         ELSE 'FLS'
         END) = LOWER('FLS')
      THEN 110
      ELSE NULL
      END AS channel_num,
     a0.cluster_id,
     a0.department_id AS dept_idnt,
     a0.class_id AS class_idnt,
     a0.supplier_id AS supplier_idnt,
     a0.class_frame,
     a0.groupid_frame,
     a0.size_1_rank,
     a0.size_1_id,
     a0.size_2_id,
     PERCENTILE_CONT(a0.ratio, 0.5) OVER (PARTITION BY d0.half_idnt, a0.price_lvl, CASE
         WHEN LOWER(a0.price_lvl) = LOWER('op')
         THEN 'RACK'
         ELSE 'FLS'
         END, CASE
         WHEN LOWER(CASE
            WHEN LOWER(a0.price_lvl) = LOWER('op')
            THEN 'RACK'
            ELSE 'FLS'
            END) = LOWER('N.COM')
         THEN 120
         WHEN LOWER(CASE
            WHEN LOWER(a0.price_lvl) = LOWER('op')
            THEN 'RACK'
            ELSE 'FLS'
            END) = LOWER('NRHL')
         THEN 250
         WHEN LOWER(CASE
            WHEN LOWER(a0.price_lvl) = LOWER('op')
            THEN 'RACK'
            ELSE 'FLS'
            END) = LOWER('RACK')
         THEN 210
         WHEN LOWER(CASE
            WHEN LOWER(a0.price_lvl) = LOWER('op')
            THEN 'RACK'
            ELSE 'FLS'
            END) = LOWER('FLS')
         THEN 110
         ELSE NULL
         END, a0.cluster_id, a0.department_id, a0.class_id, a0.supplier_id, a0.class_frame, a0.groupid_frame, CAST(a0.size_1_rank AS BIGNUMERIC)
        , a0.size_1_id, a0.size_2_id) AS med_cluster_ratio
    FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_location_baseline_class_hist AS a0
     INNER JOIN curve_dates AS d0 ON a0.insert_date = d0.insert_date AND LOWER(a0.half) = LOWER(d0.half)
     INNER JOIN (SELECT DISTINCT *
      FROM depts AS dt) AS t6 ON a0.department_id = t6.dept_idnt) AS t8 ON t3.half_idnt = t8.half_idnt AND t3.channel_num
              = t8.channel_num AND LOWER(t3.class_frame) = LOWER(t8.class_frame) AND LOWER(t3.groupid_frame) = LOWER(t8
            .groupid_frame) AND LOWER(t3.chnl_idnt) = LOWER(TRIM(t8.chnl_idnt)) AND t3.dept_idnt = t8.dept_idnt AND
        LOWER(t3.class_idnt) = LOWER(t8.class_idnt) AND LOWER(t3.supplier_idnt) = LOWER(t8.supplier_idnt) AND LOWER(t3.size_1_id
       ) = LOWER(t8.size_1_id) AND LOWER(t3.size_2_id) = LOWER(t8.size_2_id));


--COLLECT STATISTICS       COLUMN(half_idnt, channel_num, cluster_id,dept_idnt, class_idnt,supplier_idnt, class_frame, groupid_frame, size_1_rank, size_1_id, size_2_id)     ,COLUMN(half_idnt, chnl_idnt, channel_num, cluster_id, groupid_frame)     ON models_combined


-- DROP TABLE models_size_1;


CREATE TEMPORARY TABLE IF NOT EXISTS models_size_1
AS
SELECT half_idnt,
 channel_num,
 cluster_id,
 TRIM(cluster_name) AS cluster_name,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 supplier_frame,
 class_frame,
 groupid_frame,
 frame,
 size_1_rank,
 size_1_id,
 bulk_size,
 SUM(bulk_size) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, class_idnt, supplier_idnt,
    groupid_frame, frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS bulk_frame,
  bulk_size / IF((SUM(bulk_size) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, class_idnt,
        supplier_idnt, groupid_frame, frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(bulk_size
    ) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, class_idnt, supplier_idnt, groupid_frame, frame
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS bulk_ratio,
 cluster_size,
 SUM(cluster_size) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, class_idnt, supplier_idnt,
    groupid_frame, frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS cluster_frame,
  cluster_size / IF((SUM(cluster_size) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, class_idnt,
        supplier_idnt, groupid_frame, frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0, NULL, SUM(cluster_size
    ) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, class_idnt, supplier_idnt, groupid_frame, frame
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS cluster_ratio
FROM (SELECT half_idnt,
   channel_num,
   cluster_id,
   cluster_name,
   dept_idnt,
   class_idnt,
   supplier_idnt,
    CASE
    WHEN LOWER(supplier_idnt) = LOWER('NA')
    THEN 'AllSuppliers'
    ELSE supplier_idnt
    END AS supplier_frame,
   class_frame,
   groupid_frame,
   REGEXP_EXTRACT_ALL(class_frame, '[^~~]+')[SAFE_OFFSET(2)] AS frame,
   size_1_rank,
   size_1_id,
   med_bulk_ratio,
   med_cluster_ratio,
   SUM(med_bulk_ratio) AS bulk_size,
   SUM(med_cluster_ratio) AS cluster_size
  FROM models_combined AS a
  GROUP BY half_idnt,
   channel_num,
   cluster_id,
   cluster_name,
   dept_idnt,
   class_idnt,
   supplier_idnt,
   supplier_frame,
   class_frame,
   groupid_frame,
   frame,
   size_1_rank,
   size_1_id,
   med_bulk_ratio,
   med_cluster_ratio) AS t0;


--COLLECT STATISTICS       COLUMN(half_idnt, channel_num, cluster_id,dept_idnt, class_idnt, supplier_idnt, class_frame, groupid_frame, frame, size_1_rank, size_1_id)     ,COLUMN(half_idnt, channel_num, cluster_id, groupid_frame)     ON models_size_1


-- DROP TABLE models_size_1_dept;


CREATE TEMPORARY TABLE IF NOT EXISTS models_size_1_dept
AS
SELECT half_idnt,
 channel_num,
 cluster_id,
 cluster_name,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 supplier_frame,
 class_frame,
 groupid_frame,
 frame,
 size_1_rank,
 size_1_id,
 size_bulk,
 SUM(size_bulk) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS frame_bulk,
  size_bulk / (SUM(size_bulk) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, frame RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS dept_bulk_ratio,
 size_cluster,
 SUM(size_cluster) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS frame_cluster,
  size_cluster / (SUM(size_cluster) OVER (PARTITION BY half_idnt, channel_num, cluster_id, dept_idnt, frame
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS dept_cluster_ratio
FROM (SELECT half_idnt,
   channel_num,
   cluster_id,
   cluster_name,
   dept_idnt,
   NULL AS class_idnt,
   SUBSTR('NA', 1, 20) AS supplier_idnt,
   'AllSuppliers' AS supplier_frame,
   'NA' AS class_frame,
   frame,
   size_1_rank,
   size_1_id,
   'NA' AS groupid_frame,
   bulk_ratio,
   cluster_ratio,
   SUM(bulk_ratio) AS size_bulk,
   SUM(cluster_ratio) AS size_cluster
  FROM models_size_1 AS a
  GROUP BY half_idnt,
   channel_num,
   cluster_id,
   cluster_name,
   dept_idnt,
   class_idnt,
   supplier_idnt,
   supplier_frame,
   class_frame,
   frame,
   size_1_rank,
   size_1_id,
   groupid_frame,
   bulk_ratio,
   cluster_ratio) AS t0;


--COLLECT STATISTICS       COLUMN(half_idnt, channel_num, cluster_id,dept_idnt, class_idnt, supplier_idnt, class_frame, groupid_frame, frame, size_1_rank, size_1_id)     ,COLUMN(half_idnt, channel_num, cluster_id, groupid_frame)     ON models_size_1_dept


-- -- DROP TABLE cluster_dept_loc;


CREATE TEMPORARY TABLE IF NOT EXISTS cluster_dept_loc AS WITH depts AS (SELECT dept_idnt
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy
  GROUP BY dept_idnt) (SELECT *
  FROM (SELECT h.half_idnt,
       CASE
       WHEN LOWER(loc.price_lvl) = LOWER('op')
       THEN 210
       ELSE 110
       END AS channel_num,
      loc.loc_idnt AS store_num,
      loc.department_id,
      REGEXP_EXTRACT_ALL(loc.class_frame, '[^~~]+')[SAFE_OFFSET(2)] AS frame,
      loc.cluster_id
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_location_cluster_class_hist AS loc
      INNER JOIN curve_dates AS h ON loc.insert_date = h.insert_date AND LOWER(loc.half) = LOWER(h.half)
      INNER JOIN (SELECT DISTINCT *
       FROM depts AS d) AS t1 ON loc.department_id = t1.dept_idnt
     GROUP BY h.half_idnt,
      channel_num,
      store_num,
      loc.department_id,
      frame,
      loc.cluster_id
     QUALIFY (ROW_NUMBER() OVER (PARTITION BY h.half_idnt, channel_num, store_num, loc.department_id, frame ORDER BY
           count(*) DESC)) = 1
     UNION ALL
     SELECT h0.half_idnt,
       CASE
       WHEN LOWER(loc0.price_lvl) = LOWER('op')
       THEN 250
       ELSE 120
       END AS channel_num,
       CASE
       WHEN CASE
         WHEN LOWER(loc0.price_lvl) = LOWER('op')
         THEN 250
         ELSE 120
         END = 120
       THEN 808
       WHEN CASE
         WHEN LOWER(loc0.price_lvl) = LOWER('op')
         THEN 250
         ELSE 120
         END = 250
       THEN 828
       ELSE NULL
       END AS store_num,
      loc0.department_id,
      REGEXP_EXTRACT_ALL(loc0.class_frame, '[^~~]+')[SAFE_OFFSET(2)] AS frame,
      9 AS cluster_id
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curve_location_cluster_class_hist AS loc0
      INNER JOIN curve_dates AS h0 ON loc0.insert_date = h0.insert_date AND LOWER(loc0.half) = LOWER(h0.half)
      INNER JOIN (SELECT DISTINCT *
       FROM depts AS d) AS t9 ON loc0.department_id = t9.dept_idnt
     GROUP BY h0.half_idnt,
      channel_num,
      store_num,
      loc0.department_id,
      frame,
      cluster_id));


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curves_model_rec;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curves_model_rec
SELECT half_idnt,
  channel_num,
  store_num,
  cluster_id,
  cluster_name,
  dept_idnt,
  supplier_frame,
  class_frame,
  groupid_frame,
  frame,
  size_1_rank,
  size_1_id,
  bulk_ratio,
  cluster_ratio,
  update_timestamp,
  update_timestamp_tz
 FROM (SELECT a.half_idnt,
     a.channel_num,
     c.store_num,
     a.cluster_id,
     a.cluster_name,
     a.dept_idnt,
     a.supplier_frame,
     a.class_frame,
     a.groupid_frame,
     a.frame,
     a.size_1_rank,
     a.size_1_id,
     a.bulk_ratio,
     a.cluster_ratio,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS update_timestamp,
      `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() AS update_timestamp_tz
    FROM models_size_1 AS a
     LEFT JOIN cluster_loc AS c ON a.half_idnt = c.half_idnt AND a.channel_num = c.channel_num AND LOWER(a.class_frame)
        = LOWER(c.class_frame) AND a.cluster_id = c.cluster_id
    UNION DISTINCT
    SELECT a0.half_idnt,
     a0.channel_num,
     c0.store_num,
     a0.cluster_id,
     a0.cluster_name,
     CAST(TRUNC(a0.dept_idnt) AS INTEGER) AS dept_idnt,
     'AllSuppliers' AS supplier_frame,
     'AllClasses' AS class_frame,
     SUBSTR(TRIM(a0.groupid_frame), 1, 50) AS groupid_frame,
     a0.frame,
     a0.size_1_rank,
     a0.size_1_id,
     a0.dept_bulk_ratio AS bulk_ratio,
     a0.dept_cluster_ratio AS cluster_ratio,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS update_timestamp,
     `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() AS update_timestamp_tz
    FROM models_size_1_dept AS a0
     LEFT JOIN cluster_dept_loc AS c0 ON a0.half_idnt = c0.half_idnt AND a0.channel_num = c0.channel_num AND a0.dept_idnt
          = c0.department_id AND LOWER(a0.frame) = LOWER(c0.frame) AND a0.cluster_id = c0.cluster_id) AS t1
 EXCEPT DISTINCT
 SELECT half_idnt,
  channel_num,
  store_num,
  cluster_id,
  cluster_name,
  dept_idnt,
  supplier_frame,
  class_frame,
  groupid_frame,
  frame,
  size_1_rank,
  size_1_id,
  bulk_ratio,
  cluster_ratio,
  update_timestamp,
  update_timestamp_tz
 FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curves_model_rec;

