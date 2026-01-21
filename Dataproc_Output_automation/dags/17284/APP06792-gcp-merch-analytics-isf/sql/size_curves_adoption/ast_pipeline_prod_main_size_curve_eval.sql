/*
ast_pipeline_prod_main_size_curve_eval.sql
Author: Paria Avij
project: Size curve evaluation
Purpose: Backend supporting size curve evaluation dashboard
Date CREATEd: 12/20/23
Datalab: t2dl_das_size,

*/
--CREATE a product hierarchy
--drop TABLE hierarchy_; 



CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy_
CLUSTER BY sku_idnt
AS
SELECT DISTINCT h.sku_idnt,
   h.rms_style_num || '~~' || h.color_num AS choice_id,
 m.supp_size,
 m.size_1_idnt,
 m.size_2_idnt,
 h.dept_desc,
 h.dept_idnt,
 h.class_idnt,
 h.class_desc,
 b.supplier_name,
 h.supplier_idnt,
 b.groupid,
 b.groupid_size,
 CONCAT(b.groupid, '~~', REGEXP_EXTRACT_ALL(b.size_1_frame, '[^_]+')[SAFE_OFFSET(0)]) AS groupid_frame,
 CONCAT(TRIM(FORMAT('%11d', b.department_id)), '~~', TRIM(FORMAT('%11d', b.class_id)), '~~', REGEXP_EXTRACT_ALL(b.size_1_frame
   , '[^_]+')[SAFE_OFFSET(0)]) AS class_frame,
 b.size_1_rank,
 b.size_1_frame,
 b.size_2_rank
FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h
 INNER JOIN (SELECT DISTINCT sku_idnt,
   supp_size,
   size_1_idnt,
   size_2_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_ma_bado.prod_sku_sty_cur_lkup_vw AS h) AS m ON LOWER(h.sku_idnt) = LOWER(m.sku_idnt) AND LOWER(h.channel_country
    ) = LOWER('US')
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_size.size_sku_groupid AS b ON LOWER(b.sku_id) = LOWER(h.sku_idnt) AND h.dept_idnt = b.department_id;


CREATE TEMPORARY TABLE IF NOT EXISTS na_size_2_map
CLUSTER BY department_id, size_2_id_old
AS
SELECT department_id,
 department_description,
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
        END) AS NUMERIC) / COUNT(DISTINCT groupid) <= 0.1 AND CAST(COUNT(DISTINCT CASE
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
FROM `{{params.gcp_project_id}}`.t2dl_das_size.size_sku_groupid
GROUP BY department_id,
 department_description;


--collect statistics      primary index (department_id,size_2_id_old)     ,column(department_id)     ,column(size_2_id_old)      on na_size_2_map


CREATE TEMPORARY TABLE IF NOT EXISTS product_sku
CLUSTER BY sku_idnt, choice_id, size_1_idnt, size_2_idnt
AS
SELECT DISTINCT a.sku_idnt,
 a.choice_id,
 a.supp_size,
 a.size_1_idnt,
 COALESCE(b.size_2_id_new, a.size_2_idnt, 'R') AS size_2_idnt,
 a.dept_desc,
 a.dept_idnt,
 a.class_idnt,
 a.class_desc,
 a.supplier_name,
 a.supplier_idnt,
 c.div_desc,
 a.groupid,
 a.groupid_size,
 a.groupid_frame,
 a.class_frame,
 a.size_1_rank,
 a.size_1_frame,
 a.size_2_rank
FROM hierarchy_ AS a
 LEFT JOIN na_size_2_map AS b 
 ON a.dept_idnt = b.department_id 
 AND LOWER(a.size_2_idnt) = LOWER(b.size_2_id_old)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS c ON a.dept_idnt = c.dept_num AND LOWER(a.sku_idnt) = LOWER(c.rms_sku_num
    );


--collect statistics      primary index (sku_idnt,choice_id,size_1_idnt, size_2_idnt)     ,column(sku_idnt)     ,column(choice_id)     ,column(size_1_idnt)     ,column(size_2_idnt)     on product_sku


CREATE TEMPORARY TABLE IF NOT EXISTS hist_date
CLUSTER BY week_idnt
AS
SELECT week_idnt,
 month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
    'FY' || SUBSTR(SUBSTR(CAST(fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || quarter_abrv AS quarter,
 fiscal_year_num,
 fiscal_halfyear_num,
 half_label,
 week_start_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE fiscal_year_num BETWEEN 2021 AND (SELECT fiscal_year_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE)
 AND fiscal_week_num < (SELECT fiscal_week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE)
GROUP BY week_idnt,
 month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 quarter,
 fiscal_year_num,
 fiscal_halfyear_num,
 half_label,
 week_start_day_date;


--collect stats      primary index (WEEK_IDNT)      ,column(WEEK_IDNT)      ,column(MONTH_IDNT)      ,column(QUARTER_IDNT)      ,column(FISCAL_YEAR_NUM)      on hist_date


CREATE TEMPORARY TABLE IF NOT EXISTS receipts_half
CLUSTER BY sku_idnt, choice_id, size_1_idnt, size_2_idnt
AS
SELECT t9.choice_id,
 t9.sku_idnt,
 t9.supp_size,
 t9.size_1_idnt,
 t9.size_2_idnt,
 t9.channel_num,
 t9.half,
 t9.first_receipt_week,
 t9.sku_recipts_u,
 SUM(t9.sku_recipts_u) OVER (PARTITION BY t9.choice_id, t9.channel_num, t9.half RANGE BETWEEN UNBOUNDED PRECEDING AND
  UNBOUNDED FOLLOWING) AS choice_receipt_u,
 SUM(1) OVER (PARTITION BY t9.choice_id, t9.channel_num, t9.half RANGE BETWEEN UNBOUNDED PRECEDING AND
  UNBOUNDED FOLLOWING) AS skus,
 COALESCE(t9.sku_recipts_u / NULLIF(CAST(SUM(t9.sku_recipts_u) OVER (PARTITION BY t9.choice_id, t9.channel_num, t9.half RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC), 0), 0) AS pct_size
FROM (SELECT h.choice_id,
   a.sku_idnt,
   h.supp_size,
   h.size_1_idnt,
   h.size_2_idnt,
   a.channel_num,
   a.half,
   a.week_num,
   a.recipts_u,
   MIN(a.week_num) AS first_receipt_week,
   SUM(a.recipts_u) AS sku_recipts_u
  FROM (SELECT j.sku_idnt,
     j.channel_num,
     j.week_num,
     j.recipts_u,
     c.fiscal_halfyear_num AS half
    FROM (SELECT sku_idnt,
        channel_num,
        MIN(week_num) AS week_num,
        SUM(receipt_po_units) AS recipts_u
       FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact AS rcpt
       WHERE (receipt_po_units > 0 OR receipt_rsk_units > 0)
        AND channel_num IN (120, 110, 250, 210)
       GROUP BY sku_idnt,
        channel_num
       UNION ALL
       SELECT SUBSTR(sku_idnt, 1, 10) AS sku_idnt,
        channel_num,
        MIN(week_num) AS week_num,
        SUM(receipt_po_units) AS recipts_u
       FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm AS rcpt
       WHERE receipt_po_units > 0
        AND channel_num IN (120, 110, 250, 210)
       GROUP BY sku_idnt,
        channel_num) AS j
     INNER JOIN hist_date AS c ON j.week_num = c.week_idnt) AS a
   INNER JOIN product_sku AS h ON LOWER(a.sku_idnt) = LOWER(h.sku_idnt)
  GROUP BY a.sku_idnt,
   a.channel_num,
   a.week_num,
   a.recipts_u,
   a.half,
   h.choice_id,
   h.supp_size,
   h.size_1_idnt,
   h.size_2_idnt) AS t9;


--collect statistics      primary index (sku_idnt,choice_id,size_1_idnt, size_2_idnt)     ,column(sku_idnt)     ,column(choice_id)     ,column(size_1_idnt)     ,column(size_2_idnt)     on receipts_half


CREATE TEMPORARY TABLE IF NOT EXISTS receipt_1b
CLUSTER BY half, chnl_idnt, groupid_frame, size_1_id
AS
SELECT half,
 chnl_idnt,
 department_id,
 groupid_frame,
 size_1_rank,
 size_1_id,
 size_2_id,
 SUM(sku_recipts_u) AS sku_recipts_un,
 MIN(choice_receipt_u_all) AS choice_receipt_un_all,
 COALESCE(SUM(sku_recipts_u) / NULLIF(CAST(MIN(choice_receipt_u_all) AS NUMERIC), 0), 0) AS pct_size2
FROM (SELECT r.half,
    CASE
    WHEN r.channel_num = 110
    THEN 'FLS'
    WHEN r.channel_num = 210
    THEN 'RACK'
    WHEN r.channel_num = 250
    THEN 'NRHL'
    WHEN r.channel_num = 120
    THEN 'N.COM'
    ELSE NULL
    END AS chnl_idnt,
   b.dept_idnt AS department_id,
   b.groupid_frame,
   b.size_1_rank,
   b.size_1_idnt AS size_1_id,
   b.size_2_idnt AS size_2_id,
   r.sku_recipts_u,
   SUM(r.sku_recipts_u) OVER (PARTITION BY CASE
       WHEN r.channel_num = 110
       THEN 'FLS'
       WHEN r.channel_num = 210
       THEN 'RACK'
       WHEN r.channel_num = 250
       THEN 'NRHL'
       WHEN r.channel_num = 120
       THEN 'N.COM'
       ELSE NULL
       END, r.half, b.groupid_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS choice_receipt_u_all
  FROM receipts_half AS r
   INNER JOIN product_sku AS b ON LOWER(r.sku_idnt) = LOWER(b.sku_idnt)) AS a
GROUP BY half,
 chnl_idnt,
 department_id,
 groupid_frame,
 size_1_rank,
 size_1_id,
 size_2_id;


--collect statistics      primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)     ,column(half)     ,column(chnl_idnt)     ,column(groupid_frame)     ,column(size_1_id)     ,column(size_2_id)     on receipt_1b

CREATE TEMPORARY TABLE IF NOT EXISTS size_curve_baseline_dep AS
select 
curve_lvl,
		half,
		chnl_idnt,
		department_id,
		groupid_frame,
		class_frame,
		size_1_rank,
		size_1_id,
		size_2_id,
		supplier_id,
    fiscal_halfyear_num,
    med_ratio / med_ratio AS ratio
from(SELECT
		curve_lvl,
		half,
		chnl_idnt,
		department_id,
		a.groupid_frame,
		class_frame,
		sz.size_1_rank,
		a.size_1_id,
		size_2_id,
		supplier_id,
SUBSTR(CAST(EXTRACT(YEAR FROM a.insert_date) AS STRING), 1, 4) || SUBSTR(a.half, 2, 1) AS fiscal_halfyear_num,	
    APPROX_QUANTILES(ratio, 100)[OFFSET(50)] AS med_ratio
	FROM `{{params.gcp_project_id}}`.t2dl_das_size.size_curve_baseline_hist a
	JOIN (
			SELECT 
				groupid_frame
				,size_1_idnt AS size_1_id
				,size_1_rank
			FROM hierarchy_
			GROUP BY 1,2,3
		) sz 
	  ON a.groupid_frame = sz.groupid_frame
	 AND a.size_1_id = sz.size_1_id
	WHERE generate_month<>20215
	  AND ratio_used in (1,2) 
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);

--collect statistics      primary index (fiscal_halfyear_num,chnl_idnt,groupid_frame,size_1_id, size_2_id)     ,column(fiscal_halfyear_num)     ,column(chnl_idnt)     ,column(groupid_frame)     ,column(size_1_id)     ,column(size_2_id)     on size_curve_baseline_dep


CREATE TEMPORARY TABLE IF NOT EXISTS out_receipt_comp_dep_1
CLUSTER BY half, chnl_idnt, groupid_frame, size_1_id
AS
SELECT DISTINCT COALESCE(b.size_1_id, a.size_1_id) AS size_1_id,
 COALESCE(b.size_2_id, a.size_2_id) AS size_2_id,
 COALESCE(b.groupid_frame, a.groupid_frame) AS groupid_frame,
 COALESCE(b.chnl_idnt, a.chnl_idnt) AS chnl_idnt,
 TRIM(COALESCE(b.fiscal_halfyear_num, FORMAT('%11d', a.half))) AS half,
 COALESCE(a.pct_size2, 0) AS pct_size2,
 COALESCE(b.ratio, 0) AS ratio,
 COALESCE(a.size_1_rank, b.size_1_rank) AS size_1_rank,
 COALESCE(b.department_id, a.department_id) AS department_id
FROM size_curve_baseline_dep AS b
 FULL JOIN receipt_1b AS a 
 ON LOWER(a.groupid_frame) = LOWER(b.groupid_frame) 
 AND LOWER(a.chnl_idnt) = LOWER(b.chnl_idnt  ) 
 AND LOWER(a.size_1_id) = LOWER(b.size_1_id) 
 AND LOWER(a.size_2_id) = LOWER(b.size_2_id) 
 AND a.half = CAST(b.fiscal_halfyear_num AS FLOAT64)
     AND b.department_id = a.department_id;


--collect statistics      primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)     ,column(half)     ,column(chnl_idnt)     ,column(groupid_frame)     ,column(size_1_id)     ,column(size_2_id)     on out_receipt_comp_dep_1


CREATE TEMPORARY TABLE IF NOT EXISTS date_m
CLUSTER BY month_idnt
AS
SELECT month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 fiscal_halfyear_num,
 fiscal_year_num,
 half_label
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
GROUP BY month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 fiscal_halfyear_num,
 fiscal_year_num,
 half_label;


CREATE TEMPORARY TABLE IF NOT EXISTS adoption_1_m
CLUSTER BY dept_id, chnl_idnt, fiscal_month_num
AS
SELECT a.fiscal_month AS fiscal_month_num,
 ca.month_label,
 ca.fiscal_halfyear_num,
 ca.half_label,
 a.banner,
  CASE
  WHEN a.channel_id = 110
  THEN 'FLS'
  WHEN a.channel_id = 210
  THEN 'RACK'
  WHEN a.channel_id = 250
  THEN 'NRHL'
  WHEN a.channel_id = 120
  THEN 'N.COM'
  ELSE NULL
  END AS chnl_idnt,
 a.dept_id,
 a.size_profile,
 SUM(a.rcpt_dollars) AS rcpt_dollars,
 SUM(a.rcpt_units) AS rcpt_units
FROM `{{params.gcp_project_id}}`.t2dl_das_size.adoption_metrics AS a
 INNER JOIN date_m AS ca ON a.fiscal_month = ca.month_idnt
WHERE CASE
  WHEN a.channel_id = 110
  THEN 'FLS'
  WHEN a.channel_id = 210
  THEN 'RACK'
  WHEN a.channel_id = 250
  THEN 'NRHL'
  WHEN a.channel_id = 120
  THEN 'N.COM'
  ELSE NULL
  END IS NOT NULL
GROUP BY fiscal_month_num,
 ca.month_label,
 ca.fiscal_halfyear_num,
 ca.half_label,
 a.banner,
 chnl_idnt,
 a.dept_id,
 a.size_profile;


CREATE TEMPORARY TABLE IF NOT EXISTS adoption_percetange_y_1_j_rev
CLUSTER BY dept_id, chnl_idnt, fiscal_halfyear_num
AS
SELECT TRIM(FORMAT('%11d', a.fiscal_halfyear_num)) AS fiscal_halfyear_num,
 TRIM(a.half_label) AS half_label,
 a.dept_id,
 a.chnl_idnt,
 TRIM(FORMAT('%11d', b.division_num) || ',' || b.division_name) AS division,
 TRIM(FORMAT('%11d', b.subdivision_num) || ',' || b.subdivision_name) AS subdivision,
 TRIM(FORMAT('%11d', a.dept_id) || ',' || b.dept_name) AS department,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS non_sc_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('ARTS')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS arts_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('EXISTING CURVE')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS existing_curves_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('NONE')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS none_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) = LOWER('OTHER')
    THEN a.rcpt_units
    ELSE NULL
    END), 0) AS other_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_fls_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_rack_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_ncom_rcpt_units,
 COALESCE(SUM(CASE
    WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
    THEN COALESCE(a.rcpt_units, 0)
    ELSE NULL
    END), 0) AS sc_nrhl_rcpt_units,
     COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
       THEN a.rcpt_units
       ELSE NULL
       END), 0) + COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) + COALESCE(SUM(CASE
     WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
     THEN COALESCE(a.rcpt_units, 0)
     ELSE NULL
     END), 0) AS ttl_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_fls_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_rack_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_ncom_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
      THEN COALESCE(a.rcpt_units, 0)
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_sc_nrhl_rcpt_units,
  CASE
  WHEN COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) >= 70 OR COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) >= 70 OR COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) >= 70 OR COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) * 100.0 / IF(COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) = 0, NULL, COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0)) >= 70
  THEN 'High Adopting'
  WHEN COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) >= 30 AND COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) < 70 OR COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) >= 30 AND COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) < 70 OR COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) >= 30 AND COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) < 70 OR COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) >= 30 AND COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) < 70
  THEN 'Medium Adopting'
  WHEN COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) < 30 OR COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) < 30 OR COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) < 30 OR COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) * 100.0 / IF(COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) = 0, NULL, COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0)) < 30
  THEN 'Low Adopting'
  ELSE NULL
  END AS adoption,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('ARTS')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_arts_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('EXISTING CURVE')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_existing_curves_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('NONE')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_none_rcpt_units,
   COALESCE(SUM(CASE
      WHEN LOWER(a.size_profile) = LOWER('OTHER')
      THEN a.rcpt_units
      ELSE NULL
      END), 0) * 100.0 / IF(COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) = 0, NULL, COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
         THEN a.rcpt_units
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0)) AS percent_other_rcpt_units,
          COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) = LOWER('NONE')
             THEN a.rcpt_units
             ELSE NULL
             END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                 THEN a.rcpt_units
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) = 0, NULL, COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0)) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) = LOWER('OTHER')
             THEN a.rcpt_units
             ELSE NULL
             END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                  WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                  THEN COALESCE(a.rcpt_units, 0)
                  ELSE NULL
                  END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                 THEN a.rcpt_units
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) = 0, NULL, COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0)) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) = LOWER('EXISTING CURVE')
            THEN a.rcpt_units
            ELSE NULL
            END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                 WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                 THEN COALESCE(a.rcpt_units, 0)
                 ELSE NULL
                 END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
                THEN a.rcpt_units
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) = 0, NULL, COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0)) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) = LOWER('ARTS')
           THEN a.rcpt_units
           ELSE NULL
           END), 0) * 100.0 / IF(COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
                WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
                THEN COALESCE(a.rcpt_units, 0)
                ELSE NULL
                END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
               THEN a.rcpt_units
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) = 0, NULL, COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0)) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) * 100.0 / IF(COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
               WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
               THEN COALESCE(a.rcpt_units, 0)
               ELSE NULL
               END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
              THEN a.rcpt_units
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) = 0, NULL, COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0)) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) * 100.0 / IF(COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
              WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
              THEN COALESCE(a.rcpt_units, 0)
              ELSE NULL
              END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
             THEN a.rcpt_units
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) = 0, NULL, COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0)) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0) * 100.0 / IF(COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
             WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
             THEN COALESCE(a.rcpt_units, 0)
             ELSE NULL
             END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
            THEN a.rcpt_units
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) = 0, NULL, COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
           THEN a.rcpt_units
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0)) + COALESCE(SUM(CASE
       WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
       THEN COALESCE(a.rcpt_units, 0)
       ELSE NULL
       END), 0) * 100.0 / IF(COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
            WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
            THEN COALESCE(a.rcpt_units, 0)
            ELSE NULL
            END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
           THEN a.rcpt_units
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
          THEN COALESCE(a.rcpt_units, 0)
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) = 0, NULL, COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('FLS')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
           WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('RACK')
           THEN COALESCE(a.rcpt_units, 0)
           ELSE NULL
           END), 0) + COALESCE(SUM(CASE
          WHEN LOWER(a.size_profile) IN (LOWER('ARTS'), LOWER('EXISTING CURVE'), LOWER('NONE'), LOWER('OTHER'))
          THEN a.rcpt_units
          ELSE NULL
          END), 0) + COALESCE(SUM(CASE
         WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('N.COM')
         THEN COALESCE(a.rcpt_units, 0)
         ELSE NULL
         END), 0) + COALESCE(SUM(CASE
        WHEN LOWER(a.size_profile) IN (LOWER('DSA')) AND LOWER(a.chnl_idnt) = LOWER('NRHL')
        THEN COALESCE(a.rcpt_units, 0)
        ELSE NULL
        END), 0)) AS total_percent
FROM adoption_1_m AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS b ON a.dept_id = b.dept_num
GROUP BY fiscal_halfyear_num,
 half_label,
 a.dept_id,
 a.chnl_idnt,
 division,
 subdivision,
 department;


CREATE TEMPORARY TABLE IF NOT EXISTS inv_sales_agg
CLUSTER BY groupid_frame, size_1_idnt, size_2_idnt, half
AS
SELECT TRIM(FORMAT('%11d', c.fiscal_halfyear_num)) AS half,
 j.channel_num,
 j.groupid_frame,
 j.dept_idnt,
 j.size_1_idnt,
 j.size_2_idnt,
 j.size_1_rank,
 CAST(SUM(j.gross_sls_ttl_u) AS INTEGER) AS sales_u,
 CAST(SUM(j.rtn_ttl_u) AS INTEGER) AS return_u,
 SUM(j.gross_sls_ttl_rt) AS sales_gross,
 SUM(j.gross_sls_tot_reg_retl) AS sales_gross_reg,
 SUM(j.gross_sls_tot_promo_retl) AS sales_gross_promo,
 SUM(j.gross_sls_tot_clr_retl) AS sales_gross_clr,
 SUM(j.net_sales_retail) AS net_sales_retail,
 SUM(j.net_sales_cost) AS net_sales_cost,
  SUM(j.net_sales_retail) - SUM(j.net_sales_cost) AS product_margin,
    (SUM(j.net_sales_retail) - SUM(j.net_sales_cost)) * 100.0 / IF(SUM(j.net_sales_retail) = 0, NULL, SUM(j.net_sales_retail
    )) AS product_margin_percent,
 SUM(j.boh_ttl_u) AS boh_u,
 SUM(j.eoh_ttl_u) AS eoh_u,
 MAX(j.in_stock) AS instock_ind,
 MAX(j.clr_ind) AS clr_ind
FROM (SELECT i.rms_sku_num AS sku_idnt,
    h.choice_id,
    h.supp_size,
    h.size_1_idnt,
    h.size_2_idnt,
    h.size_1_rank,
    i.week_num,
    i.channel_num,
    h.groupid_frame,
    h.dept_idnt,
    0 AS gross_sls_ttl_u,
    0 AS rtn_ttl_u,
    0 AS gross_sls_ttl_rt,
    0 AS gross_sls_tot_reg_retl,
    0 AS gross_sls_tot_promo_retl,
    0 AS gross_sls_tot_clr_retl,
    0 AS net_sales_retail,
    0 AS net_sales_cost,
    SUM(i.boh_total_units) AS boh_ttl_u,
    SUM(i.eoh_total_units) AS eoh_ttl_u,
     CASE
     WHEN SUM(i.boh_total_units) > 0 OR SUM(i.eoh_total_units) > 0
     THEN 1
     ELSE 0
     END AS in_stock,
     CASE
     WHEN SUM(i.boh_clearance_units) > 0 OR SUM(i.eoh_clearance_units) > 0
     THEN 1
     ELSE 0
     END AS clr_ind
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i
    INNER JOIN product_sku AS h ON LOWER(i.rms_sku_num) = LOWER(h.sku_idnt) AND i.channel_num IN (120, 110, 250, 210)
     AND i.week_num >= 202201
   GROUP BY sku_idnt,
    h.choice_id,
    h.supp_size,
    h.size_1_idnt,
    h.size_2_idnt,
    h.size_1_rank,
    i.week_num,
    i.channel_num,
    h.groupid_frame,
    h.dept_idnt
   UNION ALL
   SELECT s.rms_sku_num AS sku_idnt,
    h0.choice_id,
    h0.supp_size,
    h0.size_1_idnt,
    h0.size_2_idnt,
    h0.size_1_rank,
    s.week_num,
    s.channel_num,
    h0.groupid_frame,
    h0.dept_idnt,
    SUM(s.gross_sales_tot_units) AS gross_sls_ttl_u,
    SUM(s.returns_tot_units) AS rtn_ttl_u,
    SUM(s.gross_sales_tot_retl) AS gross_sls_ttl_rt,
    SUM(s.gross_sales_tot_regular_retl) AS gross_sls_tot_reg_retl,
    SUM(s.gross_sales_tot_promo_retl) AS gross_sls_tot_promo_retl,
    SUM(s.gross_sales_tot_clearance_retl) AS gross_sls_tot_clr_retl,
    SUM(s.net_sales_tot_retl) AS net_sales_retail,
    SUM(s.net_sales_tot_cost) AS net_sales_cost,
    0 AS boh_ttl_u,
    0 AS eoh_ttl_u,
    0 AS in_stock,
     CASE
     WHEN SUM(s.gross_sales_tot_clearance_units) > 0
     THEN 1
     ELSE 0
     END AS clr_ind
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
    INNER JOIN product_sku AS h0 ON LOWER(s.rms_sku_num) = LOWER(h0.sku_idnt) AND s.channel_num IN (120, 110, 250, 210)
     AND s.week_num >= 202201
   GROUP BY sku_idnt,
    h0.choice_id,
    h0.supp_size,
    h0.size_1_idnt,
    h0.size_2_idnt,
    h0.size_1_rank,
    s.week_num,
    s.channel_num,
    h0.groupid_frame,
    h0.dept_idnt) AS j
 INNER JOIN hist_date AS c ON j.week_num = c.week_idnt
GROUP BY half,
 j.channel_num,
 j.groupid_frame,
 j.dept_idnt,
 j.size_1_idnt,
 j.size_2_idnt,
 j.size_1_rank;


CREATE TEMPORARY TABLE IF NOT EXISTS sales_inv_agg
CLUSTER BY groupid_frame, size_1_idnt, size_2_idnt, half
AS
SELECT half,
 chnl_idnt,
 groupid_frame,
 dept_idnt,
 size_1_idnt,
 size_1_rank,
 size_2_idnt,
 SUM(sales_u) AS sku_sales,
 SUM(return_u) AS return_u,
 SUM(eoh_u) AS eoh,
 SUM(sales_gross) AS sku_gross_sales,
 SUM(sales_gross_reg) AS sku_gross_reg_sales,
 SUM(sales_gross_promo) AS sku_gross_promo_sales,
 SUM(sales_gross_clr) AS sku_gross_clr_sales,
 SUM(net_sales_retail) AS net_sales_retail,
 SUM(net_sales_cost) AS net_sales_cost,
  SUM(net_sales_retail) - SUM(net_sales_cost) AS product_margin,
    (SUM(net_sales_retail) - SUM(net_sales_cost)) * 100.00 / IF(SUM(net_sales_retail) = 0, NULL, SUM(net_sales_retail))
 AS product_margin_percent,
 MIN(sales_choices) AS sales_choices,
 MIN(eoh_choices) AS eoh_choices,
 COALESCE(SUM(sales_u) / NULLIF(MIN(`A121821`), 0), 0) AS pct_sales,
 COALESCE(SUM(eoh_u) / NULLIF(MIN(`A121822`), 0), 0) AS pct_eoh
FROM (SELECT half,
    CASE
    WHEN channel_num = 110
    THEN 'FLS'
    WHEN channel_num = 210
    THEN 'RACK'
    WHEN channel_num = 250
    THEN 'NRHL'
    WHEN channel_num = 120
    THEN 'N.COM'
    ELSE NULL
    END AS chnl_idnt,
   groupid_frame,
   dept_idnt,
   size_1_idnt,
   size_1_rank,
   size_2_idnt,
   sales_u,
   return_u,
   eoh_u,
   sales_gross,
   sales_gross_reg,
   sales_gross_promo,
   sales_gross_clr,
   net_sales_retail,
   net_sales_cost,
   SUM(sales_u) OVER (PARTITION BY half, channel_num, groupid_frame RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS sales_choices,
   SUM(eoh_u) OVER (PARTITION BY half, channel_num, groupid_frame RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS eoh_choices,
   CAST(SUM(sales_u) OVER (PARTITION BY half, channel_num, groupid_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   AS `A121821`,
   CAST(SUM(eoh_u) OVER (PARTITION BY half, channel_num, groupid_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   AS `A121822`
  FROM inv_sales_agg) AS t
GROUP BY half,
 chnl_idnt,
 groupid_frame,
 dept_idnt,
 size_1_idnt,
 size_1_rank,
 size_2_idnt;


--collect statistics      primary index (half,chnl_idnt,groupid_frame,size_1_idnt, size_2_idnt)     ,column(half)     ,column(chnl_idnt)     ,column(groupid_frame)     ,column(size_1_idnt)     ,column(size_2_idnt)     on sales_inv_agg


CREATE TEMPORARY TABLE IF NOT EXISTS sale_inv_rcpt_mo_comb
CLUSTER BY half, chnl_idnt, groupid_frame, size_1_id
AS
SELECT a.size_1_id,
 a.size_2_id,
 a.groupid_frame,
 REGEXP_EXTRACT_ALL(a.groupid_frame, '[^~~]+')[SAFE_OFFSET(1)] AS supplier_idnt,
 REGEXP_EXTRACT_ALL(a.groupid_frame, '[^~~]+')[SAFE_OFFSET(2)] AS class_idnt,
 a.chnl_idnt,
 a.half,
 SUBSTR(a.half, 1, 4) AS fiscal_year,
 a.size_1_rank,
 a.department_id,
  CASE
  WHEN d.ty_fiscal_year_num = CAST(SUBSTR(a.half, 1, 4) AS FLOAT64)
  THEN 'Ty'
  WHEN d.ty_fiscal_year_num - 1 = CAST(SUBSTR(a.half, 1, 4) AS FLOAT64)
  THEN 'Ly'
  WHEN d.ty_fiscal_year_num - 2 = CAST(SUBSTR(a.half, 1, 4) AS FLOAT64)
  THEN 'LLy'
  ELSE 'Next Year'
  END AS ty_ly_ind,
 SUM(a.pct_procured) AS pct_procured,
 SUM(a.pct_model) AS pct_model,
 SUM(a.sku_sales) AS sku_sales,
 SUM(a.return_u) AS return_units,
 SUM(a.eoh) AS eoh,
 SUM(a.sku_gross_sales) AS sku_gross_sales,
 SUM(a.sku_gross_reg_sales) AS sku_gross_reg_sales,
 SUM(a.sku_gross_promo_sales) AS sku_gross_promo_sales,
 SUM(a.sku_gross_clr_sales) AS sku_gross_clr_sales,
 SUM(a.net_sales_retail) AS net_sales_retail,
 SUM(a.net_sales_cost) AS net_sales_cost,
 SUM(a.pct_sales) AS pct_sales,
 SUM(a.pct_eoh) AS pct_eoh
FROM (SELECT SUBSTR(size_1_id, 1, 10) AS size_1_id,
    SUBSTR(size_2_id, 1, 10) AS size_2_id,
    SUBSTR(groupid_frame, 1, 50) AS groupid_frame,
    SUBSTR(chnl_idnt, 1, 10) AS chnl_idnt,
    SUBSTR(half, 1, 5) AS half,
    CAST(size_1_rank AS FLOAT64) AS size_1_rank,
    CAST(department_id AS INTEGER) AS department_id,
    CAST(pct_size2 AS FLOAT64) AS pct_procured,
    CAST(ratio AS FLOAT64) AS pct_model,
    0 AS sku_sales,
    0 AS return_u,
    0 AS eoh,
    0 AS sku_gross_sales,
    0 AS sku_gross_reg_sales,
    0 AS sku_gross_promo_sales,
    0 AS sku_gross_clr_sales,
    0 AS net_sales_retail,
    0 AS net_sales_cost,
    0 AS pct_sales,
    0 AS pct_eoh
   FROM out_receipt_comp_dep_1
   UNION ALL
   SELECT SUBSTR(size_1_idnt, 1, 10) AS size_1_id,
    SUBSTR(size_2_idnt, 1, 10) AS size_2_id,
    SUBSTR(groupid_frame, 1, 50) AS groupid_frame,
    SUBSTR(chnl_idnt, 1, 10) AS chnl_idnt,
    SUBSTR(half, 1, 5) AS half,
    CAST(size_1_rank AS FLOAT64) AS size_1_rank,
    CAST(dept_idnt AS INTEGER) AS department_id,
    0 AS pct_procured,
    0 AS pct_model,
    sku_sales,
    return_u,
    CAST(eoh AS INTEGER) AS eoh,
    CAST(sku_gross_sales AS NUMERIC) AS sku_gross_sales,
    CAST(sku_gross_reg_sales AS NUMERIC) AS sku_gross_reg_sales,
    CAST(sku_gross_promo_sales AS NUMERIC) AS sku_gross_promo_sales,
    CAST(sku_gross_clr_sales AS NUMERIC) AS sku_gross_clr_sales,
    CAST(net_sales_retail AS NUMERIC) AS net_sales_retail,
    CAST(net_sales_cost AS NUMERIC) AS net_sales_cost,
    CAST(pct_sales AS FLOAT64) AS pct_sales,
    CAST(pct_eoh AS FLOAT64) AS pct_eoh
   FROM sales_inv_agg) AS a
 LEFT JOIN (SELECT DISTINCT fiscal_year_num AS ty_fiscal_year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE) AS d ON TRUE
WHERE CAST(SUBSTR(a.half, 1, 4) AS FLOAT64) >= (SELECT DISTINCT fiscal_year_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE) - 2
 AND CAST(SUBSTR(a.half, 1, 4) AS FLOAT64) <= (SELECT DISTINCT fiscal_year_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE) + 1
GROUP BY a.size_1_id,
 a.size_2_id,
 a.groupid_frame,
 supplier_idnt,
 class_idnt,
 a.chnl_idnt,
 a.half,
 fiscal_year,
 a.size_1_rank,
 a.department_id,
 ty_ly_ind;


--collect statistics      primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)     ,column(half)     ,column(chnl_idnt)     ,column(groupid_frame)     ,column(size_1_id)     ,column(size_2_id)     on sale_inv_rcpt_mo_comb


CREATE TEMPORARY TABLE IF NOT EXISTS sale_inv_rcpt_mo_tly
CLUSTER BY half, chnl_idnt, groupid_frame, size_1_id
AS
SELECT size_1_id,
 size_2_id,
 groupid_frame,
 supplier_idnt,
 class_idnt,
 chnl_idnt,
 half,
 fiscal_year,
 size_1_rank,
 department_id,
 ty_ly_ind,
 SUM(pct_procured) AS pct_procured,
 SUM(pct_model) AS pct_model,
 SUM(sku_sales) AS sku_sales,
 SUM(return_units) AS return_units,
 SUM(eoh) AS eoh,
 SUM(sku_gross_sales) AS sku_gross_sales,
 SUM(sku_gross_reg_sales) AS sku_gross_reg_sales,
 SUM(sku_gross_promo_sales) AS sku_gross_promo_sales,
 SUM(sku_gross_clr_sales) AS sku_gross_clr_sales,
 SUM(net_sales_retail) AS net_sales_retail,
 SUM(net_sales_cost) AS net_sales_cost,
 SUM(pct_sales) AS pct_sales,
 SUM(pct_eoh) AS pct_eoh,
 MIN(ty_gross_sales_r) AS ty_gross_sales_r,
 MIN(ly_gross_sales_r) AS ly_gross_sales_r,
 MIN(ty_net_sales_r) AS ty_net_sales_r,
 MIN(ly_net_sales_r) AS ly_net_sales_r,
 MIN(ty_return_u) AS ty_return_u,
 MIN(ly_return_u) AS ly_return_u,
 MIN(ty_sku_sales) AS ty_sku_sales,
 MIN(ly_sku_sales) AS ly_sku_sales,
 MIN(ty_net_sales_c) AS ty_net_sales_c,
 MIN(ly_net_sales_c) AS ly_net_sales_c,
 MIN(ty_gross_reg_sales) AS ty_gross_reg_sales,
 MIN(ly_gross_reg_sales) AS ly_gross_reg_sales,
 MIN(ty_gross_promo_sales) AS ty_gross_promo_sales,
 MIN(ly_gross_promo_sales) AS ly_gross_promo_sales,
 MIN(ty_gross_clr_sales) AS ty_gross_clr_sales,
 MIN(ly_gross_clr_sales) AS ly_gross_clr_sales
FROM (SELECT size_1_id,
   size_2_id,
   groupid_frame,
   supplier_idnt,
   class_idnt,
   chnl_idnt,
   half,
   fiscal_year,
   size_1_rank,
   department_id,
   ty_ly_ind,
   pct_procured,
   pct_model,
   sku_sales,
   return_units,
   eoh,
   sku_gross_sales,
   sku_gross_reg_sales,
   sku_gross_promo_sales,
   sku_gross_clr_sales,
   net_sales_retail,
   net_sales_cost,
   pct_sales,
   pct_eoh,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN sku_gross_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_gross_sales_r,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN sku_gross_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_gross_sales_r,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN net_sales_retail
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_net_sales_r,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN net_sales_retail
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_net_sales_r,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN return_units
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_return_u,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN return_units
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_return_u,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN sku_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_sku_sales,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN sku_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_sku_sales,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN net_sales_cost
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_net_sales_c,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN net_sales_cost
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_net_sales_c,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN sku_gross_reg_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_gross_reg_sales,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN sku_gross_reg_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_gross_reg_sales,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN sku_gross_promo_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_gross_promo_sales,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN sku_gross_promo_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_gross_promo_sales,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ty')
     THEN sku_gross_clr_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ty_gross_clr_sales,
   SUM(CASE
     WHEN LOWER(ty_ly_ind) = LOWER('Ly')
     THEN sku_gross_clr_sales
     ELSE NULL
     END) OVER (PARTITION BY chnl_idnt, department_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   ly_gross_clr_sales
  FROM sale_inv_rcpt_mo_comb) AS a
GROUP BY size_1_id,
 size_2_id,
 groupid_frame,
 supplier_idnt,
 class_idnt,
 chnl_idnt,
 half,
 fiscal_year,
 size_1_rank,
 department_id,
 ty_ly_ind;


--collect statistics 	primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id) 	,column(half) 	,column(chnl_idnt) 	,column(groupid_frame) 	,column(size_1_id) 	,column(size_2_id) on sale_inv_rcpt_mo_tly


CREATE TEMPORARY TABLE IF NOT EXISTS supp_cls_dept AS
SELECT dept_idnt,
 dept_desc,
 class_idnt,
 class_desc,
 supplier_idnt,
 supplier_name,
 groupid_frame,
 TRIM(supplier_idnt || ',' || supplier_name) AS supplier_,
 TRIM(FORMAT('%11d', class_idnt) || ',' || class_desc) AS class_,
 TRIM(FORMAT('%11d', dept_idnt) || ',' || dept_desc) AS department_
FROM product_sku
GROUP BY dept_idnt,
 dept_desc,
 class_idnt,
 class_desc,
 supplier_idnt,
 supplier_name,
 groupid_frame,
 supplier_,
 class_,
 department_;


CREATE TEMPORARY TABLE IF NOT EXISTS supp_class_dept_name AS
SELECT a.size_1_id,
 a.size_2_id,
 a.groupid_frame,
 a.supplier_idnt,
 a.class_idnt,
 a.chnl_idnt,
 a.half,
 a.fiscal_year,
 a.size_1_rank,
 a.department_id,
 a.ty_ly_ind,
 a.pct_procured,
 a.pct_model,
 a.sku_sales,
 a.return_units,
 a.eoh,
 a.sku_gross_sales,
 a.sku_gross_reg_sales,
 a.sku_gross_promo_sales,
 a.sku_gross_clr_sales,
 a.net_sales_retail,
 a.net_sales_cost,
 a.pct_sales,
 a.pct_eoh,
 a.ty_gross_sales_r,
 a.ly_gross_sales_r,
 a.ty_net_sales_r,
 a.ly_net_sales_r,
 a.ty_return_u,
 a.ly_return_u,
 a.ty_sku_sales,
 a.ly_sku_sales,
 a.ty_net_sales_c,
 a.ly_net_sales_c,
 a.ty_gross_reg_sales,
 a.ly_gross_reg_sales,
 a.ty_gross_promo_sales,
 a.ly_gross_promo_sales,
 a.ty_gross_clr_sales,
 a.ly_gross_clr_sales,
 TRIM(a.supplier_idnt || ',' || b.supplier_name) AS supplier_,
 TRIM(a.class_idnt || ',' || b.class_desc) AS class_,
 TRIM(FORMAT('%11d', a.department_id) || ',' || b.dept_desc) AS department_
FROM sale_inv_rcpt_mo_tly AS a
 INNER JOIN supp_cls_dept AS b ON a.department_id = b.dept_idnt AND LOWER(a.supplier_idnt) = LOWER(b.supplier_idnt) AND CAST(a.class_idnt AS FLOAT64)
    = b.class_idnt AND LOWER(a.groupid_frame) = LOWER(b.groupid_frame)
GROUP BY a.size_1_id,
 a.size_2_id,
 a.groupid_frame,
 a.supplier_idnt,
 a.class_idnt,
 a.chnl_idnt,
 a.half,
 a.fiscal_year,
 a.size_1_rank,
 a.department_id,
 a.ty_ly_ind,
 a.pct_procured,
 a.pct_model,
 a.sku_sales,
 a.return_units,
 a.eoh,
 a.sku_gross_sales,
 a.sku_gross_reg_sales,
 a.sku_gross_promo_sales,
 a.sku_gross_clr_sales,
 a.net_sales_retail,
 a.net_sales_cost,
 a.pct_sales,
 a.pct_eoh,
 a.ty_gross_sales_r,
 a.ly_gross_sales_r,
 a.ty_net_sales_r,
 a.ly_net_sales_r,
 a.ty_return_u,
 a.ly_return_u,
 a.ty_sku_sales,
 a.ly_sku_sales,
 a.ty_net_sales_c,
 a.ly_net_sales_c,
 a.ty_gross_reg_sales,
 a.ly_gross_reg_sales,
 a.ty_gross_promo_sales,
 a.ly_gross_promo_sales,
 a.ty_gross_clr_sales,
 a.ly_gross_clr_sales,
 supplier_,
 class_,
 department_;


CREATE TEMPORARY TABLE IF NOT EXISTS adop_add AS
SELECT a.size_1_id,
 a.size_2_id,
 a.groupid_frame,
 a.supplier_idnt,
 a.class_idnt,
 a.chnl_idnt,
 a.half,
 a.fiscal_year,
 a.size_1_rank,
 a.department_id,
 a.ty_ly_ind,
 a.pct_procured,
 a.pct_model,
 a.sku_sales,
 a.return_units,
 a.eoh,
 a.sku_gross_sales,
 a.sku_gross_reg_sales,
 a.sku_gross_promo_sales,
 a.sku_gross_clr_sales,
 a.net_sales_retail,
 a.net_sales_cost,
 a.pct_sales,
 a.pct_eoh,
 a.ty_gross_sales_r,
 a.ly_gross_sales_r,
 a.ty_net_sales_r,
 a.ly_net_sales_r,
 a.ty_return_u,
 a.ly_return_u,
 a.ty_sku_sales,
 a.ly_sku_sales,
 a.ty_net_sales_c,
 a.ly_net_sales_c,
 a.ty_gross_reg_sales,
 a.ly_gross_reg_sales,
 a.ty_gross_promo_sales,
 a.ly_gross_promo_sales,
 a.ty_gross_clr_sales,
 a.ly_gross_clr_sales,
 a.supplier_,
 a.class_,
 a.department_,
 b.percent_sc_fls_rcpt_units,
 b.percent_sc_rack_rcpt_units,
 b.percent_sc_nrhl_rcpt_units,
 b.percent_sc_ncom_rcpt_units,
 b.adoption
FROM supp_class_dept_name AS a
 LEFT JOIN adoption_percetange_y_1_j_rev AS b ON a.department_id = b.dept_id AND LOWER(b.chnl_idnt) = LOWER(a.chnl_idnt
     ) AND LOWER(b.fiscal_halfyear_num) = LOWER(a.half);


CREATE TEMPORARY TABLE IF NOT EXISTS rmse_sales_procure_add AS
SELECT size_1_id,
 size_2_id,
 groupid_frame,
 supplier_idnt,
 class_idnt,
 chnl_idnt,
 half,
 fiscal_year,
 size_1_rank,
 department_id,
 ty_ly_ind,
 pct_procured,
 pct_model,
 sku_sales,
 return_units,
 eoh,
 sku_gross_sales,
 sku_gross_reg_sales,
 sku_gross_promo_sales,
 sku_gross_clr_sales,
 net_sales_retail,
 net_sales_cost,
 pct_sales,
 pct_eoh,
 ty_gross_sales_r,
 ly_gross_sales_r,
 ty_net_sales_r,
 ly_net_sales_r,
 ty_return_u,
 ly_return_u,
 ty_sku_sales,
 ly_sku_sales,
 ty_net_sales_c,
 ly_net_sales_c,
 ty_gross_reg_sales,
 ly_gross_reg_sales,
 ty_gross_promo_sales,
 ly_gross_promo_sales,
 ty_gross_clr_sales,
 ly_gross_clr_sales,
 supplier_,
 class_,
 department_,
 percent_sc_fls_rcpt_units,
 percent_sc_rack_rcpt_units,
 percent_sc_nrhl_rcpt_units,
 percent_sc_ncom_rcpt_units,
 adoption,
 POWER(AVG(POWER(pct_model - pct_sales, 2)) OVER (PARTITION BY half, chnl_idnt, groupid_frame RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0.5) AS rmse_pct_sales,
 POWER(AVG(POWER(pct_model - pct_procured, 2)) OVER (PARTITION BY half, chnl_idnt, groupid_frame RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0.5) AS rmse_pct_procured
FROM adop_add AS a;


TRUNCATE TABLE `{{params.gcp_project_id}}`.t2dl_das_size.size_curve_evaluation;


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_size.size_curve_evaluation
(SELECT size_1_id,
  size_2_id,
  groupid_frame,
  supplier_idnt,
  class_idnt,
  chnl_idnt,
  half,
  fiscal_year,
  size_1_rank,
  department_id,
  ty_ly_ind,
  pct_procured,
  pct_model,
  sku_sales,
  return_units,
  eoh,
  sku_gross_sales,
  sku_gross_reg_sales,
  sku_gross_promo_sales,
  sku_gross_clr_sales,
  net_sales_retail,
  net_sales_cost,
  pct_sales,
  pct_eoh,
  CAST(ty_gross_sales_r AS NUMERIC) AS ty_gross_sales_r,
  CAST(ly_gross_sales_r AS NUMERIC) AS ly_gross_sales_r,
  CAST(ty_net_sales_r AS NUMERIC) AS ty_net_sales_r,
  CAST(ly_net_sales_r AS NUMERIC) AS ly_net_sales_r,
  ty_return_u,
  ly_return_u,
  ty_sku_sales,
  ly_sku_sales,
  CAST(ty_net_sales_c AS NUMERIC) AS ty_net_sales_c,
  CAST(ly_net_sales_c AS NUMERIC) AS ly_net_sales_c,
  CAST(ty_gross_reg_sales AS NUMERIC) AS ty_gross_reg_sales,
  CAST(ly_gross_reg_sales AS NUMERIC) AS ly_gross_reg_sales,
  CAST(ty_gross_promo_sales AS NUMERIC) AS ty_gross_promo_sales,
  CAST(ly_gross_promo_sales AS NUMERIC) AS ly_gross_promo_sales,
  CAST(ty_gross_clr_sales AS NUMERIC) AS ty_gross_clr_sales,
  CAST(ly_gross_clr_sales AS NUMERIC) AS ly_gross_clr_sales,
  supplier_,
  class_,
  department_,
  CAST(percent_sc_fls_rcpt_units AS NUMERIC)  as percent_sc_fls_rcpt_units,
  CAST(percent_sc_rack_rcpt_units AS NUMERIC) as percent_sc_rack_rcpt_units,
  CAST(percent_sc_nrhl_rcpt_units AS NUMERIC) as percent_sc_nrhl_rcpt_units,
  CAST(percent_sc_ncom_rcpt_units AS NUMERIC) as percent_sc_ncom_rcpt_units,
  adoption,
  CAST(rmse_pct_sales AS FLOAT64) AS rmse_pct_sales,
  CAST(rmse_pct_procured AS FLOAT64) AS rmse_pct_procured,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS  update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
FROM rmse_sales_procure_add AS a);