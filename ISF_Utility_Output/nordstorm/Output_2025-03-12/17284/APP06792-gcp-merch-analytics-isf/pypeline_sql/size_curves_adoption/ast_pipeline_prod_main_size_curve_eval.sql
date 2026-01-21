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
CREATE multiset volatile TABLE hierarchy_ AS (
    SELECT DISTINCT
		 h.sku_idnt
		,h.rms_style_num || '~~' || h.color_num AS choice_id
		,m.supp_size AS supp_size
		,m.size_1_idnt AS size_1_idnt
		,m.size_2_idnt AS size_2_idnt
		,h.dept_desc 
		,h.dept_idnt
		,h.class_idnt
		,h.class_desc
		,b.supplier_name
		,h.supplier_idnt
		,b.groupid
		,b.groupid_size
		,CONCAT(b.groupid,'~~',strtok(b.size_1_frame, '_', 1)) AS groupid_frame
		,CONCAT(TRIM(b.department_id),'~~',TRIM(b.class_id),'~~',strtok(b.size_1_frame, '_', 1)) AS class_frame
		,b.size_1_rank
		,b.size_1_frame
		,b.size_2_rank 
    FROM t2dl_das_assortment_dim.assortment_hierarchy h 
    JOIN (
            SELECT DISTINCT
				 sku_idnt AS sku_idnt
				,supp_size
				,size_1_idnt
				,size_2_idnt
            FROM prd_ma_bado.prod_sku_sty_cur_lkup_vw h
    ) m
      on  h.sku_idnt = m.sku_idnt 
     AND h.channel_COUNTry = 'US' 
    JOIN t2dl_das_size.size_sku_groupid b
      ON b.sku_id=h.sku_idnt 
     AND h.dept_idnt=b.department_id
) WITH DATA 
PRIMARY INDEX(sku_idnt)
 ON COMMIT PRESERVE ROWS;


--With this logic for size_2_id that are NA within a department , we replace them with NA, R, or M values to reduce the number of Null values
CREATE multiset volatile TABLE na_size_2_map AS (
	SELECT a.*,
		'NA' AS size_2_id_old,
		CASE WHEN groupid_r <= 0.1 and groupid_m <= 0.1 THEN 'NA'
			WHEN groupid_r >= 0.2 THEN 'R'
			WHEN groupid_m >= 0.2 THEN 'M'
			end AS size_2_id_new
	FROM (
		SELECT
			department_id,
			department_description,
			COUNT(DISTINCT groupid) AS groupid_total,
			1.0*COUNT(DISTINCT CASE WHEN size_2_id is null THEN groupid end)/COUNT(DISTINCT groupid) AS groupid_na,
			1.0*COUNT(DISTINCT CASE WHEN size_2_id = 'R' THEN groupid end)/COUNT(DISTINCT groupid) AS groupid_r,
			1.0*COUNT(DISTINCT CASE WHEN size_2_id = 'M' THEN groupid end)/COUNT(DISTINCT groupid) AS groupid_m
		FROM t2dl_das_size.size_sku_groupid
		group by 1,2
	)a ) WITH DATA PRIMARY INDEX (department_id,size_2_id_old) on
commit preserve rows;


collect statistics
     primary index (department_id,size_2_id_old)
    ,column(department_id)
    ,column(size_2_id_old)
     on na_size_2_map;

--Replace the new value for size_2
--drop TABLE product_sku;   
CREATE multiset volatile TABLE product_sku AS 
(
	SELECT DISTINCT
		 a.sku_idnt
		,a.choice_id
		,a.supp_size
		,a.size_1_idnt
		,COALESCE(b.size_2_id_new, a.size_2_idnt, 'R') AS size_2_idnt 
		,a.dept_desc 
		,a.dept_idnt
		,a.class_idnt
		,a.class_desc
		,a.supplier_name
		,a.supplier_idnt
		,c.div_desc
		,a.groupid
		,a.groupid_size
		,a.groupid_frame
		,a.class_frame
		,a.size_1_rank
		,a.size_1_frame
		,a.size_2_rank 
	FROM hierarchy_ a
	LEFT JOIN na_size_2_map b
		on
		a.dept_idnt = b.department_id
		and a.size_2_idnt = b.size_2_id_old --where we had no values for size_2_id
	LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw c 
		on a.dept_idnt=c.dept_num
		and a.sku_idnt=c.rms_sku_num
)WITH DATA PRIMARY INDEX (sku_idnt,choice_id,size_1_idnt, size_2_idnt) ON COMMIT preserve rows;

collect statistics
     primary index (sku_idnt,choice_id,size_1_idnt, size_2_idnt)
    ,column(sku_idnt)
    ,column(choice_id)
    ,column(size_1_idnt)
    ,column(size_2_idnt)
    on product_sku;


-- CREATE mapping date TABLE on week level
   
CREATE multiset volatile TABLE hist_date AS (
SELECT      
	 WEEK_IDNT
	,MONTH_IDNT
	,MONTH_LABEL
	,QUARTER_IDNT 
	,QUARTER_LABEL 
	,'FY' || substr(cast(fiscal_year_num AS varchar(5)),3,2) || ' '|| quarter_abrv AS QUARTER
	,FISCAL_YEAR_NUM
	,fiscal_halfyear_num
	,half_label
	,week_start_day_date
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE fiscal_year_num BETWEEN 2021 AND (SELECT fiscal_year_num FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = current_date)
  AND fiscal_week_num < (SELECT fiscal_week_num FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE day_date = current_date)
GROUP BY 1,2,3,4,5,6,7,8,9,10)
WITH DATA PRIMARY INDEX (WEEK_IDNT) ON COMMIT preserve rows;

collect stats
     primary index (WEEK_IDNT)
     ,column(WEEK_IDNT)
     ,column(MONTH_IDNT)
     ,column(QUARTER_IDNT)
     ,column(FISCAL_YEAR_NUM)
     on hist_date;

    
--Obtain receipt ratio across varios sizes of a cc 
--drop TABLE receipts_half;
    
CREATE multiset volatile TABLE receipts_half AS (
    SELECT
		 h.choice_id
		,a.sku_idnt
		,h.supp_size
		,h.size_1_idnt
		,h.size_2_idnt
		,channel_num
		,half
		,MIN(week_num) AS first_receipt_week
		,SUM(recipts_u) AS sku_recipts_u --how many skus we received
		,SUM(sku_recipts_u) over (partition by h.choice_id, channel_num, half) AS choice_receipt_u --total of CCs
		,SUM(1) over (partition by h.choice_id, channel_num, half) AS skus --how many skus are within cc
		,COALESCE (sku_recipts_u / nullif((choice_receipt_u * 1.00),0), 0) AS pct_size
	FROM (
				SELECT 
					j.*, 
					c.fiscal_halfyear_num AS half 
				FROM (
					SELECT
						 sku_idnt
						,channel_num
						,MIN(week_num) week_num
						,SUM(rcpt.receipt_po_units) AS recipts_u
					FROM t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact rcpt
					WHERE (rcpt.receipt_po_units > 0 or rcpt.receipt_rsk_units > 0 )
					  AND channel_num in (120, 110, 250, 210)
					GROUP BY 1,2

					UNION ALL 

					SELECT
						 CAST(sku_idnt AS VARCHAR(10)) AS sku_idnt
						,channel_num
						,MIN(week_num) week_num
						,SUM(rcpt.receipt_po_units) AS recipts_u
					FROM t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm rcpt
					WHERE rcpt.receipt_po_units > 0
					  AND channel_num IN (120, 110, 250, 210)
					GROUP BY  1,2
				) j
            JOIN hist_date c
              ON c.week_idnt=j.week_num
		) a
	JOIN product_sku h
	  ON a.sku_idnt = h.sku_idnt
	GROUP BY 1,2,3,4,5,6,7
) WITH DATA 
PRIMARY INDEX (sku_idnt,choice_id,size_1_idnt, size_2_idnt) 
ON COMMIT preserve rows;
           
 collect statistics
     primary index (sku_idnt,choice_id,size_1_idnt, size_2_idnt)
    ,column(sku_idnt)
    ,column(choice_id)
    ,column(size_1_idnt)
    ,column(size_2_idnt)
    on receipts_half;
           
--SELECT * FROM receipts_half
--where choice_id='39602737~~220' and channel_num=110 and half=20212;
           

--To obtain receipt ratio across diferent sizes in groupid, channel, and half level 
--drop TABLE receipt_1b;
CREATE multiset volatile TABLE receipt_1b as (
	SELECT
		half AS half,
		chnl_idnt,
		department_id,
		groupid_frame,
		size_1_rank,
		size_1_id,
		size_2_id,
		SUM(sku_recipts_u) AS sku_recipts_un,
		MIN(choice_receipt_u_all) AS choice_receipt_un_all,
		COALESCE(sku_recipts_un / nullif(choice_receipt_un_all * 1.000,0), 0) AS pct_size2   
	FROM (
			SELECT
				SUM(sku_recipts_u) over (partition by chnl_idnt, half, groupid_frame) AS choice_receipt_u_all,		       
				r.choice_id,
				r.sku_idnt,
				r.half,
				case
					WHEN r.channel_num = 110 THEN 'FLS'
					WHEN r.channel_num = 210 THEN 'RACK'
					WHEN r.channel_num = 250 THEN 'NRHL'
					WHEN r.channel_num = 120 THEN 'N.COM'
					else null
				end AS chnl_idnt,
				b.dept_idnt AS department_id,
				b.groupid_frame AS groupid_frame,
				b.size_1_rank AS size_1_rank,
				b.size_1_idnt AS size_1_id,
				b.size_2_idnt AS size_2_id,	
				sku_recipts_u,
				choice_receipt_u,
				pct_size AS ratio
			FROM receipts_half r
			JOIN product_sku b
			  ON r.sku_idnt = b.sku_idnt
		) a	
	GROUP BY 1,2,3,4,5,6,7	
) WITH DATA 
PRIMARY INDEX (half,chnl_idnt,groupid_frame,size_1_id, size_2_id) 
ON COMMIT preserve rows;

 collect statistics
     primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)
    ,column(half)
    ,column(chnl_idnt)
    ,column(groupid_frame)
    ,column(size_1_id)
    ,column(size_2_id)
    on receipt_1b;

--SELECT  * FROM receipt_1b
--where groupid_frame='859~~649106493~~23~~Letters'and chnl_idnt='RACK' and half='20211' and size_2_id='R';


--Obtain ratios across different sizes suggested by size curves for both groupid level
--drop TABLE size_curve_baseline_dep;
CREATE multiset volatile TABLE size_curve_baseline_dep AS (
	SELECT
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
		cast(year(insert_date) AS varchar(4))|| substr(half,2,1) AS fiscal_halfyear_num,
		median(ratio) AS med_ratio,
		sum(med_ratio) OVER (PARTITION BY fiscal_halfyear_num, chnl_idnt, a.groupid_frame) frame_ttl, 
		med_ratio / frame_ttl AS ratio
	FROM t2DL_DAS_size.size_curve_baseline_hist a
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
	  AND ratio_used in (1,2) --levels 1 & 2 can be used to generate a supplier-class-level curve in class level ratio 3 should be included
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA PRIMARY INDEX (fiscal_halfyear_num,chnl_idnt,groupid_frame,size_1_id, size_2_id)  ON COMMIT preserve rows;

 collect statistics
     primary index (fiscal_halfyear_num,chnl_idnt,groupid_frame,size_1_id, size_2_id)
    ,column(fiscal_halfyear_num)
    ,column(chnl_idnt)
    ,column(groupid_frame)
    ,column(size_1_id)
    ,column(size_2_id)
    on size_curve_baseline_dep;


--include receipt items
--drop TABLE out_receipt_comp_dep_1;
CREATE multiset volatile TABLE out_receipt_comp_dep_1 AS 
(
SELECT DISTINCT
	COALESCE(b.size_1_id,a.size_1_id) AS size_1_id,
	COALESCE(b.size_2_id,a.size_2_id) AS size_2_id,
	COALESCE(b.groupid_frame,a.groupid_frame) AS groupid_frame,
	COALESCE(b.chnl_idnt,a.chnl_idnt) AS chnl_idnt,
	TRIM(COALESCE(b.fiscal_halfyear_num,a.half)) AS half,
	COALESCE(a.pct_size2,0) AS pct_size2,
	COALESCE(b.ratio,0) AS ratio, 
	COALESCE(a.size_1_rank, b.size_1_rank) AS size_1_rank,
	COALESCE(b.department_id,a.department_id) AS department_id	
FROM size_curve_baseline_dep b
FULL OUTER JOIN receipt_1b a
  ON a.groupid_frame = b.groupid_frame
 AND a.chnl_idnt = b.chnl_idnt
 AND a.size_1_id = b.size_1_id
 AND a.size_2_id = b.size_2_id
 AND a.half = b.fiscal_halfyear_num
 AND a.department_id=b.department_id
) WITH DATA 
PRIMARY INDEX (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)
ON COMMIT preserve rows;

collect statistics
     primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)
    ,column(half)
    ,column(chnl_idnt)
    ,column(groupid_frame)
    ,column(size_1_id)
    ,column(size_2_id)
    on out_receipt_comp_dep_1;


--drop TABLE date_m;
CREATE MULTISET VOLATILE TABLE date_m AS(
SELECT
	 MONTH_IDNT
	,MONTH_LABEL
	,QUARTER_IDNT 
	,QUARTER_LABEL 
	,fiscal_halfyear_num
	,FISCAL_YEAR_NUM 
	,half_label
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM  
group by 1,2,3,4,5,6,7)

WITH DATA
   PRIMARY INDEX(MONTH_IDNT)
    ON COMMIT PRESERVE ROWS;


--drop TABLE adoption_1_m;
CREATE multiset volatile TABLE adoption_1_m as ( 
	SELECT
		a.fiscal_month AS fiscal_month_num,
		ca.month_label,
		ca.fiscal_halfyear_num AS fiscal_halfyear_num,
		ca.half_label,
		banner,
		CASE WHEN channel_id= 110 THEN 'FLS'
			WHEN channel_id= 210 THEN 'RACK'
			WHEN channel_id= 250 THEN 'NRHL'
			WHEN channel_id= 120 THEN 'N.COM'
			else null end AS chnl_idnt,
		dept_id,
		size_profile,
		SUM(rcpt_dollars) AS rcpt_dollars,
		SUM(rcpt_units) AS rcpt_units 
	FROM t2dl_das_size.adoption_metrics a
	JOIN date_m ca
	  ON a.fiscal_month = ca.month_idnt
	WHERE chnl_idnt IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8
) WITH DATA 
PRIMARY INDEX (dept_id, chnl_idnt,fiscal_month_num )  ON COMMIT preserve rows;


--halfyear level
CREATE multiset volatile TABLE adoption_percetange_Y_1_J_rev AS (
SELECT
	TRIM(fiscal_halfyear_num) AS fiscal_halfyear_num,
	TRIM(half_label) AS half_label,
	--TRIM(fiscal_month_num) AS fiscal_month_num,
	--TRIM(month_label) AS month_label,
	dept_id,
	chnl_idnt,
	TRIM(b.DIVISION_NUM||','||b.DIVISION_NAME) AS Division,
	TRIM(b.SUBDIVISION_NUM||','||b.SUBDIVISION_NAME) AS Subdivision,
	TRIM(a.DEPT_ID||','||b.DEPT_NAME) AS Department,	
	COALESCE(SUM(CASE WHEN size_profile in ('ARTS', 'EXISTING CURVE', 'NONE', 'OTHER') THEN rcpt_units end),0) AS Non_sc_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile='ARTS' THEN rcpt_units end),0) AS ARTS_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile='EXISTING CURVE' THEN rcpt_units end),0) AS existing_curves_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile='NONE' THEN rcpt_units end),0) AS none_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile='OTHER' THEN rcpt_units end),0) AS other_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile in ('DSA') and chnl_idnt='FLS'  THEN COALESCE(rcpt_units, 0) end),0) AS Sc_FLS_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile in ('DSA') and chnl_idnt='RACK' THEN COALESCE(rcpt_units, 0) end),0) AS Sc_RACK_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile in ('DSA') and chnl_idnt='N.COM' THEN COALESCE(rcpt_units, 0) end),0) AS Sc_NCOM_rcpt_units,
	COALESCE(SUM(CASE WHEN size_profile in ('DSA') and chnl_idnt='NRHL' THEN COALESCE(rcpt_units, 0) end),0) AS Sc_NRHL_rcpt_units,
	(Sc_FLS_rcpt_units + Sc_RACK_rcpt_units + Non_sc_rcpt_units + Sc_NCOM_rcpt_units + Sc_NRHL_rcpt_units) AS Ttl_rcpt_units,
	(Sc_FLS_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_Sc_FLS_rcpt_units,
	(Sc_RACK_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_Sc_RACK_rcpt_units,
	(Sc_NCOM_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_Sc_NCOM_rcpt_units,
	(Sc_NRHL_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_Sc_NRHL_rcpt_units,
	(CASE WHEN Percent_Sc_FLS_rcpt_units>= 70 or Percent_Sc_RACK_rcpt_units >= 70 or Percent_Sc_NCOM_rcpt_units >= 70 or Percent_Sc_NRHL_rcpt_units >= 70 THEN 'High Adopting'
		WHEN Percent_Sc_FLS_rcpt_units>=30 and Percent_Sc_FLS_rcpt_units<70 or Percent_Sc_RACK_rcpt_units >=30 and Percent_Sc_RACK_rcpt_units <70 or Percent_Sc_NCOM_rcpt_units >= 30 and Percent_Sc_NCOM_rcpt_units <70 or Percent_Sc_NRHL_rcpt_units >= 30 and Percent_Sc_NRHL_rcpt_units < 70 THEN 'Medium Adopting' 
		WHEN Percent_Sc_FLS_rcpt_units< 30 or Percent_Sc_RACK_rcpt_units < 30 or Percent_Sc_NCOM_rcpt_units < 30 or Percent_Sc_NRHL_rcpt_units < 30 THEN 'Low Adopting' end) AS adoption,
	(ARTS_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_arts_rcpt_units,
	(existing_curves_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_existing_curves_rcpt_units,
	(none_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_none_rcpt_units,
	(other_rcpt_units * 100.0 / Nullifzero(Ttl_rcpt_units)) AS Percent_other_rcpt_units,
	(Percent_none_rcpt_units+Percent_other_rcpt_units+Percent_existing_curves_rcpt_units+ Percent_arts_rcpt_units+Percent_Sc_RACK_rcpt_units+Percent_Sc_FLS_rcpt_units+Percent_Sc_NCOM_rcpt_units+Percent_Sc_NRHL_rcpt_units) AS total_percent
FROM adoption_1_m a
LEFT JOIN prd_nap_usr_vws.department_dim b
  on a.dept_id=b.dept_num
group by 1,2,3,4,5,6,7)
WITH DATA PRIMARY INDEX (dept_id,chnl_idnt,fiscal_halfyear_num ) ON COMMIT preserve rows;


--agg sales and inventory data at half, channel, group_id level
--drop TABLE inv_sales_agg;
CREATE MULTISET VOLATILE TABLE inv_sales_agg AS (
    SELECT
         TRIM(half) AS half
        ,channel_num
        ,groupid_frame
        ,dept_idnt
        ,size_1_idnt
        ,size_2_idnt
        ,size_1_rank
        ,CAST(SUM(gross_sls_ttl_u) AS INTEGER) AS sales_u
        ,CAST(SUM(rtn_ttl_u) AS INTEGER) AS return_u
        ,SUM(gross_sls_ttl_rt)  AS sales_gross
        ,SUM(gross_sls_tot_reg_retl) AS sales_gross_reg
        ,SUM(gross_sls_tot_promo_retl) AS sales_gross_promo
        ,SUM(gross_sls_tot_clr_retl) AS sales_gross_clr
        ,SUM(net_sales_retail) AS net_sales_retail
        ,SUM(net_sales_cost) AS net_sales_cost
        ,(SUM(net_sales_retail)-SUM(net_sales_cost)) AS product_margin
        ,(SUM(net_sales_retail)-SUM(net_sales_cost))*100.0/Nullifzero(SUM(net_sales_retail)) AS product_margin_percent
        ,SUM(boh_ttl_u) AS boh_u
        ,SUM(eoh_ttl_u) AS eoh_u
        ,MAX(in_stock) AS instock_ind
        ,MAX(clr_ind) AS clr_ind
    FROM (
           SELECT j.*,
                  c.fiscal_halfyear_num AS half
           FROM ((SELECT 
                 rms_sku_num AS sku_idnt
                ,h.choice_id
                ,h.supp_size
                ,h.size_1_idnt
                ,h.size_2_idnt
                ,h.size_1_rank
                ,week_num
                ,channel_num
                ,h.groupid_frame
                ,h.dept_idnt
                ,CAST(0 AS DECIMAL(12,2)) AS gross_sls_ttl_u
                ,CAST(0 AS DECIMAL(12,2)) AS rtn_ttl_u
                ,CAST(0 AS DECIMAL(12,2)) AS gross_sls_ttl_rt
                ,CAST(0 AS DECIMAL(12,2)) AS gross_sls_tot_reg_retl
                ,CAST(0 AS DECIMAL(12,2)) AS gross_sls_tot_promo_retl
                ,CAST(0 AS DECIMAL(12,2)) AS gross_sls_tot_clr_retl
                ,CAST(0 AS DECIMAL(12,2)) AS net_sales_retail
                ,CAST(0 AS DECIMAL(12,2)) AS net_sales_cost
                ,SUM(i.boh_total_units) AS boh_ttl_u
                ,SUM(i.eoh_total_units) AS eoh_ttl_u
                ,CASE WHEN boh_ttl_u > 0 OR eoh_ttl_u > 0 THEN 1 ELSE 0 END AS in_stock
                ,CASE WHEN SUM(i.boh_clearance_units) > 0 OR SUM(i.eoh_clearance_units) > 0 THEN 1 ELSE 0 END AS clr_ind
            FROM prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw i
            JOIN product_sku h
              ON i.rms_sku_num = h.sku_idnt
              AND channel_num IN (120, 110, 250, 210)
              AND week_num >= 202201
            GROUP BY 1,2,3,4,5,6,7,8,9,10)

            UNION ALL 

            (SELECT 
                 s.rms_sku_num AS sku_idnt
                ,h.choice_id
                ,h.supp_size
                ,h.size_1_idnt
                ,h.size_2_idnt
                ,h.size_1_rank
                ,s.week_num
                ,s.channel_num
                ,h.groupid_frame
                ,h.dept_idnt
                ,SUM(s.gross_sales_tot_units) AS gross_sls_ttl_u
                ,SUM(s.returns_tot_units) AS rtn_ttl_u
                ,SUM(s.gross_sales_tot_retl) AS gross_sls_ttl_rt
                ,SUM(s.gross_sales_tot_regular_retl) AS gross_sls_tot_reg_retl
                ,SUM(s.gross_sales_tot_promo_retl) AS gross_sls_tot_promo_retl
                ,SUM(s.gross_sales_tot_clearance_retl) AS gross_sls_tot_clr_retl
                ,SUM(s.NET_SALES_TOT_RETL) AS net_sales_retail
                ,SUM(s.NET_SALES_TOT_COST) AS net_sales_cost
                ,CAST(0 AS DECIMAL(12,2)) AS boh_ttl_u
                ,CAST(0 AS DECIMAL(12,2)) AS eoh_ttl_u
                ,0 AS in_stock
                ,CASE WHEN SUM(s.gross_sales_tot_clearance_units) > 0 THEN 1 ELSE 0 END AS clr_ind
            FROM prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw s
            JOIN product_sku h
              ON s.rms_sku_num = h.sku_idnt
              AND s.channel_num IN (120, 110, 250, 210)
              AND s.week_num >= 202201
            GROUP BY 1,2,3,4,5,6,7,8,9,10))j
            JOIN hist_date c
            on j.week_num=c.week_idnt
        ) a
    GROUP BY 1,2,3,4,5,6,7
) WITH DATA
PRIMARY INDEX (groupid_frame,size_1_idnt,size_2_idnt, half, channel_num)
 ON COMMIT PRESERVE ROWS;  

--SELECT top 10 * FROM prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw ;
--SELECT * FROM inv_sales_agg
--where half=20232 and channel_num=110 and groupid_frame='829~~5078846~~23~~Numbers' and size_1_idnt=42 and size_2_idnt='R' ;

--drop TABLE sales_inv_agg; 
CREATE multiset volatile TABLE sales_inv_agg as(
SELECT
	half,
	chnl_idnt,
	groupid_frame,
	dept_idnt,
	size_1_idnt,
	size_1_rank,
	size_2_idnt,
	SUM(sales_u) AS sku_sales, --add units
	SUM(return_u) AS return_u,
	SUM(eoh_u) AS eoh,
	SUM(sales_gross) AS sku_gross_sales,
	SUM(sales_gross_reg) AS sku_gross_reg_sales,
	SUM(sales_gross_promo) AS sku_gross_promo_sales,
	SUM(sales_gross_clr) AS sku_gross_clr_sales,
	SUM(net_sales_retail) AS net_sales_retail,
	SUM(net_sales_cost) AS net_sales_cost,
	SUM(net_sales_retail)-SUM(net_sales_cost) AS product_margin,
	(SUM(net_sales_retail)-SUM(net_sales_cost))*100.00/Nullifzero(SUM(net_sales_retail)) AS product_margin_percent,
	MIN(sales_choices) AS sales_choices,
	MIN(eoh_choices) AS eoh_choices,
	COALESCE(sku_sales / NULLIF((MIN(sales_choices * 1.000)),0),0) AS pct_sales,
	COALESCE(eoh / NULLIF((MIN(eoh_choices * 1.000)),0),0) AS pct_eoh
FROM
	(
	SELECT
		half,
		case
			WHEN channel_num = 110 THEN 'FLS'
			WHEN channel_num = 210 THEN 'RACK'
			WHEN channel_num = 250 THEN 'NRHL'
			WHEN channel_num = 120 THEN 'N.COM'
			else null
		end AS chnl_idnt,
		groupid_frame,
		dept_idnt,
		size_1_idnt,
		size_2_idnt,
		size_1_rank,
		sales_u,
		return_u,
		boh_u,
		eoh_u,
		sales_gross,
		sales_gross_reg,
		sales_gross_promo,
		sales_gross_clr,
		net_sales_retail,
		net_sales_cost,
		instock_ind,
		clr_ind,
		SUM(sales_u) over (partition by half,channel_num,groupid_frame) AS sales_choices,
		SUM(eoh_u) over (partition by half,channel_num,groupid_frame) AS eoh_choices
	FROM
		inv_sales_agg)a
group by 1,2,3,4,5,6,7) WITH DATA PRIMARY INDEX (groupid_frame,size_1_idnt,size_2_idnt, half, chnl_idnt)  ON COMMIT preserve rows;

collect statistics
     primary index (half,chnl_idnt,groupid_frame,size_1_idnt, size_2_idnt)
    ,column(half)
    ,column(chnl_idnt)
    ,column(groupid_frame)
    ,column(size_1_idnt)
    ,column(size_2_idnt)
    on sales_inv_agg;


--drop TABLE sale_inv_rcpt_mo_comb;   
CREATE multiset volatile TABLE sale_inv_rcpt_mo_comb AS (
	SELECT 
		a.size_1_id,
		a.size_2_id,
		a.groupid_frame, 
		strtok(a.groupid_frame,'~~',2) AS supplier_idnt,
		strtok(a.groupid_frame,'~~',3) AS class_idnt,
		a.chnl_idnt,
		a.half,
		substring(a.half,1,4) AS fiscal_year,
		a.size_1_rank,
		a.department_id,
		(CASE WHEN d.ty_fiscal_year_num = fiscal_year THEN 'Ty'
			  WHEN (d.ty_fiscal_year_num - 1) = fiscal_year THEN 'Ly'
			  WHEN (d.ty_fiscal_year_num - 2) = fiscal_year THEN 'LLy'
			  ELSE 'Next Year' 
			  END) AS ty_ly_ind,
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
	FROM (
			SELECT
				CAST(size_1_id AS varchar(10)) AS size_1_id,
				CAST(size_2_id AS varchar(10)) AS size_2_id,
				CAST(groupid_frame AS varchar(50)) AS groupid_frame,
				CAST(chnl_idnt AS varchar(10)) AS chnl_idnt,
				CAST(half AS varchar(5)) AS half ,
				CAST(size_1_rank AS float) AS size_1_rank,
				CAST(department_id AS integer) AS department_id,
				CAST(pct_size2 AS float) AS pct_procured,
				CAST(ratio AS float) AS pct_model,	 
				CAST(0 AS INTEGER) AS sku_sales,
				CAST(0 AS INTEGER) AS return_u,
				CAST(0 AS INTEGER) AS eoh,
				CAST(0 AS decimal (20,4)) AS sku_gross_sales,
				CAST(0 AS decimal (20,4)) AS sku_gross_reg_sales,
				CAST(0 AS decimal (20,4)) AS sku_gross_promo_sales,
				CAST(0 AS decimal (20,4)) AS sku_gross_clr_sales,
				CAST(0 AS decimal (20,4)) AS net_sales_retail,
				CAST(0 AS decimal (20,4)) AS net_sales_cost,
				CAST(0 AS float) AS pct_sales,
				CAST(0 AS float) AS pct_eoh
			FROM out_receipt_comp_dep_1 

			UNION ALL      

			SELECT
				CAST(size_1_idnt AS varchar(10)) AS size_1_id,
				CAST(size_2_idnt AS varchar(10)) AS size_2_id,
				CAST(groupid_frame AS varchar(50)) AS groupid_frame,
				CAST(chnl_idnt AS varchar(10)) AS chnl_idnt,
				CAST(half AS varchar(5)) AS half ,
				CAST(size_1_rank AS float) AS size_1_rank,
				CAST(dept_idnt AS integer) AS department_id,
				CAST(0 AS float) AS pct_procured,
				CAST(0 AS float) AS pct_model,	 
				CAST(sku_sales AS INTEGER) AS sku_sales,
				CAST(return_u AS INTEGER) AS return_u,
				CAST(eoh AS INTEGER) AS eoh,
				CAST(sku_gross_sales AS decimal (20,4)) AS sku_gross_sales,
				CAST(sku_gross_reg_sales AS decimal (20,4)) AS sku_gross_reg_sales,
				CAST(sku_gross_promo_sales AS decimal (20,4)) AS sku_gross_promo_sales,
				CAST(sku_gross_clr_sales AS decimal (20,4)) AS sku_gross_clr_sales,
				CAST(net_sales_retail AS decimal (20,4)) AS net_sales_retail,
				CAST(net_sales_cost AS decimal (20,4)) AS net_sales_cost,
				CAST(pct_sales AS float) AS pct_sales,
				CAST(pct_eoh AS float) AS pct_eoh
			FROM sales_inv_agg
	) a
	LEFT JOIN (SELECT DISTINCT fiscal_year_num AS ty_fiscal_year_num FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE) AS d
		ON 1=1
	WHERE substring(a.half,1,4) >= (SELECT DISTINCT fiscal_year_num FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE) - 2
		AND substring(a.half,1,4) <= (SELECT DISTINCT fiscal_year_num FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE) + 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA 
PRIMARY INDEX (half,chnl_idnt,groupid_frame,size_1_id,size_2_id) 
ON COMMIT preserve ROWS;
    
 collect statistics
     primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)
    ,column(half)
    ,column(chnl_idnt)
    ,column(groupid_frame)
    ,column(size_1_id)
    ,column(size_2_id)
    on sale_inv_rcpt_mo_comb;
   
      
   -- drop TABLE sale_inv_rcpt_mo_tly ;
    CREATE multiset volatile TABLE sale_inv_rcpt_mo_tly AS (
    SELECT 
	     size_1_id,
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
		 SUM(pct_procured) AS pct_procured ,
	     SUM(pct_model) AS pct_model ,
		 SUM(sku_sales) AS sku_sales ,
		 SUM(return_units) AS return_units,
		 SUM(eoh) AS eoh,
		 SUM(sku_gross_sales) AS sku_gross_sales,
		 SUM(sku_gross_reg_sales) AS sku_gross_reg_sales ,
		 SUM(sku_gross_promo_sales) AS sku_gross_promo_sales,
		 SUM(sku_gross_clr_sales) AS sku_gross_clr_sales ,
		 SUM(net_sales_retail) AS net_sales_retail ,
		 SUM(net_sales_cost) AS net_sales_cost,
		 SUM(pct_sales) AS pct_sales,
		 SUM(pct_eoh) AS pct_eoh,
		 MIN(Ty_gross_sales_r) AS Ty_gross_sales_r ,
		 MIN(Ly_gross_sales_r) AS Ly_gross_sales_r,
		 MIN(Ty_net_sales_r) AS Ty_net_sales_r,
		 MIN(Ly_net_sales_r) AS Ly_net_sales_r,
		 MIN(Ty_return_u) AS Ty_return_u,
		 MIN(Ly_return_u) AS Ly_return_u,
		 MIN(Ty_sku_sales) AS Ty_sku_sales,
		 MIN(Ly_sku_sales) AS Ly_sku_sales,
		 MIN(Ty_net_sales_c) AS Ty_net_sales_c,
		 MIN(Ly_net_sales_c) AS Ly_net_sales_c, 
		 MIN(Ty_gross_reg_sales) AS Ty_gross_reg_sales,
		 MIN(Ly_gross_reg_sales) AS Ly_gross_reg_sales,
		 MIN(Ty_gross_promo_sales) AS Ty_gross_promo_sales,
		 MIN(Ly_gross_promo_sales) AS Ly_gross_promo_sales,
		 MIN(Ty_gross_clr_sales) AS Ty_gross_clr_sales,
		 MIN(Ly_gross_clr_sales) AS Ly_gross_clr_sales		 
    FROM (
		SELECT
			size_1_id,
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
			SUM(CASE WHEN ty_ly_ind='Ty'THEN sku_gross_sales END) over (partition by chnl_idnt,department_id) AS Ty_gross_sales_r,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN sku_gross_sales END) over (partition by chnl_idnt,department_id) AS Ly_gross_sales_r,
			SUM(CASE WHEN ty_ly_ind='Ty'THEN net_sales_retail END) over (partition by chnl_idnt,department_id) AS Ty_net_sales_r,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN net_sales_retail END) over (partition by chnl_idnt,department_id) AS Ly_net_sales_r,
			SUM(CASE WHEN ty_ly_ind='Ty'THEN return_units END) over (partition by chnl_idnt,department_id) AS Ty_return_u,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN return_units END) over (partition by chnl_idnt,department_id) AS Ly_return_u,
			SUM(CASE WHEN ty_ly_ind='Ty'THEN sku_sales END) over (partition by chnl_idnt,department_id) AS Ty_sku_sales,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN sku_sales END) over (partition by chnl_idnt,department_id) AS Ly_sku_sales,
			SUM(CASE WHEN ty_ly_ind='Ty'THEN net_sales_cost END) over (partition by chnl_idnt,department_id) AS Ty_net_sales_c,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN net_sales_cost END) over (partition by chnl_idnt,department_id) AS Ly_net_sales_c,
			SUM(CASE WHEN ty_ly_ind='Ty'THEN sku_gross_reg_sales END) over (partition by chnl_idnt,department_id) AS Ty_gross_reg_sales,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN sku_gross_reg_sales END) over (partition by chnl_idnt,department_id) AS Ly_gross_reg_sales,
			SUM(CASE WHEN ty_ly_ind='Ty'THEN sku_gross_promo_sales END) over (partition by chnl_idnt,department_id) AS Ty_gross_promo_sales,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN sku_gross_promo_sales END) over (partition by chnl_idnt,department_id) AS Ly_gross_promo_sales,
			SUM(CASE WHEN ty_ly_ind='Ty'THEN sku_gross_clr_sales END) over (partition by chnl_idnt,department_id) AS Ty_gross_clr_sales,
			SUM(CASE WHEN ty_ly_ind='Ly'THEN sku_gross_clr_sales END) over (partition by chnl_idnt,department_id) AS Ly_gross_clr_sales
		FROM sale_inv_rcpt_mo_comb
	 ) a 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
	) WITH DATA 
	PRIMARY INDEX (half,chnl_idnt,groupid_frame,size_1_id,size_2_id)  ON COMMIT preserve rows;
    
collect statistics
	primary index (half,chnl_idnt,groupid_frame,size_1_id, size_2_id)
	,column(half)
	,column(chnl_idnt)
	,column(groupid_frame)
	,column(size_1_id)
	,column(size_2_id)
on sale_inv_rcpt_mo_tly;
    


  --drop TABLE supp_cls_dept;
  CREATE multiset volatile TABLE supp_cls_dept AS (
	SELECT 
		dept_idnt,
		dept_desc,
		class_idnt,
		class_desc,
		supplier_idnt,
		supplier_name,
		groupid_frame,
		TRIM(supplier_idnt||','||supplier_name) AS supplier_,
		TRIM(class_idnt||','||class_desc) AS class_,
		TRIM(dept_idnt||','||DEPT_desc) AS Department_
	FROM product_sku
	GROUP BY 1,2,3,4,5,6,7,8,9,10
  ) WITH DATA
  ON COMMIT preserve ROWS;


--drop TABLE Supp_class_dept_name ; 
CREATE multiset volatile TABLE Supp_class_dept_name AS (
	SELECT 
		a.*
		,TRIM(a.supplier_idnt||','||b.supplier_name) AS supplier_
		,TRIM(a.class_idnt||','||b.class_desc) AS class_
		,TRIM(a.department_id||','||b.DEPT_desc) AS Department_  
	FROM sale_inv_rcpt_mo_tly a
	JOIN supp_cls_dept b
	  ON a.department_id=b.dept_idnt
	 AND a.supplier_idnt=b.supplier_idnt
	 AND a.class_idnt=b.class_idnt
	 AND a.groupid_frame=b.groupid_frame 
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43
) WITH DATA 
ON COMMIT preserve rows;
   
--drop TABLE adop_add;
CREATE multiset volatile TABLE adop_add AS (
	SELECT 
		a.*,
		b.Percent_Sc_FLS_rcpt_units,
		b.Percent_Sc_RACK_rcpt_units,
		b.Percent_Sc_NRHL_rcpt_units,
		Percent_Sc_NCOM_rcpt_units,
		b.adoption
	FROM Supp_class_dept_name a
	LEFT JOIN adoption_percetange_Y_1_J_rev b 
	  ON a.department_id=b.dept_id
	 AND b.chnl_idnt=a.chnl_idnt
	 AND b.fiscal_halfyear_num=a.half 
) WITH DATA
ON COMMIT preserve rows;
  
   
--drop TABLE rmse_sales_procure_add;
CREATE multiset volatile TABLE rmse_sales_procure_add AS (
SELECT 
	a.*,
	(average((pct_model - pct_sales)** 2) over (partition by half, chnl_idnt,groupid_frame))**.5  rmse_pct_sales,
	(average((pct_model - pct_procured)** 2) over(partition by half, chnl_idnt, groupid_frame))**.5  rmse_pct_procured		
FROM adop_add a
WHERE department_id IS NOT NULL
) with data 
ON COMMIT preserve rows;


DELETE FROM {environment_schema}.size_curve_evaluation{env_suffix};

INSERT INTO {environment_schema}.size_curve_evaluation{env_suffix}
SELECT 
	 a.* 
	,CURRENT_TIMESTAMP AS update_timestamp
FROM rmse_sales_procure_add a