/*
Plan & LY/LLY Data Update
Author: Sara Riker & David Selover
8/15/22: Create Script

Updates Tables with plans:
    {environment_schema}.e2e_selection_planning 
*/
 

CREATE MULTISET VOLATILE TABLE hierarchy AS (
SELECT
	  dept_num 
	, dept_name 
	, division_num 
	, division_name 
	, subdivision_num 
	, subdivision_name 
FROM prd_nap_usr_vws.department_dim
WHERE division_name NOT LIKE 'INACTIVE%'
)
WITH DATA
PRIMARY INDEX (dept_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(dept_num)
     ,COLUMN (division_num)
     ,COLUMN (subdivision_num)
     ON hierarchy;
     
-- DROP TABLE month_ref_plan;
CREATE MULTISET VOLATILE TABLE month_ref AS (
SELECT DISTINCT
	  month_idnt 
	, month_label 
	, quarter_idnt 
	, quarter_label 
	, fiscal_year_num 	
	, fiscal_month_num
	, month_start_day_date
    , month_idnt AS plan_month_idnt
    , month_idnt - 100 AS ly_month_idnt
    , month_idnt - 200 AS lly_month_idnt -- edited to reference LLY to 2021 instead of 2019 
FROM prd_nap_usr_vws.day_cal_454_dim
WHERE month_idnt BETWEEN 
	((SELECT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE())) 
	AND 
	((SELECT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE() + 548))
)
WITH DATA
PRIMARY INDEX (month_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(month_idnt)
     ,COLUMN (quarter_idnt)
     ,COLUMN (fiscal_year_num)
     ON month_ref;
     
-- DROP TABLE plan_data;
CREATE MULTISET VOLATILE TABLE plan_data AS (
SELECT
    CONCAT
		('20',RIGHT(plan_fiscal_month,2)
		,CASE
			WHEN LEFT(plan_fiscal_month,3) = 'FEB' THEN '01'
			WHEN LEFT(plan_fiscal_month,3) = 'MAR' THEN '02'
			WHEN LEFT(plan_fiscal_month,3) = 'APR' THEN '03'
			WHEN LEFT(plan_fiscal_month,3) = 'MAY' THEN '04'
			WHEN LEFT(plan_fiscal_month,3) = 'JUN' THEN '05'
			WHEN LEFT(plan_fiscal_month,3) = 'JUL' THEN '06'
			WHEN LEFT(plan_fiscal_month,3) = 'AUG' THEN '07'
			WHEN LEFT(plan_fiscal_month,3) = 'SEP' THEN '08'
			WHEN LEFT(plan_fiscal_month,3) = 'OCT' THEN '09'
			WHEN LEFT(plan_fiscal_month,3) = 'NOV' THEN '10'
			WHEN LEFT(plan_fiscal_month,3) = 'DEC' THEN '11'
			WHEN LEFT(plan_fiscal_month,3) = 'JAN' THEN '12'
		END) month_idnt
	,CASE WHEN cluster_label LIKE 'N.COM' THEN 'NORDSTROM'
		  WHEN cluster_label LIKE 'RACK.COM' THEN 'NORDSTROM_RACK'
      	END AS channel_brand
	, LEFT(dept_category,(LOCATE('_',dept_category)-1)) as dept_idnt
	, RIGHT(dept_category,CAST(LENGTH(dept_category) - (LOCATE('_',dept_category)) as int)) as category
	, plan_dmd_r
	, plan_dmd_u
	, plan_net_sls_r
	, plan_net_sls_u
	, plan_rcpt_r
	, plan_rcpt_u
	, model_los_ccs
	, plan_los_ccs
	, model_new_ccs
	, plan_new_ccs
	, model_cf_ccs
	, plan_cf_ccs
	, model_sell_offs_ccs
	, plan_sell_off_ccs
	, ly_sell_off_ccs
	, lly_sell_off_ccs
	, rcd_load_date
	, row_number() OVER (PARTITION BY plan_fiscal_month, cluster_label, dept_category ORDER BY rcd_load_date DESC) AS date_rank
FROM t3dl_nap_planning.sel_dept_category_plan
WHERE dept_category NOT LIKE '%TOTAL%'
)
WITH DATA
PRIMARY INDEX (channel_brand, DEPT_IDNT, CATEGORY)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)
     ,COLUMN (month_idnt)
     ON plan_data;

-- DROP TABLE plan_insert;
CREATE MULTISET VOLATILE TABLE plan_insert AS ( 
SELECT 
      p.month_idnt
    , m.month_start_day_date
    , m.fiscal_month_num
    , m.fiscal_year_num
    , m.plan_month_idnt
    , m.ly_month_idnt 
    , m.lly_month_idnt
	, m.month_label 
	, m.quarter_idnt 
	, m.quarter_label 

    , p.channel_brand
	, h.division_num 
	, h.division_name 
	, h.subdivision_num 
	, h.subdivision_name 
	, p.dept_idnt
	, h.dept_name 
	, p.category
	, plan_dmd_r
	, plan_dmd_u
	, plan_net_sls_r
	, plan_net_sls_u
	, plan_rcpt_r
	, plan_rcpt_u
	, model_los_ccs
	, plan_los_ccs
	, model_new_ccs
	, plan_new_ccs
	, model_cf_ccs
	, plan_cf_ccs
	, model_sell_offs_ccs
	, plan_sell_off_ccs
	, ly_sell_off_ccs
	, lly_sell_off_ccs
FROM plan_data p
JOIN month_ref m 
  ON p.month_idnt = m.plan_month_idnt
JOIN hierarchy h
  ON p.dept_idnt = h.dept_num
WHERE h.division_num IN ('310', '340', '345', '351', '360', '365', '700')
  AND date_rank = 1
)
WITH DATA
PRIMARY INDEX (channel_brand, dept_idnt, category)
ON COMMIT PRESERVE ROWS;

-- plan insert
MERGE INTO {environment_schema}.e2e_selection_planning AS a
USING plan_insert AS b
   ON a.month_idnt = b.month_idnt
  AND a.fiscal_month_num = b.fiscal_month_num
  AND a.fiscal_year_num = b.fiscal_year_num
  AND a.channel_brand = b.channel_brand
  AND a.dept_idnt = b.dept_idnt
  AND a.category = b.category
  AND a.month_start_day_date = b.month_start_day_date
WHEN MATCHED
THEN UPDATE
	SET
          plan_month_idnt = b.plan_month_idnt
        , ly_month_idnt = b.ly_month_idnt
        , lly_month_idnt = b.lly_month_idnt
    	, month_label = b.month_label
    	, quarter_idnt = b.quarter_idnt
    	, quarter_label = b.quarter_label
    	, division_num = b.division_num
    	, division_name = b.division_name
    	, subdivision_num = b.subdivision_num
    	, subdivision_name = b.subdivision_name

    	, dept_name = b.dept_name

    	-- plan
    	, plan_dmd_r = b.plan_dmd_r
    	, plan_dmd_u = b.plan_dmd_u
    	, plan_net_sls_r = b.plan_net_sls_r
    	, plan_net_sls_u = b.plan_net_sls_u
    	, plan_rcpt_r = b.plan_rcpt_r
    	, plan_rcpt_u = b.plan_rcpt_u
    	, model_los_ccs = b.model_los_ccs
    	, plan_los_ccs = b.plan_los_ccs
    	, model_new_ccs = b.model_new_ccs
    	, plan_new_ccs = b.plan_new_ccs
    	, model_cf_ccs = b.model_cf_ccs
    	, plan_cf_ccs = b.plan_cf_ccs
    	, model_sell_offs_ccs = b.model_sell_offs_ccs
    	, plan_sell_off_ccs = b.plan_sell_off_ccs
    	, ly_sell_off_ccs = b.ly_sell_off_ccs
    	, lly_sell_off_ccs = b.lly_sell_off_ccs
    	, plan_update_timestamp = current_timestamp
    	, update_timestamp = current_timestamp
WHEN NOT MATCHED
THEN INSERT
	VALUES (
              b.fiscal_month_num  
            , b.fiscal_year_num  
            , b.month_idnt 
            , b.month_start_day_date  
            , b.plan_month_idnt
        	, b.ly_month_idnt  
        	, b.lly_month_idnt 
        	, b.month_label 
        	, b.quarter_idnt 
        	, b.quarter_label
            , b.channel_brand 
        	, b.division_num 
        	, b.division_name 
        	, b.subdivision_num 
        	, b.subdivision_name 
        	, b.dept_idnt 
        	, b.dept_name
        	, b.category 
            -- plan 
        	, b.plan_dmd_r 
        	, b.plan_dmd_u
        	, b.plan_net_sls_r
        	, b.plan_net_sls_u 
        	, b.plan_rcpt_r
        	, b.plan_rcpt_u 
        	, b.model_los_ccs 
        	, b.plan_los_ccs 
        	, b.model_new_ccs
        	, b.plan_new_ccs 
        	, b.model_cf_ccs 
        	, b.plan_cf_ccs 
        	, b.model_sell_offs_ccs 
        	, b.plan_sell_off_ccs 
        	, b.ly_sell_off_ccs 
        	, b.lly_sell_off_ccs 
            -- ly/lly
        	, NULL
        	, NULL 
        	, NULL
        	, NULL 
        	, NULL
        	, NULL 
        	, NULL
        	, NULL 
        	, NULL 
        	, NULL 
        	, NULL 
        	, NULL 
        	, NULL
        	, NULL 
            , NULL 
        	, NULL 
        	, NULL
        	, NULL 
			-- cm data
			, NULL
            -- update info
            , NULL 
            , current_timestamp 
			, NULL
            , current_timestamp  
    )
;

-- purge old months
DELETE FROM {environment_schema}.e2e_selection_planning WHERE month_idnt NOT IN (SELECT DISTINCT month_idnt FROM month_ref);