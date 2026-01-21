/*
Plan & LY/LLY Data Script
Author: Sara Riker & David Selover
8/15/22: Create script
Updates Weekly on Sundays

Updates Tables with historicals:
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


CREATE MULTISET VOLATILE TABLE LY_data AS (
SELECT 
      ly_month_idnt
    , plan_month_idnt
    , lly_month_idnt
	, channel_brand
	, dept_idnt
	, category
	, demand_dollars
	, demand_units
	, sales_dollars
	, sales_units
	, rcpt_units
	, rcpt_dollars
	, ccs_daily_avg_los
	, ccs_mnth_new
	, ccs_mnth_cf
FROM t2dl_das_selection.selection_planning_monthly lyd
INNER JOIN month_ref mrly 
   ON lyd.mnth_idnt = mrly.ly_month_idnt
WHERE channel_country IN 'US'
)
WITH DATA
PRIMARY INDEX (channel_brand, DEPT_IDNT, CATEGORY)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)
     ,COLUMN (plan_month_idnt)
     ON LY_data;
     

-- TEMP table for LLY DATA
-- DROP TABLE LLY_data;
CREATE MULTISET VOLATILE TABLE LLY_data AS (
SELECT 
      ly_month_idnt
    , plan_month_idnt
    , lly_month_idnt
	, channel_brand
	, dept_idnt
	, category
	, demand_dollars
	, demand_units
	, sales_dollars
	, sales_units
	, rcpt_units
	, rcpt_dollars
	, ccs_daily_avg_los
	, ccs_mnth_new
	, ccs_mnth_cf
FROM t2dl_das_selection.selection_planning_monthly llyd
INNER JOIN month_ref mr 
   ON llyd.mnth_idnt = mr.lly_month_idnt
WHERE channel_country IN 'US'
)
WITH DATA
PRIMARY INDEX (channel_brand, DEPT_IDNT, CATEGORY)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)
     ,COLUMN (plan_month_idnt)
     ON LLY_data;
    
    
-- Final Insert 
-- DROP TABLE ly_lly_insert;
CREATE MULTISET VOLATILE TABLE ly_lly_insert AS (
SELECT 
      p.*
	, lyd.channel_brand
	, h.division_num 
	, h.division_name 
	, h.subdivision_num 
	, h.subdivision_name 
	, lyd.DEPT_IDNT
	, h.dept_name 
	, lyd.category
	, lyd.demand_dollars AS ly_demand_$
	, lyd.demand_units AS ly_demand_u
	, lyd.sales_dollars AS ly_sales_$
	, lyd.sales_units AS ly_sales_u
	, lyd.rcpt_units AS ly_rcpt_u
	, lyd.rcpt_dollars AS ly_rcpt_$
	, lyd.ccs_daily_avg_los AS ly_los_ccs
	, lyd.ccs_mnth_new AS ly_new_ccs
	, lyd.ccs_mnth_cf AS ly_cf_ccs
	, llyd.demand_dollars AS lly_demand_$
	, llyd.demand_units AS lly_demand_u
	, llyd.sales_dollars AS lly_sales_$
	, llyd.sales_units AS lly_sales_u
	, llyd.rcpt_dollars AS lly_rcpt_$
	, llyd.rcpt_units AS lly_rcpt_u
	, llyd.ccs_daily_avg_los AS lly_los_ccs
	, llyd.ccs_mnth_new AS lly_new_ccs
	, llyd.ccs_mnth_cf AS lly_cf_ccs
FROM LY_data lyd
LEFT JOIN LLY_data llyd
  ON lyd.dept_idnt = llyd.dept_idnt
 AND lyd.category = llyd.category
 AND lyd.channel_brand = llyd.channel_brand
 AND lyd.plan_month_idnt = llyd.plan_month_idnt
JOIN month_ref p
  ON p.plan_month_idnt = lyd.plan_month_idnt
JOIN hierarchy h
  ON lyd.dept_idnt = h.dept_num
WHERE h.division_num IN ('310', '340', '345', '351', '360', '365', '700')
) 
WITH DATA
PRIMARY INDEX (channel_brand, dept_idnt, category)
ON COMMIT PRESERVE ROWS
;

-- Insert LY and LLY data
MERGE INTO {environment_schema}.e2e_selection_planning{env_suffix} AS a
USING ly_lly_insert AS b
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

    	, dept_name = b.dept_name

    	-- ly/lly
    	, ly_demand_$ = b.ly_demand_$
    	, ly_demand_u = b.ly_demand_u
    	, ly_sales_$ = b.ly_sales_$
    	, ly_sales_u = b.ly_sales_u
    	, ly_rcpt_u = b.ly_rcpt_u
    	, ly_rcpt_$ = b.ly_rcpt_$
    	, ly_los_ccs = b.ly_los_ccs
    	, ly_new_ccs = b.ly_new_ccs
    	, ly_cf_ccs = b.ly_cf_ccs
    	, lly_demand_$ = b.lly_demand_$
    	, lly_demand_u = b.lly_demand_u
    	, lly_sales_$ = b.lly_sales_$
    	, lly_sales_u = b.lly_sales_u
		, lly_rcpt_$ = b.lly_rcpt_$
    	, lly_rcpt_u = b.lly_rcpt_u
		, lly_los_ccs = b.lly_los_ccs
    	, lly_new_ccs = b.lly_new_ccs
    	, lly_cf_ccs = b.lly_cf_ccs 
    	, ly_lly_update_timestamp = current_timestamp
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
            -- ly/lly
        	, b.ly_demand_$
        	, b.ly_demand_u
        	, b.ly_sales_$
        	, b.ly_sales_u
			, b.ly_rcpt_$
        	, b.ly_rcpt_u
			, b.ly_los_ccs
        	, b.ly_new_ccs
        	, b.ly_cf_ccs
        	, b.lly_demand_$
        	, b.lly_demand_u
        	, b.lly_sales_$
        	, b.lly_sales_u
        	, b.lly_rcpt_u
        	, b.lly_rcpt_$
        	, b.lly_los_ccs
        	, b.lly_new_ccs
        	, b.lly_cf_ccs 
			-- cm data
			, NULL
            -- update info
            , current_timestamp 
            , NULL 
			, NULL
            , current_timestamp  
    )
;

-- DROP TABLE month_ref_plan;
CREATE MULTISET VOLATILE TABLE month_18_ref AS (
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
    , month_idnt - 200 AS lly_month_idnt
FROM prd_nap_usr_vws.day_cal_454_dim
WHERE month_idnt BETWEEN 
	((SELECT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE() + 365)) 
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
     ON month_18_ref;

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
	 
CREATE MULTISET VOLATILE TABLE LY_18_data AS (
SELECT 
      ly_month_idnt
    , plan_month_idnt
    , lly_month_idnt
	, channel_brand
	, dept_idnt
	, category
	, plan_dmd_r
	, plan_dmd_u
	, plan_net_sls_r
	, plan_net_sls_u
	, plan_rcpt_r
	, plan_rcpt_u
	, plan_los_ccs
	, plan_new_ccs
	, plan_cf_ccs
FROM plan_data lyd
INNER JOIN month_18_ref mrly 
   ON lyd.month_idnt = mrly.ly_month_idnt
)
WITH DATA
PRIMARY INDEX (channel_brand, DEPT_IDNT, CATEGORY)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)
     ,COLUMN (plan_month_idnt)
     ON LY_18_data;
     

-- TEMP table for LLY DATA
-- DROP TABLE LLY_data;
CREATE MULTISET VOLATILE TABLE lly_18_data AS (
SELECT 
      ly_month_idnt
    , plan_month_idnt
    , lly_month_idnt
	, channel_brand
	, dept_idnt
	, category
	, demand_dollars
	, demand_units
	, sales_dollars
	, sales_units
	, rcpt_dollars
	, rcpt_units
	, ccs_daily_avg_los
	, ccs_mnth_new
	, ccs_mnth_cf
FROM t2dl_das_selection.selection_planning_monthly llyd
INNER JOIN month_18_ref mr 
   ON llyd.mnth_idnt = mr.lly_month_idnt
WHERE channel_country IN 'US'
)
WITH DATA
PRIMARY INDEX (channel_brand, DEPT_IDNT, CATEGORY)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)
     ,COLUMN (plan_month_idnt)
     ON lly_18_data;
    
    
-- Final Insert 
-- DROP TABLE ly_lly_insert;
CREATE MULTISET VOLATILE TABLE ly_lly_18_insert AS (
SELECT 
      p.*
	, lyd.channel_brand
	, h.division_num 
	, h.division_name 
	, h.subdivision_num 
	, h.subdivision_name 
	, lyd.DEPT_IDNT
	, h.dept_name 
	, lyd.category
	, lyd.plan_dmd_r AS ly_demand_$
	, lyd.plan_dmd_u AS ly_demand_u
	, lyd.plan_net_sls_r AS ly_sales_$
	, lyd.plan_net_sls_u AS ly_sales_u
	, lyd.plan_rcpt_r AS ly_rcpt_$
	, lyd.plan_rcpt_u AS ly_rcpt_u
	, lyd.plan_los_ccs AS ly_los_ccs
	, lyd.plan_new_ccs AS ly_new_ccs
	, lyd.plan_cf_ccs AS ly_cf_ccs
	, llyd.demand_dollars AS lly_demand_$
	, llyd.demand_units AS lly_demand_u
	, llyd.sales_dollars AS lly_sales_$
	, llyd.sales_units AS lly_sales_u
	, llyd.rcpt_units AS lly_rcpt_u
	, llyd.rcpt_dollars AS lly_rcpt_$
	, llyd.ccs_daily_avg_los AS lly_los_ccs
	, llyd.ccs_mnth_new AS lly_new_ccs
	, llyd.ccs_mnth_cf AS lly_cf_ccs
FROM LY_18_data lyd
LEFT JOIN lly_18_data llyd
  ON lyd.dept_idnt = llyd.dept_idnt
 AND lyd.category = llyd.category
 AND lyd.channel_brand = llyd.channel_brand
 AND lyd.plan_month_idnt = llyd.plan_month_idnt
JOIN month_18_ref p
  ON p.plan_month_idnt = lyd.plan_month_idnt
JOIN hierarchy h
  ON lyd.dept_idnt = h.dept_num
WHERE h.division_num IN ('310', '340', '345', '351', '360', '365', '700')
) 
WITH DATA
PRIMARY INDEX (channel_brand, dept_idnt, category)
ON COMMIT PRESERVE ROWS
;
     
-- plan/ly/lly insert
MERGE INTO {environment_schema}.e2e_selection_planning{env_suffix} AS a
USING ly_lly_18_insert AS b
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

    	, dept_name = b.dept_name

    	-- ly/lly
    	, ly_demand_$ = b.ly_demand_$
    	, ly_demand_u = b.ly_demand_u
    	, ly_sales_$ = b.ly_sales_$
    	, ly_sales_u = b.ly_sales_u
    	, ly_rcpt_u = b.ly_rcpt_u
    	, ly_rcpt_$ = b.ly_rcpt_$
    	, ly_los_ccs = b.ly_los_ccs
    	, ly_new_ccs = b.ly_new_ccs
    	, ly_cf_ccs = b.ly_cf_ccs
    	, lly_demand_$ = b.lly_demand_$
    	, lly_demand_u = b.lly_demand_u
    	, lly_sales_$ = b.lly_sales_$
    	, lly_sales_u = b.lly_sales_u
		, lly_rcpt_$ = b.lly_rcpt_$
    	, lly_rcpt_u = b.lly_rcpt_u
		, lly_los_ccs = b.lly_los_ccs
    	, lly_new_ccs = b.lly_new_ccs
    	, lly_cf_ccs = b.lly_cf_ccs 
    	, ly_lly_update_timestamp = current_timestamp
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
            -- ly/lly
        	, b.ly_demand_$
        	, b.ly_demand_u
        	, b.ly_sales_$
        	, b.ly_sales_u
			, b.ly_rcpt_$
        	, b.ly_rcpt_u
			, b.ly_los_ccs
        	, b.ly_new_ccs
        	, b.ly_cf_ccs
        	, b.lly_demand_$
        	, b.lly_demand_u
        	, b.lly_sales_$
        	, b.lly_sales_u
        	, b.lly_rcpt_u
        	, b.lly_rcpt_$
        	, b.lly_los_ccs
        	, b.lly_new_ccs
        	, b.lly_cf_ccs 
			-- cm data
			, NULL
            -- update info
            , current_timestamp 
            , NULL 
			, NULL
            , current_timestamp  
    )
;