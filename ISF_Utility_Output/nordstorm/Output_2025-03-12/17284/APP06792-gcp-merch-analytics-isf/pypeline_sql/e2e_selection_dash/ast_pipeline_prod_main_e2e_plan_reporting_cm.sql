/*
Plan & LY/LLY Data DDL
Author: Sara Riker & David Selover
8/15/22: Create script

Updates Tables with CM:
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
     
CREATE MULTISET VOLATILE TABLE cm_data AS (
SELECT 
     CASE WHEN org_id  = 120 THEN 'NORDSTROM'
          WHEN org_id = 250 THEN 'NORDSTROM_RACK'
          END AS channel_brand
    ,fiscal_month_id AS month_idnt
    ,dept_id AS dept_idnt
    ,COALESCE(map1.category, map2.category, 'OTHER') AS category
    ,COUNT(DISTINCT dept_id || '_' || supp_id || '_' || vpn || '_' || COALESCE(TRIM(nrf_color_code), 'UNNOWN')) AS new_ccs_cm
FROM t2dl_das_open_to_buy.ab_cm_orders_current c
LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim map1
  ON c.dept_id = map1.dept_num
 AND c.class_id = map1.class_num
 AND c.subclass_id = map1.sbclass_num
LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim map2
  ON c.dept_id = map2.dept_num
 AND c.class_id = map2.class_num
 AND map2.sbclass_num = '-1'
WHERE org_id IN (120, 250)
  AND plan_type = 'FASHION'
GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX (month_idnt, channel_brand, dept_idnt, category)
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cm_insert AS ( 
SELECT 
      c.month_idnt
    , m.month_start_day_date
    , m.fiscal_month_num
    , m.fiscal_year_num
    , m.plan_month_idnt
    , m.ly_month_idnt 
    , m.lly_month_idnt
	, m.month_label 
	, m.quarter_idnt 
	, m.quarter_label 

    , c.channel_brand
	, h.division_num 
	, h.division_name 
	, h.subdivision_num 
	, h.subdivision_name 
	, c.dept_idnt
	, h.dept_name 
	, c.category
	, c.new_ccs_cm
FROM cm_data c
JOIN month_ref m 
  ON c.month_idnt = m.month_idnt
JOIN hierarchy h
  ON c.dept_idnt = h.dept_num
WHERE h.division_num IN ('310', '340', '345', '351', '360', '365', '700')
)
WITH DATA
PRIMARY INDEX (channel_brand, dept_idnt, category)
ON COMMIT PRESERVE ROWS;


MERGE INTO {environment_schema}.e2e_selection_planning AS a
USING cm_insert AS b
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

    	-- cm
    	, new_ccs_cm = b.new_ccs_cm

    	, cm_data_update_timestamp = current_timestamp
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
			, b.new_ccs_cm
            -- update info
            , NULL 
            , NULL
			, current_timestamp
            , current_timestamp  
    )
;