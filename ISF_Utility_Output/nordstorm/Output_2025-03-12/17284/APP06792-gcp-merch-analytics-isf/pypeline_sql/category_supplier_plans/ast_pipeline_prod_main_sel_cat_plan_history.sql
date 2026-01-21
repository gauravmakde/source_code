CREATE MULTISET VOLATILE TABLE plan_data AS (
SELECT
       cal.month_idnt AS snapshot_plan_month_idnt
     , cal.month_start_day_date AS snapshot_start_day_date
     , CONCAT('20',RIGHT(plan_fiscal_month,2)
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
	, model_sell_offs_ccs AS model_sell_off_ccs
	, plan_sell_off_ccs
	, ly_sell_off_ccs
	, lly_sell_off_ccs
	, model_sell_off_ccs_rate
	, plan_sell_off_ccs_rate
	, ly_sell_off_ccs_rate
	, lly_sell_off_ccs_rate
	, rcd_load_date
-- 	, row_number() OVER (PARTITION BY plan_fiscal_month, cluster_label, dept_category ORDER BY rcd_load_date DESC) AS date_rank
FROM t3dl_nap_planning.sel_dept_category_plan p
JOIN prd_nap_usr_vws.day_cal_454_dim cal
  ON p.rcd_load_date = cal.day_date
QUALIFY row_number() OVER (PARTITION BY plan_fiscal_month, cluster_label, dept_category ORDER BY rcd_load_date DESC) = 1
)
WITH DATA
PRIMARY INDEX (channel_brand, DEPT_IDNT, CATEGORY)
ON COMMIT PRESERVE ROWS
;

MERGE INTO {environment_schema}.sel_dept_category_plan_history AS a
USING plan_data AS b
   ON a.month_idnt = b.month_idnt
  AND a.snapshot_plan_month_idnt = b.snapshot_plan_month_idnt

  AND a.channel_brand = b.channel_brand
  AND a.dept_idnt = b.dept_idnt
  AND a.category = b.category
  AND a.snapshot_start_day_date = b.snapshot_start_day_date
WHEN MATCHED
THEN UPDATE
	SET
    	-- plan
    	  plan_dmd_r = b.plan_dmd_r
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
    	, model_sell_off_ccs = b.model_sell_off_ccs
    	, plan_sell_off_ccs = b.plan_sell_off_ccs
    	, ly_sell_off_ccs = b.ly_sell_off_ccs
    	, lly_sell_off_ccs = b.lly_sell_off_ccs
		, model_sell_off_ccs_rate = b.model_sell_off_ccs_rate
		, plan_sell_off_ccs_rate = b.plan_sell_off_ccs_rate
		, ly_sell_off_ccs_rate = b.ly_sell_off_ccs_rate
		, lly_sell_off_ccs_rate = b.lly_sell_off_ccs_rate
    	, rcd_load_date = b.rcd_load_date
    	, update_timestamp = current_timestamp
WHEN NOT MATCHED
THEN INSERT
	VALUES (
              b.snapshot_plan_month_idnt  
            , b.snapshot_start_day_date
            , b.month_idnt 

            , b.channel_brand 

        	, b.dept_idnt 
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
        	, b.model_sell_off_ccs 
        	, b.plan_sell_off_ccs 
        	, b.ly_sell_off_ccs 
        	, b.lly_sell_off_ccs 
			, b.model_sell_off_ccs_rate
			, b.plan_sell_off_ccs_rate
			, b.ly_sell_off_ccs_rate
			, b.lly_sell_off_ccs_rate
            -- update info
            , b.rcd_load_date 
            , current_timestamp  
    )
;