SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_sell_views_11521_ACE_ENG;
     Task_Name=remote_sell_employee_vw;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: May 6,2024
*/

REPLACE VIEW {remote_selling_t2_schema}.REMOTE_SELL_EMPLOYEE_VW
AS
LOCK ROW FOR ACCESS

/*
----------------------------------------------------------------
Step 1: Create remote sell sales table. Nordstrom to You to be
released in future. Payroll store is intentionally removed
in this view. Only the employee's most current store will be
shown to allow filtering, but keep employee's TY and LY
metrics together
----------------------------------------------------------------
*/

-- all attributed item ind columns to be removed after dashboard development
WITH remote_sell_sales_emp_daily AS
(
SELECT  a.business_day_date,
        a.remote_sell_swimlane,
        COALESCE(a.remote_sell_employee_id, 'none') AS remote_sell_employee_id,
        a.employee_name,
        0 as return_flag, 
        COUNT(DISTINCT case when trim(a.global_tran_id) not like '-%' then a.global_tran_id end)  AS orders,
        SUM(a.shipped_usd_sales) AS sales_usd,
        SUM(a.shipped_qty) AS units

FROM {remote_selling_t2_schema}.remote_sell_transactions a
--WHERE a.remote_sell_swimlane <> 'NORDSTROM TO YOU' -- discuss best way to handle N2Y in base table since fees will not likely flow
GROUP BY 1,2,3,4,5

UNION 

SELECT  a.return_date,
        a.remote_sell_swimlane,
        COALESCE(a.remote_sell_employee_id, 'none') AS remote_sell_employee_id,
        a.employee_name,
        1 as return_flag,
        COUNT(DISTINCT case when trim(a.global_tran_id) not like '-%' then a.global_tran_id end)  AS return_orders, -- does the business even need this?
        SUM(a.return_usd_amt) AS return_amt_usd,
        SUM(a.return_qty) AS return_units
        
FROM {remote_selling_t2_schema}.remote_sell_transactions a 
WHERE a.return_date IS NOT NULL -- remove untied return transactions
-- and a.remote_sell_swimlane <> 'NORDSTROM TO YOU' -- discuss best way to handle N2Y in base table since fees will not likely flow
GROUP BY 1,2,3,4,5



)


/*
-------------------------------------------------------------------
Step 3: Create remote sell net sales view. Nordstrom to You to be
released in future
-------------------------------------------------------------------
*/

SELECT  b.fiscal_year,
        b.fiscal_quarter,
        b.fiscal_month,
        b.fiscal_week,
        b.ly_day_date,
        a.business_day_date,
        a.remote_sell_swimlane,
        a.remote_sell_employee_id, -- 'return store' already computed and will not have a current payroll store
        a.employee_name,
        c.current_payroll_store, --only to be used as a way to filter down employees, not used to base calcs off of since this changes over time. agg occurs at employee to see ty/ly regardless of ly payroll store
        c.current_payroll_store_description,
        
        SUM(case when return_flag = 0 then a.orders end) as orders ,
        SUM(case when return_flag = 1 then a.orders end) as return_orders,
        COALESCE(SUM(case when return_flag = 0 then a.orders end),0) + (-1*COALESCE(return_orders,0)) as net_orders,
        
        
        SUM(case when return_flag = 0 then a.sales_usd end) as gross_sales_usd,
        SUM(case when return_flag = 1 then a.sales_usd end) as return_amt_usd,
        COALESCE(gross_sales_usd,0)+ (-1*COALESCE(return_amt_usd,0)) as net_sales_usd,
        
        
        SUM(case when return_flag = 0  then a.units end) as gross_units,
        SUM(case when return_flag = 1  then a.units end) as return_units,
        COALESCE(gross_units,0)+(-1*COALESCE(return_units,0)) as  net_units

FROM remote_sell_sales_emp_daily a

LEFT JOIN
        (
        SELECT a.day_date,
               a.week_of_fyr AS fiscal_week,
               a.month_454_num AS fiscal_month,
               a.quarter_454_num AS fiscal_quarter,
               a.year_num AS fiscal_year,
               a.last_year_day_date_realigned AS ly_day_date

        FROM prd_nap_usr_vws.DAY_CAL a
        ) b
ON a.business_day_date = b.day_date

LEFT JOIN (
        --NEW HR DATA PULL
        SELECT  lpad(a.worker_number, 15,'0') AS worker_number,
                                a.payroll_store as current_payroll_store,
                                stor.store_name AS current_payroll_store_description,
                                ROW_NUMBER() OVER (partition by a.worker_number ORDER BY a.eff_end_date DESC) as row_num

        FROM prd_nap_hr_usr_vws.HR_ORG_DETAILS_DIM_EFF_DATE_VW a
        
        LEFT JOIN (select a.worker_number, a.last_name, a.first_name
					from prd_nap_hr_usr_vws.hr_worker_v1_dim a
					join(select worker_number, max(last_updated) as max_date 
						from prd_nap_hr_usr_vws.hr_worker_v1_dim 				
						group by 1)b 
								on a.worker_number = b.worker_number 
								and a.last_updated = b.max_date
					) nme ON a.worker_number = nme.worker_number
				                        			
        LEFT JOIN (SELECT store_num, store_name, store_type_code 
					FROM prd_nap_usr_vws.STORE_DIM	
                    WHERE store_type_code IN ('FL','RK','NL')
                    ) stor ON cast(trim(LEADING '0' FROM a.payroll_store) AS varchar(10)) = cast(stor.store_num AS varchar(10))
                        		
        LEFT JOIN (select organization_code, organization_description 
					from prd_nap_hr_usr_vws.HR_WORKER_ORG_DIM	
					where organization_type = 'DEPARTMENT'
					and is_inactive = 0
					)dpt on a.payroll_department = dpt.organization_code	
        qualify row_num = 1                

        ) c
ON a.remote_sell_employee_id = c.worker_number
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
;





SET QUERY_BAND = NONE FOR SESSION;
