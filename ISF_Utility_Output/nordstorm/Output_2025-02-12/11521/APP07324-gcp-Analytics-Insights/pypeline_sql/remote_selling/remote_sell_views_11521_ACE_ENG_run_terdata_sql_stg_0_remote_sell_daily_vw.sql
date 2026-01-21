SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_sell_views_11521_ACE_ENG;
     Task_Name=remote_sell_daily_vw;'
     FOR SESSION VOLATILE;

/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: May 6,2024
*/

REPLACE VIEW T2DL_DAS_REMOTE_SELLING.REMOTE_SELL_DAILY_VW
AS
LOCK ROW FOR ACCESS

/*
----------------------------------------------------------------
Step 1: Create remote sell sales table. Nordstrom to You to be
released in future
----------------------------------------------------------------
*/

WITH remote_sell_sales_daily AS
(
SELECT  a.business_day_date,
        a.store_country_code,
        a.remote_sell_swimlane,
        COALESCE(a.remote_sell_employee_id, 'none') AS remote_sell_employee_id,
        a.employee_name,
        a.sale_payroll_store, -- sales and returns always fall under here, but the employeeid drops after x days
        a.sale_payroll_store_description,
        a.sale_payroll_department_description,
        a.employee_payroll_store_region_num,
        a.employee_payroll_region_desc,
        a.platform,
        0 as return_flag,
        COUNT(DISTINCT case when trim(a.global_tran_id) not like '-%' then a.global_tran_id end) AS orders, -- should change to transactions
        SUM(a.shipped_usd_sales) AS sales_usd,
        SUM(a.shipped_qty) AS units,
        SUM(a.order_pickup_ind) AS order_pickup_units,
        SUM(a.ship_to_store_ind) AS ship_to_store_units

FROM T2DL_DAS_REMOTE_SELLING.remote_sell_transactions a
--WHERE a.remote_sell_swimlane <> 'NORDSTROM TO YOU' -- discuss best way to handle N2Y in base table since fees will not likely flow
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

UNION

SELECT  a.return_date,
        a.store_country_code,
        a.remote_sell_swimlane,
        COALESCE(a.remote_sell_employee_id, 'none') AS remote_sell_employee_id,
        a.employee_name,
        a.sale_payroll_store, -- sales and returns always fall under here, but the employeeid drops after x days
        a.sale_payroll_store_description,
        a.sale_payroll_department_description,
        a.employee_payroll_store_region_num,
        a.employee_payroll_region_desc,
        a.platform,
        1 as return_flag,
        COUNT(DISTINCT case when trim(a.global_tran_id) not like '-%' then a.global_tran_id end)  AS return_orders, -- does the business even need this?
        SUM(a.return_usd_amt) AS return_amt_usd,
        SUM(a.return_qty) AS return_units,
        SUM(a.order_pickup_ind) AS return_order_pickup_units,
        SUM(a.ship_to_store_ind) AS return_ship_to_store_units

FROM T2DL_DAS_REMOTE_SELLING.remote_sell_transactions a
WHERE a.return_date IS NOT NULL -- remove untied return transactions
-- and a.remote_sell_swimlane <> 'NORDSTROM TO YOU' -- discuss best way to handle N2Y in base table since fees will not likely flow
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

)

/*
-------------------------------------------------------------------
Step 3: Create remote sell net sales view. Nordstrom to You to be
released in future
-------------------------------------------------------------------
*/



 SELECT b.fiscal_year,
        b.fiscal_quarter,
        b.fiscal_month,
        b.fiscal_week,
        b.ly_day_date,
        a.business_day_date,
        a.store_country_code,
        a.remote_sell_swimlane,
        a.remote_sell_employee_id,
        a.employee_name,
        a.sale_payroll_store,
        a.sale_payroll_store_description,
        a.sale_payroll_department_description,
        a.platform,

        SUM(case when return_flag = 0 then a.orders end) as orders ,
        SUM(case when return_flag = 1 then a.orders end) as return_orders,
        coalesce(SUM(case when return_flag = 0 then a.orders end),0) + (-1*coalesce(return_orders,0)) as net_orders,

        SUM(case when return_flag = 0 then a.sales_usd end) as gross_sales_usd,
        SUM(case when return_flag = 1 then a.sales_usd end) as return_amt_usd,
        coalesce(gross_sales_usd,0)+ (-1*coalesce(return_amt_usd,0)) as net_sales_usd,

        SUM(case when return_flag = 0  then a.units end) as gross_units,
        SUM(case when return_flag = 1  then a.units end) as return_units,
        coalesce(gross_units,0)+(-1*coalesce(return_units,0)) as  net_units,

        SUM(case when return_flag = 0  then a.order_pickup_units end) as order_pickup_units,
        SUM(case when return_flag = 1  then a.order_pickup_units end) as return_order_pickup_units,
        coalesce(SUM(case when return_flag = 0  then a.order_pickup_units end),0) + (-1*coalesce(return_order_pickup_units,0)) as net_order_pickup_units,

        SUM(case when return_flag = 0  then a.ship_to_store_units end) as ship_to_store_units,
        SUM(case when return_flag = 1  then a.ship_to_store_units end) as return_ship_to_store_units,
        coalesce(SUM(case when return_flag = 0  then a.ship_to_store_units end),0) + (-1*coalesce(return_ship_to_store_units,0)) as net_ship_to_store_units


 FROM remote_sell_sales_daily a
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
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;




SET QUERY_BAND = NONE FOR SESSION;
