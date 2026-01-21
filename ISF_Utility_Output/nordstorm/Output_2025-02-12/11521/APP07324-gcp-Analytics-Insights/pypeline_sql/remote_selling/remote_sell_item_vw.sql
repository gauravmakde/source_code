SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_sell_views_11521_ACE_ENG;
     Task_Name=remote_sell_item_vw;'
     FOR SESSION VOLATILE;


/*
Description:

Analytics owner: Engagement Analytics - Agatha Mak
View creator: AE #analytics-engineering
*/

REPLACE VIEW {remote_selling_t2_schema}.REMOTE_SELL_ITEM_VW
AS
LOCK ROW FOR ACCESS

/*
----------------------------------------------------------------
Step 1: Create remote sell sales table. Nordstrom to You to be
released in future
----------------------------------------------------------------
*/

WITH remote_sell_item_sales AS
(
SELECT
    a.business_day_date,
--  a.global_tran_id, a.line_item_seq_num,          --FOR VALIDATION ONLY
    a.remote_sell_swimlane,
    a.board_type,
    --only applies for nordstrom on
    a.store_country_code,
    d.division_number,
    d.division_description,
    d.subdivision_number,
    d.subdivision_description,
    d.brand_name,                   --12.14.22 SWITCH FROM c.class_number
--  c.class_description,            --12.14.22 REMOVE
    d.department_number,            --12.14.22 SWITCH FROM c.subclass_number
    d.department_description,       --12.14.22 SWITCH FROM c.subclass_description
    a.return_flag,
    a.sales,
    a.sales_usd,
    a.units,
    a.order_pickup_units,
    a.ship_to_store_units

FROM
    (
        SELECT  a.business_day_date,
--              a.global_tran_id, a.line_item_seq_num,          --FOR VALIDATION ONLY
                a.store_country_code,
                a.remote_sell_swimlane,
                a.board_type,
                0 as return_flag, 
                a.upc_num,
                COUNT(DISTINCT a.global_tran_id) AS orders, -- should change to transactions
                SUM(a.shipped_sales) AS sales,
                SUM(a.shipped_usd_sales) AS sales_usd,
                SUM(a.shipped_qty) AS units,
                SUM(a.order_pickup_ind) AS order_pickup_units,
                SUM(a.ship_to_store_ind) AS ship_to_store_units
        
        FROM {remote_selling_t2_schema}.remote_sell_transactions a
        --WHERE a.remote_sell_swimlane <> 'NORDSTROM TO YOU' -- discuss best way to handle N2Y in base table since fees will not likely flow
        GROUP BY 1,2,3,4,5,6        --,7,8      --FOR VALIDATION
        
        UNION 
        
        SELECT  a.return_date,
--              a.global_tran_id, a.line_item_seq_num,          --FOR VALIDATION ONLY
                a.store_country_code,
                a.remote_sell_swimlane,
                a.board_type,
                1 as return_flag,
                a.upc_num,
                COUNT(DISTINCT a.global_tran_id) AS return_orders, -- does the business even need this?
                SUM(a.return_amt)  AS return_amt,
                SUM(a.return_usd_amt) AS return_amt_usd,
                SUM(a.return_qty) AS return_units,
                SUM(a.order_pickup_ind) AS return_order_pickup_units,
                SUM(a.ship_to_store_ind) AS return_ship_to_store_units
        
        FROM {remote_selling_t2_schema}.remote_sell_transactions a 
        WHERE a.return_date IS NOT NULL -- remove untied return transactions
        -- and a.remote_sell_swimlane <> 'NORDSTROM TO YOU' -- discuss best way to handle N2Y in base table since fees will not likely flow
        GROUP BY 1,2,3,4,5,6        --,7,8      --FOR VALIDATION

    
    ) a     
--FULL OUTER JOIN 
LEFT JOIN           --12.16.22 UPDATE
    (
    -- Unique for every UPC + Country
    select
        upc_num,
        rms_to_upc.channel_country,
        item_information.division_number,
        division_description,
        subdivision_number,
        subdivision_description,
        brand_name,                     --12.14.22 SWITCH FROM class_number 
--      class_description,              --12.14.22 REMOVE
        department_number,              --12.14.22 SWITCH FROM subclass_number
        department_description          --12.14.22 SWITCH FROM subclass_description     
    from
        (
            SELECT
                lpad(trim(upc_num), 15, '0') as upc_num,
                channel_country,
                MAX(rms_sku_num) as rms_sku_num
            FROM prd_nap_usr_vws.product_upc_dim
            WHERE prmy_upc_ind = 'Y'
            GROUP BY
                lpad(trim(upc_num), 15, '0'),
                channel_country
        ) rms_to_upc
    INNER JOIN
        (
        select DISTINCT 
            rms_sku_num,            
            channel_country,
            div_num as division_number,
            div_desc as division_description,
            grp_num as subdivision_number,
            grp_desc as subdivision_description,
            brand_name,                             --12.14.22 SWITCH FROM class_num
--          class_desc as class_description,        --12.14.22 REMOVE
            dept_num as department_number,          --12.14.22 SWITCH FROM sbclass_num
            dept_desc as department_description     --12.14.22 SWITCH FROM sbclass_desc
        FROM
            prd_nap_usr_vws.product_sku_dim_vw
        ) item_information
    ON
        rms_to_upc.rms_sku_num = item_information.rms_sku_num
        AND rms_to_upc.channel_country = item_information.channel_country 
    ) c         
    ON a.upc_num = c.upc_num
    AND a.store_country_code = c.channel_country
    
FULL OUTER JOIN 
    (
    select DISTINCT 
            div_num as division_number,
            div_desc as division_description,
            grp_num as subdivision_number,
            grp_desc as subdivision_description,
            brand_name,                             --12.14.22 SWITCH FROM class_num
--          class_desc as class_description,        --12.14.22 REMOVE
            dept_num as department_number,          --12.14.22 SWITCH FROM sbclass_num
            dept_desc as department_description     --12.14.22 SWITCH FROM sbclass_desc
        FROM
            prd_nap_usr_vws.product_sku_dim_vw
    ) d 
    ON c.division_number = d.division_number
        and c.subdivision_number = d.subdivision_number
        and coalesce(c.brand_name,'na') = coalesce(d.brand_name,'na')
        and c.department_number = d.department_number
)



/*
-------------------------------------------------------------------
Step 3: Create remote sell item net sales view
-------------------------------------------------------------------
*/

SELECT
    b.fiscal_year,
    b.fiscal_quarter,
    b.fiscal_month,
    b.fiscal_week,
    b.ly_day_date,
    a.business_day_date,
    a.remote_sell_swimlane,
    a.board_type,
    a.store_country_code,
    a.division_number,
    a.division_description, 
    a.subdivision_number,
    a.subdivision_description,
    
    a.brand_name,                   --12.14.22 SWITCH FROM a.class_number
--  a.class_description,            --12.14.22 REMOVE
    a.department_number,            --12.14.22 SWITCH FROM a.subclass_number
    a.department_description,       --12.14.22 SWITCH FROM a.subclass_description
    
    SUM(case when return_flag = 0 then a.sales end) as gross_sales,
    SUM(case when return_flag = 1 then a.sales end) as return_amt,
    COALESCE(gross_sales,0)+ (-1*COALESCE(return_amt,0)) as net_sales,
    
    SUM(case when return_flag = 0 then a.sales_usd end) as gross_usd_sales,
    SUM(case when return_flag = 1 then a.sales_usd end) as return_amt_usd,
    COALESCE(gross_usd_sales,0)+ (-1*COALESCE(return_amt_usd,0)) as net_sales_usd,
    
    SUM(case when return_flag = 0  then a.units end) as gross_units,
    SUM(case when return_flag = 1  then a.units end) as return_units,
    COALESCE(gross_units,0)+(-1*COALESCE(return_units,0)) as  net_return_units,

    SUM(case when return_flag = 0  then a.order_pickup_units end) as order_pickup_units,
    SUM(case when return_flag = 1  then a.order_pickup_units end) as order_pickup_return_units,
    COALESCE(SUM(case when return_flag = 0  then a.order_pickup_units end),0) + (-1*COALESCE(order_pickup_return_units,0)) as net_order_pickup_units,   
    
    
    SUM(case when return_flag = 0  then a.ship_to_store_units end) as ship_to_store_units,
    SUM(case when return_flag = 1  then a.ship_to_store_units end) as ship_to_store_return_units,
    COALESCE(SUM(case when return_flag = 0  then a.ship_to_store_units end),0) + (-1*COALESCE(ship_to_store_return_units,0)) as net_ship_to_store_units

    
FROM remote_sell_item_sales a
    
LEFT JOIN
        (
    SELECT
        a.day_date,
        a.week_of_fyr AS fiscal_week,
        a.month_454_num AS fiscal_month,
        a.quarter_454_num AS fiscal_quarter,
        a.year_num AS fiscal_year,
        a.last_year_day_date_realigned AS ly_day_date
    FROM
        prd_nap_usr_vws.DAY_CAL a
        ) b
ON
    a.business_day_date = b.day_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;

SET QUERY_BAND = NONE FOR SESSION;
