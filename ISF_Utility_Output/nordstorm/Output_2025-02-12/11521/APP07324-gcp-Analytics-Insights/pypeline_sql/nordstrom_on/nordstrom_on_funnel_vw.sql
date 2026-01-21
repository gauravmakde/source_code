SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=nordstrom_on_digital_styling_funnel_vw_11521_ACE_ENG;
     Task_Name=nordstrom_on_funnel_vw;'
     FOR SESSION VOLATILE;

/*
Description:

Analytics owner: Ryan Graves, Agatha Mak
View creator: BIE #bie_support
*/

REPLACE VIEW {remote_selling_t2_schema}.NORDSTROM_ON_FUNNEL_VW
AS
LOCK ROW FOR ACCESS

/*
----------------------------------------------------------------
Step 1: Calculate demand to sales
----------------------------------------------------------------
*/

WITH styleboard_demand AS
(
SELECT  dig.board_order_date,
        dig.source_channel_country_code,
        CASE WHEN dig.attributed_item_ind = 1 and dig.board_type = 'STYLE_BOARD' THEN 'STYLEBOARD_ATTRIBUTED'
             WHEN dig.attributed_item_ind = 1 and dig.board_type = 'STYLE_LINK' THEN 'STYLELINK_ATTRIBUTED'
             WHEN dig.attributed_item_ind = 1 and dig.board_type = 'REQUEST_A_LOOK' THEN 'REQUEST_A_LOOK_ATTRIBUTED'
             WHEN dig.attributed_item_ind = 1 and dig.board_type = 'PERSONAL_REQUEST_A_LOOK' THEN 'PERSONAL_REQUEST_A_LOOK_ATTRIBUTED'
             WHEN dig.attributed_item_ind = 1 and dig.board_type = 'STYLEBOARD_PRIVATE' THEN 'STYLEBOARD_PRIVATE_ATTRIBUTED'
             WHEN dig.attributed_item_ind = 1 and dig.board_type = 'STYLEBOARD_PUBLIC' THEN 'STYLEBOARD_PUBLIC_ATTRIBUTED'
             WHEN dig.attributed_item_ind = 1 and dig.board_type = 'CHAT_BOARD' THEN 'CHAT_BOARD_ATTRIBUTED'
             WHEN dig.attributed_item_ind IS NULL and dig.order_number IS NOT NULL and dig.board_type = 'STYLE_BOARD' THEN 'STYLEBOARD_ATTACHED'
             WHEN dig.attributed_item_ind IS NULL and dig.order_number IS NOT NULL and dig.board_type = 'STYLE_LINK' THEN 'STYLELINK_ATTACHED'
             WHEN dig.attributed_item_ind IS NULL and dig.order_number IS NOT NULL and dig.board_type = 'REQUEST_A_LOOK' THEN 'REQUEST_A_LOOK_ATTACHED'
             WHEN dig.attributed_item_ind IS NULL and dig.order_number IS NOT NULL and dig.board_type = 'PERSONAL_REQUEST_A_LOOK' THEN 'PERSONAL_REQUEST_A_LOOK_ATTACHED'
             WHEN dig.attributed_item_ind IS NULL and dig.order_number IS NOT NULL and dig.board_type = 'STYLEBOARD_PRIVATE' THEN 'STYLEBOARD_PRIVATE_ATTACHED'
             WHEN dig.attributed_item_ind IS NULL and dig.order_number IS NOT NULL and dig.board_type = 'STYLEBOARD_PUBLIC' THEN 'STYLEBOARD_PUBLIC_ATTACHED'
             WHEN dig.attributed_item_ind IS NULL and dig.order_number IS NOT NULL and dig.board_type = 'CHAT_BOARD' THEN 'CHAT_BOARD_ATTACHED'
             WHEN dig.order_number IS NULL and dig.board_type = 'STYLE_BOARD' THEN 'STYLEBOARD_WO_ORDER'	
             WHEN dig.order_number IS NULL and dig.board_type = 'STYLE_LINK' THEN 'STYLELINK_WO_ORDER'
             WHEN dig.order_number IS NULL and dig.board_type = 'REQUEST_A_LOOK' THEN 'REQUEST_A_LOOK_WO_ORDER'
             WHEN dig.order_number IS NULL and dig.board_type = 'PERSONAL_REQUEST_A_LOOK' THEN 'PERSONAL_REQUEST_A_LOOK_WO_ORDER'
             WHEN dig.order_number IS NULL and dig.board_type = 'STYLEBOARD_PRIVATE' THEN 'STYLEBOARD_PRIVATE_WO_ORDER'
             WHEN dig.order_number IS NULL and dig.board_type = 'STYLEBOARD_PUBLIC' THEN 'STYLEBOARD_PUBLIC_WO_ORDER'
             WHEN dig.order_number IS NULL and dig.board_type = 'CHAT_BOARD' THEN 'CHAT_BOARD_WO_ORDER'
             ELSE 'UNKNOWN'
        END AS remote_sell_swimlane,
        dig.board_curator_id,
        dig.employee_name,
        COUNT(DISTINCT dig.board_id_sent) AS boards_sent, --conversion boards sent/orders
        COUNT(DISTINCT dig.order_number) AS orders,
        SUM(dig.demand) AS demand,
        SUM(dig.units) AS units,
        SUM(dig.order_pickup_ind) AS order_pickup_units_demanded,
        SUM(dig.ship_to_store_ind) AS ship_to_store_units_demanded

FROM {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY dig

WHERE dig.board_order_date >= '2021-09-21' -- date when event merger was corrected. no data available prior
AND dig.board_order_date IS NOT NULL

GROUP BY 1,2,3,4,5
),

styleboard_sales AS
(
SELECT  sls.business_day_date,
        sls.store_country_code,
        sls.remote_sell_swimlane,
        sls.remote_sell_employee_id,
        sls.employee_name, -- sales first due to moving returns to 'Return Store'
        SUM(sls.shipped_usd_sales) AS gross_sales_usd,
        SUM(sls.shipped_qty) AS gross_units,
        SUM(sls.order_pickup_ind) AS order_pickup_units_fulfilled,
        SUM(sls.ship_to_store_ind) AS ship_to_store_units_fulfilled,
        COUNT(DISTINCT sls.global_tran_id) as transactions,
        COUNT(DISTINCT sls.acp_id) as sales_customers -- this remains true only at day-level, not week etc

FROM {remote_selling_t2_schema}.remote_sell_transactions sls

WHERE sls.shipped_usd_sales > 0
AND sls.remote_sell_swimlane in ('PRIVATE_STYLING','PRIVATE_STYLING_ATTRIBUTED','PRIVATE_STYLING_ATTACHED',
                                 'STYLELINK_ATTRIBUTED','STYLELINK_ATTACHED','STYLEBOARD_ATTRIBUTED','STYLEBOARD_ATTACHED',
                                 'REQUEST_A_LOOK_ATTRIBUTED','REQUEST_A_LOOK_ATTACHED','PERSONAL_REQUEST_A_LOOK_ATTRIBUTED','PERSONAL_REQUEST_A_LOOK_ATTACHED',
                                 'STYLEBOARD_PRIVATE_ATTRIBUTED','STYLEBOARD_PRIVATE_ATTACHED','STYLEBOARD_PUBLIC_ATTRIBUTED','STYLEBOARD_PUBLIC_ATTACHED','CHAT_BOARD_ATTRIBUTED','CHAT_BOARD_ATTACHED',
                                 'ECF_ATTRIBUTED','ECF_ATTACHED')

GROUP BY 1,2,3,4,5
),

styleboard_returns AS
(
SELECT  rtn.return_date,
        rtn.store_country_code,
        rtn.remote_sell_swimlane,
        rtn.remote_sell_employee_id,
        rtn.employee_name,
        SUM(rtn.return_usd_amt) as return_amt_usd,
        SUM(rtn.return_qty) as return_qty,
        SUM(rtn.order_pickup_ind) AS order_pickup_units_returned,
        SUM(rtn.ship_to_store_ind) AS ship_to_store_units_returned,
        COUNT(DISTINCT rtn.global_tran_id) as return_transactions,
        COUNT(DISTINCT rtn.acp_id) as return_customers

FROM {remote_selling_t2_schema}.remote_sell_transactions rtn		

WHERE rtn.return_usd_amt > 0
AND rtn.remote_sell_swimlane in ('PRIVATE_STYLING','PRIVATE_STYLING_ATTRIBUTED','PRIVATE_STYLING_ATTACHED',
                                 'STYLELINK_ATTRIBUTED','STYLELINK_ATTACHED','STYLEBOARD_ATTRIBUTED','STYLEBOARD_ATTACHED',
                                 'REQUEST_A_LOOK_ATTRIBUTED','REQUEST_A_LOOK_ATTACHED','PERSONAL_REQUEST_A_LOOK_ATTRIBUTED','PERSONAL_REQUEST_A_LOOK_ATTACHED',
                                 'STYLEBOARD_PRIVATE_ATTRIBUTED','STYLEBOARD_PRIVATE_ATTACHED','STYLEBOARD_PUBLIC_ATTRIBUTED','STYLEBOARD_PUBLIC_ATTACHED','CHAT_BOARD_ATTRIBUTED','CHAT_BOARD_ATTACHED',
                                 'ECF_ATTRIBUTED','ECF_ATTACHED')

GROUP BY 1,2,3,4,5
),

styleboard_sales_returns AS
(
SELECT  COALESCE(s.business_day_date, r.return_date) as activity_date,
        COALESCE(s.store_country_code, r.store_country_code) as store_country_code,
        COALESCE(s.remote_sell_swimlane, r.remote_sell_swimlane) as remote_sell_swimlane,
        COALESCE(s.remote_sell_employee_id, r.remote_sell_employee_id) as remote_sell_employee_id,
        COALESCE(s.employee_name, r.employee_name) as employee_name,
        s.gross_sales_usd,
        s.gross_units,
        s.order_pickup_units_fulfilled,
        s.ship_to_store_units_fulfilled,
        s.transactions,
        s.sales_customers,
        r.return_amt_usd,
        r.return_qty,
        r.order_pickup_units_returned,
        r.ship_to_store_units_returned,
        r.return_transactions,
        r.return_customers

FROM styleboard_sales s

FULL OUTER JOIN styleboard_returns r
ON s.business_day_date = r.return_date
AND s.store_country_code = r.store_country_code
AND s.remote_sell_swimlane = r.remote_sell_swimlane
AND s.remote_sell_employee_id = r.remote_sell_employee_id
AND s.employee_name = r.employee_name
)


SELECT  cal.fiscal_year,
        cal.fiscal_quarter,
        cal.fiscal_month,
        cal.fiscal_week,
        cal.ly_day_date,
        a.activity_date,
        a.store_country_code,
        a.remote_sell_swimlane,
        a.remote_sell_employee_id,
        a.employee_name,
        COALESCE(hr.current_payroll_store, 'left company or inactive') as current_payroll_store,
        COALESCE(hr.current_payroll_store_description, 'left company or inactive') as current_payroll_store_description,
        COALESCE(hr.current_payroll_department_description, 'left company or inactive') as current_payroll_department_description,
        COALESCE(estr.region_num, 'left company or inactive') as current_employee_payroll_store_region_num,
        COALESCE(estr.region_desc, 'left company or inactive') as current_employee_payroll_region_desc,
        a.boards_sent,
        a.orders,
        a.demand,
        a.units,
        a.order_pickup_units_demanded,
        a.ship_to_store_units_demanded,
        a.gross_sales_usd,
        a.gross_units,
        a.order_pickup_units_fulfilled,
        a.ship_to_store_units_fulfilled,
        a.transactions,
        a.sales_customers,
        a.return_amt_usd,
        a.return_qty,
        NVL(a.gross_sales_usd,0) - NVL(a.return_amt_usd,0) AS net_sales_usd,
        NVL(a.gross_units,0) - NVL(a.return_qty,0) AS net_units,
        a.order_pickup_units_returned,
        a.ship_to_store_units_returned,
        a.return_transactions,
        a.return_customers

FROM
        (
        SELECT  COALESCE(dmd.board_order_date, snr.activity_date) as activity_date,
                COALESCE(dmd.source_channel_country_code, snr.store_country_code) as store_country_code,
                COALESCE(dmd.remote_sell_swimlane, snr.remote_sell_swimlane) as remote_sell_swimlane,
                COALESCE(snr.remote_sell_employee_id, dmd.board_curator_id) as remote_sell_employee_id, --reverse for employees who've left. returns go to 'return store'
                COALESCE(dmd.employee_name, snr.employee_name) as employee_name,
                dmd.boards_sent,
                dmd.orders,
                dmd.demand,
                dmd.units,
                dmd.order_pickup_units_demanded,
                dmd.ship_to_store_units_demanded,
                snr.gross_sales_usd,
                snr.gross_units,
                snr.order_pickup_units_fulfilled,
                snr.ship_to_store_units_fulfilled,
                snr.transactions,
                snr.sales_customers,
                snr.return_amt_usd,
                snr.return_qty,
                snr.order_pickup_units_returned,
                snr.ship_to_store_units_returned,
                snr.return_transactions,
                snr.return_customers

        FROM styleboard_demand dmd

        LEFT JOIN styleboard_sales_returns snr
        ON dmd.board_order_date = snr.activity_date
        AND dmd.source_channel_country_code = snr.store_country_code
        AND dmd.remote_sell_swimlane = snr.remote_sell_swimlane
        AND dmd.board_curator_id = snr.remote_sell_employee_id
        AND dmd.employee_name = snr.employee_name
        ) a

LEFT JOIN (
        SELECT cl.day_date,
               cl.week_of_fyr AS fiscal_week,
               cl.month_454_num AS fiscal_month,
               cl.quarter_454_num AS fiscal_quarter,
               cl.year_num AS fiscal_year,
               cl.last_year_day_date_realigned AS ly_day_date

        FROM prd_nap_usr_vws.DAY_CAL cl
        ) cal
ON a.activity_date = cal.day_date

-- find most current payroll store number so employee sales and returns stays in single row
LEFT JOIN 
		(
        -- AM EDIT ON HR CODE
        SELECT  lpad(a.worker_number, 15,'0') AS worker_number,
        		nme.worker_status,
                a.payroll_store as current_payroll_store,
                stor.store_name AS current_payroll_store_description,
                dpt.organization_description AS current_payroll_department_description,
                ROW_NUMBER() OVER (partition by a.worker_number ORDER BY a.eff_end_date DESC) as row_num

        FROM prd_nap_hr_usr_vws.HR_ORG_DETAILS_DIM_EFF_DATE_VW a
        LEFT JOIN (select a.worker_number, a.last_name, a.first_name, a.worker_status
					from prd_nap_hr_usr_vws.hr_worker_v1_dim a
					join(select worker_number, max(last_updated) as max_date 
						from prd_nap_hr_usr_vws.hr_worker_v1_dim 		
						group by 1)b 
								on a.worker_number = b.worker_number 
								and a.last_updated = b.max_date
					) nme ON a.worker_number = nme.worker_number
				                        			
        LEFT JOIN (SELECT store_num, store_name, store_type_code 
					FROM prd_nap_usr_vws.STORE_DIM	
                    WHERE store_type_code IN ('FL','RK','NL')) stor 
							ON cast(trim(LEADING '0' FROM a.payroll_store) AS varchar(10)) = cast(stor.store_num AS varchar(10))
                        		
        LEFT JOIN (select organization_code, organization_description 
					from prd_nap_hr_usr_vws.HR_WORKER_ORG_DIM	
					where organization_type = 'DEPARTMENT'
					and is_inactive = 0)dpt  
							on a.payroll_department = dpt.organization_code

        qualify row_num = 1
        ) hr
				ON a.remote_sell_employee_id = hr.worker_number
					and upper(hr.worker_status) = 'ACTIVE'
		

LEFT JOIN prd_nap_usr_vws.store_dim estr
ON cast(trim(LEADING '0' FROM hr.current_payroll_store) as varchar(10)) = cast(estr.store_num as varchar(10))
;

GRANT SELECT ON {remote_selling_t2_schema}.NORDSTROM_ON_FUNNEL_VW TO PUBLIC;

SET QUERY_BAND = NONE FOR SESSION;
