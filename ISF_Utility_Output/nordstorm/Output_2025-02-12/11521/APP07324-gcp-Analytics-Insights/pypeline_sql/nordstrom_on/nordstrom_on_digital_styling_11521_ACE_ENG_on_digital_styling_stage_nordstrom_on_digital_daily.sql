SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=nordstrom_on_digital_styling_11521_ACE_ENG;
     Task_Name=nordstrom_on_digital_daily;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: May 6,2024
*/

/*
---------------------------------------------------
Step 1: Pull Styleboards and Stylelinks events
---------------------------------------------------
*/
--drop table styleboards_ALL;
CREATE  MULTISET VOLATILE TABLE styleboards_ALL AS
(
SELECT MIN(a.activity_date) as activity_date, --updates to most recent activity, so from board created to board sent
       a.board_id,
       MIN(CASE WHEN a.board_type IS NULL THEN 'UNKNOWN' ELSE a.board_type END) AS board_type,
       MIN(COALESCE(a.board_created_pst, a.board_sent_pst)) AS board_created_pst,
       MIN(a.board_sent_pst) as board_sent_pst, --use for conversion
       lpad(a.board_curator_id, 15,'0') AS board_curator_id
FROM prd_nap_usr_vws.curation_fact a
WHERE a.activity_date >= '2021-09-21' -- leave hard-coded. when event merger data was fixed
AND COALESCE(a.board_created_pst, a.board_sent_pst) IS NOT NULL
AND a.board_curator_id IS NOT NULL -- ask why there are null employee id's
GROUP BY 2,6
)
WITH DATA PRIMARY INDEX (board_created_pst) ON COMMIT PRESERVE ROWS;
--NEW ADD! 12.13.22
--drop table styleboards;
CREATE  MULTISET VOLATILE TABLE styleboards AS (
	SELECT activity_date,
		board_id,
		board_type,
		board_created_pst,
		board_sent_pst,
		board_curator_id
	FROM styleboards_ALL
	WHERE CAST(BOARD_CREATED_PST AS DATE ) BETWEEN current_date()-100 AND current_date()-1
) WITH DATA PRIMARY INDEX (board_created_pst) ON COMMIT PRESERVE ROWS;

/*
---------------------------------------------------
Step 2: Find Styleboards and Stylelinks viewed
---------------------------------------------------
*/
-- drop table styleboards_viewed;
/*
CREATE  MULTISET VOLATILE TABLE styleboards_viewed AS
(
select distinct b.activity_date,
        b.customer_id,
        b.board_id,
        b.min_tmstp_pst_any_product_viewed
from
(
select  a.activity_date,
        a.customer_id,
        a.board_id,
        min(a.activity_tmstp_pst) over (partition by a.board_id order by a.board_id) as min_tmstp_pst_any_product_viewed,
        max(a.product_viewed) over (partition by a.board_id order by a.board_id) as max_product_viewed -- finding any product in a board where true. T is max in this case

from PRD_NAP_USR_VWS.STYLEBOARD_PRODUCT_INTERACTION_FACT a
where board_id > '0'
) b

where lower(max_product_viewed) = 'true'
)
WITH DATA PRIMARY INDEX (min_tmstp_pst_any_product_viewed) ON COMMIT PRESERVE ROWS;
*/
/*
---------------------------------------------------
Step 3: Pull Styleboards and Stylelinks orders,
order pickup, and ship to store details
---------------------------------------------------
*/
-- drop table styleboard_orders;
CREATE  MULTISET VOLATILE TABLE styleboard_orders AS
(
with solf as (sel distinct order_line_id, order_number, min(board_id) as board_id from prd_nap_usr_vws.curation_order_line_fact group by 1,2)		--- AM FIX 9/13 MIN AND DISTINCT ADDED
SELECT  a.order_date_pst,
        a.order_tmstp_pst,
        a.order_num,
        a.order_line_id,
        a.rms_sku_num,
        a.source_channel_country_code,
        a.demand,
        a.units,
        a.padded_upc,
        a.upc_seq_num,
        a.order_pickup_ind, -- expedited next day pickup from FC is being counted as ship to store for remote sell
        a.ship_to_store_ind,
        a.attributed_item_ind,
        a.board_id,			--ADD IN FOR THE NUMERATOR 7/18/23
        a.adj_board_id,			--KEEP FOR ATTACHED SALES 7/18/23
        a.platform_code as platform
FROM
    (
    SELECT  f.order_date_pacific as order_date_pst,
            f.order_tmstp_pacific as order_tmstp_pst,
            a.board_id,
            f.order_num,
            f.order_line_id,
            f.rms_sku_num,
            f.source_channel_country_code,
            f.order_line_amount_usd AS demand,
            f.order_line_quantity AS units,
            lpad(trim(f.upc_code),15,'0') padded_upc,
            row_number() OVER (PARTITION BY f.order_num, f.upc_code ORDER BY f.upc_code ) AS upc_seq_num,
            CASE WHEN f.requested_level_of_service_code = '07' THEN 1
                    WHEN upper(f.delivery_method_code) = 'PICK' then 1
                    ELSE NULL
            END AS order_pickup_ind, -- expedited next day pickup from FC is being counted as ship to store for remote sell
            CASE WHEN f.requested_level_of_service_code = '11' THEN 1 --Part of "Next-Day Pickup"
                    WHEN f.destination_node_num > 0 AND upper(F.delivery_method_code) <> 'PICK' AND f.requested_level_of_service_code = '42' THEN 1 --Part of "Next-Day Pickup"
                    WHEN f.destination_node_num BETWEEN 0 AND 799 AND upper(f.delivery_method_code) <> 'PICK' THEN 1
                    ELSE NULL
            END AS ship_to_store_ind,
            CASE WHEN a.board_id IS NOT NULL THEN 1 ELSE NULL END AS attributed_item_ind,
            MAX(a.board_id) over (partition by f.order_num ORDER BY f.order_num) as adj_board_id, -- to attach a board_id to attached items
            MAX(f.source_platform_code) over (partition by f.order_num) as platform_code
    FROM  prd_nap_usr_vws.order_line_detail_fact f
    LEFT JOIN solf a -- 1:53 putting dates in join
    ON a.order_number = f.order_num
    AND a.order_line_id = f.order_line_id
    WHERE f.order_date_pacific >= '2021-09-21' -- leave hard-coded
    AND f.order_date_pacific >= current_date()-100
    ) a
WHERE a.adj_board_id IS NOT NULL
)
WITH DATA PRIMARY INDEX (order_date_pst) ON COMMIT PRESERVE ROWS;
/*
---------------------------------------------------
Step 4: Insert statement
---------------------------------------------------
*/
DELETE FROM T2DL_DAS_REMOTE_SELLING.NORDSTROM_ON_DIGITAL_DAILY
WHERE board_created_date >= current_date()-100
AND board_created_date <= current_date()-1;
INSERT INTO T2DL_DAS_REMOTE_SELLING.NORDSTROM_ON_DIGITAL_DAILY
SELECT  coalesce(coalesce(b.board_id, b.adj_board_id),a.board_id) as board_id,	--ADD IN FOR CONVERSION ATTRIBUTED NUMERATOR 7/18/23,  RETAIN FOR ATTACHED, RETAIN FOR CONVERSION DENOMINATOR 7/18/23
        a.board_curator_id,
        hr.employee_name,
        hr.payroll_store,
        hr.payroll_store_description,
        hr.payroll_department_description,
        hr.region_num AS employee_payroll_store_region_num,
        hr.region_desc AS employee_payroll_region_desc,
        CAST(a.board_created_pst AS DATE) AS board_created_date, --not converting in prior steps bc will need timestamps when bringing in product views
        CAST(a.board_sent_pst AS DATE) AS board_sent_date,
        b.order_date_pst as board_order_date,
        b.order_tmstp_pst,
        b.platform,
        a.board_type,
        CASE WHEN a.board_sent_pst IS NOT NULL THEN a.board_id ELSE NULL END AS board_id_sent,
        b.attributed_item_ind,
        b.order_num as order_number,
        b.order_line_id,
        b.rms_sku_num,
        b.padded_upc,
        b.upc_seq_num,
        b.source_channel_country_code,
        b.demand,
        b.units,
        b.order_pickup_ind,
        b.ship_to_store_ind,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM styleboards a
LEFT JOIN styleboard_orders b
on a.board_id = coalesce(b.board_id, b.adj_board_id)		--joining on attributed board_id. and if that's not available using rolled up adj_board_id  7/18/23 CHANGE
LEFT JOIN
            (
                SELECT  lpad(a.worker_number, 15,'0') AS worker_number,
                        CONCAT(nme.last_name, ', ', nme.first_name) AS employee_name,
                        trim(leading '0' from a.payroll_store) as payroll_store,
                        stor.store_name AS payroll_store_description,
                        dpt.organization_description AS payroll_department_description,
                        stor.region_num,   --- AM EDIT
                        stor.region_desc, --- AM EDIT
                        a.eff_begin_date AS eff_begin_tmstp,
                        a.eff_end_date - INTERVAL '0:01' MINUTE TO SECOND AS eff_end_tmstp
                        FROM prd_nap_hr_usr_vws.HR_ORG_DETAILS_DIM_EFF_DATE_VW a			--- AM HR TABLES UPDATE
                        LEFT JOIN (select a.worker_number, a.last_name, a.first_name
									from prd_nap_hr_usr_vws.hr_worker_v1_dim a
									join(select worker_number, max(last_updated) as max_date
										from prd_nap_hr_usr_vws.hr_worker_v1_dim
										group by 1)b
											on a.worker_number = b.worker_number
											and a.last_updated = b.max_date) nme ON a.worker_number = nme.worker_number
                        LEFT JOIN (SELECT store_num, store_name, store_type_code, region_num, region_desc FROM prd_nap_usr_vws.STORE_DIM			--CHANGED FROM INNER JOIN TO LEFT JOIN ( AM 9.28.22)
                                WHERE store_type_code IN ('FL','RK','NL')) stor ON cast(trim(LEADING '0' FROM a.payroll_store) AS varchar(10)) = cast(stor.store_num AS varchar(10))
                        LEFT JOIN (SELECT organization_code, organization_description from prd_nap_hr_usr_vws.HR_WORKER_ORG_DIM
								where organization_type = 'DEPARTMENT'
								and is_inactive = 0)dpt  on a.payroll_department = dpt.organization_code			--CHANGED FROM INNER JOIN TO LEFT JOIN ( AM 9.28.22)
            ) hr
ON a.board_curator_id = hr.worker_number
AND a.board_created_pst >= hr.eff_begin_tmstp
AND a.board_created_pst < hr.eff_end_tmstp
WHERE board_created_date >= current_date()-100
AND board_created_date <= current_date()-1
;

COLLECT STATISTICS  COLUMN (board_id),
                    COLUMN (PARTITION),
                    COLUMN (board_created_date)
ON T2DL_DAS_REMOTE_SELLING.NORDSTROM_ON_DIGITAL_DAILY;

SET QUERY_BAND = NONE FOR SESSION;
