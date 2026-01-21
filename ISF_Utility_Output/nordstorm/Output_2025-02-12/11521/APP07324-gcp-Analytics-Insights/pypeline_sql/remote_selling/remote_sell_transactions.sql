SET QUERY_BAND = 'App_ID=APP08154;
     DAG_ID=remote_sell_transactions_11521_ACE_ENG;
     Task_Name=remote_sell_transactions;'
     FOR SESSION VOLATILE;


/*
Team/Owner: Engagement Analytics - Agatha Mak
Date Created/Last Modified: May 6, 2024
*/


/*
---------------------------------------------------
Step 1: Pull Remote Sell sales transactions.
---------------------------------------------------
*/


--drop table remote_sell_trans_base;
CREATE  MULTISET VOLATILE TABLE remote_sell_trans_base AS (
		
	        SELECT  a.business_day_date,
	                a.tran_time,
	                a.global_tran_id,
	                a.line_item_seq_num,
	                a.intent_store_num, --needed for country
	                td_sysfnlib.lpad(trim(a.upc_num), 15,'0') as upc_num,
	                a.commission_slsprsn_num,
	                a.ps_employee_id as ps_commission_slsprsn_num,		--08.03.23 add
	                a.trunk_club_ind,
	                a.private_styling_ind,
	                a.cof_ind,
	                a.n2y_ind,
	                a.seq_num,
	                MAX(a.ps_employee_id) OVER (PARTITION BY a.business_day_date, a.global_tran_id) AS ps_employee_id,
	                MAX(a.private_styling_ind) OVER (PARTITION BY a.business_day_date, a.global_tran_id) AS ps_swimlane_ind,
	                MAX(a.cof_ind) OVER (PARTITION BY a.business_day_date, a.global_tran_id) AS cof_swimlane_ind,
	                MAX(a.handkey_ind) OVER (PARTITION BY a.business_day_date, a.global_tran_id) AS handkey_swimlane_ind,
	                MAX(a.n2y_ind) OVER (PARTITION BY a.business_day_date, a.global_tran_id) AS n2y_swimlane_ind
	
	        FROM
	            (
	            SELECT  dtl.business_day_date,
	                    dtl.tran_time,
	                    dtl.global_tran_id,
	                    dtl.intent_store_num,
	                    CASE WHEN dtl.employee_discount_amt <> 0 AND dtl.employee_discount_amt IS NOT NULL THEN 1 ELSE 0 END AS employee_discount_ind,
	                    dtl.upc_num,
	                    dtl.sku_num,
	                    dtl.commission_slsprsn_num,
	                    dtl.line_item_seq_num,
	                    CASE WHEN dtl.item_source = 'SB_SALESEMPINIT' THEN dtl.commission_slsprsn_num ELSE NULL END AS ps_employee_id, -- attached sales has dummy employee id that needs to be written OVER w/actual emp id
	                    CASE WHEN dtl.data_source_code = 'TRUNK' THEN 1 ELSE 0 END AS trunk_club_ind,
	                    CASE WHEN dtl.item_source = 'SB_SALESEMPINIT' THEN 1 ELSE 0 END AS private_styling_ind,
	                    CASE WHEN lower(dtl.line_item_order_type) NOT IN ('storeinitdtcauto','storeinitdtcmanual') AND tnd.global_tran_id IS NOT NULL AND tnd.swimlane = 'COF' THEN 1 ELSE 0 END AS cof_ind, -- CARD_ON_FILE, remove DTCs
	                    CASE WHEN lower(dtl.line_item_order_type) NOT IN ('storeinitdtcauto','storeinitdtcmanual') AND tnd.global_tran_id IS NOT NULL AND tnd.swimlane = 'HANDKEY' THEN 1 ELSE 0 END as handkey_ind, -- handkey, remove DTCs
	                    CASE WHEN dtl.upc_num IN ('439027332977','439027332984') THEN 1 ELSE 0 END AS n2y_ind, -- NORDSTROM_TO_YOU UPCs
	                    ROW_NUMBER() OVER (PARTITION BY dtl.global_tran_id, dtl.upc_num ORDER BY dtl.upc_num) AS seq_num --needed to join to sales and returns table for multiple upcs in same tran
	
	            FROM prd_nap_vws.retail_tran_detail_fact_vw dtl		
	
	            --find COF and Hand Key transactions
	            LEFT JOIN  (
	                        SELECT  tndr.business_day_date,
		                        tndr.global_tran_id,
	                                MIN(CASE WHEN b.tender_type_code = 'CREDIT_CARD' AND b.tender_item_entry_method_code = '6'
	                                     AND b.business_day_date >= '2021-05-24' -- COF start date is '2021-05-24' for pilot
	                                     THEN 'COF'
	                                     WHEN b.tender_item_entry_method_code = '3'
	                                        AND NULLIF(TRIM(c.ship_to_address),'') IS NOT NULL
	                                     THEN 'HANDKEY'
	                                     ELSE NULL
	                                END) AS swimlane  --USING MIN INSTEAD OF MAX TO REMAIN CONSISTENT WITH CURRENT REPORTING
	
	                        FROM
	                                (
	                                SELECT  t.business_day_date,
	                                        t.global_tran_id,
	                                        max(t.tender_item_seq_num) as max_tender_cnt
	
	                                FROM prd_nap_usr_vws.retail_tran_tender_fact t
	
	                                WHERE t.tran_type_code = 'SALE'
	                                AND t.tran_latest_version_ind = 'Y'
	                                AND t.tender_item_seq_num is not null                   --DBA Recommended
	                                AND t.business_day_date >= {start_date}
                                        AND t.business_day_date <= {end_date}                   --DBA Recommended
	
	                                GROUP BY 1,2
	                                ) tndr
	
	                        INNER JOIN prd_nap_usr_vws.retail_tran_tender_fact b ON tndr.business_day_date = b.business_day_date
	                        														AND tndr.global_tran_id = b.global_tran_id
	
	                        LEFT JOIN --prd_nap_usr_vws.retail_tran_hdr_fact c ON tndr.global_tran_id = c.global_tran_id
                                        ( --DBA Recommended
                                        SELECT DISTINCT
                                                ship_to_address, 
                                                global_tran_id, 
                                                business_day_date
                                        FROM prd_nap_usr_vws.retail_tran_hdr_fact
                                        WHERE business_day_date >= {start_date}                 
                                                and business_day_date <= {end_date}
                                                and ship_to_address is not null                 --DBA Recommended
                                        ) c ON tndr.global_tran_id = c.global_tran_id
                                              AND tndr.business_day_date = c.business_day_date      -- shrikrishna 
	                    
							WHERE --tndr.max_tender_cnt = 1 AND            --fy 2023 requirement change to NOT limit max tender count to 1
	                        	                        --NULLIF(TRIM(c.ship_to_address),'') IS NOT NULL -- limit to only charge sends. COF and handkey must have a shipping address
	                                                                        --removing requirement from COF 1.26.23
								tndr.business_day_date >= {start_date}	
								and tndr.business_day_date <= {end_date}	
				GROUP BY 1,2			
	                        ) tnd
	            ON dtl.global_tran_id = tnd.global_tran_id
	            	AND dtl.business_day_date = tnd.business_day_date
                        AND tnd.swimlane is not null                                            --DBA Recommended
                        
	            WHERE --UPPER(dtl.tran_type_code) = 'SALE'                  
		            dtl.line_net_usd_amt > 0                                --Update Sales to include Exchanges
		            AND COALESCE(dtl.nonmerch_fee_code, '-9999') = '-9999'
		            AND COALESCE(dtl.upc_num, '-1') <> '-1'
		            AND COALESCE(dtl.merch_dept_num, '9999') <> '482'
		            AND dtl.business_day_date >= {start_date}
		            AND dtl.business_day_date <= {end_date}
			) a
				
) WITH DATA PRIMARY INDEX (global_tran_id) 
PARTITION BY RANGE_N (business_day_date BETWEEN DATE '2020-01-01' AND date '2025-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;



--drop table remote_sell_trans;
CREATE  MULTISET VOLATILE TABLE remote_sell_trans AS (
	select c.*
	from 
	(
		SELECT  b.business_day_date,
	        b.tran_time,
	        CASE WHEN b.trunk_club_ind = 1 THEN 'TRUNK_CLUB'
	                WHEN b.ps_swimlane_ind = 1 THEN 'PRIVATE_STYLING'
	                WHEN b.cof_swimlane_ind = 1 THEN 'CARD_ON_FILE'
	                when b.handkey_swimlane_ind = 1 THEN 'HANDKEY'
	                WHEN b.n2y_swimlane_ind = 1 THEN 'NORDSTROM_TO_YOU'
	                ELSE NULL
	        END AS remote_sell_swimlane,
	        b.global_tran_id,
	        b.intent_store_num,
	        b.upc_num,
	        b.line_item_seq_num,
	        b.seq_num,
	        /*lpad(CASE WHEN b.ps_employee_id IS NOT NULL THEN b.ps_employee_id
	                WHEN b.commission_slsprsn_num IS NOT NULL THEN b.commission_slsprsn_num
	                ELSE NULL -- using as placeholder for TRUNK_CLUB
	        END, 15,'0') AS remote_sell_employee_id,*/
	        lpad(CASE WHEN b.ps_commission_slsprsn_num IS NOT NULL THEN b.commission_slsprsn_num			--UPDATE 08.03.23 PRIORITIZING COMMISSION SLSPRSN OVER ROLLED UP
		        	WHEN b.ps_employee_id IS NOT NULL THEN b.ps_employee_id	
		        	WHEN b.commission_slsprsn_num IS NOT NULL THEN b.commission_slsprsn_num
	                ELSE NULL -- using as placeholder for TRUNK_CLUB
	        END, 15,'0') AS remote_sell_employee_id,
	        CASE WHEN b.private_styling_ind = 1 THEN 1 ELSE 0 END AS private_styling_attributed_item_ind, -- to separate attributed from attached
	        CASE WHEN b.n2y_swimlane_ind = 1 AND b.n2y_ind IS NULL THEN 1 ELSE 0 END AS nordstrom_to_you_item_ind -- identifies items and not the service code in the order
	
	    FROM remote_sell_trans_base b
	--WHERE b.trunk_club_ind = 1 OR b.ps_swimlane_ind = 1 OR b.cof_swimlane_ind = 1 OR b.n2y_swimlane_ind = 1 OR b.handkey_swimlane_ind = 1 -- reduce rows to remote-sell trans
	) c
	WHERE c.REMOTE_SELL_SWIMLANE IS NOT NULL    
	
) WITH DATA PRIMARY INDEX (global_tran_id) 
PARTITION BY RANGE_N (business_day_date BETWEEN DATE '2020-01-01' AND date '2025-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;




/*
---------------------------------------------------
Step 2: Pull retail sales
---------------------------------------------------
*/

--drop table remote_sales_and_returns_base;
CREATE  MULTISET VOLATILE TABLE remote_sales_and_returns_base AS
(
SELECT  rst.business_day_date,
        rst.tran_time,
        rst.remote_sell_swimlane,
        rst.global_tran_id,
        rst.line_item_seq_num,		-- AM 
        rst.intent_store_num,
        rst.upc_num,
        rst.remote_sell_employee_id,
        rst.private_styling_attributed_item_ind, -- to separate attributed from attached
        rst.nordstrom_to_you_item_ind,
        b.acp_id,
        b.order_num,
        b.order_date,
        b.shipped_sales,
        b.shipped_usd_sales,
        b.shipped_qty,
        b.return_date,
        b.return_amt,
        b.return_qty,
        b.return_usd_amt,
        b.days_to_return,
        b.return_global_tran_id,
        b.order_line_id,
        b.order_pickup_ind, -- expedited next day pickup from FC is being counted as ship to store for remote sell
        b.ship_to_store_ind

FROM remote_sell_trans rst

LEFT JOIN  (
            SELECT  sls.global_tran_id,
                    sls.business_day_date,
                    sls.acp_id,
                    sls.order_num,
                    sls.order_date,
                    lpad(trim(sls.upc_num), 15, '0') as upc_num,
                    sls.shipped_sales,
                    sls.shipped_usd_sales,
                    sls.shipped_qty,
                    sls.return_date,
                    sls.return_amt,
                    sls.return_qty,
                    sls.return_usd_amt,
                    sls.days_to_return,
                    sls.return_global_tran_id,
                    sls.line_item_seq_num,
                    sls.order_line_id,
                    CASE WHEN sls.requested_level_of_service_code = '07' THEN 1 -- accounts for when bopus next day was ship orders < '2019-06-20' 09:00:00 pst
             			 WHEN upper(sls.delivery_method_code) = 'PICK' AND sls.promise_type_code <> 'SAME_DAY_COURIER' THEN 1
             			ELSE NULL -- remove same day delivery...note to self, test this
        			END AS order_pickup_ind, -- expedited next day pickup from FC is being counted as ship to store for remote sell
        			CASE WHEN sls.requested_level_of_service_code = '11' THEN 1 --Part of "Next-Day Pickup"
             		     WHEN sls.destination_node_num > 0 AND UPPER(sls.delivery_method_code) <> 'PICK' THEN 1 ELSE NULL
        			END AS ship_to_store_ind

                    FROM T2DL_DAS_SALES_RETURNS.sales_and_returns_fact sls

                    WHERE sls.business_day_date >= {start_date}
                    AND sls.business_day_date <= {end_date}
                    AND sls.business_day_date IS NOT NULL -- nulls are when there are untied returns
            ) b
ON rst.business_day_date = b.business_day_date
AND rst.global_tran_id = b.global_tran_id
--AND rst.upc_num = b.upc_num
--AND rst.seq_num = b.seq_num
AND rst.line_item_seq_num = b.line_item_seq_num
)
WITH DATA NO PRIMARY INDEX --PRIMARY INDEX (global_tran_id)              --DBA Rec
--PARTITION BY RANGE_N (business_day_date BETWEEN DATE '2020-01-01' AND date '2025-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;




/*
---------------------------------------------------
Step 3: Find ringing time for returns
---------------------------------------------------
*/

--drop table remote_sales_and_returns;
CREATE  MULTISET VOLATILE TABLE remote_sales_and_returns AS
(
SELECT  a.business_day_date,
        a.tran_time,
        a.remote_sell_swimlane,
        a.global_tran_id,
        a.line_item_seq_num,		-- AM 
        a.intent_store_num,
        a.upc_num,
        a.remote_sell_employee_id,
        a.private_styling_attributed_item_ind, -- to separate attributed from attached
        a.nordstrom_to_you_item_ind,
        a.acp_id,
        a.order_num,
        a.shipped_sales,
        a.shipped_usd_sales,
        a.shipped_qty,
        a.return_date,
        a.return_amt,
        a.return_qty,
        a.return_usd_amt,
        a.days_to_return,
        a.return_global_tran_id, -- and business_day_date to header fact
        a.order_line_id,
        a.order_pickup_ind, -- expedited next day pickup from FC is being counted as ship to store for remote sell
        a.ship_to_store_ind,
        hdr.tran_time AS return_tran_time

FROM remote_sales_and_returns_base a

LEFT JOIN prd_nap_usr_vws.retail_tran_hdr_fact hdr
on a.return_global_tran_id = hdr.global_tran_id   
and a.return_date = hdr.business_day_date         

)
WITH DATA PRIMARY INDEX (global_tran_id) 
PARTITION BY RANGE_N (business_day_date BETWEEN DATE '2020-01-01' AND date '2025-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;

     COLLECT STATISTICS COLUMN (REMOTE_SELL_EMPLOYEE_ID) ON     remote_sales_and_returns;
     COLLECT STATISTICS COLUMN (PARTITION) ON     remote_sales_and_returns;
     COLLECT STATISTICS COLUMN (INTENT_STORE_NUM) ON     remote_sales_and_returns;
     COLLECT STATISTICS COLUMN (BUSINESS_DAY_DATE) ON    remote_sales_and_returns;
     COLLECT STATISTICS COLUMN (TRAN_TIME) ON     remote_sales_and_returns;
     COLLECT STATISTICS COLUMN (RETURN_TRAN_TIME) ON     remote_sales_and_returns;


/*
---------------------------------------------------
Step 4: Incorporate HR sale and return attribution
rules by employee and store
---------------------------------------------------
*/


--drop table hr_dat;
CREATE  MULTISET VOLATILE TABLE hr_dat AS (

		SELECT  DISTINCT 
				TD_SYSFNLIB.lpad(a.worker_number, 15,'0') AS worker_number,
                CONCAT(nme.last_name, ', ', nme.first_name) AS employee_name,
                nme.worker_status,
                nme.termination_date,
                trim(leading '0' from a.payroll_store) as payroll_store,
                stor.store_name AS payroll_store_description,
                dpt.organization_description AS payroll_department_description,
                a.eff_begin_date AS eff_begin_tmstp,
                a.eff_end_date - INTERVAL '0:01' MINUTE TO SECOND AS eff_end_tmstp

        FROM prd_nap_hr_usr_vws.HR_ORG_DETAILS_DIM_EFF_DATE_VW a			
        
        LEFT JOIN (select a.worker_number, a.last_name, a.first_name
        					, a.worker_status, a.termination_date
					from prd_nap_hr_usr_vws.hr_worker_v1_dim a
					join(select worker_number, 
						max(last_updated) as max_date 
						from prd_nap_hr_usr_vws.hr_worker_v1_dim 
						group by 1)b 
							on a.worker_number = b.worker_number and 
					  		 a.last_updated = b.max_date) nme ON a.worker_number = nme.worker_number
                        			
        LEFT JOIN (SELECT store_num, store_name, store_type_code FROM prd_nap_usr_vws.STORE_DIM			--CHANGED FROM INNER JOIN TO LEFT JOIN ( AM 9.28.22)
        		WHERE store_type_code IN ('FL','RK','NL')) stor ON cast(trim(LEADING '0' FROM a.payroll_store) AS varchar(10)) = cast(stor.store_num AS varchar(10))
        		
        LEFT JOIN (select organization_code, organization_description from prd_nap_hr_usr_vws.HR_WORKER_ORG_DIM									--CHANGED FROM INNER JOIN TO LEFT JOIN ( AM 9.28.22)
				where organization_type = 'DEPARTMENT'
                                and cast(is_inactive as int)= 0)dpt  on a.payroll_department = dpt.organization_code            --DBA RECOMMENDED
				--and is_inactive = 0)dpt  on a.payroll_department = dpt.organization_code

) WITH DATA NO PRIMARY INDEX --PRIMARY INDEX (worker_number)   --DBA Rec
ON COMMIT PRESERVE ROWS;


     COLLECT STATISTICS COLUMN (PAYROLL_STORE) ON HR_DAT;
     COLLECT STATISTICS COLUMN (WORKER_NUMBER) ON HR_DAT;
     COLLECT STATISTICS COLUMN (EFF_BEGIN_TMSTP) ON HR_DAT;
     COLLECT STATISTICS COLUMN (EFF_END_TMSTP) ON HR_DAT;


--drop table hr_appended;
CREATE  MULTISET VOLATILE TABLE hr_appended AS (
		
        SELECT  b.business_day_date,
                b.remote_sell_swimlane,
                --max(c.board_type) over (partition by b.order_num order by order_num) as board_type, --need styleboard/link populated for whole order to derive attached
                b.global_tran_id,
                b.line_item_seq_num,		-- AM 
                stor.store_country_code, --based on the intent store num
                b.upc_num,
                b.private_styling_attributed_item_ind, -- to separate attributed from attached
                b.nordstrom_to_you_item_ind,
                b.acp_id,
                b.order_num,
                b.order_line_id,
                b.shipped_sales,
                b.shipped_usd_sales,
                b.shipped_qty,
                b.return_date,
                b.return_amt,
                b.return_qty,
                b.return_usd_amt,
                b.days_to_return,
                CASE WHEN UPPER(b.worker_status) = 'ACTIVE' AND b.days_to_return >= 365 THEN 'Return Store' --should Onleave be included?
                WHEN b.num_days_since_termination > 60 THEN 'Return Store' -- do i want to put the actual return store here?
                ELSE b.remote_sell_employee_id
                END AS remote_sell_employee_id,
                b.employee_name,
                b.sale_payroll_store, -- sales and returns always fall under here, but the employeeid drops after x days
                b.sale_payroll_store_description,
                b.sale_payroll_department_description,
                estr.region_num as employee_payroll_store_region_num,
                estr.region_desc as employee_payroll_region_desc,
                b.num_days_since_termination, -- will need this to process HR rules
                b.order_pickup_ind,
                b.ship_to_store_ind
		
        FROM
        (
	        SELECT a.business_day_date,
	                a.tran_time,
	                a.remote_sell_swimlane,
	                a.global_tran_id,
	                a.intent_store_num,
	                a.upc_num,
	                a.remote_sell_employee_id,
	                s.employee_name,
	                a.private_styling_attributed_item_ind, -- to separate attributed from attached
	                a.nordstrom_to_you_item_ind,
	                a.acp_id,
	                --a.order_num,
                        CASE WHEN a.order_num IS NULL THEN RANDOM(-1000000,-20) ELSE a.order_num END as order_num,      --DBA Recommended but will require logic changes so not necessary at the tiem
	                a.order_line_id,
	                a.shipped_sales,
	                a.shipped_usd_sales,
	                a.shipped_qty,
	                a.return_date,
	                a.return_amt,
	                a.return_qty,
	                a.return_usd_amt,
	                a.days_to_return,
	                a.order_pickup_ind,
	                a.ship_to_store_ind,
	                s.payroll_store AS sale_payroll_store, --temp because termination length changes store attribution. outer query case will handle
	                s.payroll_store_description AS sale_payroll_store_description,
	                s.payroll_department_description AS sale_payroll_department_description,
	                r.worker_status,
	                CASE WHEN UPPER(r.worker_status) = 'TERMINATED' THEN a.return_date - r.termination_date ELSE NULL END AS num_days_since_termination,
	                a.line_item_seq_num		-- AM 		-- AM RETURN ATTRIBUTION FIX --r.num_days_since_termination
	
	        FROM remote_sales_and_returns a																--sel store_num from prd_nap_usr_vws.store_dim sample 10		--int
	
	        LEFT JOIN  HR_DAT s ON a.remote_sell_employee_id = s.worker_number							--sel cast(payroll_store as int) from hr_dat sample 10		--varchar
	        					AND a.tran_time BETWEEN s.eff_begin_tmstp AND s.eff_end_tmstp
	
	        -- find the return employee attribution and return payroll store number
	        LEFT JOIN  HR_DAT r ON a.remote_sell_employee_id = r.worker_number
	        					AND a.return_tran_time between r.eff_begin_tmstp and r.eff_end_tmstp

                WHERE a.business_day_date >= {start_date}                               --DBA REC
                        and a.business_day_date <= {end_date}
        
        ) b

        --find the country for filtering
        LEFT JOIN prd_nap_usr_vws.store_dim stor ON b.intent_store_num = stor.store_num

        --find the employee payroll store region
        LEFT JOIN prd_nap_usr_vws.store_dim estr ON b.sale_payroll_store = cast(estr.store_num as varchar(100))         --DBA REC
		
        WHERE b.business_day_date >= {start_date}
        AND b.business_day_date <= {end_date}
		
) WITH DATA NO PRIMARY INDEX --PRIMARY INDEX (global_tran_id)                            --DBA Rec
--PARTITION BY RANGE_N (business_day_date BETWEEN DATE '2020-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY)
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (ORDER_NUM) ON HR_APPENDED;
COLLECT STATISTICS COLUMN (order_line_id) ON HR_APPENDED;



DELETE FROM {remote_selling_t2_schema}.remote_sell_transactions
WHERE business_day_date >= {start_date}
AND business_day_date <= {end_date};

INSERT INTO {remote_selling_t2_schema}.remote_sell_transactions


select distinct   							--- AM 
		d.business_day_date,
		CASE WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLE_BOARD' THEN 'STYLEBOARD_ATTRIBUTED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLE_BOARD' THEN 'STYLEBOARD_ATTACHED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLE_LINK' THEN 'STYLELINK_ATTRIBUTED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLE_LINK' THEN 'STYLELINK_ATTACHED'
              WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'REQUEST_A_LOOK' THEN 'REQUEST_A_LOOK_ATTRIBUTED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'REQUEST_A_LOOK' THEN 'REQUEST_A_LOOK_ATTACHED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'PERSONAL_REQUEST_A_LOOK' THEN 'PERSONAL_REQUEST_A_LOOK_ATTRIBUTED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'PERSONAL_REQUEST_A_LOOK' THEN 'PERSONAL_REQUEST_A_LOOK_ATTACHED'
              WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLEBOARD_PRIVATE' THEN 'STYLEBOARD_PRIVATE_ATTRIBUTED'	
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLEBOARD_PRIVATE' THEN 'STYLEBOARD_PRIVATE_ATTACHED'
              WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLEBOARD_PUBLIC' THEN 'STYLEBOARD_PUBLIC_ATTRIBUTED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'STYLEBOARD_PUBLIC' THEN 'STYLEBOARD_PUBLIC_ATTACHED'
              WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'CHAT_BOARD' THEN 'CHAT_BOARD_ATTRIBUTED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'CHAT_BOARD' THEN 'CHAT_BOARD_ATTACHED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1
             			AND coalesce(d.ps_board_type, d.board_type) = 'ECF' THEN 'ECF_ATTRIBUTED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0
             			AND coalesce(d.ps_board_type, d.board_type) = 'ECF' THEN 'ECF_ATTACHED'
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 1 THEN 'PRIVATE_STYLING_ATTRIBUTED' 
             WHEN d.remote_sell_swimlane = 'PRIVATE_STYLING' AND d.private_styling_attributed_item_ind = 0 THEN 'PRIVATE_STYLING_ATTACHED'
             ELSE d.remote_sell_swimlane END AS remote_sell_swimlane,
        coalesce(d.ps_board_type, d.board_type) as board_type,													--BUG FIX NEEDED: replace coalesce(d.ps_board_type, d.board_type) with coalesce(coalesce(d.ps_board_type, d.board_type)_attrb, coalesce(d.ps_board_type, d.board_type)_attch) as board_type,     
        d.global_tran_id,
        d.store_country_code, --based on the intent store num
        d.upc_num,
        d.private_styling_attributed_item_ind, -- to separate attributed from attached
        d.nordstrom_to_you_item_ind,
        d.acp_id,
        d.order_num,
        d.platform,
        d.shipped_sales,
        d.shipped_usd_sales,
        d.shipped_qty,
        d.return_date,
        d.return_amt,
        d.return_qty,
        d.return_usd_amt,
        d.days_to_return,
        d.remote_sell_employee_id,
        d.employee_name,
        d.sale_payroll_store, -- sales and returns always fall under here, but the employeeid drops after x days
        d.sale_payroll_store_description,
        d.sale_payroll_department_description,
        d.employee_payroll_store_region_num,
        d.employee_payroll_region_desc,
        d.num_days_since_termination, -- will need this to process HR rules
        d.order_pickup_ind,
        d.ship_to_store_ind,       
		d.line_item_seq_num,		-- AM 
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
from 
(
        SELECT  b.business_day_date,
                b.remote_sell_swimlane,
                c.board_type as ps_board_type,		--NEW ADD 08.03.23
                max(c.board_type) over (partition by b.order_num order by order_num) as board_type, --need styleboard/link populated for whole order to derive attached
                													--as board_type_attch, c.board_type as board_type_attrb	<-- BUG FIX NEEDED: Needs to Include Attributed Board Type at order_line_id level
                                                                                                                  min(c.platform_code) over (partition by b.order_num) as platform,		-- to filter out erroneous unknowns
                b.global_tran_id,
                b.line_item_seq_num,		-- AM 
                b.store_country_code, --based on the intent store num
                b.upc_num,
                b.private_styling_attributed_item_ind, -- to separate attributed from attached
                b.nordstrom_to_you_item_ind,
                b.acp_id,
                b.order_num,
                b.shipped_sales,
                b.shipped_usd_sales,
                b.shipped_qty,
                b.return_date,
                b.return_amt,
                b.return_qty,
                b.return_usd_amt,
                b.days_to_return,
                b.remote_sell_employee_id,
                b.employee_name,
                b.sale_payroll_store, -- sales and returns always fall under here, but the employeeid drops after x days
                b.sale_payroll_store_description,
                b.sale_payroll_department_description,
                b.employee_payroll_store_region_num,
                b.employee_payroll_region_desc,
                b.num_days_since_termination, -- will need this to process HR rules
                b.order_pickup_ind,
                b.ship_to_store_ind

        FROM HR_APPENDED b
        
        --find the type of board
        LEFT JOIN (
                sel order_number, order_line_id, platform_code, max(board_type) as board_type 
                from(
	              	select DISTINCT 
	              		case when b.board_type is not null then board_type 
	              			when length(b.board_id) < 14 then 'ECF'
	              			else b.board_type end as board_type,
	                        a.order_number,
	                        a.order_line_id,
	                        oldf.source_platform_code as platform_code
	                        
	                from prd_nap_usr_vws.curation_order_line_fact a	
	                left join prd_nap_usr_vws.ORDER_LINE_DETAIL_FACT oldf on a.order_number = oldf.order_num
	                														--and a.order_line_id = oldf.order_line_id
	                														and oldf.source_platform_code <> 'POS'
	                																
	                inner join (sel distinct board_type, board_id from prd_nap_usr_vws.curation_fact) b --DBA RECOMMENDATION; THIS WAS CAUSING SKEW!
	                on a.board_id = b.board_id
					) fix 
				group by 1,2,3

                ) c ON b.order_num = c.order_number
        			AND b.order_line_id = c.order_line_id

) d
;
-------------
COLLECT STATISTICS  COLUMN (global_tran_id), --not sure what else goes here
                    COLUMN (PARTITION),
                    COLUMN (BUSINESS_DAY_DATE)
ON {remote_selling_t2_schema}.remote_sell_transactions;


SET QUERY_BAND = NONE FOR SESSION;
