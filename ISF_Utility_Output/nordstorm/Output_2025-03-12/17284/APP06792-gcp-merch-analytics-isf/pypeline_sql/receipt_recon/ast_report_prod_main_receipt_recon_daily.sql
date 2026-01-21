/***********************************************************/
/******************* FILTER AND LOOKUP TABLES **************/
/***********************************************************/

/******************* DATES ********************/
--DROP TABLE date_filter;
CREATE MULTISET VOLATILE TABLE date_filter AS (
	SELECT
		dt.month_start_day_date
		,dt.month_idnt
		,dt.month_label
		,rnk.month_rank
	FROM prd_nap_usr_vws.day_cal_454_dim dt
	INNER JOIN
		(
		SELECT 
			month_idnt
			,DENSE_RANK() OVER (ORDER BY month_idnt DESC) AS month_rank
		FROM prd_nap_usr_vws.day_cal_454_dim 
		WHERE day_date BETWEEN CURRENT_DATE - 70 AND CURRENT_DATE
		QUALIFY month_rank <=2
		)rnk
			ON dt.month_idnt = rnk.month_idnt
			GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX(month_rank) ON COMMIT PRESERVE ROWS; 

--update stats
COLLECT STATS 
	PRIMARY INDEX (month_rank)
	,COLUMN (month_start_day_date)
	,COLUMN (month_label)
		ON date_filter;

/******************* HIERARCHY ********************/

--DROP TABLE hier;
CREATE MULTISET VOLATILE TABLE hier AS (
	SELECT 
		sku.rms_sku_num
		,sku.channel_country
		,TRIM(dept.division_num) || ', ' || dept.division_short_name AS division
		,TRIM(dept.subdivision_num) || ', ' || dept.subdivision_short_name AS subdivision
		,TRIM(dept.dept_num) || ', ' || dept.dept_short_name AS department
		,TRIM(sku.class_num) || ', ' || sku.class_desc AS "class"
		,TRIM(sku.sbclass_num) || ', ' || sku.sbclass_desc AS subclass
		,sku.supp_part_num AS vpn
		,sku.style_desc
		,sku.supp_part_num || ', ' || sku.style_desc AS vpn_label
		,sku.color_num AS nrf_color_code
		,sku.color_desc AS color_desc
		,sku.supp_color AS supplier_color
		,sku.smart_sample_ind
		,sku.gwp_ind
		,sku.prmy_supp_num
		,CASE WHEN sku.prmy_supp_num IN
			(
			 '5126449'
			,'5126999'
			,'5100390'
			,'5120919'
			,'5152177'
			,'5161101'
			,'5161102'
			,'5176832'
			,'150644198'
			,'5104976'
			,'5181094'
			,'5181095'
			,'5181089'
			,'5181089'
			,'5181091'
			,'5181090'
			,'5181093'
			,'5181100'
			,'5183652'
			,'5166161'
			,'5181099'
			,'5183653'
			,'5091625'
			,'5178535'
			,'5171696'
			,'5183720'
			,'5183719'
			,'5124721'
			,'5178414'
			,'5096063'
			,'5178075'
			,'5181600'
			,'829373414'
			,'5105467'
			,'5074184'
			,'5128244'
			,'782328831'
			,'5088180'
			,'5104979'
			,'5154667'
			,'5154668'
			,'217647238'
			,'5111027'
			,'5172126'
			,'5171642'
			,'5099697'
			,'5104036'
			,'5109967'
			,'281265053'
			,'5155947'
			,'46737022'
			,'43121461'
			,'463750533'
			,'5104978'
			,'5082460'
			,'459799368'
			,'5104980'
			,'5160819'
			,'5167910'
			,'5113519'
			,'5104600'
			,'5090481'
			,'5090361'
			,'5090341'
			,'5090272'
			) --prepaid suppliers from Vanessa Priddy
			THEN 'Y' ELSE 'N' END AS prepaid_supplier_ind
    FROM prd_nap_usr_vws.product_sku_dim_vw sku
    INNER JOIN prd_nap_usr_vws.department_dim dept
      ON  sku.dept_num = dept.dept_num
) WITH DATA PRIMARY INDEX(rms_sku_num, channel_country) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (rms_sku_num, channel_country)
	,COLUMN (rms_sku_num, channel_country)
		ON hier;
		
	
/******************* LOCATIONS ********************/

--DROP TABLE dist_org;
CREATE MULTISET VOLATILE TABLE dist_org AS ( 
SELECT DISTINCT
	store_num
	,banner_country
	,TRIM(channel_num || ', ' || channel_desc) AS channel
FROM t2dl_das_phase_zero.ban_cntry_store_dim_vw
) WITH DATA PRIMARY INDEX(store_num) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (store_num)
	,COLUMN (store_num)
		ON dist_org;
	
/******************* HISTORICAL FILTER (temp) ********************/
--These are POs that closed before the historical cutoff (4/23) from Jonny Siu
--Could be removed once historical backfill is complete

--DROP TABLE hist_po;
CREATE MULTISET VOLATILE TABLE hist_po AS ( 
SELECT DISTINCT
	purchase_order_num
FROM prd_nap_usr_vws.purchase_order_fact
WHERE close_date < '2022-04-23'
	AND (dropship_ind = 'f' OR dropship_ind IS NULL)
	AND sendonly_ticket_partner_edi_ind = 'f'
--	AND open_to_buy_endofweek_date BETWEEN '2022-07-03' AND '2022-07-30'
) WITH DATA PRIMARY INDEX(purchase_order_num) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (purchase_order_num)
	,COLUMN (purchase_order_num)
		ON hist_po;	

/***********************************************************/
/****************** STAGING MAIN SOURCE TABLE **************/
/***********************************************************/

--DROP TABLE main;
CREATE MULTISET VOLATILE TABLE main AS ( 
SELECT
	 bom_otb_eow_date
	,bom_status
	,bom_unit_cost
	,bom_total_duty_per_unit
	,bom_total_expenses_per_unit
	,edi_ind
	,end_ship_date
	,eom_otb_eow_date
	,eom_status
	,eom_unit_cost
	,eom_total_duty_per_unit
	,eom_total_expenses_per_unit
	,exclude_from_backorder_avail_ind
	,first_approval_event_tmstp_pacific
	,import_country
	,latest_edi_date
	,month_end_day_date
	,month_start_day_date
	,npg_ind
	,internal_po_ind
	,order_from_vendor_id
	,order_type
	,original_end_ship_date
	,original_quantity_ordered
	,ownership_price_amt
	,po_type
	,po_vasn_signal
	,purchase_order_number
	,epo_purchase_order_id
	,purchase_type
	,rms_sku_num
	,ship_location_id
	,start_ship_date
	,written_date
	,COALESCE(bom_quantity_canceled  ,0) AS bom_quantity_canceled 
	,COALESCE(bom_quantity_ordered   ,0) AS bom_quantity_ordered  
	,COALESCE(bom_receipt_qty        ,0) AS bom_receipt_qty       
	,COALESCE(bom_vasn_qty           ,0) AS bom_vasn_qty          
	,COALESCE(eom_quantity_canceled  ,0) AS eom_quantity_canceled 
	,COALESCE(eom_quantity_ordered   ,0) AS eom_quantity_ordered  
	,COALESCE(eom_receipt_qty        ,0) AS eom_receipt_qty       
	,COALESCE(EOM_vasn_qty           ,0) AS eom_vasn_qty          
FROM prd_nap_vws.receipt_recon_fact_vw
WHERE month_start_day_date >= (SELECT MIN(month_start_day_date) FROM date_filter)
) WITH DATA PRIMARY INDEX(month_start_day_date, purchase_order_number) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (month_start_day_date, purchase_order_number)
	,COLUMN (month_start_day_date)
	,COLUMN (purchase_order_number)
		ON main;
	

/***********************************************************/
/********** STAGING I (LAST MONTH + CURRENT MONTH) ***********/
/***********************************************************/

--DROP TABLE staging1;
CREATE MULTISET VOLATILE TABLE staging1 AS ( 
SELECT
	 COALESCE(lmo.purchase_order_number                ,cmo.purchase_order_number                ) AS purchase_order_number                
	,COALESCE(lmo.rms_sku_num                          ,cmo.rms_sku_num                          ) AS rms_sku_num                          
	,COALESCE(lmo.ship_location_id                     ,cmo.ship_location_id                     ) AS ship_location_id                     
	,COALESCE(lmo.purchase_type                        ,cmo.purchase_type                        ) AS purchase_type             
	,COALESCE(lmo.edi_ind                              ,cmo.edi_ind                              ) AS edi_ind             
	,COALESCE(lmo.first_approval_event_tmstp_pacific   ,cmo.first_approval_event_tmstp_pacific   ) AS first_approval_event_tmstp_pacific             
	,COALESCE(lmo.original_end_ship_date               ,cmo.original_end_ship_date               ) AS original_end_ship_date                        
	,COALESCE(lmo.ownership_price_amt                  ,cmo.ownership_price_amt                  ) AS ownership_price_amt             
	,COALESCE(lmo.written_date                         ,cmo.written_date                         ) AS written_date             
	,COALESCE(cmo.latest_edi_date                      ,lmo.latest_edi_date                      ) AS latest_edi_date             
	,COALESCE(cmo.npg_ind                              ,lmo.npg_ind                              ) AS npg_ind   
	,COALESCE(cmo.internal_po_ind					   ,lmo.internal_po_ind 					 ) AS internal_po_ind         
	,COALESCE(cmo.exclude_from_backorder_avail_ind     ,lmo.exclude_from_backorder_avail_ind     ) AS exclude_from_backorder_avail_ind             
	,COALESCE(cmo.po_vasn_signal                       ,lmo.po_vasn_signal                       ) AS po_vasn_signal             
	,COALESCE(cmo.po_type                              ,lmo.po_type                              ) AS po_type             
	,COALESCE(cmo.order_type                           ,lmo.order_type                           ) AS order_type             
	,COALESCE(cmo.order_from_vendor_id                 ,lmo.order_from_vendor_id                 ) AS order_from_vendor_id             
	,COALESCE(cmo.import_country                       ,lmo.import_country                       ) AS import_country             
	,lmo.month_start_day_date            AS recon_month_start_day_date       
	,lmo.month_end_day_date              AS recon_month_end_day_date       
	,lmo.bom_otb_eow_date                AS bom_otb_eow_date       
	,lmo.bom_status                      AS bom_status             
	,lmo.bom_unit_cost                   AS bom_unit_cost  
	,lmo.bom_total_duty_per_unit		 AS bom_total_duty_per_unit
	,lmo.bom_total_expenses_per_unit	 AS bom_total_expenses_per_unit
	,lmo.eom_otb_eow_date                AS eom_otb_eow_date       
	,lmo.eom_status                      AS eom_status             
	,lmo.eom_unit_cost                   AS eom_unit_cost
	,lmo.eom_total_duty_per_unit		 AS eom_total_duty_per_unit
	,lmo.eom_total_expenses_per_unit	 AS eom_total_expenses_per_unit
	,lmo.start_ship_date                 AS eom_start_ship_date                     
	,lmo.end_ship_date                   AS eom_end_ship_date                    
	,cmo.eom_otb_eow_date                AS cur_otb_eow_date       
	,cmo.eom_status                      AS cur_status             
	,cmo.eom_unit_cost                   AS cur_unit_cost 
	,cmo.eom_total_duty_per_unit 		 AS cur_total_duty_per_unit
	,cmo.eom_total_expenses_per_unit	 AS cur_total_expenses_per_unit
	,cmo.start_ship_date                 AS cur_start_ship_date                     
	,cmo.end_ship_date                   AS cur_end_ship_date   
	,SUM(lmo.original_quantity_ordered)  AS original_quantity_ordered                 
	,SUM(lmo.bom_quantity_canceled  ) AS bom_quantity_canceled               
	,SUM(lmo.bom_quantity_ordered   ) AS bom_quantity_ordered                
	,SUM(lmo.bom_receipt_qty        ) AS bom_receipt_qty                     
	,SUM(lmo.bom_vasn_qty           ) AS bom_vasn_qty                        
	,SUM(lmo.eom_quantity_canceled  ) AS eom_quantity_canceled               
	,SUM(lmo.eom_quantity_ordered   ) AS eom_quantity_ordered                
	,SUM(lmo.eom_receipt_qty        ) AS eom_receipt_qty                     
	,SUM(lmo.eom_vasn_qty           ) AS eom_vasn_qty                        
	,SUM(cmo.eom_quantity_canceled  ) AS cur_quantity_canceled               
	,SUM(cmo.eom_quantity_ordered   ) AS cur_quantity_ordered                
	,SUM(cmo.eom_receipt_qty        ) AS cur_receipt_qty                     
	,SUM(cmo.eom_vasn_qty           ) AS cur_vasn_qty                        
FROM  
	(
	SELECT *         
	FROM main
	WHERE month_start_day_date = (SELECT DISTINCT month_start_day_date FROM date_filter WHERE month_rank = 2)
		AND purchase_order_number NOT IN (SELECT * FROM hist_po) --temp
	) lmo
FULL OUTER JOIN
	(
	SELECT *   
	FROM main
	WHERE month_start_day_date = (SELECT DISTINCT month_start_day_date FROM date_filter WHERE month_rank = 1)
		AND purchase_order_number NOT IN (SELECT * FROM hist_po) --temp
	) cmo
		 ON lmo.purchase_order_number               = cmo.purchase_order_number             
		AND lmo.rms_sku_num                         = cmo.rms_sku_num                       
		AND lmo.ship_location_id                    = cmo.ship_location_id                  
GROUP BY 1,2,3,4,5,6,7,8,9,10
	,11,12,13,14,15,16,17,18,19,20
	,21,22,23,24,25,26,27,28,29,30
	,31,32,33,34,35,36,37,38,39
) WITH DATA PRIMARY INDEX(purchase_order_number, rms_sku_num, import_country) ON COMMIT PRESERVE ROWS; 

COLLECT STATS
	PRIMARY INDEX (purchase_order_number, rms_sku_num, import_country)
	,COLUMN (purchase_order_number, ship_location_id)
	,COLUMN (ship_location_id)
	,COLUMN (purchase_order_number)
	,COLUMN (rms_sku_num, import_country)
	,COLUMN (order_from_vendor_id)
	,COLUMN (recon_month_start_day_date)
	,COLUMN (cur_otb_eow_date)
		ON staging1;

/***********************************************************/
/******************* INTERNAL XREF ORDERS ******************/
/***********************************************************/

--DROP TABLE voh;
CREATE MULTISET VOLATILE TABLE voh AS (
	SELECT purchase_order_num as order_no, crossreference_external_id as rms_xref_po, written_date
	from prd_nap_usr_vws.purchase_order_fact
	WHERE written_date >= CURRENT_DATE -365 --ADDED BY ME FOR CPU ISSUES
) WITH DATA PRIMARY INDEX(order_no) ON COMMIT PRESERVE ROWS; 

COLLECT STATS 
	PRIMARY INDEX (order_no)
	,COLUMN (rms_xref_po)
	,COLUMN (written_date)
		ON voh;

/***********************************************************/
/******************* INTERNAL ORDERS STAGE *****************/
/***********************************************************/
--Creates table of internal POs, with their parent PO numbers. Commented out original code with CTE instead of temp table
--INCLUDES POS WITH PARENT_PO AND INTERNAL PO THAT ARE THE SAME NUMBER SO EXCLUDING BELOW

--DROP TABLE internal_orders_stage;
CREATE MULTISET VOLATILE TABLE internal_orders_stage AS (
--with voh as 
--(
--	SELECT purchase_order_num as order_no, crossreference_external_id as rms_xref_po, written_date
--	from prd_nap_usr_vws.purchase_order_fact
--)
select
greatest(
greatest(
greatest(
greatest(
greatest(
greatest(
greatest(
greatest(
      COALESCE(case when voh.written_date  - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then  voh.order_no else null end, 0)
    , COALESCE(case when voh2.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh2.order_no else null end, 0)
	)
    , COALESCE(case when voh3.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh3.order_no else null end, 0)
    )
    , COALESCE(case when voh4.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh4.order_no else null end, 0)
    )
    , COALESCE(case when voh5.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh5.order_no else null end, 0)
    )
    , COALESCE(case when voh6.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh6.order_no else null end, 0)
    )
    , COALESCE(case when voh7.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh7.order_no else null end, 0)
    )
    , COALESCE(case when voh8.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh8.order_no else null end, 0)
    )
    , COALESCE(case when voh9.written_date - CURRENT_DATE  = TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(TD_SYSFNLIB.LEAST(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh9.order_no else null end, 0)
    ) as parent_po
, voh.order_no
from voh
inner join (select purchase_order_num as order_no from prd_nap_usr_vws.PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT where ship_location_id not in (808, 828) group by purchase_order_num) ol on voh.order_no = ol.order_no
left join voh voh2 on voh.rms_xref_po = voh2.order_no
left join voh voh3 on case when voh2.rms_xref_po = voh.order_no then null else voh2.rms_xref_po end = voh3.order_no 
left join voh voh4 on case when voh3.rms_xref_po = voh2.order_no then null when voh3.rms_xref_po = voh.order_no then null else voh3.rms_xref_po end = voh4.order_no
left join voh voh5 on case when voh4.rms_xref_po = voh3.order_no then null when voh4.rms_xref_po = voh2.order_no then null when voh4.rms_xref_po = voh.order_no then null else voh4.rms_xref_po end = voh5.order_no
left join voh voh6 on case when voh5.rms_xref_po = voh4.order_no then null when voh5.rms_xref_po = voh3.order_no then null when voh5.rms_xref_po = voh2.order_no then null when voh5.rms_xref_po = voh.order_no then null else voh5.rms_xref_po end = voh6.order_no
left join voh voh7 on case when voh6.rms_xref_po = voh5.order_no then null when voh6.rms_xref_po = voh4.order_no then null when voh6.rms_xref_po = voh3.order_no then null when voh6.rms_xref_po = voh2.order_no then null when voh6.rms_xref_po = voh.order_no then null else voh6.rms_xref_po end = voh7.order_no
left join voh voh8 on case when voh7.rms_xref_po = voh6.order_no then null when voh7.rms_xref_po = voh5.order_no then null when voh7.rms_xref_po = voh4.order_no then null when voh7.rms_xref_po = voh3.order_no then null when voh7.rms_xref_po = voh2.order_no then null when voh7.rms_xref_po = voh.order_no then null else voh7.rms_xref_po end = voh8.order_no
left join voh voh9 on case when voh8.rms_xref_po = voh7.order_no then null when voh8.rms_xref_po = voh6.order_no then null when voh8.rms_xref_po = voh5.order_no then null when voh8.rms_xref_po = voh4.order_no then null when voh8.rms_xref_po = voh3.order_no then null when voh8.rms_xref_po = voh2.order_no then null when voh8.rms_xref_po = voh.order_no then null else voh8.rms_xref_po end = voh9.order_no
) WITH DATA PRIMARY INDEX(order_no) ON COMMIT PRESERVE ROWS; 
;

COLLECT STATS 
	PRIMARY INDEX (order_no)
	,COLUMN (parent_po)
		ON internal_orders_stage;
;


/***********************************************************/
/**************** PARENT ORDERS BASE *********************/
/***********************************************************/
--Creates a distinct list of Parent POs

--DROP TABLE parent_orders_base;
CREATE MULTISET VOLATILE TABLE parent_orders_base AS (
SELECT DISTINCT parent_po
FROM internal_orders_stage
WHERE parent_po <> order_no
) WITH DATA PRIMARY INDEX(parent_po) ON COMMIT PRESERVE ROWS; 
;
COLLECT STATS 
	PRIMARY INDEX (parent_po)
		ON parent_orders_base;

/***********************************************************/
/**************** INTERNAL ORDERS BASE *********************/
/***********************************************************/
--Filters out where parent and internal are the same

--DROP TABLE internal_orders_base;
CREATE MULTISET VOLATILE TABLE internal_orders_base AS (
SELECT *
FROM internal_orders_stage
WHERE parent_po <> order_no
) WITH DATA PRIMARY INDEX(order_no) ON COMMIT PRESERVE ROWS; 
;
COLLECT STATS 
	PRIMARY INDEX (order_no)
	,COLUMN (parent_po)
		ON internal_orders_base;

/***********************************************************/
/******************* STAGING II **********************/
/***********************************************************/
--Joining in Internal Receipts and creating Open OO
--For daily, pulling last month (month 2) data

--DROP TABLE staging2;
CREATE MULTISET VOLATILE TABLE staging2 AS (
	SELECT
		dt.month_label	
		,otb.month_label AS otb_month_label
		,fct.purchase_order_number
		,fct.rms_sku_num
--		,fct.ship_location_id 
		,fct.purchase_type
		,CAST(fct.first_approval_event_tmstp_pacific AS DATE) AS approval_date
		,fct.original_end_ship_date
		,fct.ownership_price_amt --not critical, but check that Daryl's using this
		,fct.written_date
		,fct.latest_edi_date
		,fct.po_vasn_signal
		,fct.po_type
		,fct.order_type
		,fct.import_country
		,fct.recon_month_start_day_date 
		,fct.recon_month_end_day_date    
		,fct.bom_otb_eow_date
		,fct.bom_status
		,fct.bom_unit_cost + fct.bom_total_duty_per_unit + fct.bom_total_expenses_per_unit AS bom_unit_cost   
		,fct.eom_otb_eow_date
		,fct.eom_status
		,fct.eom_unit_cost + fct.eom_total_duty_per_unit + fct.eom_total_expenses_per_unit AS eom_unit_cost
		,fct.eom_start_ship_date
		,fct.eom_end_ship_date
		,fct.cur_otb_eow_date
		,fct.cur_status
		,fct.cur_unit_cost + fct.cur_total_duty_per_unit + fct.cur_total_expenses_per_unit AS cur_unit_cost
		,fct.cur_start_ship_date
		,fct.cur_end_ship_date
		,CASE WHEN fct.edi_ind = 'T' THEN 'Y' ELSE 'N' END AS edi_ind
		,CASE WHEN fct.npg_ind = 'T' THEN 'Y' ELSE 'N' END AS npg_ind
		,CASE WHEN fct.internal_po_ind = 'T' THEN 'Y' ELSE 'N' END AS internal_po_ind
		,CASE WHEN fct.exclude_from_backorder_avail_ind = 'T' THEN 'Y' ELSE 'N' END AS exclude_from_backorder_avail_ind
		,CASE WHEN inx.order_no IS NOT NULL THEN 'Y' ELSE 'N' END AS xref_po_ind
		,CASE WHEN par.parent_po IS NOT NULL THEN 'Y' ELSE 'N' END AS parent_po_ind
		,COALESCE(par.parent_po, inx.parent_po) AS parent_po
		,COALESCE(dist_org.banner_country, dist_org_temp.banner_country) AS banner_country
		,COALESCE(dist_org.channel, dist_org_temp.channel) AS channel
		,hier.division
		,hier.subdivision
		,hier.department
		,hier."class"
		,hier.subclass
		,hier.vpn
		,hier.style_desc
		,hier.vpn_label
		,hier.nrf_color_code
		,hier.color_desc
		,hier.supplier_color
		,hier.prmy_supp_num
		,supp.vendor_name AS supplier
		,manu.vendor_name AS manufacturer
		,COALESCE(hier.smart_sample_ind, 'N') AS smart_sample_ind
		,COALESCE(hier.gwp_ind, 'N') AS gwp_ind
		,COALESCE(hier.prepaid_supplier_ind, 'N') AS prepaid_supplier_ind
		,SUM(fct.original_quantity_ordered                                     				  ) AS original_quantity_ordered 

		,SUM(fct.bom_quantity_ordered                                         			      ) AS bom_quantity_ordered      
		,SUM(fct.bom_quantity_canceled                                        				  ) AS bom_quantity_canceled     
		,SUM(fct.bom_receipt_qty                                             				  ) AS bom_receipt_qty           
		,SUM(fct.bom_vasn_qty                                                  				  ) AS bom_vasn_qty   
		
		,SUM(fct.eom_quantity_ordered                                          				  ) AS eom_quantity_ordered      
		,SUM(fct.eom_quantity_canceled                                        				  ) AS eom_quantity_canceled     
		,SUM(fct.eom_receipt_qty                                               				  ) AS eom_receipt_qty           
		,SUM(fct.eom_vasn_qty                                                  				  ) AS eom_vasn_qty   
		
		,SUM(fct.cur_quantity_ordered                                          				  ) AS cur_quantity_ordered      
		,SUM(fct.cur_quantity_canceled                                        				  ) AS cur_quantity_canceled     
		,SUM(fct.cur_receipt_qty                                               				  ) AS cur_receipt_qty           
		,SUM(fct.cur_vasn_qty                                                 				  ) AS cur_vasn_qty              
		,SUM(fct.eom_quantity_ordered - fct.bom_quantity_ordered               				  ) AS m_quantity_ordered  
		,SUM(fct.eom_quantity_canceled - fct.bom_quantity_canceled             				  ) AS m_quantity_canceled 
		,SUM(fct.eom_receipt_qty - fct.bom_receipt_qty                         				  ) AS m_receipt_qty
		,SUM(fct.eom_vasn_qty - fct.bom_vasn_qty                               				  ) AS m_vasn_qty
		,SUM(fct.cur_quantity_ordered - fct.bom_quantity_ordered              				  ) AS c_quantity_ordered  
		,SUM(fct.cur_quantity_canceled - fct.bom_quantity_canceled             				  ) AS c_quantity_canceled 
		,SUM(fct.cur_receipt_qty - fct.bom_receipt_qty                         				  ) AS c_receipt_qty
		,SUM(fct.cur_vasn_qty - fct.bom_vasn_qty                               				  ) AS c_vasn_qty
	FROM staging1 fct
	LEFT JOIN t2dl_das_phase_zero.po_distribute_store_vw dist
		ON fct.ship_location_id = dist.ship_location_id
		AND fct.purchase_order_number = dist.purchase_order_num
	LEFT JOIN dist_org
		ON dist.min_dist_loc = dist_org.store_num
	LEFT JOIN dist_org AS dist_org_temp -------------temporary fix from P0 for ship locations not joining like 399?
		ON fct.ship_location_id = dist_org_temp.store_num
	LEFT JOIN internal_orders_base inx
		ON fct.purchase_order_number = inx.order_no
	LEFT JOIN parent_orders_base par
		ON fct.purchase_order_number = par.parent_po
	LEFT JOIN hier
		ON fct.rms_sku_num = hier.rms_sku_num
		AND fct.import_country = hier.channel_country
	LEFT JOIN prd_nap_usr_vws.vendor_dim supp
		ON COALESCE(hier.prmy_supp_num, fct.order_from_vendor_id) = supp.vendor_num
	LEFT JOIN prd_nap_usr_vws.vendor_dim manu
		ON fct.order_from_vendor_id = manu.vendor_num
	LEFT JOIN (SELECT DISTINCT month_start_day_date, month_label FROM date_filter WHERE month_rank = 2) dt
		ON fct.recon_month_start_day_date = dt.month_start_day_date
	LEFT JOIN prd_nap_usr_vws.day_cal_454_dim otb
		ON fct.cur_otb_eow_date = otb.day_date 
	GROUP BY 1,2,3,4,5,6,7,8,9,10
		,11,12,13,14,15,16,17,18,19,20
		,21,22,23,24,25,26,27,28,29,30
		,31,32,33,34,35,36,37,38,39,40
		,41,42,43,44,45,46,47,48,49,50
		,51,52,53,54,55--,56--,57,58,59,60
) WITH DATA PRIMARY INDEX(rms_sku_num, purchase_order_number, cur_otb_eow_date) ON COMMIT PRESERVE ROWS; 
;


COLLECT STATS 
	PRIMARY INDEX (rms_sku_num, purchase_order_number, cur_otb_eow_date)
--	,COLUMN (bom_status)
--	,COLUMN (bom_otb_eow_date)
--	,COLUMN (recon_month_start_day_date)
--	,COLUMN (recon_month_end_day_date)
--	,COLUMN (cur_status)
--	,COLUMN (cur_otb_eow_date)
--	,COLUMN (eom_status)
--	,COLUMN (eom_otb_eow_date)
--	,COLUMN (internal_po_ind)
--	,COLUMN (approval_date)
		ON staging2;
	

	
/***********************************************************/
/******************* STAGING III **********************/
/***********************************************************/
--Joining in Internal Receipts and creating Open OO
--For daily, pulling last month (month 2) data

	
CREATE MULTISET VOLATILE TABLE staging3 AS (
	SELECT
		fct.month_label	
		,fct.otb_month_label
		,fct.purchase_order_number
		,fct.rms_sku_num
--		,fct.ship_location_id 
		,fct.purchase_type
		,fct.approval_date
		,fct.original_end_ship_date
		,fct.ownership_price_amt
		,fct.written_date
		,fct.latest_edi_date
		,fct.po_vasn_signal
		,fct.po_type
		,fct.order_type
		,fct.import_country
		,fct.recon_month_start_day_date 
		,fct.recon_month_end_day_date    
		,fct.bom_otb_eow_date
		,fct.bom_status
		,fct.bom_unit_cost
		,fct.eom_otb_eow_date
		,fct.eom_status
		,fct.eom_unit_cost
		,fct.eom_start_ship_date
		,fct.eom_end_ship_date
		,fct.cur_otb_eow_date
		,fct.cur_status
		,fct.cur_unit_cost
		,fct.cur_start_ship_date
		,fct.cur_end_ship_date
		,fct.edi_ind
		,fct.npg_ind
		,fct.internal_po_ind
		,fct.exclude_from_backorder_avail_ind
		,fct.xref_po_ind
		,fct.parent_po_ind
		,fct.parent_po
		,fct.banner_country
		,fct.channel
		,fct.division
		,fct.subdivision
		,fct.department
		,fct."class"
		,fct.subclass
		,fct.vpn
		,fct.style_desc
		,fct.vpn_label
		,fct.nrf_color_code
		,fct.color_desc
		,fct.supplier_color
		,fct.prmy_supp_num
		,fct.supplier
		,fct.manufacturer
		,fct.smart_sample_ind
		,fct.gwp_ind
		,fct.prepaid_supplier_ind
		,fct.original_quantity_ordered
		,fct.bom_quantity_ordered      
		,fct.bom_quantity_canceled     
		,fct.bom_receipt_qty           
		,fct.bom_vasn_qty              
		,fct.eom_quantity_ordered      
		,fct.eom_quantity_canceled     
		,fct.eom_receipt_qty           
		,fct.eom_vasn_qty              
		,fct.cur_quantity_ordered      
		,fct.cur_quantity_canceled     
		,fct.cur_receipt_qty           
		,fct.cur_vasn_qty              
		,fct.m_quantity_ordered  
		,fct.m_quantity_canceled 
		,fct.m_receipt_qty
		,fct.m_vasn_qty
		,fct.c_quantity_ordered  
		,fct.c_quantity_canceled 
		,fct.c_receipt_qty
		,fct.c_vasn_qty
		,COALESCE(fct.eom_quantity_ordered,0) - COALESCE(fct.eom_receipt_qty,0) AS eom_net_receipts_qty
		,COALESCE(fct.cur_quantity_ordered,0) - COALESCE(fct.cur_receipt_qty,0) AS cur_net_receipts_qty
		--eom - eom = eom net for over then just output the over receipts
		,CASE WHEN cur_net_receipts_qty > 0 AND CURRENT_DATE > fct.cur_otb_eow_date THEN cur_net_receipts_qty ELSE 0 END AS cur_open_qty --should this be filtering to OTB EOW
		,CASE WHEN cur_net_receipts_qty < 0 THEN (cur_net_receipts_qty * -1) ELSE 0 END AS cur_over_qty_test
		,CASE WHEN eom_net_receipts_qty < 0 THEN (eom_net_receipts_qty * -1) ELSE 0 END AS eom_over_qty_test
		,CASE WHEN eom_net_receipts_qty < 0 THEN (eom_net_receipts_qty * -1) ELSE 0 END AS eom_over_qty
--		,--if eom_over_qty is more than the recon month's receipts, then just that recon month's receipts... right? yes
		,CASE WHEN eom_over_qty > m_receipt_qty THEN m_receipt_qty ELSE eom_over_qty END AS m_over_qty
--		,CASE WHEN net_receipts_qty < 0 AND over_qty > m_receipt_qty THEN m_receipt_qty
--			WHEN net_receipts_qty < 0 AND over_qty <= m_receipt_qty THEN over_qty
--			ELSE 0 END AS m_over_qty
--		,CASE WHEN net_receipts_qty < 0 AND over_qty > c_receipt_qty THEN c_receipt_qty
--			WHEN net_receipts_qty < 0 AND over_qty <= c_receipt_qty THEN over_qty
--			ELSE 0 END AS c_over_qty
		,CASE WHEN c_vasn_qty > cur_open_qty THEN cur_open_qty ELSE 0 END AS c_vasn_qty_dedupe
--		,CASE WHEN m_vasn_qty > open_qty THEN open_qty ELSE 0 END AS m_vasn_qty_dedupe
	FROM staging2 fct
--	LEFT JOIN t2dl_das_phase_zero.po_distribute_store_vw dist
--		ON fct.ship_location_id = dist.ship_location_id
--		AND fct.purchase_order_number = dist.purchase_order_num
--	LEFT JOIN dist_org
--		ON dist.min_dist_loc = dist_org.store_num
--	LEFT JOIN dist_org AS dist_org_temp -------------temporary fix from P0 for ship locations not joining like 399?
--		ON fct.ship_location_id = dist_org_temp.store_num
--	LEFT JOIN internal_orders_base inx
--		ON fct.purchase_order_number = inx.order_no
--	LEFT JOIN parent_orders_base par
--		ON fct.purchase_order_number = par.parent_po
--	LEFT JOIN hier
--		ON fct.rms_sku_num = hier.rms_sku_num
--		AND fct.import_country = hier.channel_country
--	LEFT JOIN prd_nap_usr_vws.vendor_dim supp
--		ON COALESCE(hier.prmy_supp_num, fct.order_from_vendor_id) = supp.vendor_num
--	LEFT JOIN prd_nap_usr_vws.vendor_dim manu
--		ON fct.order_from_vendor_id = manu.vendor_num
--	LEFT JOIN (SELECT DISTINCT month_start_day_date, month_label FROM date_filter WHERE month_rank = 2) dt
--		ON fct.recon_month_start_day_date = dt.month_start_day_date
--	LEFT JOIN prd_nap_usr_vws.day_cal_454_dim otb
--		ON fct.cur_otb_eow_date = otb.day_date --updated to EOM OTB EOW Date
) WITH DATA PRIMARY INDEX(purchase_order_number, rms_sku_num, channel) ON COMMIT PRESERVE ROWS; 
;	






/***********************************************************/
/************* Aggregate before Joining Outbound ***********/
/***********************************************************/	
	
	
--DROP TABLE staging4;
CREATE MULTISET VOLATILE TABLE staging4 AS (
	SELECT
		fct.month_label	
		,fct.otb_month_label
		,fct.purchase_order_number
		,fct.purchase_type
		,fct.approval_date
		,fct.original_end_ship_date

		,fct.written_date
		,fct.latest_edi_date
		,fct.po_vasn_signal
		,fct.po_type
		,fct.order_type
		,fct.import_country
		,fct.recon_month_start_day_date 
		,fct.recon_month_end_day_date    
		,fct.bom_otb_eow_date
		,fct.bom_status

		,fct.eom_otb_eow_date
		,fct.eom_status

		,fct.eom_start_ship_date
		,fct.eom_end_ship_date
		,fct.cur_otb_eow_date
		,fct.cur_status

		,fct.cur_start_ship_date
		,fct.cur_end_ship_date
		,fct.edi_ind
		,fct.npg_ind
		,fct.internal_po_ind
		,fct.exclude_from_backorder_avail_ind
		,fct.xref_po_ind
		,fct.parent_po_ind
		,fct.parent_po
		,fct.banner_country
		,fct.channel
		,fct.division
		,fct.subdivision
		,fct.department
		,fct."class"
		,fct.subclass
		,fct.vpn
		,fct.style_desc
		,fct.vpn_label
		,fct.nrf_color_code
		,fct.color_desc
		,fct.supplier_color
		,fct.prmy_supp_num
		,fct.supplier
		,fct.manufacturer
		,fct.smart_sample_ind
		,fct.gwp_ind
		,fct.prepaid_supplier_ind
		,MAX(fct.ownership_price_amt				) AS ownership_price_amt
		,MAX(fct.bom_unit_cost						) AS bom_unit_cost
		,MAX(fct.eom_unit_cost						) AS eom_unit_cost
		,MAX(fct.cur_unit_cost						) AS cur_unit_cost
		,SUM(fct.original_quantity_ordered			) AS original_quantity_ordered
		,SUM(fct.bom_quantity_ordered				) AS bom_quantity_ordered
		,SUM(fct.bom_quantity_canceled				) AS bom_quantity_canceled
		,SUM(fct.bom_receipt_qty					) AS bom_receipt_qty
		,SUM(fct.bom_vasn_qty						) AS bom_vasn_qty
		,SUM(fct.eom_quantity_ordered				) AS eom_quantity_ordered
		,SUM(fct.eom_quantity_canceled				) AS eom_quantity_canceled
		,SUM(fct.eom_receipt_qty					) AS eom_receipt_qty
		,SUM(fct.eom_vasn_qty						) AS eom_vasn_qty
		,SUM(fct.cur_quantity_ordered				) AS cur_quantity_ordered
		,SUM(fct.cur_quantity_canceled				) AS cur_quantity_canceled
		,SUM(fct.cur_receipt_qty					) AS cur_receipt_qty
		,SUM(fct.cur_vasn_qty						) AS cur_vasn_qty
		,SUM(fct.m_quantity_ordered					) AS m_quantity_ordered
		,SUM(fct.m_quantity_canceled				) AS m_quantity_canceled
		,SUM(fct.m_receipt_qty						) AS m_receipt_qty
		,SUM(fct.m_vasn_qty							) AS m_vasn_qty
		,SUM(fct.c_quantity_ordered					) AS c_quantity_ordered
		,SUM(fct.c_quantity_canceled				) AS c_quantity_canceled
		,SUM(fct.c_receipt_qty						) AS c_receipt_qty
		,SUM(fct.c_vasn_qty							) AS c_vasn_qty
		,SUM(eom_net_receipts_qty 					) AS eom_net_receipts_qty
		,SUM(cur_net_receipts_qty					) AS cur_net_receipts_qty
		,SUM(cur_open_qty							) AS cur_open_qty 
		,SUM(cur_over_qty_test						) AS cur_over_qty_test
		,SUM(eom_over_qty_test						) AS eom_over_qty_test
		,SUM(eom_over_qty							) AS eom_over_qty
		,SUM(m_over_qty								) AS m_over_qty		
		,SUM(c_vasn_qty_dedupe						) AS c_vasn_qty_dedupe
	FROM staging3 fct

 
	GROUP BY 1,2,3,4,5,6,7,8,9,10
		,11,12,13,14,15,16,17,18,19,20
		,21,22,23,24,25,26,27,28,29,30
		,31,32,33,34,35,36,37,38,39,40
		,41,42,43,44,45,46,47,48,49,50
		
) WITH DATA PRIMARY INDEX(purchase_order_number, vpn, channel) ON COMMIT PRESERVE ROWS; 
;


COLLECT STATS 
	PRIMARY INDEX (purchase_order_number, vpn, channel)
	,COLUMN (bom_status)
	,COLUMN (bom_otb_eow_date)
	,COLUMN (recon_month_start_day_date)
	,COLUMN (recon_month_end_day_date)
	,COLUMN (cur_status)
	,COLUMN (cur_otb_eow_date)
	,COLUMN (eom_status)
	,COLUMN (eom_otb_eow_date)
	,COLUMN (internal_po_ind)
	,COLUMN (approval_date)
		ON staging4;


/***********************************************************/
/**************** Grab Outbound Receipt Data ***************/
/***********************************************************/


--drop table outbound_receipt_data
   CREATE MULTISET VOLATILE TABLE outbound_receipt_data AS (	
	SELECT 
		(SELECT DISTINCT month_label FROM date_filter WHERE month_rank = 2) as month_label
		, otbdt.month_label as otb_month_label
		, purchase_order_num
		, vpn
		, color_num 
		, TRIM(channel) as channel
		, TRIM(division) as division
		, TRIM(subdivision) as subdivision
		, TRIM(department) as department
		, TRIM("class") as "class"
		, TRIM(subclass) as subclass
		, supp_num 
		, supp_color
		, style_desc
		, MAX(otb_eow_date) as otb_eow_date
		, SUM(receipt_cost) as store_receipt_cost
		, SUM(receipt_retail) as store_receipt_retail
		, SUM(receipt_units) AS store_quantity_received
		, SUM(store_quantity_open) AS store_quantity_open
		, SUM(c_vasn_qty_dedupe_outbound) AS c_vasn_qty_dedupe_outbound
	FROM (
	--get receipts from last month
	
	SELECT 
		str.month_idnt
		, otb_month_idnt
		, purchase_order_num
		, vpn
		, color_num
		, supp_num 
		, supp_color
		, CONCAT(channel_num, ', ' , channel_desc) as channel
		, CONCAT(division_num, ', ' , division_name) as division
		, CONCAT(subdivision_num, ', ', subdivision_name) as subdivision
		, CONCAT(dept_num, ', ' , dept_name) as department
		, CONCAT(class_num, ', ' , class_desc) as "class"
		, CONCAT(sbclass_num, ', ', sbclass_desc) as subclass
		, style_desc
		, RCPT_COST as receipt_cost
		, RCPT_RETAIL as receipt_retail
		, RCPT_UNITS as receipt_units
		, otb_eow_date
		, CAST(0 AS INTEGER)as store_quantity_open
		, CAST(0 AS INTEGER) as c_vasn_qty_dedupe_outbound
	
	FROM T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL str
	  JOIN date_filter dt 
	  ON str.month_idnt = dt.month_idnt 
	  WHERE month_rank = 2
	
	  UNION ALL
	  
	SELECT 
		str.month_idnt
		, otb_month_idnt
		, purchase_order_num
		, vpn
		, color_num
		, supp_num 
		, supp_color
		, CONCAT(channel_num, ', ' , channel_desc) as channel
		, CONCAT(division_num, ', ' , division_name) as division
		, CONCAT(subdivision_num, ', ', subdivision_name) as subdivision
		, CONCAT(dept_num, ', ' , dept_name) as department
		, CONCAT(class_num, ', ' , class_desc) as "class"
		, CONCAT(sbclass_num, ', ', sbclass_desc) as subclass
		, style_desc
		, CAST(0 AS DECIMAL(20,2)) as receipt_cost
		, CAST(0 AS DECIMAL(20,2)) as receipt_retail
		, CAST(0 AS INTEGER) as receipt_units
		, otb_eow_date
		, quantity_open as store_quantity_open
		, CASE WHEN vasn_sku_qty > quantity_open THEN quantity_open ELSE 0 END as c_vasn_qty_dedupe_outbound
	
	  FROM T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL str
	  JOIN date_filter dt ON str.month_idnt = dt.month_idnt
	  WHERE month_rank = 1
	  
	) a 
	JOIN (SELECT DISTINCT month_idnt, month_label FROM prd_nap_usr_vws.day_cal_454_dim) otbdt
	ON a.otb_month_idnt = otbdt.month_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
	) WITH DATA PRIMARY INDEX(purchase_order_num, vpn, month_label) ON COMMIT PRESERVE ROWS; 


/***********************************************************/
/*************** Join to Outbound Receipt Data *************/
/***********************************************************/

--drop table staging5;
CREATE MULTISET VOLATILE TABLE staging5 AS (

	SELECT
		COALESCE(fct.month_label, str.month_label) as month_label	
		,COALESCE(fct.otb_month_label, str.otb_month_label) as otb_month_label
		,COALESCE(fct.purchase_order_number, str.purchase_order_num) as purchase_order_number
		,fct.purchase_type
		,fct.approval_date
		,fct.original_end_ship_date
		,fct.ownership_price_amt
		,fct.written_date
		,fct.latest_edi_date
		,fct.po_vasn_signal
		,fct.po_type
		,fct.order_type
		,fct.import_country
		,fct.recon_month_start_day_date 
		,fct.recon_month_end_day_date    
		,fct.bom_otb_eow_date
		,fct.bom_status
		,fct.bom_unit_cost
		,fct.eom_otb_eow_date
		,fct.eom_status
		,fct.eom_unit_cost
		,fct.eom_start_ship_date
		,fct.eom_end_ship_date
		,COALESCE(fct.cur_otb_eow_date, str.otb_eow_date) as cur_otb_eow_date
		,fct.cur_status
		,fct.cur_unit_cost
		,fct.cur_start_ship_date
		,fct.cur_end_ship_date
		,fct.edi_ind
		,fct.npg_ind
		,fct.internal_po_ind
		,fct.exclude_from_backorder_avail_ind
		,fct.xref_po_ind
		,fct.parent_po_ind
		,fct.parent_po
		,fct.banner_country
		,COALESCE(fct.channel, str.channel) as channel
		,COALESCE(fct.division, str.division) as division
		,COALESCE(fct.subdivision, str.subdivision) as subdivision
		,COALESCE(fct.department, str.department) as department
		,COALESCE(fct."class", str."class") as "class"
		,COALESCE(fct.subclass, str.subclass) as subclass
		,COALESCE(fct.vpn, str.vpn) as vpn
		,fct.style_desc
		,fct.vpn_label
		,COALESCE(fct.nrf_color_code, str.color_num) as nrf_color_code
		,fct.color_desc
		,fct.supplier_color
		,fct.prmy_supp_num
		,fct.supplier
		,fct.manufacturer
		,fct.smart_sample_ind
		,fct.gwp_ind
		,fct.prepaid_supplier_ind
		,fct.original_quantity_ordered
		,fct.bom_quantity_ordered			
		,fct.bom_quantity_canceled			
		,fct.bom_receipt_qty				
		,fct.bom_vasn_qty					
		,fct.eom_quantity_ordered			
		,fct.eom_quantity_canceled			
		,fct.eom_receipt_qty				
		,fct.eom_vasn_qty					
		,fct.cur_quantity_ordered			
		,fct.cur_quantity_canceled			
		,fct.cur_receipt_qty				
		,fct.cur_vasn_qty					
		,fct.m_quantity_ordered				
		,fct.m_quantity_canceled			
		,fct.m_receipt_qty					
		,fct.m_vasn_qty						
		,fct.c_quantity_ordered				
		,fct.c_quantity_canceled			
		,fct.c_receipt_qty					
		,fct.c_vasn_qty						
		,eom_net_receipts_qty 				
		,cur_net_receipts_qty				
		,cur_open_qty						
		,cur_over_qty_test					
		,eom_over_qty_test					
		,eom_over_qty						
		,m_over_qty							
		,c_vasn_qty_dedupe					
		,COALESCE(store_receipt_cost,0) AS store_receipt_cost				
		,COALESCE(store_receipt_retail,0) AS store_receipt_retail					
		,COALESCE(store_quantity_received,0) AS store_quantity_received			
		,COALESCE(store_quantity_open,0) AS store_quantity_open				
		,COALESCE(c_vasn_qty_dedupe_outbound,0) AS c_vasn_qty_dedupe_outbound 				
		,COALESCE(eom_quantity_ordered - store_quantity_received,0)  as net_store_receipts
		,COALESCE(CASE WHEN net_store_receipts < 0 THEN net_store_receipts * -1 ELSE 0 END,0) as store_over_qty
		
	FROM staging4 fct
	full outer JOIN outbound_receipt_data str
	ON fct.month_label = str.month_label
	AND fct.otb_month_label = str.otb_month_label
	AND fct.purchase_order_number = str.purchase_order_num
	AND fct.vpn = str.vpn
	AND fct.nrf_color_code = str.color_num
	AND fct.prmy_supp_num = str.supp_num
	AND COALESCE(fct.supplier_color, '') = COALESCE(str.supp_color, '')
	AND fct.channel = TRIM(str.channel)
	AND fct."class" = TRIM(str."class")
	AND fct.subclass = TRIM(str.subclass)
	AND fct.style_desc = str.style_desc

) WITH DATA PRIMARY INDEX(purchase_order_number, vpn, month_label) ON COMMIT PRESERVE ROWS; 
 

/***********************************************************/
/*************************** Final *************************/
/***********************************************************/

DELETE FROM {environment_schema}.receipt_recon_daily;
INSERT INTO {environment_schema}.receipt_recon_daily
	SELECT
		 CURRENT_TIMESTAMP AS refresh_timestamp
		,fct.month_label
		,fct.otb_month_label
		,fct.recon_month_start_day_date 
		,fct.recon_month_end_day_date    
		,fct.purchase_order_number
		,fct.purchase_type
		,fct.approval_date
		,fct.original_end_ship_date
		,fct.ownership_price_amt
		,fct.written_date
		,fct.latest_edi_date
		,fct.po_vasn_signal
		,fct.po_type
		,fct.order_type
		,fct.bom_otb_eow_date
		,fct.bom_status
		,fct.eom_otb_eow_date
		,fct.eom_status
		,fct.eom_start_ship_date
		,fct.eom_end_ship_date
		,fct.cur_otb_eow_date
		,fct.cur_status
		,fct.cur_start_ship_date
		,fct.cur_end_ship_date		
		,fct.edi_ind
		,fct.npg_ind
		,fct.internal_po_ind
		,fct.exclude_from_backorder_avail_ind
		,fct.xref_po_ind
		,fct.parent_po_ind
		,fct.parent_po
		,fct.banner_country
		,fct.channel
		,fct.division
		,fct.subdivision
		,fct.department
		,fct."class"
		,fct.subclass
		,fct.vpn
		,TRIM(fct.style_desc) AS style_desc
		,TRIM(fct.vpn_label) AS vpn_label
		,fct.nrf_color_code
		,fct.color_desc
		,fct.supplier_color
		,fct.prmy_supp_num
		,fct.supplier
		,fct.manufacturer
		,fct.smart_sample_ind
		,fct.gwp_ind
		,fct.prepaid_supplier_ind
		--15 indicators
	--	MONTH ORDER INTENT
		,CASE WHEN fct.bom_status IN ('APPROVED','SUBMITTED','WORKSHEET')
			AND fct.bom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
			AND (fct.internal_po_ind <> 'Y' OR (fct.internal_po_ind = 'Y' AND fct.eom_otb_eow_date >= ADD_MONTHS(fct.recon_month_start_day_date, -12)))
				THEN 1 ELSE 0 END AS moi_bom_ind
		,CASE WHEN fct.eom_status IN ('APPROVED','SUBMITTED','WORKSHEET','CLOSED')
			AND fct.eom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
			AND (fct.internal_po_ind <> 'Y' OR (fct.internal_po_ind = 'Y' AND fct.eom_otb_eow_date >= ADD_MONTHS(fct.recon_month_start_day_date, -12)))
				THEN 1 ELSE 0 END AS moi_eom_ind
		,CASE WHEN fct.eom_status IN ('APPROVED','SUBMITTED','WORKSHEET','CLOSED')
			AND fct.eom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
			AND fct.approval_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date
			AND (fct.internal_po_ind <> 'Y' OR (fct.internal_po_ind = 'Y' AND fct.eom_otb_eow_date >= ADD_MONTHS(fct.recon_month_start_day_date, -12)))
				THEN 1 ELSE 0 END AS moi_add_ind
		,CASE WHEN fct.eom_status IN ('APPROVED','SUBMITTED','WORKSHEET','CLOSED')
			AND fct.eom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
			AND fct.bom_otb_eow_date NOT BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
			AND (fct.internal_po_ind <> 'Y' OR (fct.internal_po_ind = 'Y' AND fct.eom_otb_eow_date >= ADD_MONTHS(fct.recon_month_start_day_date, -12)))
				THEN 1 ELSE 0 END AS moi_moved_in_ind
		,CASE WHEN fct.eom_status IN ('APPROVED','SUBMITTED','WORKSHEET','CLOSED')
			AND fct.bom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
			AND fct.eom_otb_eow_date NOT BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
			AND (fct.internal_po_ind <> 'Y' OR (fct.internal_po_ind = 'Y' AND fct.eom_otb_eow_date >= ADD_MONTHS(fct.recon_month_start_day_date, -12)))
				THEN 1 ELSE 0 END AS moi_moved_out_ind
	--	OPEN ORDERS
		,CASE WHEN fct.cur_status IN ('APPROVED','SUBMITTED','WORKSHEET')
			AND store_quantity_open > 0
			AND fct.cur_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
				THEN 1 ELSE 0 END AS oo_in_mth_ind
		,CASE WHEN fct.cur_status IN ('APPROVED','SUBMITTED','WORKSHEET')
			AND store_quantity_open > 0
			AND fct.cur_otb_eow_date < fct.recon_month_start_day_date
				THEN 1 ELSE 0 END AS oo_prior_mth_ind
	--	CANCELS
		,CASE WHEN fct.eom_status = 'CLOSED'
			AND fct.bom_status <> 'CLOSED'
			AND fct.m_quantity_canceled > 0
				THEN 1 ELSE 0 END AS cancel_all_ind
		,CASE WHEN fct.eom_status = 'CLOSED'
			AND fct.bom_status <> 'CLOSED'
			AND fct.m_quantity_canceled > 0
			AND fct.eom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
				THEN 1 ELSE 0 END AS cancel_in_mth_ind
	--	RECEIPTS
		,CASE WHEN fct.eom_status IN ('APPROVED','WORKSHEET','SUBMITTED','CLOSED')
			AND store_quantity_received > 0
				THEN 1 ELSE 0 END AS rcpt_total_ind
		,CASE WHEN fct.eom_status IN ('APPROVED','WORKSHEET','SUBMITTED','CLOSED')
			AND store_quantity_received > 0
			AND fct.eom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
				THEN 1 ELSE 0 END AS rcpt_in_mth_ind
		,CASE WHEN fct.internal_po_ind = 'Y'
			AND store_quantity_received > 0
			AND fct.eom_otb_eow_date BETWEEN fct.recon_month_start_day_date AND fct.recon_month_end_day_date 
				THEN 1 ELSE 0 END AS rcpt_xref_ind
		,CASE WHEN store_quantity_received > 0
			AND fct.eom_otb_eow_date < fct.recon_month_start_day_date
				THEN 1 ELSE 0 END AS rcpt_shift_in_prior_ind
		,CASE WHEN store_quantity_received > 0
			AND fct.eom_otb_eow_date > fct.recon_month_end_day_date 
				THEN 1 ELSE 0 END AS rcpt_shift_in_future_ind
		,CASE WHEN store_quantity_received > 0
			AND store_over_qty > 0
				THEN 1 ELSE 0 END AS rcpt_over_ind
		, SUM(fct.original_quantity_ordered) AS original_orders_u
		, SUM(fct.original_quantity_ordered * fct.ownership_price_amt) AS original_orders_r
		, SUM(fct.original_quantity_ordered * fct.cur_unit_cost) AS original_orders_c				
		, SUM(fct.bom_quantity_ordered) AS bom_orders_u
		, SUM(fct.bom_quantity_ordered * fct.ownership_price_amt) AS bom_orders_r
		, SUM(fct.bom_quantity_ordered * fct.cur_unit_cost) AS bom_orders_c
		, SUM(fct.eom_quantity_ordered) AS eom_orders_u
		, SUM(fct.eom_quantity_ordered * fct.ownership_price_amt) AS eom_orders_r
		, SUM(fct.eom_quantity_ordered * fct.cur_unit_cost) AS eom_orders_c
		, SUM(fct.cur_quantity_ordered) AS cur_orders_u
		, SUM(fct.cur_quantity_ordered * fct.ownership_price_amt) AS cur_orders_r
		, SUM(fct.cur_quantity_ordered * fct.cur_unit_cost) AS cur_orders_c
		, SUM(fct.cur_open_qty) AS open_u
		, SUM(fct.cur_open_qty * fct.ownership_price_amt) AS open_r
		, SUM(fct.cur_open_qty * fct.cur_unit_cost) AS open_c
		, SUM(fct.m_over_qty) AS over_u
		, SUM(fct.m_over_qty * fct.ownership_price_amt) AS over_r
		, SUM(fct.m_over_qty * fct.cur_unit_cost) AS over_c
		, SUM(fct.m_over_qty) AS m_over_u
		, SUM(fct.m_over_qty * fct.ownership_price_amt) AS m_over_r
		, SUM(fct.m_over_qty * fct.cur_unit_cost) AS m_over_c
		, SUM(fct.m_quantity_canceled) AS m_cancels_u
		, SUM(fct.m_quantity_canceled * fct.ownership_price_amt) AS m_cancels_r
		, SUM(fct.m_quantity_canceled * fct.cur_unit_cost) AS m_cancels_c
		, SUM(fct.m_receipt_qty) AS m_receipts_u
		, SUM(fct.m_receipt_qty * fct.ownership_price_amt) AS m_receipts_r
		, SUM(fct.m_receipt_qty * cur_unit_cost) AS m_receipts_c
		, SUM(fct.c_quantity_canceled) AS c_cancels_u
		, SUM(fct.c_quantity_canceled * fct.ownership_price_amt) AS c_cancels_r
		, SUM(fct.c_quantity_canceled * fct.cur_unit_cost) AS c_cancels_c
		, SUM(fct.c_vasn_qty_dedupe) AS c_vasn_u
		, SUM(fct.c_vasn_qty_dedupe * fct.ownership_price_amt) AS c_vasn_r
		, SUM(fct.c_vasn_qty_dedupe * fct.cur_unit_cost) AS c_vasn_c
		, SUM(fct.c_receipt_qty) AS c_receipts_u
		, SUM(fct.c_receipt_qty * fct.ownership_price_amt) AS c_receipts_r
		, SUM(fct.c_receipt_qty * fct.cur_unit_cost) AS c_receipts_c
		--get outbound values
		, SUM(store_quantity_open * fct.cur_unit_cost) as outbound_open_c 
		, SUM(store_quantity_open) as outbound_open_u
		, SUM(store_quantity_open * fct.ownership_price_amt) as outbound_open_r
		, SUM(store_receipt_cost) as outbound_receipts_c 
		, SUM(store_quantity_received) as outbound_receipts_u
		, SUM(store_receipt_retail) as outbound_receipts_r
		, SUM(store_over_qty) as outbound_over_u
		, SUM(store_over_qty * fct.cur_unit_cost) as outbound_over_c
		, SUM(store_over_qty * fct.ownership_price_amt) as outbound_over_r
		, SUM(c_vasn_qty_dedupe_outbound) AS c_vasn_u_outbound
		, SUM(c_vasn_qty_dedupe_outbound * fct.ownership_price_amt) as c_vasn_r_outbound
		, SUM(c_vasn_qty_dedupe_outbound * fct.cur_unit_cost) as c_vasn_c_outbound
	FROM staging5 fct
	GROUP BY 1,2,3,4,5,6,7,8,9,10
		,11,12,13,14,15,16,17,18,19,20
		,21,22,23,24,25,26,27,28,29,30
		,31,32,33,34,35,36,37,38,39,40
		,41,42,43,44,45,46,47,48,49,50
		,51,52,53,54,55,56,57,58,59,60
		,61,62,63,64,65,66

;