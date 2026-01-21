/***********************************************************/
/******************* FILTER AND LOOKUP TABLES **************/
/***********************************************************/


/******************* HIERARCHY ********************/

--DROP TABLE hier;
CREATE MULTISET VOLATILE TABLE hier AS (
	SELECT 
		sku.rms_sku_num
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
      WHERE channel_country = 'US'
) WITH DATA PRIMARY INDEX(rms_sku_num) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (rms_sku_num)
	,COLUMN (rms_sku_num)
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
	 purchase_order_num
	 , TRIM(channel_num) || ', ' || TRIM(channel_desc) as channel_num 
	 , TRIM(channel_brand) as channel_brand 
	 , TRIM(division_num) || ', ' || TRIM(division_name) as division
	 , TRIM(subdivision_num) || ', ' || TRIM(subdivision_name) as subdivision 
	 , TRIM(dept_num) || ', ' || TRIM(dept_name) as department 
	 , TRIM(class_num) || ', ' || TRIM(class_desc) as "class"
	 , TRIM(sbclass_num) || ', ' || TRIM(sbclass_desc) as subclass
	 , TRIM(supp_num) as supplier_number
	 , supp_name as supplier_name
	 , vpn 
	 , style_desc 
	 , color_num 
	 , supp_color 
	 , customer_choice 
	 , o.STATUS as po_status
	 , comments
	 , latest_close_event_tmstp_pacific
	 , o.edi_ind 
	 , o.start_ship_date 
	 , o.end_ship_date
	 , o.otb_eow_date 
	 , o.latest_approval_date 
	 , o.otb_month 
	 , o.otb_month_idnt 
	 , o.order_type 
	 , o.internal_po_ind 
	 , o.npg_ind 
	 , o.po_type 
	 , o.purchase_type 
	 , o.month_idnt 
	 , "Month" 
	 , DENSE_RANK() OVER (PARTITION BY purchase_order_num, vpn, color_num, supp_color, style_desc, supplier_number, division, "class", subclass ORDER BY month_idnt) as month_join
	 , SUM(RCPT_COST) AS RCPT_COST
	 , SUM(RCPT_RETAIL) AS RCPT_RETAIL
	 , SUM(RCPT_UNITS) AS RCPT_UNITS
	 , SUM(QUANTITY_ORDERED) AS QUANTITY_ORDERED
	 , SUM(QUANTITY_RECEIVED) AS  QUANTITY_RECEIVED
	 , SUM(QUANTITY_CANCELED) AS QUANTITY_CANCELED
	 , SUM(QUANTITY_OPEN) AS QUANTITY_OPEN
	 , MAX(UNIT_COST_AMT) AS UNIT_COST_AMT
	 , SUM(TOTAL_ANTICIPATED_RETAIL_AMT) AS TOTAL_ANTICIPATED_RETAIL_AMT
	 , SUM(vasn_sku_qty) AS vasn_sku_qty
	 , SUM(vasn_cost) AS vasn_cost
	 , SUM(dc_received_qty) as DC_RCPT_UNITS
	 , SUM(dc_received_c) AS DC_RCPT_COST
	FROM T2DL_DAS_OPEN_TO_BUY.PO_OTB_DETAIL o
	LEFT JOIN PRD_NAP_USR_VWS.PURCHASE_ORDER_HEADER_FACT h
	ON o.purchase_order_num = h.purchase_order_number 
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
) WITH DATA PRIMARY INDEX(otb_eow_date, purchase_order_num, vpn) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (otb_eow_date, purchase_order_num, vpn)
	,COLUMN (otb_eow_date)
	,COLUMN (purchase_order_num)
		ON main;
	

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
      COALESCE(case when voh.written_date  - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then  voh.order_no else null end, 0)
    , COALESCE(case when voh2.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh2.order_no else null end, 0)
	)
    , COALESCE(case when voh3.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh3.order_no else null end, 0)
    )
    , COALESCE(case when voh4.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh4.order_no else null end, 0)
    )
    , COALESCE(case when voh5.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh5.order_no else null end, 0)
    )
    , COALESCE(case when voh6.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh6.order_no else null end, 0)
    )
    , COALESCE(case when voh7.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh7.order_no else null end, 0)
    )
    , COALESCE(case when voh8.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh8.order_no else null end, 0)
    )
    , COALESCE(case when voh9.written_date - CURRENT_DATE  = least(least(least(least(least(least(least(least(case when voh.written_date is null then 0 else voh.written_date - CURRENT_DATE end, case when voh2.written_date is null then 0 else voh2.written_date - CURRENT_DATE end), case when voh3.written_date is null then 0 else voh3.written_date - CURRENT_DATE end), case when voh4.written_date is null then 0 else voh4.written_date - CURRENT_DATE end), case when voh5.written_date is null then 0 else voh5.written_date - CURRENT_DATE end), case when voh6.written_date is null then 0 else voh6.written_date - CURRENT_DATE end), case when voh7.written_date is null then 0 else voh7.written_date - CURRENT_DATE end), case when voh8.written_date is null then 0 else voh8.written_date - CURRENT_DATE end), case when voh9.written_date is null then 0 else voh9.written_date - CURRENT_DATE end) then voh9.order_no else null end, 0)
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
/************************ DATES ****************************/
/***********************************************************/	
--rolls dates up to week so we can join to PO table
	
--DROP TABLE po_dates
CREATE MULTISET VOLATILE TABLE po_dates AS (
SELECT 
	 month_idnt
	, month_label
	, month_end_day_date
	, CASE WHEN month_start_day_date <= current_date and month_end_day_date >= current_date then 1 else 0 END as curr_month_ind 
FROM prd_nap_usr_vws.day_cal_454_dim 
GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX(month_idnt) ON COMMIT PRESERVE ROWS; 
COLLECT STATS 
	PRIMARY INDEX (month_idnt)
	,COLUMN (month_idnt)
		ON po_dates;
/***********************************************************/
/****************** Total Quantities ***********************/
/***********************************************************/
--grabs the values from the RR table that we can't get from Outbound table


--DROP TABLE rr_quantity
CREATE MULTISET VOLATILE TABLE rr_quantity AS ( 
SELECT
	
	eom_status
	,rr.month_end_day_date
	,internal_po_ind
	,purchase_order_number
	,rms_sku_num
	,latest_edi_date 
	,po_vasn_signal 
	,SUM(eom_quantity_ordered) AS eom_quantity_ordered
	,SUM(eom_quantity_ordered*eom_unit_cost) as eom_quantity_ordered_c
	,SUM(original_quantity_ordered) as original_quantity_ordered
	,SUM(eom_receipt_qty) AS EOM_receipt_qty 
	,SUM(eom_receipt_qty*eom_unit_cost) as EOM_receipt_c
	,MAX(ownership_price_amt) as ownership_price_amt
	,MAX(eom_unit_cost) as eom_unit_cost
FROM prd_nap_vws.receipt_recon_fact_vw rr
JOIN po_dates d on rr.month_end_day_date = d.month_end_day_date
AND d.curr_month_ind = 1
GROUP BY 1,2,3,4,5,6,7
) WITH DATA PRIMARY INDEX(month_end_day_date, purchase_order_number, rms_sku_num) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (month_end_day_date, purchase_order_number, rms_sku_num)
	,COLUMN (month_end_day_date)
	,COLUMN (purchase_order_number)
		ON rr_quantity;

/***********************************************************/
/********************* PO SKU Table ************************/
/***********************************************************/
--joins outbound sku table to RR metrics and internal / parent PO tables

--DROP Table po_sku_table
CREATE MULTISET VOLATILE TABLE po_sku_table AS (	
SELECT 
	s.purchase_order_num
	,s.sku_num
	,rr.eom_status
	,rr.internal_po_ind
	,rr.eom_unit_cost
	,rr.ownership_price_amt
    ,CASE WHEN inx.order_no IS NOT NULL THEN 'Y' ELSE 'N' END AS xref_po_ind
	,CASE WHEN par.parent_po IS NOT NULL THEN 'Y' ELSE 'N' END AS parent_po_ind
	,COALESCE(par.parent_po, inx.parent_po) AS parent_po
	,SUM(s.RCPT_COST) as RCPT_C
	,SUM(s.RCPT_UNITS) as RCPT_U
	,SUM(s.QUANTITY_OPEN*EOM_UNIT_COST) as OO_C
	,SUM(s.QUANTITY_OPEN) as OO_U
	,SUM(s.QUANTITY_ORDERED) as ORDERED_U
	,SUM(rr.eom_quantity_ordered) as ORDERED_U_RR
	,SUM(rr.original_quantity_ordered) as ORIGINAL_QUANTITY_ORDERED
FROM T2DL_DAS_OPEN_TO_BUY.OUTBOUND_PO_SKU_LKP s	
LEFT JOIN internal_orders_base inx
	ON s.purchase_order_num = inx.order_no
LEFT JOIN parent_orders_base par
	ON s.purchase_order_num = par.parent_po
LEFT JOIN rr_quantity rr
	ON s.sku_num = rr.rms_sku_num 
	and s.purchase_order_num = rr.purchase_order_number
GROUP BY 1,2,3,4,5,6,7,8,9
) WITH DATA PRIMARY INDEX(purchase_order_num, sku_num) ON COMMIT PRESERVE ROWS; 

COLLECT STATS 
    PRIMARY INDEX (purchase_order_num, sku_num)
	,COLUMN (purchase_order_num)
	,COLUMN (sku_num)
		ON po_sku_table;

/***********************************************************/
/******************** Parent PO Table **********************/
/***********************************************************/
--Grabs just the parent PO's in worksheet and approved status

--DROP table parent_po_test
CREATE MULTISET VOLATILE TABLE parent_po AS (
	SELECT * FROM po_sku_table
	WHERE parent_po_ind = 'Y' and (eom_status = 'WORKSHEET' OR eom_status = 'APPROVED')
) WITH DATA PRIMARY INDEX(sku_num, purchase_order_num) ON COMMIT PRESERVE ROWS; 

COLLECT STATS 
	PRIMARY INDEX (sku_num, purchase_order_num)
	,COLUMN (sku_num)
	,COLUMN (purchase_order_num)
		ON parent_po;
/***********************************************************/
/********************* XREF PO Table ***********************/
/***********************************************************/
--Grabs just the XREF PO's 

--DROP TABLE xref_po_test
CREATE MULTISET VOLATILE TABLE xref_po AS (
	SELECT * FROM po_sku_table
	WHERE xref_po_ind = 'Y' OR internal_po_ind = 'T'
) WITH DATA PRIMARY INDEX(sku_num, purchase_order_num) ON COMMIT PRESERVE ROWS; 

COLLECT STATS 
	PRIMARY INDEX (sku_num, purchase_order_num)
    ,COLUMN (sku_num)
	,COLUMN (purchase_order_num)
		ON xref_po;
/***********************************************************/
/****************** Double Booking Table *******************/
/***********************************************************/
--Joins parent and XREF po tables to get the double booking values 

--DROP table double_booking_cases;
CREATE MULTISET VOLATILE TABLE double_booking_cases AS (

SELECT 
x.purchase_order_num as xref_po_num
, x.parent_po as parent_po
, x.sku_num
, p.eom_status
, x.RCPT_C as XREF_RCPT_C
, P.RCPT_C AS PARENT_RCPT_C
, X.RCPT_U AS XREF_RCPT_U
, P.RCPT_U AS PARENT_RCPT_U
, X.OO_C AS XREF_OO_C
, P.OO_C AS PARENT_OO_C
, X.OO_U AS XREF_OO_U
, P.OO_U AS PARENT_OO_U
, COALESCE(x.ORDERED_U_RR - x.RCPT_U,0)  as net_store_receipts
, COALESCE(CASE WHEN net_store_receipts < 0 THEN net_store_receipts * -1 ELSE 0 END,0) as store_over_u
, X.RCPT_U - store_over_u as RCPT_U_LESS_OVER_U
, CASE WHEN p.OO_U > 0 AND RCPT_U_LESS_OVER_U > 0 THEN RCPT_U_LESS_OVER_U ELSE 0 END AS parent_po_open_w_xref_receipts_u
, CASE WHEN p.OO_U > 0 AND RCPT_U_LESS_OVER_U > 0 THEN RCPT_U_LESS_OVER_U*x.eom_unit_cost ELSE 0 END AS parent_po_open_w_xref_receipts_c
, CASE WHEN p.OO_U > 0 AND RCPT_U_LESS_OVER_U > 0 THEN RCPT_U_LESS_OVER_U*x.ownership_price_amt ELSE 0 END AS parent_po_open_w_xref_receipts_r
, CASE WHEN p.OO_U > 0 AND x.OO_U > 0 AND x.eom_status IN ('APPROVED', 'WORKSHEET') THEN x.OO_U ELSE 0 END AS parent_po_open_w_xref_open_u
, CASE WHEN p.OO_U > 0 AND x.OO_U > 0 AND x.eom_status IN ('APPROVED', 'WORKSHEET') THEN x.OO_C ELSE 0 END AS parent_po_open_w_xref_open_c
, CASE WHEN p.OO_U > 0 AND x.OO_U > 0 AND x.eom_status IN ('APPROVED', 'WORKSHEET') THEN x.OO_U*x.ownership_price_amt ELSE 0 END AS parent_po_open_w_xref_open_r
FROM 
xref_po x
JOIN parent_po p 
ON x.parent_po = p.purchase_order_num
and x.sku_num = p.sku_num
) WITH DATA PRIMARY INDEX (xref_po_num, parent_po) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (xref_po_num, parent_po)
	,COLUMN (xref_po_num)
	,COLUMN (parent_po)
		ON double_booking_cases;
/***********************************************************/
/********************** RR Rollups *************************/
/***********************************************************/
--Rolls the RR table up to month and VPN

--drop table rr_rollup;
CREATE MULTISET VOLATILE TABLE rr_rollup AS (

SELECT
	eom_status
	, internal_po_ind
	, purchase_order_number
	, hier.division
	, hier.subdivision
	, hier.department
	, hier."class"
	, hier.subclass
	, hier.vpn
	, hier.style_desc
	, hier.vpn_label
	, hier.nrf_color_code
	, hier.color_desc
	, hier.supplier_color
	, hier.prmy_supp_num
	, hier.gwp_ind
	, po_vasn_signal
	, latest_edi_date
	,SUM(eom_quantity_ordered) AS eom_quantity_ordered
	,SUM(original_quantity_ordered) as original_quantity_ordered
	,SUM(eom_receipt_qty) as eom_receipt_qty
	,MAX(ownership_price_amt) as ownership_price_amt
	,MAX(eom_unit_cost) as eom_unit_cost 
FROM rr_quantity s
LEFT JOIN hier
	ON s.rms_sku_num = hier.rms_sku_num
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX (purchase_order_number, vpn) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (purchase_order_number, vpn)
	,COLUMN (purchase_order_number)
	,COLUMN (vpn)
		ON rr_rollup;
/***********************************************************/
/********************* Staging Table ***********************/
/***********************************************************/
--Joins hierarchy to the double booking values to get ready to join to main table

--drop table staging_1
CREATE MULTISET VOLATILE TABLE staging_1 AS (
SELECT 		
xref_po_num
, parent_po
, eom_status
, hier.division
, hier.subdivision
, hier.department
, hier."class"
, hier.subclass
, hier.vpn
, hier.style_desc
, hier.vpn_label
, hier.nrf_color_code
, hier.color_desc
, hier.supplier_color
, hier.prmy_supp_num
, SUM(XREF_RCPT_C) AS XREF_RCPT_C
, SUM(PARENT_RCPT_C) AS PARENT_RCPT_C
, SUM(XREF_RCPT_U) AS XREF_RCPT_U
, SUM(PARENT_RCPT_U) AS PARENT_RCPT_U
, SUM(XREF_OO_C) AS XREF_OO_C
, SUM(PARENT_OO_C) AS PARENT_OO_C
, SUM(XREF_OO_U) AS XREF_OO_U
, SUM(PARENT_OO_U) AS PARENT_OO_U
, SUM(parent_po_open_w_xref_receipts_u) AS parent_po_open_w_xref_receipts_u
, SUM(parent_po_open_w_xref_receipts_c) AS parent_po_open_w_xref_receipts_c
, SUM(parent_po_open_w_xref_receipts_r) AS parent_po_open_w_xref_receipts_r
, SUM(parent_po_open_w_xref_open_u) AS parent_po_open_w_xref_open_u
, SUM(parent_po_open_w_xref_open_c) AS parent_po_open_w_xref_open_c
, SUM(parent_po_open_w_xref_open_r) AS parent_po_open_w_xref_open_r


FROM double_booking_cases d
LEFT JOIN hier
	ON d.sku_num = hier.rms_sku_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA PRIMARY INDEX (xref_po_num, parent_po, vpn) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (xref_po_num, parent_po, vpn)
	,COLUMN (xref_po_num)
	,COLUMN (parent_po)
    ,COLUMN (vpn)
		ON staging_1;
/***********************************************************/
/*********************** Full Data *************************/
/***********************************************************/
--Joins double booked values to the main table to get the full data set



DELETE FROM {environment_schema}.xref_po_daily{env_suffix};
INSERT INTO {environment_schema}.xref_po_daily{env_suffix}
SELECT
	CURRENT_TIMESTAMP AS refresh_timestamp
	 , o.purchase_order_num
	 , o.channel_num 
	 , o.channel_brand 
	 , o.division
	 , o.subdivision 
	 , o.department 
	 , o."class"
	 , o.subclass
	 , o.supplier_number
	 , o.supplier_name
	 , o.vpn 
	 , o.style_desc 
	 , o.color_num 
	 , o.supp_color 
	 , o.customer_choice 
	 , o.po_status
	 , CASE WHEN o.edi_ind = 't' THEN 'Y' ELSE 'N' END AS edi_ind
	 , o.otb_eow_date 
	 , o.otb_month 
	 , o.otb_month_idnt 
	 , CASE WHEN COALESCE(par.parent_po, inx.parent_po) IS NOT NULL then otb_month_idnt END as parent_otb_month_idnt
	 , o.start_ship_date
	 , o.end_ship_date
	 , o.latest_approval_date
	 , o.latest_close_event_tmstp_pacific
	 , o.comments
	 , o.order_type 
	 , o.internal_po_ind 
	 , CASE WHEN inx.order_no IS NOT NULL THEN 'Y' ELSE 'N' END AS xref_po_ind
	 , CASE WHEN par.parent_po IS NOT NULL THEN 'Y' ELSE 'N' END AS parent_po_ind
	 , COALESCE(par.parent_po, inx.parent_po) AS parent_po
	 , CASE WHEN o.npg_ind = 'T' THEN 'Y' ELSE 'N' END as npg_ind 
	 , r.gwp_ind
	 , o.po_type 
	 , o.purchase_type 
	 , o.month_idnt 
	 , o."Month" 
	 , o.RCPT_COST 
	 , o.RCPT_RETAIL 
	 , o.RCPT_UNITS 
	 , o.QUANTITY_ORDERED 
	 , o.QUANTITY_RECEIVED 
	 , o.QUANTITY_CANCELED 
	 , o.QUANTITY_OPEN 
	 , o.UNIT_COST_AMT 
	 , o.TOTAL_ANTICIPATED_RETAIL_AMT 
	 , r.eom_receipt_qty AS DC_RCPT_UNITS
	 , r.eom_receipt_qty*eom_unit_cost AS DC_RCPT_COST
	 , o.vasn_sku_qty 
	 , o.vasn_cost 
	 , CASE WHEN o.vasn_sku_qty > 0 THEN 'Y' ELSE 'N' END as vasn_ind
	 , XREF_RCPT_C
	 , PARENT_RCPT_C
	 , XREF_RCPT_U
	 , PARENT_RCPT_U
	 , XREF_OO_C
	 , PARENT_OO_C
	 , XREF_OO_U
	 , PARENT_OO_U		
	 , COALESCE(eom_quantity_ordered - rcpt_units,0)  as net_store_receipts
	 , COALESCE(CASE WHEN net_store_receipts < 0 THEN net_store_receipts * -1 ELSE 0 END,0) as store_over_u
	 , store_over_u*eom_unit_cost as store_over_c
	 , parent_po_open_w_xref_receipts_u
	 , parent_po_open_w_xref_receipts_c
	 , parent_po_open_w_xref_receipts_r
	 , parent_po_open_w_xref_open_u
	 , parent_po_open_w_xref_open_c
	 , parent_po_open_w_xref_open_r
	 , o.quantity_canceled as canceled_u
	 , o.quantity_canceled*unit_cost_amt as canceled_c
	 , o.quantity_canceled*ownership_price_amt as canceled_r
	 , o.quantity_open as open_u
	 , o.quantity_open*unit_cost_amt AS open_c
	 , o.quantity_open*ownership_price_amt AS open_r
	 , eom_quantity_ordered as eom_ordered_u
	 , eom_quantity_ordered*eom_unit_cost AS eom_ordered_c
	 , eom_quantity_ordered*ownership_price_amt as eom_ordered_r
	 , original_quantity_ordered as og_ordered_u
	 , original_quantity_ordered*eom_unit_cost AS og_ordered_c
	 , original_quantity_ordered*ownership_price_amt as og_ordered_r
	 
  FROM main o
  LEFT JOIN internal_orders_base inx
	 ON o.purchase_order_num = inx.order_no
  LEFT JOIN parent_orders_base par
	 ON o.purchase_order_num = par.parent_po
  LEFT JOIN staging_1 s
	 ON s.xref_po_num = o.purchase_order_num
	 AND s.vpn = o.vpn 
	 AND s.nrf_color_code = o.color_num 
	 AND s.supplier_color = o.supp_color 
	 AND s.style_desc = o.style_desc
	 AND s.prmy_supp_num = o.supplier_number
	 AND s.division = o.division
	 AND s."class" = o."class"
	 AND s.subclass = o.subclass
	 AND o.month_join = 1
  LEFT JOIN rr_rollup r
  	 ON r.purchase_order_number = o.purchase_order_num
  	 AND r.vpn = o.vpn 
	 AND r.nrf_color_code = o.color_num 
	 AND r.supplier_color = o.supp_color 
	 AND r.style_desc = o.style_desc
	 AND r.prmy_supp_num = o.supplier_number
	 AND r.division = o.division
	 AND r."class" = o."class"
	 AND r.subclass = o.subclass
	 AND o.month_join = 1;