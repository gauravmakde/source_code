CREATE MULTISET VOLATILE TABLE po_dist AS (
SELECT 
     dist.purchase_order_num AS purchase_order_number
    ,dist.rms_sku_num
    ,st.channel_brand AS banner
FROM prd_nap_usr_vws.purchase_order_item_distributelocation_fact dist
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON dist.distribute_location_id = st.store_num
JOIN (SELECT DISTINCT purchase_order_number FROM T2DL_DAS_OPEN_TO_BUY.po_sku_cancels) c
  ON c.purchase_order_number = dist.purchase_order_num
GROUP BY 1,2,3
QUALIFY ROW_NUMBER() OVER (PARTITION BY dist.purchase_order_num, dist.rms_sku_num ORDER BY count(*) DESC) = 1		-- take channel with most records in case of dups
) WITH DATA
PRIMARY INDEX(purchase_order_number, rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number, rms_sku_num)
    ON po_dist
;

--drop table po_sku_base;

CREATE MULTISET VOLATILE TABLE po_sku_base AS (
Select 
		a.purchase_order_number 
		,e.banner
		,a.rms_sku_num
		,d.div_idnt
    	,d.div_desc
    	,d.grp_idnt
    	,d.grp_desc
    	,d.dept_idnt
    	,d.dept_desc
    	,TRIM(d.div_idnt) || ': ' || d.div_desc AS Division
    	,TRIM(d.grp_idnt) || ': ' || d.grp_desc AS Subdivision
    	,TRIM(d.dept_idnt) || ': ' || d.dept_desc AS Department
    	,d.vendor_name
		,a.status
		,a.npg_ind
		,a.po_type
		,a.purchase_type
		,CASE WHEN a.order_type IN ('AUTOMATIC_REORDER', 'BUYER_REORDER') THEN 'RP' ELSE 'NRP' END AS rp_ind
		--,a.otb_eow_date
		,f.month_idnt as otb_month_idnt
		,f.month_label as otb_month_label
		,c.comments 
		,c.internal_po_ind 
from prd_nap_usr_vws.PURCHASE_ORDER_ITEM_FACT a
Join(
		select distinct 
			purchase_order_number 
		from T2DL_DAS_OPEN_TO_BUY.po_sku_cancels
		)b
 on a.purchase_order_number = b.purchase_order_number
join PRD_NAP_USR_VWS.PURCHASE_ORDER_HEADER_FACT c
 on a.purchase_order_number = c.purchase_order_number
join t2dl_das_assortment_dim.assortment_hierarchy d
 on a.rms_sku_num = d.sku_idnt
 and d.channel_country = 'US'
join po_dist e
on a.purchase_order_number = e.purchase_order_number
and a.rms_sku_num = e.rms_sku_num
join PRD_NAP_USR_VWS.DAY_CAL_454_DIM f
on a.otb_eow_date = f.day_date
--where a.purchase_order_number = '39903764'
 )
WITH DATA
PRIMARY INDEX(purchase_order_number,rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number,rms_sku_num)
    ON po_sku_base
;

CREATE MULTISET VOLATILE TABLE po_cost AS (
select 
		a.purchase_order_number 
		,a.banner
		,a.rms_sku_num
		,a.div_idnt
    	,a.div_desc
    	,a.grp_idnt
    	,a.grp_desc
    	,a.dept_idnt
    	,a.dept_desc
    	,a.Division
    	,a.Subdivision
    	,a.Department
    	,a.vendor_name
		,a.status
		,a.npg_ind
		,a.po_type
		,a.purchase_type
		,a.rp_ind
		,a.otb_month_idnt
		,a.otb_month_label
		,a.comments 
		,a.internal_po_ind 
		,b.quantity_ordered as ttl_ordered_qty
		,SUM((b.unit_cost * b.quantity_ordered) + (b.total_expenses_per_unit * b.quantity_ordered)+(b.total_duty_per_unit * b.quantity_ordered)) as ttl_ordered_c
from po_sku_base a
join (
        SELECT
             po_fct.purchase_order_number
            ,po_fct.rms_sku_num
            ,AVG(po_fct.unit_cost) AS unit_cost
            ,AVG(po_fct.total_expenses_per_unit) AS total_expenses_per_unit
            ,AVG(po_fct.total_duty_per_unit) AS total_duty_per_unit
            ,SUM(COALESCE(po_fct.quantity_ordered, 0)) AS quantity_ordered
            FROM prd_nap_usr_vws.purchase_order_shiplocation_fact po_fct
            --where po_fct.purchase_order_number = 39903764
        GROUP BY 1,2
    ) b
on a.purchase_order_number = b.purchase_order_number 
and a.rms_sku_num = b.rms_sku_num 
--where a.purchase_order_number = 39903764
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)
WITH DATA
PRIMARY INDEX(purchase_order_number,rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (purchase_order_number,rms_sku_num)
    ON po_cost
;

DELETE FROM T2DL_DAS_OPEN_TO_BUY.PO_CANCELS_OTB_MONTH ALL;

INSERT INTO T2DL_DAS_OPEN_TO_BUY.PO_CANCELS_OTB_MONTH
select
		a.purchase_order_number 
		,a.banner
		--,a.rms_sku_num
		,a.div_idnt
    	,a.div_desc
    	,a.grp_idnt
    	,a.grp_desc
    	,a.dept_idnt
    	,a.dept_desc
    	,a.Division
    	,a.Subdivision
    	,a.Department
    	,a.vendor_name
		,a.status
		,a.npg_ind
		,a.po_type
		,a.purchase_type
		,a.rp_ind
		,a.otb_month_idnt
		,a.otb_month_label
		,a.comments 
		,a.internal_po_ind 
		,sum(a.ttl_ordered_qty) as ttl_ordered_qty
		,sum(a.ttl_ordered_c) as ttl_ordered_c
		,sum(b.buyer_cancel_u) as buyer_cancel_u
	    ,sum(b.buyer_cancel_c) as buyer_cancel_c
	    ,sum(b.supplier_cancel_u) as supplier_cancel_u
	    ,sum(b.supplier_cancel_c) as supplier_cancel_c
	    ,sum(b.other_cancel_u) as other_cancel_u
	    ,sum(b.other_cancel_c) as other_cancel_c
	    ,sum(b.invalid_cancel_u) as invalid_cancel_u
	    ,sum(b.invalid_cancel_c) as invalid_cancel_c
	    ,sum(b.true_cancel_u) as true_supplier_cancel_u
	    ,sum(b.true_cancel_c) as true_supplier_cancel_c
	    ,CURRENT_DATE as process_dt 
from po_cost a
left join( select purchase_order_number
		,rms_sku_num
		,sum(buyer_cancel_u) as buyer_cancel_u
	    ,sum(buyer_cancel_c) as buyer_cancel_c
	    ,sum(supplier_cancel_u) as supplier_cancel_u
	    ,sum(supplier_cancel_c) as supplier_cancel_c
	    ,sum(other_cancel_u) as other_cancel_u
	    ,sum(other_cancel_c) as other_cancel_c
	    ,sum(invalid_cancel_u) as invalid_cancel_u
	    ,sum(invalid_cancel_c) as invalid_cancel_c
	    ,sum(true_cancel_u) as true_cancel_u
	    ,sum(true_cancel_c) as true_cancel_c
		from T2DL_DAS_OPEN_TO_BUY.po_sku_cancels
		--where purchase_order_number = '39903764'
		group by 1,2
		) b
on a.purchase_order_number = b.purchase_order_number 
and a.rms_sku_num = b.rms_sku_num 
--where a.purchase_order_number = '39903764'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,34
;

DELETE FROM T2DL_DAS_OPEN_TO_BUY.PO_CANCELS_ACTION_MONTH ALL;

INSERT INTO T2DL_DAS_OPEN_TO_BUY.PO_CANCELS_ACTION_MONTH
select
	a.banner
	,a.purchase_order_number 
	,a.div_idnt
    ,a.div_desc
    ,a.grp_idnt
    ,a.grp_desc
    ,a.dept_idnt
    ,a.dept_desc
    ,a.Division
    ,a.Subdivision
    ,a.Department
    ,a.vendor_name
    ,a.npg_ind
    ,a.rp_ind
    ,a.po_type
    ,a.purchase_type
    ,a.comments
    ,a.internal_po_ind
	,a.otb_month_idnt
    ,a.otb_month_label
    ,a.month_idnt 
    ,a.action_month_label 
	,sum(supplier_cancel_u) as supplier_cancel_u
	,sum(supplier_cancel_c) as supplier_cancel_c
	,sum(buyer_cancel_u) as buyer_cancel_u
	,sum(buyer_cancel_c) as buyer_cancel_c
	,sum(other_cancel_u) as other_cancel_u
	,sum(other_cancel_c) as other_cancel_c
	,sum(invalid_cancel_u) as invalid_cancel_u
	,sum(invalid_cancel_c) as invalid_cancel_c
	,sum(a.true_cancel_u) as true_supplier_cancel_u
	,sum(a.true_cancel_c) as true_supplier_cancel_c
	,CURRENT_DATE as process_dt 
from T2DL_DAS_OPEN_TO_BUY.po_sku_cancels a 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
;