locking row for access
select	
wk_idnt,
wk_idnt wk_idnt1,
supp_idnt,
dept_idnt,
loc_idnt,
cast((sum(coalesce(f_ty_total_sls_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ty_total_sls_amt,
cast((sum(coalesce(f_ty_total_sls_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_total_sls_qty,
cast((sum(coalesce(f_ty_total_rcpt_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ty_total_rcpt_amt,
cast((sum(coalesce(f_total_rcpt_qty,0))(format '-(30)9')) as varchar(100)) as f_total_rcpt_qty,
cast((sum(coalesce(f_ty_total_soh_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ty_total_soh_amt,
cast((sum(coalesce(f_ty_total_soh_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_total_soh_qty,
cast((sum(coalesce(f_ty_soh_rglr_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_soh_rglr_qty,
cast((sum(coalesce(f_ty_soh_prmtn_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_soh_prmtn_qty,
cast((sum(coalesce(f_total_oo_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_oo_amt,
cast((sum(coalesce(f_total_oo_qty,0))(format '-(30)9')) as varchar(100)) as f_total_oo_qty,
cast((sum(coalesce(total_sales_cost,0))(format '-(30)9.99')) as varchar(100)) as total_sales_cost,
cast((sum(coalesce(total_receipt_cost,0))(format '-(30)9.99')) as varchar(100)) as total_receipt_cost,
cast((sum(coalesce(total_soh_cost,0))(format '-(30)9.99')) as varchar(100)) as total_soh_cost,
cast((sum(coalesce(total_oo_cost,0))(format '-(30)9.99')) as varchar(100)) as total_oo_cost
from	
(
sel 	sb.week_num as wk_idnt,
        supp_num as supp_idnt,
        sb.dept_num as dept_idnt,
        sb.store_num as loc_idnt,
        (jwn_operational_gmv_total_retail_amt_ty) as f_ty_total_sls_amt,
        (jwn_operational_gmv_total_units_ty) as f_ty_total_sls_qty,
        (receipts_total_retail_amt_ty) as f_ty_total_rcpt_amt,
        (receipts_total_units_ty) as f_total_rcpt_qty,
        (inventory_eoh_total_retail_amt_ty) as f_ty_total_soh_amt,
        (inventory_eoh_total_units_ty) as f_ty_total_soh_qty,
        (inventory_eoh_regular_units_ty) as f_ty_soh_rglr_qty,
        cast(0 as decimal(12,0)) as f_ty_soh_prmtn_qty,
		cast(0 as decimal(38,4)) as f_total_oo_amt,
        cast(0 as integer) as f_total_oo_qty,
        (jwn_operational_gmv_total_cost_amt_ty) as total_sales_cost,
        (receipts_total_cost_amt_ty) as total_receipt_cost	,
        (inventory_eoh_total_cost_amt_ty) as total_soh_cost,
       cast(0 as decimal(38,4)) as total_oo_cost
from {db_env}_nap_base_vws.merch_transaction_sbclass_store_week_agg_fact sb 
where sb.week_num =  ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date ) 
	union all
sel 	 ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
		on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date )  as wk_idnt,
        prmy_supp_num as supp_idnt,
        dept_num as dept_idnt,
        oo.store_num as loc_idnt,
        cast(0 as decimal(38,4))  as f_ty_total_sls_amt,
        cast(0 as decimal(12,0)) as f_ty_total_sls_qty,
        cast(0 as decimal(38,4))  as f_ty_total_rcpt_amt,
        cast(0 as decimal(12,0)) as f_total_rcpt_qty,
        cast(0 as decimal(38,4))  as f_ty_total_soh_amt,
        cast(0 as decimal(12,0)) as f_ty_total_soh_qty,
        cast(0 as decimal(12,0)) as f_ty_soh_rglr_qty,
        cast(0 as decimal(12,0)) as f_ty_soh_prmtn_qty,
        cast(sum(total_anticipated_retail_amt) as decimal(38,4))  as f_total_oo_amt,
        sum(quantity_open)  as f_total_oo_qty,
        cast(0 as decimal(38,4))  as total_sales_cost,
        cast(0 as decimal(38,4))  as total_receipt_cost	,
        cast(0 as decimal(38,4))  as total_soh_cost,
        cast(sum(total_estimated_landing_cost) as decimal(38,4)) as total_oo_cost		
from {db_env}_nap_base_vws.merch_on_order_fact_vw oo
		 join {db_env}_nap_base_vws.store_dim orgstore
			on oo.store_num = orgstore.store_num 
		 join  {db_env}_nap_base_vws.merch_product_sku_dim_as_is_vw psd 
			on psd.rms_sku_num = oo.rms_sku_num
			and orgstore.store_country_code = psd.channel_country
		where ((status = 'closed' and end_ship_date >= current_date - 45) or status in ('approved','worksheet') )
and quantity_open > 0 and first_approval_date is not null
	group by 1,2,3,4

) qry
group by 
wk_idnt,
wk_idnt1,
supp_idnt,
dept_idnt,
loc_idnt
having	sum(f_ty_total_sls_amt) <>0
	or sum(f_ty_total_sls_qty) <>0
	or sum(f_ty_total_rcpt_amt) <>0
	or sum(f_total_rcpt_qty) <>0
	or sum(f_ty_total_soh_amt) <>0
	or sum(f_ty_total_soh_qty) <>0
	or sum(f_ty_soh_rglr_qty) <>0
	or sum(f_ty_soh_prmtn_qty) <>0
	or sum(f_total_oo_amt) <>0
	or sum(total_sales_cost) <>0
	or sum(total_receipt_cost) <>0
	or sum(total_soh_cost) <>0
	or sum(total_oo_cost) <>0
	;


