locking row for access
select	
wk_idnt,
wk_idnt wk_idnt1,
supp_idnt,
dept_idnt,
loc_idnt,
cast((sum(coalesce(f_ty_total_sls_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ty_total_sls_amt,
cast((sum(coalesce(f_ty_total_sls_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_total_sls_qty,
cast((sum(coalesce(f_ty_total_soh_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ty_total_soh_amt,
cast((sum(coalesce(f_ty_total_soh_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_total_soh_qty,
cast((sum(coalesce(f_ty_soh_rglr_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_soh_rglr_qty,
cast((sum(coalesce(f_ty_soh_prmtn_qty,0))(format '-(30)9')) as varchar(100)) as f_ty_soh_prmtn_qty,
cast((sum(coalesce(f_ty_total_rcpt_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ty_total_rcpt_amt,
cast((sum(coalesce(f_ly_total_sls_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ly_total_sls_amt,
cast((sum(coalesce(f_ly_total_sls_qty,0))(format '-(30)9')) as varchar(100)) as f_ly_total_sls_qty,
cast((sum(coalesce(f_ly_total_soh_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ly_total_soh_amt,
cast((sum(coalesce(f_ly_total_soh_qty,0))(format '-(30)9')) as varchar(100)) as f_ly_total_soh_qty,
cast((sum(coalesce(f_ly_soh_rglr_qty,0))(format '-(30)9')) as varchar(100)) as f_ly_soh_rglr_qty,
cast((sum(coalesce(f_ly_soh_prmtn_qty,0))(format '-(30)9.99')) as varchar(100)) as f_ly_soh_prmtn_qty,
cast((sum(coalesce(f_ly_total_rcpt_amt,0))(format '-(30)9.99')) as varchar(100)) as f_ly_total_rcpt_amt,
cast((sum(coalesce(total_sales_cost_ty,0))(format '-(30)9.99')) as varchar(100)) as total_sales_cost_ty,
cast((sum(coalesce(total_receipt_cost_ty,0))(format '-(30)9.99')) as varchar(100)) as total_receipt_cost_ty,
cast((sum(coalesce(total_soh_cost_ty,0))(format '-(30)9.99')) as varchar(100)) as total_soh_cost_ty,
cast((sum(coalesce(total_sales_cost_ly,0))(format '-(30)9.99')) as varchar(100)) as total_sales_cost_ly,
cast((sum(coalesce(total_receipt_cost_ly,0))(format '-(30)9.99')) as varchar(100)) as total_receipt_cost_ly,
cast((sum(coalesce(total_soh_cost_ly,0))(format '-(30)9.99')) as varchar(100)) as total_soh_cost_ly
from	
(
select sb.week_num as wk_idnt,
        supp_num as supp_idnt,
        sb.dept_num as dept_idnt,
        sb.store_num as loc_idnt,
        (jwn_operational_gmv_total_retail_amt_ty) as f_ty_total_sls_amt,
        (jwn_operational_gmv_total_units_ty) as f_ty_total_sls_qty,
        (inventory_eoh_total_retail_amt_ty) as f_ty_total_soh_amt,
        (inventory_eoh_total_units_ty) as f_ty_total_soh_qty,
        (inventory_eoh_regular_units_ty) as f_ty_soh_rglr_qty,
        cast(0 as decimal(12,0)) as f_ty_soh_prmtn_qty,
		(receipts_total_retail_amt_ty) as f_ty_total_rcpt_amt,
		(jwn_operational_gmv_total_retail_amt_ly) as f_ly_total_sls_amt,
		(jwn_operational_gmv_total_units_ly) as f_ly_total_sls_qty,
		(inventory_eoh_total_retail_amt_ly) as f_ly_total_soh_amt,
		(inventory_eoh_total_units_ly) as f_ly_total_soh_qty,
		(inventory_eoh_regular_units_ly) as f_ly_soh_rglr_qty,
		cast(0 as decimal(12,0)) as f_ly_soh_prmtn_qty,
		(receipts_total_retail_amt_ly) as f_ly_total_rcpt_amt,
		(jwn_operational_gmv_total_cost_amt_ty) as total_sales_cost_ty,
        (receipts_total_cost_amt_ty) as total_receipt_cost_ty,
        (inventory_eoh_total_cost_amt_ty) as total_soh_cost_ty,
		(jwn_operational_gmv_total_cost_amt_ly) as total_sales_cost_ly,
        (receipts_total_cost_amt_ly) as total_receipt_cost_ly,
        (inventory_eoh_total_cost_amt_ly) as total_soh_cost_ly
from {db_env}_nap_base_vws.merch_transaction_sbclass_store_week_agg_fact sb 
where sb.week_num =  ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date )

) qry
group by 
wk_idnt,
wk_idnt1,
supp_idnt,
dept_idnt,
loc_idnt
having	sum(f_ty_total_sls_amt) <>0
	or sum(f_ty_total_sls_qty) <>0
	or sum(f_ty_total_soh_amt) <>0
	or sum(f_ty_total_soh_qty) <>0
	or sum(f_ty_soh_rglr_qty) <>0
	or sum(f_ty_total_rcpt_amt) <>0
	or sum(f_ly_total_sls_amt) <>0
	or sum(f_ly_total_sls_qty) <>0
	or sum(f_ly_total_soh_amt) <>0
	or sum(f_ly_total_soh_qty) <>0
	or sum(f_ly_soh_rglr_qty) <>0
	or sum(f_ly_total_rcpt_amt) <>0
	or sum(total_sales_cost_ty) <>0
	or sum(total_receipt_cost_ty) <>0
	or sum(total_soh_cost_ty) <>0
		or sum(total_sales_cost_ly) <>0
	or sum(total_receipt_cost_ly) <>0
	or sum(total_soh_cost_ly) <>0
