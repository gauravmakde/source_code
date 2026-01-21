locking row for access
select	
supp_idnt,
dept_idnt,
class_idnt,
coalesce(class_label,'') class_label,
vpn,
loc_idnt,
start_week_idnt wk_idnt,
start_week_idnt wk_idnt1,
style_idnt,
cast((sum(coalesce(f_total_sls_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_sls_amt,
cast((sum(coalesce(f_total_sls_qty,0))(format '-(30)9')) as varchar(100)) as f_total_sls_qty,
cast((sum(coalesce(f_i_total_soh_amt,0))(format '-(30)9.99')) as varchar(100)) as f_i_total_soh_amt,
cast((sum(coalesce(f_i_total_soh_qty,0))(format '-(30)9')) as varchar(100)) as f_i_total_soh_qty,
cast((sum(coalesce(f_i_soh_rglr_qty,0))(format '-(30)9')) as varchar(100)) as f_i_soh_rglr_qty,
cast((sum(coalesce(f_i_soh_prmtn_qty,0))(format '-(30)9')) as varchar(100)) as f_i_soh_prmtn_qty,
cast((sum(coalesce(f_total_rcpt_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_rcpt_amt,
cast((sum(coalesce(f_total_rcpt_qty,0))(format '-(30)9')) as varchar(100)) as f_total_rcpt_qty,
cast((sum(coalesce(f_total_oo_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_oo_amt,
cast((sum(coalesce(f_total_oo_qty,0))(format '-(30)9')) as varchar(100)) as f_total_oo_qty,
coalesce(last_receipt_dt,'') as last_receipt_dt,
cast((sum(coalesce(total_sales_cost,0))(format '-(30)9.99')) as varchar(100)) as total_sales_cost,
cast((sum(coalesce(total_receipt_cost,0))(format '-(30)9.99')) as varchar(100)) as total_receipt_cost,
cast((sum(coalesce(total_soh_cost,0))(format '-(30)9.99')) as varchar(100)) as total_soh_cost,
cast((sum(coalesce(total_oo_cost,0))(format '-(30)9.99')) as varchar(100)) as total_oo_cost
from 
(
select   supp_num as supp_idnt,
        sty.dept_num as dept_idnt,
		sty.class_num  as class_idnt ,
		trim(pstyd.class_num||', '||pstyd.class_desc) as class_label,
		sty.supp_part_num as vpn,
        sty.store_num as loc_idnt,
		sty.week_num as start_week_idnt,
		sty.rms_style_num as style_idnt,	
		(jwn_operational_gmv_total_retail_amt_ty) as f_total_sls_amt,
        (jwn_operational_gmv_total_units_ty) as f_total_sls_qty,
		0 as f_i_total_soh_amt,
        0 as f_i_total_soh_qty,
		0 as f_i_soh_rglr_qty,
		0 as f_i_soh_prmtn_qty,
		(receipts_total_retail_amt_ty) as f_total_rcpt_amt,
        (receipts_total_units_ty) as f_total_rcpt_qty,
        0 as f_total_oo_amt,
        0 as f_total_oo_qty,
		null as last_receipt_dt,
		(jwn_operational_gmv_total_cost_amt_ty) as total_sales_cost,
        (receipts_total_cost_amt_ty) as total_receipt_cost,
        0 as total_soh_cost,
        0 as total_oo_cost
from
{db_env}_nap_base_vws.merch_transaction_style_store_week_agg_fact sty
join {db_env}_nap_base_vws.store_dim s01 
	on sty.store_num = s01.store_num  
join {db_env}_nap_base_vws.product_style_rms_dim_vw pstyd  
	on sty.rms_style_num = pstyd.rms_style_num 
	and s01.store_country_code   =pstyd.channel_country 
where sty.week_num between 
( select end_week_idnt from    (
select c.week_num as end_week_idnt , rank() over (order by c.week_num desc) 	wk_rank
from {db_env}_nap_base_vws.day_cal_454_dim a join {db_env}_nap_base_vws.etl_batch_dt_lkup b
on b.interface_code='mric_day_agg_wkly' and b.dw_batch_dt=a.day_date
join {db_env}_nap_base_vws.week_cal_vw c on c.week_num<= a.week_idnt 
) end_wk where wk_rank = 26 )
and 
( select start_week_idnt from    (
select c.week_num as start_week_idnt , rank() over (order by c.week_num desc) 	wk_rank
from {db_env}_nap_base_vws.day_cal_454_dim a join {db_env}_nap_base_vws.etl_batch_dt_lkup b
on b.interface_code='mric_day_agg_wkly' and b.dw_batch_dt=a.day_date
join {db_env}_nap_base_vws.week_cal_vw c on c.week_num<= a.week_idnt 
) end_wk where wk_rank = 2 )
) qry
group by 
1,2,3,4,5,6,7,8,9
having	sum(f_total_sls_amt) <>0
	or sum(f_total_sls_qty) <>0
	or sum(f_total_rcpt_amt) <>0
	or sum(f_total_rcpt_qty) <>0
	or sum(total_sales_cost) <>0
	or sum(total_receipt_cost) <>0
;

