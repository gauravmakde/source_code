locking row for access
select
    loc_idnt as loc_idnt,
    sku_idnt as sku_idnt,
    wk_idnt as wk_idnt,
	wk_idnt as wk_idnt1,
cast((sum(coalesce(f_total_sls_qty,0))(format '-(30)9')) as varchar(100)) as f_total_sls_qty,
cast((sum(coalesce(f_i_total_soh_qty,0))(format '-(30)9')) as varchar(100)) as f_i_total_soh_qty,
cast((sum(coalesce(f_total_rcpt_qty,0))(format '-(30)9')) as varchar(100)) as f_total_rcpt_qty,
cast((sum(coalesce(f_total_oo_qty,0))(format '-(30)9')) as varchar(100)) as f_total_oo_qty,
cast((sum(coalesce(f_total_sales_retail_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_sales_retail_amt,
cast((sum(coalesce(f_total_sales_cost_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_sales_cost_amt,
cast((sum(coalesce(f_total_rcpt_retail_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_rcpt_retail_amt,
cast((sum(coalesce(f_total_rcpt_cost_amt,0))(format '-(30)9.99')) as varchar(100)) as f_total_rcpt_cost_amt,
cast((sum(coalesce(f_i_total_soh_retail_amt,0))(format '-(30)9.99')) as varchar(100)) as f_i_total_soh_retail_amt,
cast((sum(coalesce(f_i_total_soh_cost_amt,0))(format '-(30)9.99')) as varchar(100)) as f_i_total_soh_cost_amt

from
    (
    select
        store_num as loc_idnt,
        rms_sku_num as sku_idnt,
        week_num as wk_idnt,
		wk_idnt as wk_idnt1,
         jwn_operational_gmv_total_units_ty  as f_total_sls_qty,
        cast(0 as decimal(12,0)) as f_i_total_soh_qty,
        cast(0 as decimal(12,0)) as f_total_rcpt_qty,
        cast(0 as decimal(12,0)) as f_total_oo_qty,
		jwn_operational_gmv_total_retail_amt_ty as f_total_sales_retail_amt,
		jwn_operational_gmv_total_cost_amt_ty as  f_total_sales_cost_amt,
		cast(0 as decimal(38,4)) as f_total_rcpt_retail_amt,
		cast(0 as decimal(38,4))  as f_total_rcpt_cost_amt,
		cast(0 as decimal(38,4))  as f_i_total_soh_retail_amt,
		cast(0 as decimal(38,4))  as f_i_total_soh_cost_amt
    from {db_env}_nap_base_vws.merch_jwn_sale_return_sku_store_week_agg_fact  a 
where a.week_num between 
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
    union all
    
    select
        store_num as loc_idnt,
        rms_sku_num as sku_idnt,
        week_num as wk_idnt,
		wk_idnt as wk_idnt1,
        cast(0 as decimal(12,0)) as f_total_sls_qty,
        cast(0 as decimal(12,0)) as f_i_total_soh_qty,
        receipts_total_units_ty as f_total_rcpt_qty,
        cast(0 as decimal(12,0)) as f_total_oo_qty,
		cast(0 as decimal(38,4)) as f_total_sales_retail_amt,
		cast(0 as decimal(38,4)) as f_total_sales_cost_amt,
		receipts_total_retail_amt_ty as f_total_rcpt_retail_amt,
		receipts_total_cost_amt_ty  as f_total_rcpt_cost_amt,
		cast(0 as decimal(38,4))  as f_i_total_soh_retail_amt,
		cast(0 as decimal(38,4))  as f_i_total_soh_cost_amt
    from
        {db_env}_nap_base_vws.merch_poreceipt_sku_store_week_agg_fact a  
where a.week_num between 
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
	loc_idnt,
        sku_idnt,
        wk_idnt,wk_idnt1
having  (sum(f_total_sls_qty)<>0
or sum(f_i_total_soh_qty)<>0
or sum(f_total_rcpt_qty)<>0 
or sum(f_total_sales_retail_amt)<>0
or sum(f_total_sales_cost_amt)<>0
or sum(f_total_rcpt_retail_amt)<>0
or sum(f_total_rcpt_cost_amt)<>0
or sum(f_i_total_soh_retail_amt)<>0
or sum(f_i_total_soh_cost_amt)<>0

);
