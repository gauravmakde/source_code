locking row for access
select
    loc_idnt as loc_idnt,
    sku_idnt as sku_idnt,
    wk_idnt as wk_idnt,
	wk_idnt1 as wk_idnt1,
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
		week_num as wk_idnt1,
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
    where week_num =  ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
		on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date )
    union all
	
    select
        oo.store_num as loc_idnt,
        oo.rms_sku_num as sku_idnt,
         ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
		on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date )  as wk_idnt,
		 ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
		on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date )  as wk_idnt1,
        cast(0 as decimal(12,0)) as f_total_sls_qty,
        cast(0 as decimal(12,0)) as f_i_total_soh_qty,
        cast(0 as decimal(12,0)) as f_total_rcpt_qty,
        quantity_open as f_total_oo_qty,
		cast(0 as decimal(38,4)) as f_total_sales_retail_amt,
		cast(0 as decimal(38,4)) as f_total_sales_cost_amt,
		cast(0 as decimal(38,4)) as f_total_rcpt_retail_amt,
		cast(0 as decimal(38,4))  as f_total_rcpt_cost_amt,
		cast(0 as decimal(38,4))  as f_i_total_soh_retail_amt,
		cast(0 as decimal(38,4))  as f_i_total_soh_cost_amt
    from {db_env}_nap_base_vws.merch_on_order_fact_vw oo
	where ((status = 'closed' and end_ship_date >= current_date - 45) or status in ('approved','worksheet') )
and quantity_open > 0 and first_approval_date is not null
    union all
    select
        store_num as loc_idnt,
        rms_sku_num as sku_idnt,
        week_num as wk_idnt,
		week_num as wk_idnt1,
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
    where a.week_num = ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
		on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date )
union all  
    select
        store_num as loc_idnt,
        rms_sku_num as sku_idnt,
        week_num as wk_idnt,
		week_num as wk_idnt1,
        cast(0 as decimal(12,0)) as f_total_sls_qty,
        inventory_eoh_total_units_ty as f_i_total_soh_qty,
        cast(0 as decimal(12,0)) as f_total_rcpt_qty,
        cast(0 as decimal(12,0)) as f_total_oo_qty,
		cast(0 as decimal(38,4)) as f_total_sales_retail_amt,
		cast(0 as decimal(38,4)) as f_total_sales_cost_amt,
		cast(0 as decimal(38,4)) as f_total_rcpt_retail_amt,
		cast(0 as decimal(38,4))  as f_total_rcpt_cost_amt,
		inventory_eoh_total_retail_amt_ty as f_i_total_soh_retail_amt,
		inventory_eoh_total_cost_amt_ty as f_i_total_soh_cost_amt
    from
        {db_env}_nap_base_vws.merch_inventory_sku_store_week_agg_fact a
	where a.week_num = ( select week_idnt as curr_week_idnt from {db_env}_nap_base_vws.day_cal_454_dim b join {db_env}_nap_base_vws.etl_batch_dt_lkup c
		on c.interface_code='mric_day_agg_wkly' and c.dw_batch_dt=b.day_date )    
    ) qry
    group by     
	loc_idnt,
        sku_idnt,
        wk_idnt,
		wk_idnt1
having (sum(f_total_sls_qty)<>0
or sum(f_i_total_soh_qty)<>0
or sum(f_total_rcpt_qty)<>0
or sum(f_total_oo_qty)<>0
or sum(f_total_sales_retail_amt)<>0
or sum(f_total_sales_cost_amt)<>0
or sum(f_total_rcpt_retail_amt)<>0
or sum(f_total_rcpt_cost_amt)<>0
or sum(f_i_total_soh_retail_amt)<>0
or sum(f_i_total_soh_cost_amt)<>0
)
;
