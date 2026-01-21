create volatile multiset table week_dim as (
	with week_dim_base as (
		select distinct
			week_num
			, week_end_date
			, week_label
			, week_index
			, first_week_of_month_flag
			, last_week_of_month_flag

			, month_num
			, month_abrv
			, month_label
			, month_index

			, quarter_num
			, quarter_label

			, half_num
			, half_label

			, year_num
			, year_label
			, true_week_num
		from {environment_schema}.pilot_day_dim
	)

	select w.*
		, count(week_num) over (partition by month_num) as week_count
	from week_dim_base w
	where week_num in (select week_num from {environment_schema}.pilot_day_dim)
)
with data
primary index (week_num)
on commit preserve rows;


create volatile multiset table month_dim as (
	with month_dim_base as (
		select
			month_num
			, month_abrv
			, month_label
			, month_index
			, count(distinct week_num) as week_count

			, quarter_num
			, quarter_label

			, half_num
			, half_label

			, year_num
			, year_label
		from {environment_schema}.pilot_day_dim
		group by 1, 2, 3, 4, 6, 7, 8, 9, 10, 11
	)

	select *
	from month_dim_base
	where month_num in (select month_num from {environment_schema}.pilot_day_dim)
)
with data
primary index (month_num)
on commit preserve rows;

collect stats
	primary index (month_num)
	on month_dim;


create volatile multiset table sales as (
	select
		p.cc_num
		, s.store_num
		, s.channel_num
		, w.month_num
		, rp_ind

		, 1 as sales_flag
		, 0 as inv_flag
		, 0 as rcpt_flag
		, 0 as twist_flag
		, 0 as sff_flag
		, 0 as trans_flag

		, sum(m.net_sales_tot_retl) as sales_retail
		, sum(m.net_sales_tot_cost) as sales_cost
		, sum(m.net_sales_tot_units) as sales_units
        , sum(m.net_sales_tot_regular_retl) as sales_reg_retail
        , sum(m.net_sales_tot_regular_cost) as sales_reg_cost
		, sum(m.net_sales_tot_regular_units) as sales_reg_units
        , sum(m.net_sales_tot_promo_retl) as sales_pro_retail
        , sum(m.net_sales_tot_promo_cost) as sales_pro_cost
		, sum(m.net_sales_tot_promo_units) as sales_pro_units
        , sum(m.net_sales_tot_clearance_retl) as sales_clr_retail
        , sum(m.net_sales_tot_clearance_cost) as sales_clr_cost
		, sum(m.net_sales_tot_clearance_units) as sales_clr_units
		, sum(case when m.fulfill_type_code = 'VendorDropShip' then m.net_sales_tot_retl end) as sales_ds_retail
		, sum(case when m.fulfill_type_code = 'VendorDropShip' then m.net_sales_tot_cost end) as sales_ds_cost
		, sum(case when m.fulfill_type_code = 'VendorDropShip' then m.net_sales_tot_units end) as sales_ds_units
		, sum(m.gross_sales_tot_retl) as sales_gross_retail
		, sum(m.gross_sales_tot_cost) as sales_gross_cost
		, sum(m.gross_sales_tot_units) as sales_gross_units
		, sum(m.returns_tot_retl) as return_retail
		, sum(m.returns_tot_cost) as return_cost
		, sum(m.returns_tot_units) as return_units
		, sum(case when fulfill_type_code = 'VendorDropShip' then m.returns_tot_retl end) as return_ds_retail
		, sum(case when fulfill_type_code = 'VendorDropShip' then m.returns_tot_cost end) as return_ds_cost
		, sum(case when fulfill_type_code = 'VendorDropShip' then m.returns_tot_units end) as return_ds_units

		, cast(0 as decimal(20, 4)) as boh_retail
		, cast(0 as decimal(20, 4)) as boh_cost
		, cast(0 as decimal(16, 0)) as boh_units
		, cast(0 as decimal(20, 4)) as eoh_retail
		, cast(0 as decimal(20, 4)) as eoh_cost
		, cast(0 as decimal(16, 0)) as eoh_units
		, cast(0 as decimal(20, 4)) as avg_inv_retail
		, cast(0 as decimal(20, 4)) as avg_inv_cost
		, cast(0 as decimal(20, 4)) as avg_inv_units

		, cast(0 as decimal(20, 4)) as rcpt_po_retail
		, cast(0 as decimal(20, 4)) as rcpt_po_cost
		, cast(0 as decimal(16, 0)) as rcpt_po_units
        , cast(0 as decimal(20, 4)) as rcpt_ds_retail
		, cast(0 as decimal(20, 4)) as rcpt_ds_cost
		, cast(0 as decimal(16, 0)) as rcpt_ds_units

        , cast(0 as decimal(20, 4)) as tsfr_rs_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rs_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rs_in_units
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rack_in_units
        , cast(0 as decimal(20, 4)) as tsfr_pah_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_pah_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_pah_in_units

		, cast(0 as decimal(20, 4)) as twist_instock_traffic
		, cast(0 as decimal(20, 4)) as twist_total_traffic

		, cast(0 as decimal(16, 0)) as sff_units

		, cast(0 as decimal(16, 0)) as bopus_units
		, cast(0 as decimal(16, 0)) as dsr_units

	from prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw m
	join {environment_schema}.pilot_sku_dim p on 1=1
		and m.rms_sku_num = p.rms_sku_num
	join {environment_schema}.pilot_store_dim s on 1=1
		and m.store_num = s.store_num
	join week_dim w on 1=1
		and m.week_num = w.true_week_num
	group by 1, 2, 3, 4, 5
	having sales_gross_units > 0
		or return_units > 0
)
with data
primary index(cc_num)
on commit preserve rows;

collect stats
	primary index (cc_num)
	,column (
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_ind)
		on sales;


create volatile multiset table inv as (
	select
		p.cc_num
		, s.store_num
		, s.channel_num
		, w.month_num
		, rp_ind

		, 0 as sales_flag
		, 1 as inv_flag
		, 0 as rcpt_flag
		, 0 as twist_flag
		, 0 as sff_flag
		, 0 as trans_flag

		, cast(0 as decimal(20, 4)) as sales_retail
		, cast(0 as decimal(20, 4)) as sales_cost
		, cast(0 as decimal(16, 0)) as sales_units
		, cast(0 as decimal(20, 4)) as sales_reg_retail
		, cast(0 as decimal(20, 4)) as sales_reg_cost
		, cast(0 as decimal(16, 0)) as sales_reg_units
        , cast(0 as decimal(20, 4)) as sales_pro_retail
		, cast(0 as decimal(20, 4)) as sales_pro_cost
		, cast(0 as decimal(16, 0)) as sales_pro_units
        , cast(0 as decimal(20, 4)) as sales_clr_retail
		, cast(0 as decimal(20, 4)) as sales_clr_cost
		, cast(0 as decimal(16, 0)) as sales_clr_units
		, cast(0 as decimal(20, 4)) as sales_ds_retail
		, cast(0 as decimal(20, 4)) as sales_ds_cost
		, cast(0 as decimal(16, 0)) as sales_ds_units
		, cast(0 as decimal(20, 4)) as sales_gross_retail
		, cast(0 as decimal(20, 4)) as sales_gross_cost
		, cast(0 as decimal(16, 0)) as sales_gross_units
		, cast(0 as decimal(20, 4)) as return_retail
		, cast(0 as decimal(20, 4)) as return_cost
		, cast(0 as decimal(16, 0)) as return_units
		, cast(0 as decimal(20, 4)) as return_ds_retail
		, cast(0 as decimal(20, 4)) as return_ds_cost
		, cast(0 as decimal(16, 0)) as return_ds_units

		, sum(case when w.first_week_of_month_flag = 1 then i.inventory_boh_total_retail_amt_ty else 0 end) as boh_retail
		, sum(case when w.first_week_of_month_flag = 1 then i.inventory_boh_total_cost_amt_ty else 0 end) as boh_cost
		, sum(case when w.first_week_of_month_flag = 1 then i.inventory_boh_total_units_ty else 0 end) as boh_units
		, sum(case when w.last_week_of_month_flag = 1 then i.inventory_eoh_total_retail_amt_ty else 0 end) as eoh_retail
		, sum(case when w.last_week_of_month_flag = 1 then i.inventory_eoh_total_cost_amt_ty else 0 end) as eoh_cost
		, sum(case when w.last_week_of_month_flag = 1 then i.inventory_eoh_total_units_ty else 0 end) as eoh_units
		, sum(i.inventory_eoh_total_retail_amt_ty / w.week_count) as avg_inv_retail
		, sum(i.inventory_eoh_total_cost_amt_ty / w.week_count) as avg_inv_cost
		, sum(cast(i.inventory_eoh_total_units_ty as decimal(20, 4))/ w.week_count) as avg_inv_units

		, cast(0 as decimal(20, 4)) as rcpt_po_retail
		, cast(0 as decimal(20, 4)) as rcpt_po_cost
		, cast(0 as decimal(16, 0)) as rcpt_po_units
        , cast(0 as decimal(20, 4)) as rcpt_ds_retail
		, cast(0 as decimal(20, 4)) as rcpt_ds_cost
		, cast(0 as decimal(16, 0)) as rcpt_ds_units

        , cast(0 as decimal(20, 4)) as tsfr_rs_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rs_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rs_in_units
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rack_in_units
        , cast(0 as decimal(20, 4)) as tsfr_pah_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_pah_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_pah_in_units

		, cast(0 as decimal(20, 4)) as twist_instock_traffic
		, cast(0 as decimal(20, 4)) as twist_total_traffic

		, cast(0 as decimal(16, 0)) as sff_units

		, cast(0 as decimal(16, 0)) as bopus_units
		, cast(0 as decimal(16, 0)) as dsr_units

	from prd_nap_usr_vws.merch_inventory_sku_store_week_agg_fact i
	join {environment_schema}.pilot_store_dim s on 1=1
		and i.store_num = s.store_num
	join {environment_schema}.pilot_sku_dim p on 1=1
		and i.rms_sku_num = p.rms_sku_num
	join week_dim w on 1=1
		and i.week_num = w.true_week_num
	group by 1, 2, 3, 4, 5
	having boh_units > 0
		or eoh_units > 0
		or avg_inv_units > 0
)
with data
primary index(cc_num)
on commit preserve rows;

collect stats
	primary index (cc_num)
	,column (
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_ind)
		on inv;


create volatile multiset table rcpt as (
	select
		p.cc_num
		, s.store_num
		, s.channel_num
		, w.month_num
		, rp_ind

		, 0 as sales_flag
		, 0 as inv_flag
		, 1 as rcpt_flag
		, 0 as twist_flag
		, 0 as sff_flag
		, 0 as trans_flag

		, cast(0 as decimal(20, 4)) as sales_retail
		, cast(0 as decimal(20, 4)) as sales_cost
		, cast(0 as decimal(16, 0)) as sales_units
		, cast(0 as decimal(20, 4)) as sales_reg_retail
		, cast(0 as decimal(20, 4)) as sales_reg_cost
		, cast(0 as decimal(16, 0)) as sales_reg_units
        , cast(0 as decimal(20, 4)) as sales_pro_retail
		, cast(0 as decimal(20, 4)) as sales_pro_cost
		, cast(0 as decimal(16, 0)) as sales_pro_units
        , cast(0 as decimal(20, 4)) as sales_clr_retail
		, cast(0 as decimal(20, 4)) as sales_clr_cost
		, cast(0 as decimal(16, 0)) as sales_clr_units
		, cast(0 as decimal(20, 4)) as sales_ds_retail
		, cast(0 as decimal(20, 4)) as sales_ds_cost
		, cast(0 as decimal(16, 0)) as sales_ds_units
		, cast(0 as decimal(20, 4)) as sales_gross_retail
		, cast(0 as decimal(20, 4)) as sales_gross_cost
		, cast(0 as decimal(16, 0)) as sales_gross_units
		, cast(0 as decimal(20, 4)) as return_retail
		, cast(0 as decimal(20, 4)) as return_cost
		, cast(0 as decimal(16, 0)) as return_units
		, cast(0 as decimal(20, 4)) as return_ds_retail
		, cast(0 as decimal(20, 4)) as return_ds_cost
		, cast(0 as decimal(16, 0)) as return_ds_units

		, cast(0 as decimal(20, 4)) as boh_retail
		, cast(0 as decimal(20, 4)) as boh_cost
		, cast(0 as decimal(16, 0)) as boh_units
		, cast(0 as decimal(20, 4)) as eoh_retail
		, cast(0 as decimal(20, 4)) as eoh_cost
		, cast(0 as decimal(16, 0)) as eoh_units
		, cast(0 as decimal(20, 4)) as avg_inv_retail
		, cast(0 as decimal(20, 4)) as avg_inv_cost
		, cast(0 as decimal(20, 4)) as avg_inv_units

		, sum(r.receipts_po_retail_amt_ty) as rcpt_po_retail
		, sum(r.receipts_po_cost_amt_ty) as rcpt_po_cost
		, sum(r.receipts_po_units_ty) as rcpt_po_units
        , sum(r.receipts_dropship_retail_amt_ty) as rcpt_ds_retail
        , sum(r.receipts_dropship_cost_amt_ty) as rcpt_ds_cost
        , sum(r.receipts_dropship_units_ty) as rcpt_ds_units

        , cast(0 as decimal(20, 4)) as tsfr_rs_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rs_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rs_in_units
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rack_in_units
        , cast(0 as decimal(20, 4)) as tsfr_pah_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_pah_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_pah_in_units

		, cast(0 as decimal(20, 4)) as twist_instock_traffic
		, cast(0 as decimal(20, 4)) as twist_total_traffic

		, cast(0 as decimal(16, 0)) as sff_units

		, cast(0 as decimal(16, 0)) as bopus_units
		, cast(0 as decimal(16, 0)) as dsr_units

	from prd_nap_usr_vws.merch_poreceipt_sku_store_week_agg_fact r
	join {environment_schema}.pilot_store_dim s on 1=1
		and r.store_num = s.store_num
	join {environment_schema}.pilot_sku_dim p on 1=1
		and r.rms_sku_num = p.rms_sku_num
	join week_dim w on 1=1
		and r.week_num = w.true_week_num
	group by 1, 2, 3, 4, 5
	having rcpt_po_units > 0
		or rcpt_ds_units > 0
)
with data
primary index(cc_num)
on commit preserve rows;

collect stats
	primary index (cc_num)
	,column (
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_ind)
		on rcpt;


create volatile multiset table tsfr as (
	select
		p.cc_num
		, s.store_num
		, s.channel_num
		, w.month_num
		, rp_ind

		, 0 as sales_flag
		, 0 as inv_flag
		, 1 as rcpt_flag
		, 0 as twist_flag
		, 0 as sff_flag
		, 0 as trans_flag

		, cast(0 as decimal(20, 4)) as sales_retail
		, cast(0 as decimal(20, 4)) as sales_cost
		, cast(0 as decimal(16, 0)) as sales_units
		, cast(0 as decimal(20, 4)) as sales_reg_retail
		, cast(0 as decimal(20, 4)) as sales_reg_cost
		, cast(0 as decimal(16, 0)) as sales_reg_units
        , cast(0 as decimal(20, 4)) as sales_pro_retail
		, cast(0 as decimal(20, 4)) as sales_pro_cost
		, cast(0 as decimal(16, 0)) as sales_pro_units
        , cast(0 as decimal(20, 4)) as sales_clr_retail
		, cast(0 as decimal(20, 4)) as sales_clr_cost
		, cast(0 as decimal(16, 0)) as sales_clr_units
		, cast(0 as decimal(20, 4)) as sales_ds_retail
		, cast(0 as decimal(20, 4)) as sales_ds_cost
		, cast(0 as decimal(16, 0)) as sales_ds_units
		, cast(0 as decimal(20, 4)) as sales_gross_retail
		, cast(0 as decimal(20, 4)) as sales_gross_cost
		, cast(0 as decimal(16, 0)) as sales_gross_units
		, cast(0 as decimal(20, 4)) as return_retail
		, cast(0 as decimal(20, 4)) as return_cost
		, cast(0 as decimal(16, 0)) as return_units
		, cast(0 as decimal(20, 4)) as return_ds_retail
		, cast(0 as decimal(20, 4)) as return_ds_cost
		, cast(0 as decimal(16, 0)) as return_ds_units

		, cast(0 as decimal(20, 4)) as boh_retail
		, cast(0 as decimal(20, 4)) as boh_cost
		, cast(0 as decimal(16, 0)) as boh_units
		, cast(0 as decimal(20, 4)) as eoh_retail
		, cast(0 as decimal(20, 4)) as eoh_cost
		, cast(0 as decimal(16, 0)) as eoh_units
		, cast(0 as decimal(20, 4)) as avg_inv_retail
		, cast(0 as decimal(20, 4)) as avg_inv_cost
		, cast(0 as decimal(20, 4)) as avg_inv_units

		, cast(0 as decimal(20, 4)) as rcpt_po_retail
		, cast(0 as decimal(20, 4)) as rcpt_po_cost
		, cast(0 as decimal(16, 0)) as rcpt_po_units
        , cast(0 as decimal(20, 4)) as rcpt_ds_retail
		, cast(0 as decimal(20, 4)) as rcpt_ds_cost
		, cast(0 as decimal(16, 0)) as rcpt_ds_units

        , sum(t.transfer_in_reserve_stock_retail_amt_ty) as tsfr_rs_in_retail
		, sum(t.transfer_in_reserve_stock_cost_amt_ty) as tsfr_rs_in_cost
		, sum(t.transfer_in_reserve_stock_units_ty) as tsfr_rs_in_units
		, sum(t.transfer_in_racking_retail_amt_ty) as tsfr_rack_in_retail
		, sum(t.transfer_in_racking_cost_amt_ty) as tsfr_rack_in_cost
		, sum(t.transfer_in_racking_units_ty) as tsfr_rack_in_units
        , sum(t.transfer_in_pack_and_hold_retail_amt_ty) as tsfr_pah_in_retail
		, sum(t.transfer_in_pack_and_hold_cost_amt_ty) as tsfr_pah_in_cost
		, sum(t.transfer_in_pack_and_hold_units_ty) as tsfr_pah_in_units

		, cast(0 as decimal(20, 4)) as twist_instock_traffic
		, cast(0 as decimal(20, 4)) as twist_total_traffic

		, cast(0 as decimal(16, 0)) as sff_units

		, cast(0 as decimal(16, 0)) as bopus_units
		, cast(0 as decimal(16, 0)) as dsr_units

	from prd_nap_usr_vws.merch_transfer_sku_store_week_agg_fact t
	join {environment_schema}.pilot_store_dim s on 1=1
		and t.store_num = s.store_num
	join {environment_schema}.pilot_sku_dim p on 1=1
		and t.rms_sku_num = p.rms_sku_num
	join week_dim w on 1=1
		and t.week_num = w.true_week_num
	group by 1, 2, 3, 4, 5
	having tsfr_rs_in_units > 0
		or tsfr_rack_in_units > 0
		OR tsfr_pah_in_units > 0
)
with data and stats
primary index(cc_num)
on commit preserve rows;

collect stats
	primary index (cc_num)
	, column (
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_ind)
		on tsfr;


create volatile multiset table cc_store_month_fact1 as (
	select
		cc_num
		, store_num
		, channel_num
		, month_num
		, case when rp_ind = 'Y' then 1 else 0 end as rp_flag

		, max(sales_flag) as sales_flag
		, max(inv_flag) as inv_flag
		, max(rcpt_flag) as rcpt_flag
		, max(twist_flag) as twist_flag
		, max(sff_flag) as sff_flag
		, max(trans_flag) as trans_flag

		, sum(sales_retail) as sales_retail
		, sum(sales_cost) as sales_cost
		, sum(sales_units) as sales_units
		, sum(sales_reg_retail) as sales_reg_retail
		, sum(sales_reg_cost) as sales_reg_cost
		, sum(sales_reg_units) as sales_reg_units
		, sum(sales_pro_retail) as sales_pro_retail
		, sum(sales_pro_cost) as sales_pro_cost
		, sum(sales_pro_units) as sales_pro_units
		, sum(sales_clr_retail) as sales_clr_retail
		, sum(sales_clr_cost) as sales_clr_cost
		, sum(sales_clr_units) as sales_clr_units
		, sum(sales_ds_retail) as sales_ds_retail
		, sum(sales_ds_cost) as sales_ds_cost
		, sum(sales_ds_units) as sales_ds_units
		, sum(sales_gross_retail) as sales_gross_retail
		, sum(sales_gross_cost) as sales_gross_cost
		, sum(sales_gross_units) as sales_gross_units
		, sum(return_retail) as return_retail
		, sum(return_cost) as return_cost
		, sum(return_units) as return_units
		, sum(return_ds_retail) as return_ds_retail
		, sum(return_ds_cost) as return_ds_cost
		, sum(return_ds_units) as return_ds_units

		, sum(boh_retail) as boh_retail
		, sum(boh_cost) as boh_cost
		, sum(boh_units) as boh_units
		, sum(eoh_retail) as eoh_retail
		, sum(eoh_cost) as eoh_cost
		, sum(eoh_units) as eoh_units
		, sum(avg_inv_retail) as avg_inv_retail
		, sum(avg_inv_cost) as avg_inv_cost
		, sum(avg_inv_units) as avg_inv_units

		, sum(rcpt_po_retail) as rcpt_po_retail
		, sum(rcpt_po_cost) as rcpt_po_cost
		, sum(rcpt_po_units) as rcpt_po_units
        , sum(rcpt_ds_retail) as rcpt_ds_retail
		, sum(rcpt_ds_cost) as rcpt_ds_cost
		, sum(rcpt_ds_units) as rcpt_ds_units

        , sum(tsfr_rs_in_retail) as tsfr_rs_in_retail
		, sum(tsfr_rs_in_cost) as tsfr_rs_in_cost
		, sum(tsfr_rs_in_units) as tsfr_rs_in_units
        , sum(tsfr_rack_in_retail) as tsfr_rack_in_retail
		, sum(tsfr_rack_in_cost) as tsfr_rack_in_cost
		, sum(tsfr_rack_in_units) as tsfr_rack_in_units
        , sum(tsfr_pah_in_retail) as tsfr_pah_in_retail
		, sum(tsfr_pah_in_cost) as tsfr_pah_in_cost
		, sum(tsfr_pah_in_units) as tsfr_pah_in_units

		, sum(twist_instock_traffic) as twist_instock_traffic
		, sum(twist_total_traffic) as twist_total_traffic

		, sum(sff_units) as sff_units

		, sum(bopus_units) as bopus_units
		, sum(dsr_units) as dsr_units

	FROM (
		select * from sales

		union all

		select * from inv

		union all

		select * from rcpt

		union all

		select * from tsfr
	) u
	group by 1, 2, 3, 4, 5
)
with data
primary index (cc_num) on commit preserve rows;

drop table sales;
drop table inv;
drop table rcpt;
drop table tsfr;


create volatile multiset table twist_base as (
	select
		p.cc_num
		, s.store_num
		, s.channel_num
		, d.month_num
		, rp_idnt as rp_flag
		, mc_instock_ind
		, allocated_traffic

	from t2dl_das_twist.twist_daily t
	join {environment_schema}.pilot_sku_dim p on 1=1
		and t.rms_sku_num = p.rms_sku_num
	join {environment_schema}.pilot_day_dim d on 1=1
		and t.day_date = d.day_date
	join {environment_schema}.pilot_store_dim s on 1=1
		and t.store_num = s.store_num
	where 1=1
		and t.day_date between (select min(day_date) from {environment_schema}.pilot_day_dim) and (select max(day_date) from {environment_schema}.pilot_day_dim)
		and allocated_traffic > 0
)
with data and stats
primary index (cc_num, store_num, channel_num, month_num, rp_flag)
on commit preserve rows;

collect stats
	primary index (cc_num, store_num, channel_num, month_num, rp_flag)
	, column (cc_num)
	, column (store_num)
	, column (month_num)
	, column(rp_flag)
		on twist_base;


create volatile multiset table twist as (
	select
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_flag

		, 0 as sales_flag
		, 0 as inv_flag
		, 0 as rcpt_flag
		, 1 as twist_flag
		, 0 as sff_flag
		, 0 as trans_flag

		, cast(0 as decimal(20, 4)) as sales_retail
		, cast(0 as decimal(20, 4)) as sales_cost
		, cast(0 as decimal(16, 0)) as sales_units
		, cast(0 as decimal(20, 4)) as sales_reg_retail
		, cast(0 as decimal(20, 4)) as sales_reg_cost
		, cast(0 as decimal(16, 0)) as sales_reg_units
        , cast(0 as decimal(20, 4)) as sales_pro_retail
		, cast(0 as decimal(20, 4)) as sales_pro_cost
		, cast(0 as decimal(16, 0)) as sales_pro_units
        , cast(0 as decimal(20, 4)) as sales_clr_retail
		, cast(0 as decimal(20, 4)) as sales_clr_cost
		, cast(0 as decimal(16, 0)) as sales_clr_units
		, cast(0 as decimal(20, 4)) as sales_ds_retail
		, cast(0 as decimal(20, 4)) as sales_ds_cost
		, cast(0 as decimal(16, 0)) as sales_ds_units
		, cast(0 as decimal(20, 4)) as sales_gross_retail
		, cast(0 as decimal(20, 4)) as sales_gross_cost
		, cast(0 as decimal(16, 0)) as sales_gross_units
		, cast(0 as decimal(20, 4)) as return_retail
		, cast(0 as decimal(20, 4)) as return_cost
		, cast(0 as decimal(16, 0)) as return_units
		, cast(0 as decimal(20, 4)) as return_ds_retail
		, cast(0 as decimal(20, 4)) as return_ds_cost
		, cast(0 as decimal(16, 0)) as return_ds_units

		, cast(0 as decimal(20, 4)) as boh_retail
		, cast(0 as decimal(20, 4)) as boh_cost
		, cast(0 as decimal(16, 0)) as boh_units
		, cast(0 as decimal(20, 4)) as eoh_retail
		, cast(0 as decimal(20, 4)) as eoh_cost
		, cast(0 as decimal(16, 0)) as eoh_units
		, cast(0 as decimal(20, 4)) as avg_inv_retail
		, cast(0 as decimal(20, 4)) as avg_inv_cost
		, cast(0 as decimal(20, 4)) as avg_inv_units

		, cast(0 as decimal(20, 4)) as rcpt_po_retail
		, cast(0 as decimal(20, 4)) as rcpt_po_cost
		, cast(0 as decimal(16, 0)) as rcpt_po_units
        , cast(0 as decimal(20, 4)) as rcpt_ds_retail
		, cast(0 as decimal(20, 4)) as rcpt_ds_cost
		, cast(0 as decimal(16, 0)) as rcpt_ds_units

        , cast(0 as decimal(20, 4)) as tsfr_rs_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rs_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rs_in_units
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rack_in_units
        , cast(0 as decimal(20, 4)) as tsfr_pah_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_pah_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_pah_in_units

		, sum(mc_instock_ind * allocated_traffic) as twist_instock_traffic
		, sum(allocated_traffic) as twist_total_traffic

		, cast(0 as decimal(16, 0)) as sff_units

		, cast(0 as decimal(16, 0)) as bopus_units
		, cast(0 as decimal(16, 0)) as dsr_units

	from twist_base t
	group by 1, 2, 3, 4, 5
	having twist_total_traffic > 0
)
with data and stats
primary index (cc_num)
on commit preserve rows;

collect stats
	primary index (cc_num)
	,column (
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_flag)
		on twist;


drop table twist_base;


create volatile multiset table rp as (
	select
		rms_sku_num
		, location_num
		, on_sale_date
		, off_sale_date
		, lead(on_sale_date, 1) over (partition by rms_sku_num, location_num order by on_sale_date, off_sale_date) as next_on_sale_date
		, case
			when off_sale_date - next_on_sale_date >= 0
				then next_on_sale_date - 1
			else off_sale_date
		end as off_sale_date_adj
	from (
		select
			rp.rms_sku_num
			, rp.location_num
			, rp.on_sale_date
			, rp.off_sale_date
		from prd_nap_usr_vws.rp_sku_loc_dim_hist rp
		join {environment_schema}.pilot_sku_dim p on 1=1
			and rp.rms_sku_num = p.rms_sku_num
		join {environment_schema}.pilot_store_dim s on 1=1
			and rp.location_num = s.store_num
		qualify row_number() over (partition by rp.rms_sku_num, rp.location_num, rp.on_sale_date order by rp.change_date desc) = 1
	) x
)
with data and stats
primary index (rms_sku_num, location_num)
on commit preserve rows;


create volatile multiset table sff as (
	select
		p.cc_num
		, s.store_num
		, s.channel_num
		, d.month_num
		, case when rp.rms_sku_num is not null then 1 else 0 end as rp_flag

		, 0 as sales_flag
		, 0 as inv_flag
		, 0 as rcpt_flag
		, 0 as twist_flag
		, 1 as sff_flag
		, 0 as trans_flag

		, cast(0 as decimal(20, 4)) as sales_retail
		, cast(0 as decimal(20, 4)) as sales_cost
		, cast(0 as decimal(16, 0)) as sales_units
		, cast(0 as decimal(20, 4)) as sales_reg_retail
		, cast(0 as decimal(20, 4)) as sales_reg_cost
		, cast(0 as decimal(16, 0)) as sales_reg_units
        , cast(0 as decimal(20, 4)) as sales_pro_retail
		, cast(0 as decimal(20, 4)) as sales_pro_cost
		, cast(0 as decimal(16, 0)) as sales_pro_units
        , cast(0 as decimal(20, 4)) as sales_clr_retail
		, cast(0 as decimal(20, 4)) as sales_clr_cost
		, cast(0 as decimal(16, 0)) as sales_clr_units
		, cast(0 as decimal(20, 4)) as sales_ds_retail
		, cast(0 as decimal(20, 4)) as sales_ds_cost
		, cast(0 as decimal(16, 0)) as sales_ds_units
		, cast(0 as decimal(20, 4)) as sales_gross_retail
		, cast(0 as decimal(20, 4)) as sales_gross_cost
		, cast(0 as decimal(16, 0)) as sales_gross_units
		, cast(0 as decimal(20, 4)) as return_retail
		, cast(0 as decimal(20, 4)) as return_cost
		, cast(0 as decimal(16, 0)) as return_units
		, cast(0 as decimal(20, 4)) as return_ds_retail
		, cast(0 as decimal(20, 4)) as return_ds_cost
		, cast(0 as decimal(16, 0)) as return_ds_units

		, cast(0 as decimal(20, 4)) as boh_retail
		, cast(0 as decimal(20, 4)) as boh_cost
		, cast(0 as decimal(16, 0)) as boh_units
		, cast(0 as decimal(20, 4)) as eoh_retail
		, cast(0 as decimal(20, 4)) as eoh_cost
		, cast(0 as decimal(16, 0)) as eoh_units
		, cast(0 as decimal(20, 4)) as avg_inv_retail
		, cast(0 as decimal(20, 4)) as avg_inv_cost
		, cast(0 as decimal(20, 4)) as avg_inv_units

		, cast(0 as decimal(20, 4)) as rcpt_po_retail
		, cast(0 as decimal(20, 4)) as rcpt_po_cost
		, cast(0 as decimal(16, 0)) as rcpt_po_units
        , cast(0 as decimal(20, 4)) as rcpt_ds_retail
		, cast(0 as decimal(20, 4)) as rcpt_ds_cost
		, cast(0 as decimal(16, 0)) as rcpt_ds_units

        , cast(0 as decimal(20, 4)) as tsfr_rs_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rs_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rs_in_units
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rack_in_units
        , cast(0 as decimal(20, 4)) as tsfr_pah_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_pah_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_pah_in_units

		, cast(0 as decimal(20, 4)) as twist_instock_traffic
		, cast(0 as decimal(20, 4)) as twist_total_traffic

		, sum(order_line_quantity) as sff_units

		, cast(0 as decimal(16, 0)) as bopus_units
		, cast(0 as decimal(16, 0)) as dsr_units

	from prd_nap_usr_vws.order_line_detail_fact o
	join {environment_schema}.pilot_sku_dim p on 1=1
		and o.rms_sku_num = p.rms_sku_num
	join {environment_schema}.pilot_store_dim s on 1=1
		and case when o.shipped_node_num = 209 then 210 else o.shipped_node_num end = s.store_num
	join {environment_schema}.pilot_day_dim d on 1=1
		and o.order_date_pacific = d.day_date
	left join rp on 1=1
		and o.rms_sku_num = rp.rms_sku_num
		and s.store_num = rp.location_num
		and o.order_date_pacific between rp.on_sale_date and rp.off_sale_date_adj
	where 1=1
		and o.delivery_method_code = 'SHIP'
		and o.canceled_tmstp_pacific is null
		and o.shipped_node_num is not null
		and o.shipped_node_type_code in ('SS', 'FL')
		and o.gift_with_purchase_ind = 'N'
		and o.beauty_sample_ind = 'N'
	group by 1, 2, 3, 4, 5--, 6, 7, 8
)
with data and stats
primary index(cc_num)
on commit preserve rows;

collect stats
	primary index (cc_num)
	,column (
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_flag)
		on sff;


create volatile multiset table trans_base as (
	select
		coalesce(t.sku_num, t.hl_sku_num) as sku_num

		, t.business_day_date

        , case
	    	when t.line_net_usd_amt > 0
	    		and t.tran_type_code in ('SALE', 'EXCH')
	    		and t.line_item_order_type = 'CustInitWebOrder'
	    		and t.line_item_fulfillment_type = 'StorePickup'
	    		then 'bopus'

    		when t.line_net_usd_amt > 0
	    		and t.tran_type_code in ('SALE', 'EXCH')
	    		and t.line_item_fulfillment_type = 'StoreShipSend'
	    		then 'sff'

	    	when t.line_net_usd_amt < 0
	    		and t.tran_type_code in ('RETN', 'EXCH')
	    		and t.line_item_fulfillment_type = 'VendorDropShip'
	    		then 'dsr'
	    end as tran_type

        , case
        	when tran_type = 'bopus'
        		then h.fulfilling_store_num
        	when tran_type = 'sff'
        		then coalesce(h.fulfilling_store_num, t.intent_store_num)
        	when tran_type = 'dsr'
        		then h.ringing_store_num
        end as store_num

        , t.line_item_quantity
from prd_nap_usr_vws.retail_tran_detail_fact t
join prd_nap_usr_vws.retail_tran_hdr_fact h on 1=1
	and t.global_tran_id = h.global_tran_id
	and t.business_day_date = h.business_day_date
where 1=1
	and t.line_item_merch_nonmerch_ind = 'MERCH'
	and t.line_item_fulfillment_type in ('StorePickup', 'StoreShipSend', 'VendorDropShip')
	and h.acp_id is not null
	and t.business_day_date between (select min(day_date) from {environment_schema}.pilot_day_dim) and (select max(day_date) from {environment_schema}.pilot_day_dim)
	and store_num in (select store_num from {environment_schema}.pilot_store_dim)
)
with data and stats
primary index (sku_num)
on commit preserve rows;

collect stats
	primary index (sku_num)
	, column(store_num)
	, column(business_day_date)
	, column(sku_num, store_num, business_day_date)
	on trans_base;


create volatile multiset table trans as (
	select
		p.cc_num
		, s.store_num
		, s.channel_num
		, d.month_num
		, case when rp.rms_sku_num is not null then 1 else 0 end as rp_flag

		, 0 as sales_flag
		, 0 as inv_flag
		, 0 as rcpt_flag
		, 0 as twist_flag
		, 0 as sff_flag
		, 1 as trans_flag

		, cast(0 as decimal(20, 4)) as sales_retail
		, cast(0 as decimal(20, 4)) as sales_cost
		, cast(0 as decimal(16, 0)) as sales_units
		, cast(0 as decimal(20, 4)) as sales_reg_retail
		, cast(0 as decimal(20, 4)) as sales_reg_cost
		, cast(0 as decimal(16, 0)) as sales_reg_units
        , cast(0 as decimal(20, 4)) as sales_pro_retail
		, cast(0 as decimal(20, 4)) as sales_pro_cost
		, cast(0 as decimal(16, 0)) as sales_pro_units
        , cast(0 as decimal(20, 4)) as sales_clr_retail
		, cast(0 as decimal(20, 4)) as sales_clr_cost
		, cast(0 as decimal(16, 0)) as sales_clr_units
		, cast(0 as decimal(20, 4)) as sales_ds_retail
		, cast(0 as decimal(20, 4)) as sales_ds_cost
		, cast(0 as decimal(16, 0)) as sales_ds_units
		, cast(0 as decimal(20, 4)) as sales_gross_retail
		, cast(0 as decimal(20, 4)) as sales_gross_cost
		, cast(0 as decimal(16, 0)) as sales_gross_units
		, cast(0 as decimal(20, 4)) as return_retail
		, cast(0 as decimal(20, 4)) as return_cost
		, cast(0 as decimal(16, 0)) as return_units
		, cast(0 as decimal(20, 4)) as return_ds_retail
		, cast(0 as decimal(20, 4)) as return_ds_cost
		, cast(0 as decimal(16, 0)) as return_ds_units

		, cast(0 as decimal(20, 4)) as boh_retail
		, cast(0 as decimal(20, 4)) as boh_cost
		, cast(0 as decimal(16, 0)) as boh_units
		, cast(0 as decimal(20, 4)) as eoh_retail
		, cast(0 as decimal(20, 4)) as eoh_cost
		, cast(0 as decimal(16, 0)) as eoh_units
		, cast(0 as decimal(20, 4)) as avg_inv_retail
		, cast(0 as decimal(20, 4)) as avg_inv_cost
		, cast(0 as decimal(20, 4)) as avg_inv_units

		, cast(0 as decimal(20, 4)) as rcpt_po_retail
		, cast(0 as decimal(20, 4)) as rcpt_po_cost
		, cast(0 as decimal(16, 0)) as rcpt_po_units
        , cast(0 as decimal(20, 4)) as rcpt_ds_retail
		, cast(0 as decimal(20, 4)) as rcpt_ds_cost
		, cast(0 as decimal(16, 0)) as rcpt_ds_units

        , cast(0 as decimal(20, 4)) as tsfr_rs_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rs_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rs_in_units
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_rack_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_rack_in_units
        , cast(0 as decimal(20, 4)) as tsfr_pah_in_retail
		, cast(0 as decimal(20, 4)) as tsfr_pah_in_cost
		, cast(0 as decimal(16, 0)) as tsfr_pah_in_units

		, cast(0 as decimal(20, 4)) as twist_instock_traffic
		, cast(0 as decimal(20, 4)) as twist_total_traffic

		, cast(0 as decimal(16, 0)) as sff_units

		, sum(case when tran_type = 'bopus' then line_item_quantity end) as bopus_units
		, sum(case when tran_type = 'dsr' then line_item_quantity end) as dsr_units
	from trans_base t
	join {environment_schema}.pilot_sku_dim p on 1=1
		and t.sku_num = p.rms_sku_num
	join {environment_schema}.pilot_store_dim s on 1=1
		and t.store_num = s.store_num
	join {environment_schema}.pilot_day_dim d on 1=1
		and t.business_day_date = d.day_date
	left join rp on 1=1
		and t.sku_num = rp.rms_sku_num
		and t.store_num = rp.location_num
		and t.business_day_date between rp.on_sale_date and rp.off_sale_date_adj
	group by 1, 2, 3, 4, 5
	having bopus_units > 0
		or dsr_units > 0
)
with data
primary index (cc_num)
on commit preserve rows;

collect stats
	primary index (cc_num)
	,column (
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_flag)
		on trans;

create volatile multiset table cc_store_month_fact as (
	select
		cc_num
		, store_num
		, channel_num
		, month_num
		, rp_flag

		, max(sales_flag) as sales_flag
		, max(inv_flag) as inv_flag
		, max(rcpt_flag) as rcpt_flag
		, max(twist_flag) as twist_flag
		, max(sff_flag) as sff_flag
		, max(trans_flag) as trans_flag

		, sum(sales_retail) as sales_retail
		, sum(sales_cost) as sales_cost
		, sum(sales_units) as sales_units
		, sum(sales_reg_retail) as sales_reg_retail
		, sum(sales_reg_cost) as sales_reg_cost
		, sum(sales_reg_units) as sales_reg_units
		, sum(sales_pro_retail) as sales_pro_retail
		, sum(sales_pro_cost) as sales_pro_cost
		, sum(sales_pro_units) as sales_pro_units
		, sum(sales_clr_retail) as sales_clr_retail
		, sum(sales_clr_cost) as sales_clr_cost
		, sum(sales_clr_units) as sales_clr_units
		, sum(sales_ds_retail) as sales_ds_retail
		, sum(sales_ds_cost) as sales_ds_cost
		, sum(sales_ds_units) as sales_ds_units
		, sum(sales_gross_retail) as sales_gross_retail
		, sum(sales_gross_cost) as sales_gross_cost
		, sum(sales_gross_units) as sales_gross_units
		, sum(return_retail) as return_retail
		, sum(return_cost) as return_cost
		, sum(return_units) as return_units
		, sum(return_ds_retail) as return_ds_retail
		, sum(return_ds_cost) as return_ds_cost
		, sum(return_ds_units) as return_ds_units

		, sum(boh_retail) as boh_retail
		, sum(boh_cost) as boh_cost
		, sum(boh_units) as boh_units
		, sum(eoh_retail) as eoh_retail
		, sum(eoh_cost) as eoh_cost
		, sum(eoh_units) as eoh_units
		, sum(avg_inv_retail) as avg_inv_retail
		, sum(avg_inv_cost) as avg_inv_cost
		, sum(avg_inv_units) as avg_inv_units

		, sum(rcpt_po_retail) as rcpt_po_retail
		, sum(rcpt_po_cost) as rcpt_po_cost
		, sum(rcpt_po_units) as rcpt_po_units
        , sum(rcpt_ds_retail) as rcpt_ds_retail
		, sum(rcpt_ds_cost) as rcpt_ds_cost
		, sum(rcpt_ds_units) as rcpt_ds_units

        , sum(tsfr_rs_in_retail) as tsfr_rs_in_retail
		, sum(tsfr_rs_in_cost) as tsfr_rs_in_cost
		, sum(tsfr_rs_in_units) as tsfr_rs_in_units
        , sum(tsfr_rack_in_retail) as tsfr_rack_in_retail
		, sum(tsfr_rack_in_cost) as tsfr_rack_in_cost
		, sum(tsfr_rack_in_units) as tsfr_rack_in_units
        , sum(tsfr_pah_in_retail) as tsfr_pah_in_retail
		, sum(tsfr_pah_in_cost) as tsfr_pah_in_cost
		, sum(tsfr_pah_in_units) as tsfr_pah_in_units

		, sum(twist_instock_traffic) as twist_instock_traffic
		, sum(twist_total_traffic) as twist_total_traffic

		, sum(sff_units) as sff_units

		, sum(bopus_units) as bopus_units
		, sum(dsr_units) as dsr_units

	FROM (
		select * from cc_store_month_fact1

		union all

		select * from twist

		union all

		select * from sff

		union all

		select * from trans
	) u
	group by 1, 2, 3, 4, 5
)
with data
primary index (cc_num) on commit preserve rows;

collect stats
	primary index(cc_num)
	, column (month_num)
	, column (store_num)
	, column (channel_num, cc_num)
	on cc_store_month_fact;

drop table twist;


create volatile multiset table age as (
	select
		p.cc_num
		, s.store_num_nyc
		, a.month_num
		, sum(cast(inventory_age_days as decimal(20, 4))* end_of_period_total_units) / sum(end_of_period_total_units) as ending_age
	from prd_nap_vws.merch_inventory_age_sku_store_vw a
	join {environment_schema}.pilot_store_dim s on 1=1
		and a.store_num = s.store_num
	join {environment_schema}.pilot_sku_dim p on 1=1
		and a.rms_sku_num = p.rms_sku_num
		and a.channel_country = 'US'
	where 1=1
		and a.month_num in (select month_num from month_dim)
		and inventory_age_days <> -1
	group by 1, 2, 3
)
with data and stats
primary index (cc_num, store_num_nyc, month_num)
on commit preserve rows;


create volatile multiset table fact_age as (
	select
		f.*
		, a.ending_age
	from cc_store_month_fact f
	join {environment_schema}.pilot_store_dim s on 1=1
		and f.store_num = s.store_num
	left join age a on 1=1
		and f.cc_num = a.cc_num
		and s.store_num_nyc = a.store_num_nyc
		and f.month_num = a.month_num
)
with data and stats
primary index (cc_num, store_num, month_num)
on commit preserve rows;

collect stats
	column (cc_num)
	, column (month_num)
	, column (store_num)
	, column (channel_num, cc_num)
	on fact_age;


-- 5/6/24 added Anniversary CC lookup
create volatile multiset table anniv as (
	select
		p.cc_num
		, d.month_num
		, max(a.anniv_item_ind) as anniv_flag
	from t2dl_das_scaled_events.anniversary_sku_chnl_date a
	join {environment_schema}.pilot_sku_dim p on 1=1
		and a.sku_idnt = p.rms_sku_num
	join {environment_schema}.pilot_day_dim d on 1=1
		and a.day_dt = d.day_date
	where 1=1
		and channel_country = 'US'
		and selling_channel = 'STORE'
	group by 1, 2
)
with data and stats
primary index (cc_num, month_num)
on commit preserve rows;

collect stats
	primary index (cc_num, month_num)
	on anniv;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'fls_assortment_pilot_merch_base', OUT_RETURN_MSG);
create multiset table {environment_schema}.fls_assortment_pilot_merch_base as  (
	select
		f.cc_num
		, f.store_num
		, channel_num
		, f.month_num
		, coalesce(rp_flag, 0) as rp_flag

		, sales_flag
		, inv_flag
		, rcpt_flag
		, twist_flag
		, sff_flag
		, trans_flag

		, sales_retail
		, sales_cost
		, sales_units
		, sales_reg_retail
		, sales_reg_cost
		, sales_reg_units
		, sales_pro_retail
		, sales_pro_cost
		, sales_pro_units
		, sales_clr_retail
		, sales_clr_cost
		, sales_clr_units
		, sales_ds_retail
		, sales_ds_cost
		, sales_ds_units
		, sales_gross_retail
		, sales_gross_cost
		, sales_gross_units
		, return_retail
		, return_cost
		, return_units
		, return_ds_retail
		, return_ds_cost
		, return_ds_units

		, boh_retail
		, boh_cost
		, boh_units
		, eoh_retail
		, eoh_cost
		, eoh_units
		, avg_inv_retail
		, avg_inv_cost
		, avg_inv_units

		, rcpt_po_retail
		, rcpt_po_cost
		, rcpt_po_units
        , rcpt_ds_retail
		, rcpt_ds_cost
		, rcpt_ds_units

        , tsfr_rs_in_retail
		, tsfr_rs_in_cost
		, tsfr_rs_in_units
        , tsfr_rack_in_retail
		, tsfr_rack_in_cost
		, tsfr_rack_in_units
        , tsfr_pah_in_retail
		, tsfr_pah_in_cost
		, tsfr_pah_in_units

		, twist_instock_traffic
		, twist_total_traffic

		, sff_units

		, bopus_units
		, dsr_units

		, ending_age

		, round(coalesce(
			--gross sales aur
			sum(sales_gross_retail) over (partition by f.channel_num, f.cc_num)
			/ nullif(sum(sales_gross_units) over (partition by f.channel_num, f.cc_num), 0)

			--avg_inv aur
			, sum(avg_inv_retail) over (partition by f.channel_num, f.cc_num)
			/ nullif(sum(avg_inv_units) over (partition by f.channel_num, f.cc_num), 0)

			--boh aur
			, sum(boh_retail) over (partition by channel_num, f.cc_num)
			/ nullif(sum(boh_units) over (partition by channel_num, f.cc_num), 0)

			--rcpt aur
			, sum(rcpt_po_retail + rcpt_ds_retail + tsfr_rs_in_retail + tsfr_rack_in_retail + tsfr_pah_in_retail) over (partition by f.channel_num, f.cc_num)
			/ nullif(sum(rcpt_po_units + rcpt_ds_units + tsfr_rs_in_units + tsfr_rack_in_units + tsfr_pah_in_units) over (partition by f.channel_num, f.cc_num), 0)

			--return aur
			, sum(return_retail) over (partition by f.channel_num, f.cc_num)
			/ nullif(sum(return_units) over (partition by f.channel_num, f.cc_num), 0)
		), 2) as aur_dim

		, sum(sales_units + eoh_units) over (partition by f.cc_num, f.store_num, f.month_num) as qual_ats_units

		, coalesce(a.anniv_flag,0) as anniv_flag -- 5/6/24 added Anniversary flag

		, current_timestamp as data_update_tmstp
	from fact_age f
	left join anniv a on 1=1 -- 5/6/24 added join to Anniversary CC lookup
		and f.cc_num = a.cc_num
		and f.month_num = a.month_num
)
with data and stats
primary index(cc_num, store_num, month_num);

collect stats
	primary index (cc_num, store_num, month_num)
	, column (cc_num)
	, column (store_num)
	, column (month_num)
on {environment_schema}.fls_assortment_pilot_merch_base ;

grant select on {environment_schema}.fls_assortment_pilot_merch_base  to public;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'pilot_cc_dim', OUT_RETURN_MSG);
create multiset table {environment_schema}.pilot_cc_dim as (
	select
		cc_num

		, max(style_desc) as style_desc
		, max(style_group_num) as style_group_num
		, max(web_style_num) as web_style_num
		, max(color_num) as color_num
		, max(color_desc) as color_desc

		, max(div_num) as div_num
		, max(div_label) as div_label
		, max(sdiv_num) as sdiv_num
		, max(sdiv_label) as sdiv_label
		, max(dept_num) as dept_num
		, max(dept_label) as dept_label
		, max(class_num) as class_num
		, max(class_label) as class_label
		, max(sbclass_num) as sbclass_num
		, max(sbclass_label) as sbclass_label

		, max(supplier_num) as supplier_num
		, max(supplier_name) as supplier_name
		, max(brand_name) as brand_name
		, max(npg_ind) as npg_ind

		, max(quantrix_category) as quantrix_category
		, current_date as data_update_dt
	from {environment_schema}.pilot_sku_dim
	where cc_num in (select distinct cc_num from {environment_schema}.fls_assortment_pilot_merch_base)
	group by cc_num
)
with data and stats
primary index (cc_num);

grant select on {environment_schema}.pilot_cc_dim to public;
